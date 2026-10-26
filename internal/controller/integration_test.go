//go:build integration

// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/fence"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// startEnvtest runs a real kube-apiserver with the JobSet and Kueue
// Workload CRDs and returns a direct client.
func startEnvtest(t *testing.T) client.Client {
	t.Helper()
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../test/crd"}, ErrorIfCRDPathMissing: true}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("starting envtest (KUBEBUILDER_ASSETS set? run make test-integration): %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	kube, err := client.New(restCfg, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := kube.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "slurm-jobs"}}); err != nil {
		t.Fatal(err)
	}
	return kube
}

func envHarness(t *testing.T, kube client.Client, fs *fakeSlurm) *harness {
	t.Helper()
	cfg := testConfig()
	h := &harness{t: t, kube: kube, fs: fs, cfg: cfg, now: time.Now()}
	h.pol = &Poller{Slurm: fs, Interval: time.Hour, Claims: func(j *slurm.Job) bool { _, ok := cfg.PriorityClassFor(j.Partition); return ok }}
	clock := func() time.Time { return h.now }
	fencer := fence.Manual{Reader: kube, ConfirmAnnotation: cfg.Fencing.ConfirmAnnotation}
	h.jobs = &JobReconciler{Client: kube, Slurm: fs, Poller: h.pol, Cfg: cfg, Translate: translate.New(cfg), Fencer: fencer, Clock: clock}
	h.nodes = &NodeRecordReconciler{Client: kube, Slurm: fs, Poller: h.pol, Cfg: cfg, Fencer: fencer, Clock: clock,
		vanishedSince: map[string]time.Time{}, orphanSeen: map[string]int{}}
	return h
}

// The P0 slice against a real API server, with the test playing the
// JobSet controller (creating pods) and slurmd (registering records):
// claim, admission, clearance, registration, verified release, a pod
// replacement with a requeue, and cleanup. A second set of reconcilers
// (a restarted controller) sees the same state and changes nothing.
func TestVerticalSliceAgainstRealAPIServer(t *testing.T) {
	kube := startEnvtest(t)
	fs := newFakeSlurm(heldJob(50))
	h := envHarness(t, kube, fs)
	n0 := nodeName(50, 0)

	h.job(50)
	js := h.jobSet(50)
	if js == nil {
		t.Fatal("JobSet not created on the real API server")
	}
	if got := js.Labels[translate.ManagedByLabel]; got != translate.ManagedByValue {
		t.Fatalf("labels %v", js.Labels)
	}
	wl := admittedWorkload(js.Name)
	status := wl.Object["status"]
	delete(wl.Object, "status")
	if err := kube.Create(h.ctx(), wl); err != nil {
		t.Fatal(err)
	}
	wl.Object["status"] = status
	if err := kube.Status().Update(h.ctx(), wl); err != nil {
		t.Fatal(err)
	}
	pod := slurmdPod("w0", 50, 0, "10.0.0.2", 63000, corev1.PodRunning)
	h.create(pod)
	h.job(50)
	if len(fs.released) != 0 {
		t.Fatal("released before registration")
	}
	h.node(n0)
	if h.pod("w0").Annotations[translate.NodeRecordClearAnnotation] == "" {
		t.Fatal("pod not cleared")
	}
	fs.register(n0, h.pod("w0"), "10.0.0.2", 63000)
	h.job(50)
	if !contains(fs.released, uint64(50)) {
		t.Fatalf("not released: %q", fs.job(50).Comment)
	}
	fs.set(50, func(j *slurm.Job) { j.JobState = []string{"RUNNING"}; j.Hold = false })

	// Pod dies; slurmctld keeps the job on the node until told.
	h.setPhase("w0", corev1.PodFailed)
	fs.busy[n0] = true
	h.node(n0)
	if _, downed := fs.downed[n0]; !downed {
		t.Fatalf("record not DOWN'd: %v", fs.downed)
	}
	h.node(n0)
	if !contains(fs.deleted, n0) {
		t.Fatalf("record not deleted: %v", fs.deleted)
	}
	fs.set(50, func(j *slurm.Job) {
		j.JobState = []string{"PENDING"}
		j.RestartCnt = 1
		j.StateReason = "BeginTime"
		j.SubmitTime.Number = 5000
	})
	h.job(50)
	if !contains(fs.held, uint64(50)) {
		t.Fatalf("requeued job not held: %q", fs.job(50).Comment)
	}
	cur := slurmdPod("w0-b", 50, 0, "10.0.0.2", 63001, corev1.PodRunning)
	h.create(cur)
	h.node(n0)
	if h.pod("w0-b").Annotations[translate.NodeRecordClearAnnotation] == "" {
		t.Fatal("replacement not cleared")
	}
	fs.register(n0, h.pod("w0-b"), "10.0.0.2", 63001)
	fs.set(50, func(j *slurm.Job) { j.StateReason = "JobHeldAdmin" })
	h.job(50)
	if len(fs.released) != 2 {
		t.Fatalf("not released after the replacement: %q", fs.job(50).Comment)
	}

	// A restarted controller: fresh reconcilers, same truth, no action.
	h2 := envHarness(t, kube, fs)
	fs.set(50, func(j *slurm.Job) { j.JobState = []string{"RUNNING"}; j.Hold = false })
	before, downs := len(fs.deleted), len(fs.downed)
	for i := 0; i < 3; i++ {
		h2.job(50)
		h2.node(n0)
	}
	if len(fs.deleted) != before || len(fs.downed) != downs || len(fs.held) != 1 {
		t.Errorf("a restarted controller acted on a healthy job: deleted=%v downed=%v held=%v", fs.deleted, fs.downed, fs.held)
	}

	// End: records and JobSet gone.
	fs.set(50, func(j *slurm.Job) { j.JobState = []string{"COMPLETED"} })
	h2.job(50)
	var gone jobsetv1alpha2.JobSet
	if err := kube.Get(h.ctx(), client.ObjectKeyFromObject(js), &gone); err == nil {
		t.Error("JobSet not deleted after the job ended")
	}
	if _, ok := fs.nodes[n0]; ok {
		t.Error("record not released after the job ended")
	}
}
