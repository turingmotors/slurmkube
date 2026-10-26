// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// gatedPod is a slurmd pod as the JobSet creates it from our template:
// our labels, the range annotation, scheduled to host (or not yet).
func gatedPod(name, host, jobID, portRange string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "slurm-jobs", UID: types.UID("uid-" + name),
			Labels:      map[string]string{translate.ManagedByLabel: translate.ManagedByValue, translate.SlurmJobIDLabel: jobID},
			Annotations: map[string]string{translate.NodePortRangeAnnotation: portRange},
		},
		Spec: corev1.PodSpec{NodeName: host, Containers: []corev1.Container{{Name: "slurmd", Image: "slurmd:test"}}},
	}
}

func portClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(testScheme(t)).
		WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexer).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(objs...).Build()
}

// allocate runs one port-allocation pass for the pod.
func allocate(t *testing.T, r *NodePortReconciler, pod string) ctrl.Result {
	t.Helper()
	var req ctrl.Request
	req.Namespace, req.Name = "slurm-jobs", pod
	res, err := r.Reconcile(t.Context(), req)
	if err != nil {
		t.Fatalf("allocating a port for %s: %v", pod, err)
	}
	return res
}

func stampOf(t *testing.T, kube client.Client, name string) string {
	t.Helper()
	var pod corev1.Pod
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "slurm-jobs", Name: name}, &pod); err != nil {
		t.Fatal(err)
	}
	return pod.Annotations[translate.NodePortAnnotation]
}

func getPod(t *testing.T, kube client.Client, name string) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: "slurm-jobs", Name: name}, &pod); err != nil {
		t.Fatal(err)
	}
	return &pod
}

// Ports are per host, lowest free first, across jobs.
func TestNodePortsAreDistinctPerHostAcrossJobs(t *testing.T) {
	a0 := gatedPod("a0", "host-a", "1", "63000-63009")
	a1 := gatedPod("a1", "host-a", "1", "63000-63009")
	b0 := gatedPod("b0", "host-a", "2", "63000-63009")
	c0 := gatedPod("c0", "host-b", "3", "63000-63009")
	kube := portClient(t, a0, a1, b0, c0)
	r := &NodePortReconciler{Client: kube}
	for _, p := range []*corev1.Pod{a0, a1, b0, c0} {
		allocate(t, r, p.Name)
	}
	for name, port := range map[string]string{"a0": "63000", "a1": "63001", "b0": "63002", "c0": "63000"} {
		if got := stampOf(t, kube, name); got != port {
			t.Errorf("%s port = %q, want %q", name, got, port)
		}
	}
}

// Ten pods at once in any order get ten distinct ports; a restarted
// allocator (nothing in memory) changes none and continues from the
// pods' own stamps.
func TestNodePortTenPodsThenRestart(t *testing.T) {
	var pods []client.Object
	for i := 0; i < 10; i++ {
		pods = append(pods, gatedPod(fmt.Sprintf("p%d", i), "host-a", fmt.Sprint(i%3), "63000-63019"))
	}
	kube := portClient(t, pods...)
	r := &NodePortReconciler{Client: kube}
	for i := 9; i >= 0; i-- {
		allocate(t, r, fmt.Sprintf("p%d", i))
	}
	before := map[string]string{}
	seen := map[string]bool{}
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("p%d", i)
		p := stampOf(t, kube, name)
		if p == "" || seen[p] {
			t.Errorf("%s port %q must be set and unique", name, p)
		}
		seen[p] = true
		before[name] = p
	}
	restarted := &NodePortReconciler{Client: kube}
	for name, want := range before {
		allocate(t, restarted, name)
		if got := stampOf(t, kube, name); got != want {
			t.Errorf("restart changed %s's port %q -> %q", name, want, got)
		}
	}
	for _, n := range []string{"q0", "q1"} {
		if err := kube.Create(context.Background(), gatedPod(n, "host-a", "9", "63000-63019")); err != nil {
			t.Fatal(err)
		}
		allocate(t, restarted, n)
	}
	if a, b := stampOf(t, kube, "q0"), stampOf(t, kube, "q1"); a != "63010" || b != "63011" {
		t.Errorf("new pods after a restart got %q and %q, want 63010 and 63011", a, b)
	}
}

// A finished pod's port is reusable only once the node-record reconciler
// stamped it released; a Terminating pod whose containers run keeps it;
// an exhausted range requeues without a stamp.
func TestNodePortReuseWaitsForTheRecordRelease(t *testing.T) {
	a0 := gatedPod("a0", "host-a", "1", "63000-63001")
	a1 := gatedPod("a1", "host-a", "1", "63000-63001")
	kube := portClient(t, a0, a1)
	r := &NodePortReconciler{Client: kube}
	allocate(t, r, "a0")
	allocate(t, r, "a1")

	a2 := gatedPod("a2", "host-a", "4", "63000-63001")
	if err := kube.Create(context.Background(), a2); err != nil {
		t.Fatal(err)
	}
	if res := allocate(t, r, "a2"); res.RequeueAfter == 0 || stampOf(t, kube, "a2") != "" {
		t.Errorf("an exhausted range must requeue without a stamp, got %q", stampOf(t, kube, "a2"))
	}

	p1 := getPod(t, kube, "a1")
	p1.Finalizers = []string{"test/keep"}
	if err := kube.Update(context.Background(), p1); err != nil {
		t.Fatal(err)
	}
	if err := kube.Delete(context.Background(), p1); err != nil {
		t.Fatal(err)
	}
	allocate(t, r, "a2")
	if got := stampOf(t, kube, "a2"); got != "" {
		t.Errorf("a Terminating pod still holds its port, a2 got %q", got)
	}

	p1 = getPod(t, kube, "a1")
	p1.Status.Phase = corev1.PodSucceeded
	if err := kube.Status().Update(context.Background(), p1); err != nil {
		t.Fatal(err)
	}
	allocate(t, r, "a2")
	if got := stampOf(t, kube, "a2"); got != "" {
		t.Errorf("a finished pod's port is held until its record is released, a2 got %q", got)
	}

	p1 = getPod(t, kube, "a1")
	p1.Annotations[translate.NodeRecordReleasedAnnotation] = "2026-10-24T00:00:00Z"
	if err := kube.Update(context.Background(), p1); err != nil {
		t.Fatal(err)
	}
	allocate(t, r, "a2")
	if got := stampOf(t, kube, "a2"); got != "63001" {
		t.Errorf("a2 should take the released port 63001, got %q", got)
	}
}

func TestPortReusable(t *testing.T) {
	p := gatedPod("p", "host-a", "1", "63000-63000")
	p.Status.Phase = corev1.PodFailed
	if portReusable(p) {
		t.Error("a gated finished pod without the released stamp keeps its port")
	}
	delete(p.Annotations, translate.NodePortRangeAnnotation)
	if !portReusable(p) {
		t.Error("an ungated finished pod frees its port")
	}
	p.Annotations[translate.NodePortRangeAnnotation] = "63000-63000"
	p.Annotations[translate.NodeRecordReleasedAnnotation] = "t"
	if !portReusable(p) {
		t.Error("a released finished pod frees its port")
	}
	p.Status.Phase = corev1.PodRunning
	if portReusable(p) {
		t.Error("a live pod never frees its port")
	}
}

// A valid stamp is final for the pod's life; a stamp that is not a port
// in range is replaced with a free one.
func TestNodePortStampIsFinalUnlessInvalid(t *testing.T) {
	a0 := gatedPod("a0", "host-a", "1", "63000-63009")
	a0.Annotations[translate.NodePortAnnotation] = "63005"
	bad := gatedPod("bad", "host-a", "2", "63000-63009")
	bad.Annotations[translate.NodePortAnnotation] = "abc"
	out := gatedPod("out", "host-a", "3", "63000-63009")
	out.Annotations[translate.NodePortAnnotation] = "70000"
	kube := portClient(t, a0, bad, out)
	r := &NodePortReconciler{Client: kube}
	allocate(t, r, "a0")
	if got := stampOf(t, kube, "a0"); got != "63005" {
		t.Errorf("a valid stamp must never change, got %q", got)
	}
	allocate(t, r, "bad")
	allocate(t, r, "out")
	if b, o := stampOf(t, kube, "bad"), stampOf(t, kube, "out"); b != "63000" || o != "63001" {
		t.Errorf("invalid stamps must be replaced with free ports, got %q and %q", b, o)
	}
}

// staleFirstReader answers the first List from a frozen snapshot: what a
// second allocator's patch landing between the read and the verification
// looks like.
type staleFirstReader struct {
	client.Reader
	snapshot *corev1.PodList
	calls    int
}

func (s *staleFirstReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	s.calls++
	if s.calls == 1 {
		*list.(*corev1.PodList) = *s.snapshot.DeepCopy()
		return nil
	}
	return s.Reader.List(ctx, list, opts...)
}

// Two overlapping allocators could stamp one port twice; the younger pod
// withdraws, is requeued and avoids the port next time.
func TestNodePortConflictIsResolvedForTheYoungerPod(t *testing.T) {
	older := gatedPod("older", "host-a", "1", "63000-63009")
	older.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
	older.Annotations[translate.NodePortAnnotation] = "63000"
	younger := gatedPod("younger", "host-a", "2", "63000-63009")
	younger.CreationTimestamp = metav1.NewTime(time.Now())
	kube := portClient(t, older, younger)
	snap := &corev1.PodList{Items: []corev1.Pod{*younger}}
	r := &NodePortReconciler{Client: kube, Reader: &staleFirstReader{Reader: kube, snapshot: snap}}

	if res := allocate(t, r, "younger"); !res.Requeue {
		t.Error("a conflict must requeue the younger pod")
	}
	if got := stampOf(t, kube, "younger"); got != "" {
		t.Errorf("the younger pod's conflicting stamp must be withdrawn, got %q", got)
	}
	if got := stampOf(t, kube, "older"); got != "63000" {
		t.Errorf("the older pod keeps its port, got %q", got)
	}
	allocate(t, r, "younger")
	if got := stampOf(t, kube, "younger"); got != "63001" {
		t.Errorf("the retry must avoid the taken port, got %q", got)
	}
}

// Unscheduled pods wait; foreign, ungated and other-namespace pods are
// left alone; an invalid range is a configuration error, not a retry.
func TestNodePortSkipsUnscheduledAndForeignPods(t *testing.T) {
	pending := gatedPod("pending", "", "1", "63000-63009")
	foreign := gatedPod("foreign", "host-a", "1", "63000-63009")
	foreign.Labels = nil
	plain := gatedPod("plain", "host-a", "1", "")
	delete(plain.Annotations, translate.NodePortRangeAnnotation)
	elsewhere := gatedPod("elsewhere", "host-a", "1", "63000-63009")
	elsewhere.Namespace = "other"
	elsewhere.Annotations[translate.NodePortAnnotation] = "63000"
	kube := portClient(t, pending, foreign, plain, elsewhere)
	r := &NodePortReconciler{Client: kube}
	for _, n := range []string{"pending", "foreign", "plain"} {
		allocate(t, r, n)
		if got := stampOf(t, kube, n); got != "" {
			t.Errorf("%s must not be stamped, got %q", n, got)
		}
	}
	p := getPod(t, kube, "pending")
	p.Spec.NodeName = "host-a"
	if err := kube.Update(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	allocate(t, r, "pending")
	if got := stampOf(t, kube, "pending"); got != "63000" {
		t.Errorf("once scheduled the pod gets a port (another namespace's 63000 does not count), got %q", got)
	}
	bad := gatedPod("bad", "host-a", "1", "64999-63000")
	if err := kube.Create(context.Background(), bad); err != nil {
		t.Fatal(err)
	}
	if res := allocate(t, r, "bad"); res.RequeueAfter != 0 || stampOf(t, kube, "bad") != "" {
		t.Error("an invalid range is a config error, not something to retry")
	}
}

// The stamp is one annotation added by merge patch.
func TestNodePortPatchTouchesOnlyItsAnnotation(t *testing.T) {
	p := gatedPod("p", "host-a", "1", "63000-63009")
	p.Annotations["someone-else/note"] = "keep"
	p.Labels["extra"] = "keep"
	kube := portClient(t, p)
	allocate(t, &NodePortReconciler{Client: kube}, "p")
	got := getPod(t, kube, "p")
	if got.Annotations["someone-else/note"] != "keep" || got.Labels["extra"] != "keep" || got.Spec.NodeName != "host-a" ||
		got.Annotations[translate.NodePortRangeAnnotation] != "63000-63009" || got.Annotations[translate.NodePortAnnotation] != "63000" {
		t.Errorf("patch changed more than the stamp: %+v", got.ObjectMeta)
	}
}

type fakeRecords struct {
	nodes []slurm.Node
	err   error
}

func (f *fakeRecords) ListNodes(context.Context) ([]slurm.Node, error) { return f.nodes, f.err }

// A port a record of ours still names on the host is never handed out;
// foreign records and other hosts do not count; nothing is assigned
// before the host IP is known or while slurmctld cannot be read.
func TestNodePortSkipsPortsStillNamedBySlurmRecords(t *testing.T) {
	p := gatedPod("p", "host-a", "7", "63000-63003")
	kube := portClient(t, p)
	records := &fakeRecords{nodes: []slurm.Node{
		{Name: "slurmkube-w0-5", Address: "10.0.0.2", Port: 63000}, // a vanished pod's record on this host
		{Name: "slurmkube-w0-6", Address: "10.0.0.3", Port: 63001}, // another host
		{Name: "gpu-node-1", Address: "10.0.0.2", Port: 63002},     // not ours
	}}
	r := &NodePortReconciler{Client: kube, Records: records, Naming: translate.NewNaming("")}
	if res := allocate(t, r, "p"); res.RequeueAfter == 0 || stampOf(t, kube, "p") != "" {
		t.Fatalf("no host IP yet: must wait, got stamp %q", stampOf(t, kube, "p"))
	}
	pod := getPod(t, kube, "p")
	pod.Status.HostIP = "10.0.0.2"
	if err := kube.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	records.err = errBoom
	if res := allocate(t, r, "p"); res.RequeueAfter == 0 || stampOf(t, kube, "p") != "" {
		t.Fatalf("slurmctld unreadable: must wait, got stamp %q", stampOf(t, kube, "p"))
	}
	records.err = nil
	allocate(t, r, "p")
	if got := stampOf(t, kube, "p"); got != "63001" {
		t.Errorf("port = %q, want 63001 (63000 is still a record's on this host)", got)
	}
}

// With a custom node name prefix only records of that prefix count.
func TestNodePortRecordsFollowTheConfiguredPrefix(t *testing.T) {
	p := gatedPod("p", "host-a", "7", "63000-63003")
	p.Status.HostIP = "10.0.0.2"
	kube := portClient(t, p)
	records := &fakeRecords{nodes: []slurm.Node{
		{Name: "slurmkube-w0-5", Address: "10.0.0.2", Port: 63000},
		{Name: "hpc-w0-5", Address: "10.0.0.2", Port: 63001},
	}}
	r := &NodePortReconciler{Client: kube, Records: records, Naming: translate.NewNaming("hpc-w")}
	allocate(t, r, "p")
	if got := stampOf(t, kube, "p"); got != "63000" {
		t.Errorf("port = %q, want 63000 (the other prefix's record is not ours)", got)
	}
}
