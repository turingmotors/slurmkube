//go:build integration

// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/turingmotors/slurmkube/internal/fence"
)

// The manager wires exactly as main.go does (cache options, scheme, the
// three reconcilers, the poller) and starts against a real API server.
// Found end to end: a cluster-scoped type with a namespace setting in
// ByObject made NewManager fail at startup; unit tests do not reach it.
func TestManagerWiresAndStartsAgainstRealAPIServer(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{"../../test/crd"}, ErrorIfCRDPathMissing: true}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	scheme, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0",
		LeaderElection: false, Cache: CacheOptions(cfg.Namespace),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	fs := newFakeSlurm(heldJob(70))
	jobEvents := make(chan Event, 16)
	nodeEvents := make(chan Event, 16)
	poller := &Poller{Slurm: fs, Interval: 200 * time.Millisecond, JobEvents: jobEvents, NodeEvents: nodeEvents, Claims: func(*slurmJobAlias) bool { return true }}
	if err := mgr.Add(poller); err != nil {
		t.Fatal(err)
	}
	fencer := fence.Manual{Reader: mgr.GetAPIReader(), ConfirmAnnotation: cfg.Fencing.ConfirmAnnotation}
	jobs := &JobReconciler{Client: mgr.GetClient(), Slurm: fs, Poller: poller, Cfg: cfg, Fencer: fencer}
	if err := jobs.SetupWithManager(mgr, jobEvents); err != nil {
		t.Fatal(err)
	}
	nodes := &NodeRecordReconciler{Client: mgr.GetClient(), Slurm: fs, Poller: poller, Cfg: cfg, Fencer: fencer}
	if err := nodes.SetupWithManager(mgr, nodeEvents); err != nil {
		t.Fatal(err)
	}
	ports := &NodePortReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Records: fs}
	if err := ports.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	// Namespace for the JobSet.
	kube := mgr.GetClient()
	if ok := mgr.GetCache().WaitForCacheSync(ctx); !ok {
		t.Fatal("cache did not sync")
	}
	if err := kube.Create(ctx, nsObject("slurm-jobs")); err != nil {
		t.Fatal(err)
	}
	// The poller claims the held job and the job reconciler creates its
	// JobSet through the running manager.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var js jobSetAlias
		if err := kube.Get(ctx, jobSetKey("slurm-jobs", 70), &js); err == nil {
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("manager: %v", err)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("JobSet not created by the running manager; comment=%q", fs.job(70).Comment)
}
