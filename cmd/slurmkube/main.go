// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Command slurmkube runs the SlurmKube controller manager.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/controller"
	"github.com/turingmotors/slurmkube/internal/fence"
	"github.com/turingmotors/slurmkube/internal/metrics"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

func main() {
	configPath := flag.String("config", "/etc/slurmkube/config.yaml", "configuration file")
	metricsAddr := flag.String("metrics-addr", ":8080", "/metrics listen address")
	probeAddr := flag.String("health-addr", ":8081", "/healthz and /readyz listen address")
	leaderElect := flag.Bool("leader-elect", true, "run only one active replica (coordination.k8s.io Lease)")
	leaseNS := flag.String("leader-election-namespace", os.Getenv("POD_NAMESPACE"), "namespace of the leader Lease (default: POD_NAMESPACE)")
	logLevel := flag.String("log-level", "info", "debug, info, warn or error")
	allowedImages := flag.String("allowed-slurmd-images", "", "comma-separated prefixes the slurmd image must match (empty: any)")
	flag.Parse()

	log := newLogger(*logLevel)
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("loading configuration", "error", err)
		os.Exit(1)
	}
	if *allowedImages != "" && !imageAllowed(cfg.Slurmd.Image, *allowedImages) {
		log.Error("slurmd image is not in --allowed-slurmd-images", "image", cfg.Slurmd.Image)
		os.Exit(1)
	}

	scheme, err := controller.Scheme()
	if err != nil {
		log.Error("scheme", "error", err)
		os.Exit(1)
	}

	ns := *leaseNS
	if ns == "" {
		ns = cfg.Namespace
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress:  *probeAddr,
		LeaderElection:          *leaderElect,
		LeaderElectionID:        "slurmkube-leader",
		LeaderElectionNamespace: ns,
		Cache:                   controller.CacheOptions(cfg.Namespace),
	})
	if err != nil {
		log.Error("creating manager", "error", err)
		os.Exit(1)
	}

	slurmClient, err := slurm.NewClient(slurm.Options{
		BaseURL: cfg.Slurm.RestURL, User: cfg.Slurm.User, TokenFile: cfg.Slurm.TokenFile,
		JWTKeyFile: cfg.Slurm.JWTKeyFile, JWTUser: cfg.Slurm.JWTUser, JWTLifetime: cfg.Slurm.JWTLifetime.Duration,
		CACertFile: cfg.Slurm.CACertFile, InsecureSkipVerify: cfg.Slurm.InsecureSkipTLSVerify,
		RequestTimeout: cfg.Slurm.RequestTimeout.Duration, RequestsPerSecond: cfg.Slurm.RequestsPerSecond,
	})
	if err != nil {
		log.Error("creating slurm client", "error", err)
		os.Exit(1)
	}
	slurmClient.OnRequest = func(method string, code int) {
		metrics.SlurmRequestsTotal.WithLabelValues(method, fmt.Sprint(code)).Inc()
	}
	slurmClient.OnRequestDuration = func(method string, d time.Duration) {
		metrics.SlurmRequestDuration.WithLabelValues(method).Observe(d.Seconds())
	}

	var fencer fence.Fencer
	switch cfg.Fencing.Mode {
	case config.FencingTrustKubernetes:
		fencer = fence.TrustKubernetes{}
	default:
		fencer = fence.Manual{Reader: mgr.GetAPIReader(), ConfirmAnnotation: cfg.Fencing.ConfirmAnnotation}
	}

	translator := translate.New(cfg)
	jobEvents := make(chan controller.Event, 1024)
	nodeEvents := make(chan controller.Event, 1024)
	poller := &controller.Poller{
		Slurm: slurmClient, Interval: cfg.Recovery.PollInterval.Duration, Log: log, Naming: translator.Naming,
		JobEvents: jobEvents, NodeEvents: nodeEvents,
		Claims: func(j *slurm.Job) bool { _, ok := cfg.PriorityClassFor(j.Partition); return ok },
	}
	if err := mgr.Add(poller); err != nil {
		log.Error("adding poller", "error", err)
		os.Exit(1)
	}
	recorder := mgr.GetEventRecorderFor("slurmkube")
	jobs := &controller.JobReconciler{Client: mgr.GetClient(), Slurm: slurmClient, Poller: poller, Cfg: cfg, Translate: translator, Recorder: recorder, Log: log, Fencer: fencer}
	if err := jobs.SetupWithManager(mgr, jobEvents); err != nil {
		log.Error("wiring job reconciler", "error", err)
		os.Exit(1)
	}
	nodes := &controller.NodeRecordReconciler{Client: mgr.GetClient(), Slurm: slurmClient, Poller: poller, Cfg: cfg, Naming: translator.Naming, Recorder: recorder, Log: log, Fencer: fencer}
	if err := nodes.SetupWithManager(mgr, nodeEvents); err != nil {
		log.Error("wiring node-record reconciler", "error", err)
		os.Exit(1)
	}
	if cfg.Slurmd.NodePorts.Enabled() {
		ports := &controller.NodePortReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Records: slurmClient, Naming: translator.Naming, Recorder: recorder, Log: log}
		if err := ports.SetupWithManager(mgr); err != nil {
			log.Error("wiring node-port reconciler", "error", err)
			os.Exit(1)
		}
	}
	if err := mgr.AddHealthzCheck("ping", healthz.Ping); err != nil {
		log.Error("healthz", "error", err)
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("slurm", func(_ *http.Request) error {
		if s := poller.Current(); s != nil && s.Err != nil {
			return s.Err
		}
		return nil
	}); err != nil {
		log.Error("readyz", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("slurmkube starting", "namespace", cfg.Namespace, "partitions", len(cfg.Partitions), "pollInterval", cfg.Recovery.PollInterval.String(), "fencing", cfg.Fencing.Mode)
	if err := mgr.Start(ctx); err != nil {
		log.Error("manager exited", "error", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

func imageAllowed(image, allowed string) bool {
	for _, p := range strings.Split(allowed, ",") {
		if p = strings.TrimSpace(p); p != "" && strings.HasPrefix(image, p) {
			return true
		}
	}
	return false
}
