// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package controller holds SlurmKube's reconcilers.
//
// Slurm is the source of truth for jobs, so the loop is driven by reading
// slurmctld: a Poller reads the job and node lists every PollInterval
// into an immutable Snapshot and enqueues one reconcile per claimed job
// and per dynamic node name. Kubernetes watches (JobSet, Workload, Pod)
// enqueue the same keys between polls so the controller reacts to pod
// changes within a second. Each reconcile reads the snapshot and the
// informer cache, decides, and applies at most a few idempotent writes;
// repeating a reconcile on unchanged state changes nothing.
package controller

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/turingmotors/slurmkube/internal/metrics"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// SlurmAPI is everything the controllers need from slurmrestd.
type SlurmAPI interface {
	ListJobs(ctx context.Context) ([]slurm.Job, error)
	GetJob(ctx context.Context, id uint64) (*slurm.Job, bool, error)
	ListNodes(ctx context.Context) ([]slurm.Node, error)
	SetJobFeatures(ctx context.Context, id uint64, features string) error
	ReleaseJob(ctx context.Context, id uint64) error
	HoldJob(ctx context.Context, id uint64) error
	SetJobComment(ctx context.Context, id uint64, comment string) error
	CancelJob(ctx context.Context, id uint64) error
	DeleteNode(ctx context.Context, name string) error
	SetNodeState(ctx context.Context, name, state, reason string) error
}

// Snapshot is one consistent read of slurmctld.
type Snapshot struct {
	At    time.Time
	Jobs  map[uint64]*slurm.Job
	Nodes map[string]*slurm.Node
	// Err is non-nil when the read failed; the previous snapshot stays
	// current and reconciles see its age.
	Err error
}

// Job returns a job of the snapshot.
func (s *Snapshot) Job(id uint64) (*slurm.Job, bool) {
	if s == nil {
		return nil, false
	}
	j, ok := s.Jobs[id]
	return j, ok
}

// arrayLive: some task of the array with this root ID (the root record
// included) has not ended. The array's worker pool serves them all, so a
// task that ended does not end the pool while another may still run,
// e.g. one requeued and waiting out requeue_delay.
func (s *Snapshot) arrayLive(root uint64) bool {
	if s == nil {
		return false
	}
	for _, j := range s.Jobs {
		if j.IsArray() && j.ArrayJobID.Number == root && !j.IsTerminal() {
			return true
		}
	}
	return false
}

// Node returns a dynamic node record of the snapshot.
func (s *Snapshot) Node(name string) (*slurm.Node, bool) {
	if s == nil {
		return nil, false
	}
	n, ok := s.Nodes[name]
	return n, ok
}

// Poller reads slurmctld periodically and fans the result out: the
// Snapshot for reconcilers, and reconcile requests for every claimed job
// (JobEvents) and every node record of ours (NodeEvents). It is a
// manager Runnable that needs leader election.
type Poller struct {
	Slurm    SlurmAPI
	Interval time.Duration
	// Claims says whether a job is this instance's to manage.
	Claims func(j *slurm.Job) bool
	// Naming recognizes this instance's node records.
	Naming translate.Naming
	// JobEvents and NodeEvents feed source.Channel of the reconcilers.
	JobEvents  chan event.TypedGenericEvent[reconcile.Request]
	NodeEvents chan event.TypedGenericEvent[reconcile.Request]
	// Log is slog-compatible via logr; nil disables.
	Log Logger

	current atomic.Pointer[Snapshot]
	nudge   chan struct{}
	once    sync.Once
}

// Logger is the subset of slog the controllers use.
type Logger interface {
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
	Debug(msg string, kv ...any)
}

func (p *Poller) init() {
	p.once.Do(func() { p.nudge = make(chan struct{}, 1) })
}

// Current is the latest snapshot (nil before the first read).
func (p *Poller) Current() *Snapshot { return p.current.Load() }

// Nudge asks for a poll before the next interval (coalesced).
func (p *Poller) Nudge() {
	p.init()
	select {
	case p.nudge <- struct{}{}:
	default:
	}
}

// NeedLeaderElection: only the leader polls and enqueues.
func (p *Poller) NeedLeaderElection() bool { return true }

// Start runs until ctx ends.
func (p *Poller) Start(ctx context.Context) error {
	p.init()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		case <-p.nudge:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		p.poll(ctx)
		timer.Reset(p.Interval)
	}
}

// Poll reads slurmctld once and enqueues. Exported for tests.
func (p *Poller) Poll(ctx context.Context) *Snapshot {
	p.init()
	return p.poll(ctx)
}

func (p *Poller) poll(ctx context.Context) *Snapshot {
	start := time.Now()
	snap := &Snapshot{At: start, Jobs: map[uint64]*slurm.Job{}, Nodes: map[string]*slurm.Node{}}
	jobs, err := p.Slurm.ListJobs(ctx)
	for i := range jobs {
		snap.Jobs[jobs[i].JobID] = &jobs[i]
	}
	if err == nil {
		var nodes []slurm.Node
		nodes, err = p.Slurm.ListNodes(ctx)
		for i := range nodes {
			snap.Nodes[nodes[i].Name] = &nodes[i]
		}
	}
	metrics.PollsTotal.Inc()
	metrics.PollDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.PollErrorsTotal.Inc()
		if p.Log != nil {
			p.Log.Warn("reading slurmctld failed; reconciles keep the previous snapshot", "error", err)
		}
		if prev := p.current.Load(); prev != nil {
			stale := *prev
			stale.Err = err
			p.current.Store(&stale)
			return &stale
		}
		snap.Err = err
		p.current.Store(snap)
		return snap
	}
	p.current.Store(snap)
	claimed, refused := 0, 0
	for id, j := range snap.Jobs {
		if p.Claims != nil && !p.Claims(j) {
			continue
		}
		if !j.IsTerminal() {
			claimed++
			if j.IsPending() && strings.HasPrefix(j.Comment, commentRefused) {
				refused++
			}
		}
		p.enqueue(ctx, p.JobEvents, jobKey(id))
	}
	metrics.JobsClaimed.Set(float64(claimed))
	metrics.JobsRefused.Set(float64(refused))
	for name := range snap.Nodes {
		if _, _, ours := p.Naming.ParseNodeName(name); ours {
			p.enqueue(ctx, p.NodeEvents, name)
		}
	}
	return snap
}

func (p *Poller) enqueue(ctx context.Context, ch chan event.TypedGenericEvent[reconcile.Request], name string) {
	if ch == nil {
		return
	}
	select {
	case ch <- event.TypedGenericEvent[reconcile.Request]{Object: reconcile.Request{NamespacedName: key(name)}}:
	case <-ctx.Done():
	}
}
