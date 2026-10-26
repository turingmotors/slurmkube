// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/fence"
	"github.com/turingmotors/slurmkube/internal/metrics"
	"github.com/turingmotors/slurmkube/internal/ownership"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// NodeRecordReconciler keeps one dynamic node name honest: the slurmctld
// record under the name belongs to the pod that registered it, a pod
// starts slurmd only when no other record exists under its name, a dead
// pod's record is released, and a finished pod's port stays reserved until
// then. Keyed by node name; triggered by the poller and by pod events.
type NodeRecordReconciler struct {
	client.Client
	Slurm  SlurmAPI
	Poller *Poller
	Cfg    *config.Config
	// Naming parses node names and names JobSets.
	Naming   translate.Naming
	Recorder record.EventRecorder
	Log      Logger
	Fencer   fence.Fencer
	Clock    func() time.Time

	mu            sync.Mutex
	vanishedSince map[string]time.Time
	orphanSeen    map[string]int
}

func (r *NodeRecordReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

func (r *NodeRecordReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	name := req.Name
	id, index, ok := r.Naming.ParseNodeName(name)
	if !ok {
		return ctrl.Result{}, nil
	}
	snap := r.Poller.Current()
	if snap == nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	rec, found := snap.Node(name)
	job, jobKnown := snap.Job(id)
	jobLive := jobKnown && !job.IsTerminal()

	var js *jobsetv1alpha2.JobSet
	var got jobsetv1alpha2.JobSet
	if err := r.Get(ctx, client.ObjectKey{Namespace: r.Cfg.Namespace, Name: r.Naming.JobSetName(id)}, &got); err == nil {
		js = &got
	} else if client.IgnoreNotFound(err) != nil {
		return ctrl.Result{}, err
	}

	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(r.Cfg.Namespace),
		client.MatchingLabels{translate.ManagedByLabel: translate.ManagedByValue, translate.SlurmJobIDLabel: jobKey(id)}); err != nil {
		return ctrl.Result{}, err
	}
	var pods []*corev1.Pod
	var views []ownership.PodView
	for i := range podList.Items {
		p := &podList.Items[i]
		if podIndex(p) != index {
			continue
		}
		pods = append(pods, p)
		views = append(views, ownership.FromPod(p, podKeys))
	}
	log := r.Log
	if log == nil {
		log = nopLogger{}
	}

	// A record under one of our names without our pin feature is another
	// manager's (the node name prefix is shared, e.g. to reuse a site's
	// gres.conf): it is never released, DOWN'd or swept. A pod of ours
	// under the name waits at the clearance gate until it is gone.
	if found && !rec.HasFeature(r.Naming.PinFeature(id)) {
		r.forgetOrphan(name)
		if len(ownership.Live(views)) > 0 {
			log.Warn("a record of another manager holds this node name; the pod stays gated until it is gone", "node", name, "features", rec.Features)
			return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
		}
		return ctrl.Result{}, nil
	}

	// Orphan sweep: a record with no JobSet and no pods. Its job is gone,
	// terminal, or live with nothing serving it (JobSet deleted while the
	// controller was down). Released after the grace, counted in polls.
	if found && js == nil && len(pods) == 0 {
		return r.sweepOrphan(ctx, snap, rec, jobLive, log)
	}
	r.forgetOrphan(name)

	r.mu.Lock()
	since := r.vanishedSince[name]
	r.mu.Unlock()
	in := ownership.Input{
		Record: nil, Pods: views, JobLive: jobLive, Now: r.now(),
		VanishedSince: since, VanishedGrace: r.Cfg.Recovery.VanishedOwnerGrace.Duration,
		RegistrationTimeout: r.Cfg.Slurmd.RegistrationTimeout.Duration,
	}
	if found {
		in.Record = rec
	}
	d := ownership.Decide(in)
	if d.Action == ownership.WaitVanished && since.IsZero() {
		r.mu.Lock()
		r.vanishedSince[name] = r.now()
		r.mu.Unlock()
	} else if d.Action != ownership.WaitVanished && d.Action != ownership.ReleaseVanished {
		r.mu.Lock()
		delete(r.vanishedSince, name)
		r.mu.Unlock()
	}

	for _, p := range d.ReleasePods {
		if p.Gated && !p.Released {
			r.stamp(ctx, byName(pods, p.Name), translate.NodeRecordReleasedAnnotation, "no record points at the pod")
		}
	}
	if d.Action != ownership.Nothing {
		metrics.NodeRecordActionsTotal.WithLabelValues(d.Action.String()).Inc()
	}
	switch d.Action {
	case ownership.Nothing:
		if found || len(ownership.Live(views)) == 0 {
			return ctrl.Result{}, nil
		}
		// A cleared pod waiting for its record: check again at the timeout.
		return ctrl.Result{RequeueAfter: r.Cfg.Slurmd.RegistrationTimeout.Duration}, nil
	case ownership.Conflict:
		log.Warn("two live pods claim one dynamic node name; neither is cleared until one is gone", "node", name)
		return ctrl.Result{}, nil
	case ownership.ClearPod:
		pod := byName(pods, d.Pod.Name)
		if err := patchAnnotation(ctx, r.Client, pod, translate.NodeRecordClearAnnotation, r.now().UTC().Format(time.RFC3339)); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("cleared pod to start slurmd: no record exists under its node name", "node", name, "pod", pod.Name)
		r.event(js, corev1.EventTypeNormal, "SlurmdCleared", "node %s: pod %s may start slurmd (no record under the name)", name, pod.Name)
		return ctrl.Result{RequeueAfter: r.Cfg.Slurmd.RegistrationTimeout.Duration}, nil
	case ownership.ReplacePod:
		pod := byName(pods, d.Pod.Name)
		if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
			return ctrl.Result{}, err
		}
		log.Warn("slurmd never registered; deleted the pod so the JobSet replaces it", "node", name, "pod", pod.Name, "why", d.Reason)
		r.event(js, corev1.EventTypeWarning, "NodeRegistrationTimeout",
			"node %s: %s (if slurmctld logs 'node table is full', raise MaxNodeCount or set slurmd.maxDynamicNodes); pod deleted so the JobSet replaces it", name, d.Reason)
		return ctrl.Result{}, nil
	case ownership.WaitVanished:
		remaining := r.Cfg.Recovery.VanishedOwnerGrace.Duration - r.now().Sub(since)
		if remaining < time.Second {
			remaining = time.Second
		}
		return ctrl.Result{RequeueAfter: remaining}, nil
	case ownership.ReleaseRecord, ownership.ReleaseVanished, ownership.ReleaseStale:
		return r.release(ctx, rec, js, pods, views, d, log)
	}
	return ctrl.Result{}, nil
}

func (r *NodeRecordReconciler) release(ctx context.Context, rec *slurm.Node, js *jobsetv1alpha2.JobSet, pods []*corev1.Pod, views []ownership.PodView, d ownership.Decision, log Logger) (ctrl.Result, error) {
	// Fencing: before the job can run again, the owner's host must be
	// known to have stopped it. A gone pod whose kubelet did not report
	// the end on a host that is not Ready is remembered on the JobSet so
	// the job reconciler holds the job until the fencer clears it.
	if d.Action == ownership.ReleaseRecord && js != nil && d.Pod != nil {
		pod := byName(pods, d.Pod.Name)
		if pod != nil && pod.Annotations[FenceVerdictAnnotation] == "" {
			v, err := r.Fencer.Confirm(ctx, fence.Incident{
				JobID: idOf(js), Node: rec.Name, PodName: pod.Name, PodUID: string(pod.UID), Host: pod.Spec.NodeName,
				PodTerminated: containersTerminated(pod), Owner: js,
			})
			if err != nil {
				return ctrl.Result{}, err
			}
			if v.Safe {
				_ = patchAnnotation(ctx, r.Client, pod, FenceVerdictAnnotation, "safe")
			} else {
				metrics.FencingWaitsTotal.Inc()
				if err := patchAnnotation(ctx, r.Client, js, FencePendingAnnotation, addToCSV(js.Annotations[FencePendingAnnotation], pod.Spec.NodeName)); err != nil {
					return ctrl.Result{}, err
				}
				r.event(js, corev1.EventTypeWarning, "FencingRequired", "node %s: %s", rec.Name, v.Reason)
				log.Warn("the job will not run again until its old host is fenced", "node", rec.Name, "host", pod.Spec.NodeName, "why", v.Reason)
			}
		}
	}
	st, err := releaseRecord(ctx, r.Slurm, r.Naming, rec, views, "slurmkube: "+d.Reason, log)
	if err != nil {
		log.Warn("releasing a node record failed, will retry", "node", rec.Name, "why", d.Reason, "error", err)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if st == recordDowned || st == recordBusy {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	r.mu.Lock()
	delete(r.vanishedSince, rec.Name)
	r.mu.Unlock()
	switch d.Action {
	case ownership.ReleaseRecord:
		if pod := byName(pods, d.Pod.Name); pod != nil {
			r.stamp(ctx, pod, translate.NodeRecordReleasedAnnotation, "its record is gone")
		}
		log.Info("released the record of a finished pod; a replacement may now register", "node", rec.Name, "pod", d.Pod.Name)
		r.event(js, corev1.EventTypeNormal, "NodeRecordReleased", "node %s: record of finished pod %s (%s:%d) released", rec.Name, d.Pod.Name, rec.Address, rec.Port)
	case ownership.ReleaseVanished:
		log.Warn("released a record whose pod no longer exists", "node", rec.Name, "extra", rec.Extra)
		r.event(js, corev1.EventTypeWarning, "NodeRecordOrphaned", "node %s: %s; record released", rec.Name, d.Reason)
	case ownership.ReleaseStale:
		if pod := byName(pods, d.Pod.Name); pod != nil {
			if err := client.IgnoreNotFound(r.Delete(ctx, pod)); err != nil {
				return ctrl.Result{}, err
			}
		}
		log.Warn("record pointed elsewhere than its pod's slurmd: record and pod released", "node", rec.Name, "why", d.Reason)
		r.event(js, corev1.EventTypeWarning, "NodeRecordStale", "node %s: %s; record and pod deleted so the JobSet replaces it", rec.Name, d.Reason)
	}
	// The poller's snapshot still shows the record until the next poll.
	r.Poller.Nudge()
	return ctrl.Result{}, nil
}

func (r *NodeRecordReconciler) sweepOrphan(ctx context.Context, snap *Snapshot, rec *slurm.Node, jobLive bool, log Logger) (ctrl.Result, error) {
	if r.Cfg.Recovery.SweepOrphanedNodes != nil && !*r.Cfg.Recovery.SweepOrphanedNodes {
		return ctrl.Result{}, nil
	}
	id, _, _ := r.Naming.ParseNodeName(rec.Name)
	if jobLive {
		// A live job with neither JobSet nor pods: only when the cache is
		// credible (some JobSet of ours exists) — a mis-scoped cache looks
		// the same.
		var any jobsetv1alpha2.JobSetList
		if err := r.List(ctx, &any, client.InNamespace(r.Cfg.Namespace), client.MatchingLabels{translate.ManagedByLabel: translate.ManagedByValue}, client.Limit(1)); err != nil || len(any.Items) == 0 {
			return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, err
		}
	}
	r.mu.Lock()
	r.orphanSeen[rec.Name]++
	seen := r.orphanSeen[rec.Name]
	r.mu.Unlock()
	if seen < r.Cfg.Recovery.OrphanPolls {
		return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
	}
	reason := "slurmkube: orphaned node record (job gone)"
	if jobLive {
		reason = "slurmkube: orphaned node record (the job's JobSet and pods are gone)"
	}
	st, err := releaseRecord(ctx, r.Slurm, r.Naming, rec, nil, reason, log)
	if err != nil {
		log.Info("releasing an orphaned node record failed, will retry", "node", rec.Name, "slurmJobID", id, "error", err)
		return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
	}
	if st == recordDowned || st == recordBusy {
		return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
	}
	metrics.NodeRecordActionsTotal.WithLabelValues("release-orphan").Inc()
	log.Info("released an orphaned node record", "node", rec.Name, "slurmJobID", id, "jobLive", jobLive)
	r.forgetOrphan(rec.Name)
	r.Poller.Nudge()
	return ctrl.Result{}, nil
}

func (r *NodeRecordReconciler) forgetOrphan(name string) {
	r.mu.Lock()
	delete(r.orphanSeen, name)
	r.mu.Unlock()
}

func (r *NodeRecordReconciler) stamp(ctx context.Context, pod *corev1.Pod, k, why string) {
	if pod == nil {
		return
	}
	if err := patchAnnotation(ctx, r.Client, pod, k, r.now().UTC().Format(time.RFC3339)); err != nil && r.Log != nil {
		r.Log.Debug("stamping a pod failed, will retry", "pod", pod.Name, "annotation", k, "why", why, "error", err)
	}
}

// event records on the JobSet; js is nil once the JobSet is deleted while
// its pods (and records) linger. Typed as *JobSet so that the nil check
// sees it: a nil *JobSet in a client.Object is not nil.
func (r *NodeRecordReconciler) event(js *jobsetv1alpha2.JobSet, kind, reason, format string, args ...any) {
	if r.Recorder == nil || js == nil {
		return
	}
	r.Recorder.Eventf(js, kind, reason, format, args...)
}

// SetupWithManager registers the reconciler: one worker per node name
// key, triggered by the poller's channel and by our pod events.
func (r *NodeRecordReconciler) SetupWithManager(mgr ctrl.Manager, events chan eventOf) error {
	if r.vanishedSince == nil {
		r.vanishedSince = map[string]time.Time{}
		r.orphanSeen = map[string]int{}
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("node-record").
		WatchesRawSource(channelSource(events)).
		Watches(&corev1.Pod{}, podToNodeName(r.Naming), ownPodsOnly()).
		Complete(r)
}

func byName(pods []*corev1.Pod, name string) *corev1.Pod {
	for _, p := range pods {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func idOf(js *jobsetv1alpha2.JobSet) uint64 {
	id, _ := parseUint(js.Labels[translate.SlurmJobIDLabel])
	return id
}

// containersTerminated: the kubelet reported every container of the pod
// terminated (the pod really stopped), as opposed to a pod that is only
// marked for deletion or whose status is unknown.
func containersTerminated(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return true
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated == nil {
			return false
		}
	}
	return true
}

type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
func (nopLogger) Debug(string, ...any) {}

func parseUint(s string) (uint64, error) {
	var v uint64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}
