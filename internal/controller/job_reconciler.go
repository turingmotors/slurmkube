// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/fence"
	"github.com/turingmotors/slurmkube/internal/kueue"
	"github.com/turingmotors/slurmkube/internal/metrics"
	"github.com/turingmotors/slurmkube/internal/ownership"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// JobReconciler drives one Slurm job from held to running on its own
// dynamic nodes, and cleans up after it. Slurm is the source of truth:
// the reconciler reads the job from the poller's snapshot and derives
// what Kubernetes should hold for it.
//
// Lifecycle (every step idempotent, re-derived from the current state):
//
//	held by the submit plugin ──claim──> JobSet created (suspended by Kueue)
//	  ──Kueue admits──> pods start, slurmd registers (gated per node name)
//	  ──pin to its nodes, verify every record is a live pod's──> release
//	RUNNING ──ends──> records released, JobSet deleted
//	requeued (pending, pinned, not held) ──records not served──> held again
//	JobSet Failed before the job ──> job cancelled with the reason
//
// Two controllers must never run the same job: a job is claimed only when
// its comment is the plugin's hold comment (or already SlurmKube's), and a
// job that another manager (config.Claim.OtherManagers) holds a JobSet
// for is left alone.
type JobReconciler struct {
	client.Client
	Slurm  SlurmAPI
	Poller *Poller
	Cfg    *config.Config
	// Translate builds JobSets and names objects (translate.New(Cfg)
	// when nil).
	Translate *translate.Translator
	Recorder  record.EventRecorder
	Log       Logger
	Fencer    fence.Fencer
	Clock     func() time.Time

	// written is when this instance last held or released each job; a
	// snapshot older than that is not acted on (see Reconcile).
	mu      sync.Mutex
	written map[uint64]time.Time
}

func (r *JobReconciler) naming() translate.Naming { return r.Translate.Naming }

// noteWrite records a hold or release of job id at wall-clock time (the
// poller stamps snapshots with the same clock).
func (r *JobReconciler) noteWrite(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.written == nil {
		r.written = map[uint64]time.Time{}
	}
	r.written[id] = time.Now()
}

// wroteAfter reports whether job id was held or released after at.
func (r *JobReconciler) wroteAfter(id uint64, at time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.written[id]
	return ok && !at.After(t)
}

func (r *JobReconciler) forgetWrite(id uint64) {
	r.mu.Lock()
	delete(r.written, id)
	r.mu.Unlock()
}

func (r *JobReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

func (r *JobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	id, err := strconv.ParseUint(req.Name, 10, 64)
	if err != nil {
		return ctrl.Result{}, nil
	}
	snap := r.Poller.Current()
	if snap == nil {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if r.wroteAfter(id, snap.At) {
		// Our own hold or release is newer than the snapshot (a poll may
		// even have been in flight across it): deciding on it again would
		// repeat the write. The nudged poll follows within a second.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	log := r.Log
	if log == nil {
		log = nopLogger{}
	}
	job, known := snap.Job(id)
	pinID := id
	if known {
		pinID = job.PinID()
	}
	js, err := r.jobSet(ctx, pinID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !known {
		// Not in slurmctld's list: finished long ago (MinJobAge) or never
		// ours. A JobSet of ours under the ID is cleaned up once a live
		// read confirms the job is gone or terminal.
		if js == nil {
			return ctrl.Result{}, nil
		}
		live, found, err := r.Slurm.GetJob(ctx, id)
		if err != nil {
			return ctrl.Result{}, err
		}
		if (found && !live.IsTerminal()) || snap.arrayLive(id) {
			return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
		}
		return r.cleanup(ctx, snap, pinID, js, log)
	}
	if !r.claimable(ctx, job, js) {
		return ctrl.Result{}, nil
	}
	if job.IsTerminal() {
		if js == nil || (job.IsArray() && snap.arrayLive(pinID)) {
			// An ended array task leaves the pool to the tasks still to
			// run; the last one to end cleans it up.
			return ctrl.Result{}, nil
		}
		return r.cleanup(ctx, snap, pinID, js, log)
	}
	switch {
	case job.IsPending() && job.IsHeld():
		return r.admit(ctx, snap, job, js, log)
	case job.IsPending() && !job.PinnedTo(r.naming().PinFeature(pinID)):
		return r.rehold(ctx, job, "released before SlurmKube pinned it to its dynamic nodes; held again", "unpinned", log)
	case job.IsPending() && job.RestartCnt > 0 && !job.IsArray():
		return r.requeued(ctx, snap, job, js, log)
	}
	if js != nil && !job.IsTerminal() && (jobSetIs(js, jobsetv1alpha2.JobSetFailed) || jobSetIs(js, jobsetv1alpha2.JobSetCompleted)) {
		return r.failForDeadJobSet(ctx, job, js, log)
	}
	if js != nil && !job.IsPending() {
		if err := r.syncDeadline(ctx, job, js, log); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// syncDeadline keeps the Kubernetes deadline of a running job's Jobs in
// step with Slurm's time limit: `scontrol update TimeLimit` moves Slurm's
// end_time, and each child Job's activeDeadlineSeconds is recomputed from
// it and the Job's own start (translate.RunningDeadlineSeconds). The
// JobSet's template is immutable, so the Jobs are patched directly; a Job
// the JobSet recreates gets the template's value and is corrected on the
// next reconcile. Nothing is written when the value already matches.
func (r *JobReconciler) syncDeadline(ctx context.Context, job *slurm.Job, js *jobsetv1alpha2.JobSet, log Logger) error {
	if job.IsArray() {
		return nil
	}
	var list batchv1.JobList
	if err := r.List(ctx, &list, client.InNamespace(r.Cfg.Namespace), client.MatchingLabels{jobSetNameLabel: js.Name}); err != nil {
		return err
	}
	for i := range list.Items {
		child := &list.Items[i]
		if child.Status.StartTime == nil {
			continue // not started: the template's value holds until it does
		}
		want, ok := translate.RunningDeadlineSeconds(job, child.Status.StartTime.Time)
		if !ok || ptrEqual(want, child.Spec.ActiveDeadlineSeconds) {
			continue
		}
		before := child.DeepCopy()
		child.Spec.ActiveDeadlineSeconds = want
		if err := r.Patch(ctx, child, client.MergeFrom(before)); err != nil {
			return fmt.Errorf("updating the deadline of Job %s: %w", child.Name, err)
		}
		was, now := deadlineString(before.Spec.ActiveDeadlineSeconds), deadlineString(want)
		limit := "UNLIMITED"
		if end := job.RunEnd(); !end.IsZero() {
			limit = fmt.Sprintf("%d min, ends %s", job.TimeLimitSeconds()/60, end.UTC().Format(time.RFC3339))
		}
		log.Info("updated the Job's deadline to follow Slurm's time limit", "slurmJobID", job.JobID, "job", child.Name, "from", was, "to", now, "slurmTimeLimit", limit)
		r.event(js, corev1.EventTypeNormal, "DeadlineUpdated", "Job %s: activeDeadlineSeconds %s -> %s (Slurm time limit %s)", child.Name, was, now, limit)
	}
	return nil
}

// refusalOption is the metrics label for a translation refusal: the
// unsupported option's name (a small fixed set), "other" for anything else.
func refusalOption(err error) string {
	var u *translate.Unsupported
	if errors.As(err, &u) {
		return u.Option
	}
	return "other"
}

func ptrEqual(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func deadlineString(v *int64) string {
	if v == nil {
		return "none"
	}
	return strconv.FormatInt(*v, 10) + "s"
}

// claimable: the job is this instance's to run.
func (r *JobReconciler) claimable(ctx context.Context, job *slurm.Job, js *jobsetv1alpha2.JobSet) bool {
	if _, mapped := r.Cfg.PriorityClassFor(job.Partition); !mapped {
		return false
	}
	if js != nil {
		return true // we created it
	}
	c := job.Comment
	if c != r.Cfg.Claim.HeldComment && !strings.HasPrefix(c, CommentPrefix) {
		return false
	}
	for _, other := range r.Cfg.Claim.OtherManagers {
		var list jobsetv1alpha2.JobSetList
		labels := client.MatchingLabels{}
		for k, v := range other.Labels {
			labels[k] = v
		}
		labels[other.JobIDLabel] = jobKey(job.PinID())
		if err := r.List(ctx, &list, client.InNamespace(r.Cfg.Namespace), labels); err == nil && len(list.Items) > 0 {
			if r.Log != nil {
				r.Log.Info("job has a JobSet of another manager; not claiming it", "slurmJobID", job.JobID, "manager", other.Name)
			}
			return false
		}
	}
	return true
}

func (r *JobReconciler) jobSet(ctx context.Context, id uint64) (*jobsetv1alpha2.JobSet, error) {
	var js jobsetv1alpha2.JobSet
	err := r.Get(ctx, client.ObjectKey{Namespace: r.Cfg.Namespace, Name: r.naming().JobSetName(id)}, &js)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if js.Labels[translate.ManagedByLabel] != translate.ManagedByValue {
		return nil, nil // somebody else's object under our name
	}
	return &js, nil
}

// admit takes a held job to release: JobSet, Kueue admission, pin,
// verification of every dynamic node, fencing, release.
func (r *JobReconciler) admit(ctx context.Context, snap *Snapshot, job *slurm.Job, js *jobsetv1alpha2.JobSet, log Logger) (ctrl.Result, error) {
	id := job.JobID
	if job.IsArray() && !job.IsArrayRoot() && js == nil {
		return ctrl.Result{}, nil // the array's root owns the pool
	}
	if js == nil {
		if job.Comment != r.Cfg.Claim.HeldComment && !strings.HasPrefix(job.Comment, commentHeld) && job.Comment != commentClaimed {
			// Held by SlurmKube for another reason (orphan, refusal) or by
			// an operator: no new JobSet.
			return ctrl.Result{}, nil
		}
		if why := waitReason(job, r.now()); why != "" {
			r.explain(ctx, job, commentHeld+why)
			return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
		}
		if why := r.slotGate(ctx, job); why != "" {
			r.explain(ctx, job, commentHeld+why)
			metrics.DynamicNodeSlotWaitsTotal.Inc()
			return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
		}
		desired, err := r.Translate.JobSet(job)
		if err != nil {
			// Logged and counted once: the comment marks the job and later
			// reconciles leave it alone (above) until it is released again.
			metrics.TranslationRefusalsTotal.WithLabelValues(refusalOption(err)).Inc()
			r.explain(ctx, job, commentRefused+err.Error())
			log.Warn("job refused at translation; held with the reason in its comment", "slurmJobID", id, "option", refusalOption(err), "error", err)
			if r.Cfg.Recovery.CancelRefusedJobs {
				if err := r.Slurm.CancelJob(ctx, id); err != nil {
					return ctrl.Result{}, err
				}
				metrics.JobsCancelledTotal.WithLabelValues("refused").Inc()
				log.Warn("cancelled the refused job (recovery.cancelRefusedJobs)", "slurmJobID", id)
			}
			return ctrl.Result{}, nil
		}
		created, err := r.ensureJobSet(ctx, desired, job)
		if err != nil {
			return ctrl.Result{}, err
		}
		if created {
			metrics.JobSetsCreatedTotal.Inc()
			log.Info("created JobSet", "slurmJobID", id, "jobset", desired.Name)
			r.explain(ctx, job, commentClaimed)
			r.event(desired, corev1.EventTypeNormal, "Created", "JobSet created for Slurm job %d", id)
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	if !jobSetIdentityMatches(js, job) {
		metrics.JobsCancelledTotal.WithLabelValues("identity-conflict").Add(0)
		log.Error("JobSet under this job's name belongs to another incarnation of the job ID; refusing to use it", "slurmJobID", id, "jobset", js.Name)
		r.event(js, corev1.EventTypeWarning, "JobSetIdentityConflict", "JobSet %s belongs to a different incarnation of Slurm job ID %d; operator action required", js.Name, id)
		return ctrl.Result{}, nil
	}
	r.reanchor(ctx, js, job)
	wl, err := kueue.WorkloadForOwner(ctx, r.Client, r.Cfg.Namespace, js.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !kueue.IsAdmitted(wl) {
		why := commentWaitQuota
		if wl != nil {
			if q := kueue.QuotaReason(wl); q != "" {
				why = commentWaitQuota + ": " + q
			}
		}
		r.explain(ctx, job, why)
		return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
	}
	pin := r.naming().PinFeature(job.PinID())
	if !job.PinnedTo(pin) {
		if err := r.Slurm.SetJobFeatures(ctx, id, pin); err != nil {
			switch {
			case slurm.IsFeatureUnknown(err):
				r.explain(ctx, job, commentAdmitted)
				log.Debug("dynamic nodes not registered yet", "slurmJobID", id)
			case slurm.IsPinUnavailable(err):
				log.Info("pin refused while no node carries it yet, will retry", "slurmJobID", id)
			default:
				log.Warn("pinning the job failed, will retry", "slurmJobID", id, "error", err)
			}
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		// Read the pin back before trusting it (slurmrestd can acknowledge
		// an update it did not store).
		fresh, found, err := r.Slurm.GetJob(ctx, id)
		if err != nil || !found || !fresh.PinnedTo(pin) {
			log.Warn("job not pinned after the update, will retry", "slurmJobID", id, "found", found, "error", err)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		job = fresh
	}
	// Fencing first: it is the condition an operator has to act on, and a
	// replacement pod on a lost host may never register.
	if ok, why, err := r.fenced(ctx, js); err != nil {
		return ctrl.Result{}, err
	} else if !ok {
		r.explain(ctx, job, commentHeld+why)
		return ctrl.Result{RequeueAfter: r.Cfg.Recovery.PollInterval.Duration}, nil
	}
	if ok, why, err := r.verifyNodes(ctx, snap, job, js); err != nil {
		return ctrl.Result{}, err
	} else if !ok {
		r.explain(ctx, job, commentHeld+why)
		log.Info("job stays held", "slurmJobID", id, "why", why)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}
	if err := r.Slurm.ReleaseJob(ctx, id); err != nil {
		if slurm.IsPinUnavailable(err) {
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("releasing job %d: %w", id, err)
	}
	metrics.JobsReleasedTotal.Inc()
	r.noteWrite(id)
	r.explain(ctx, job, commentReleased)
	log.Info("released job onto its dynamic nodes", "slurmJobID", id)
	r.event(js, corev1.EventTypeNormal, "Released", "Slurm job %d released onto its dynamic nodes", id)
	r.Poller.Nudge()
	return ctrl.Result{}, nil
}

// slotGate keeps slurmctld's node table within slurmd.maxDynamicNodes.
func (r *JobReconciler) slotGate(ctx context.Context, job *slurm.Job) string {
	limit := r.Cfg.Slurmd.MaxDynamicNodes
	if limit <= 0 {
		return ""
	}
	var list jobsetv1alpha2.JobSetList
	if err := r.List(ctx, &list, client.InNamespace(r.Cfg.Namespace), client.MatchingLabels{translate.ManagedByLabel: translate.ManagedByValue}); err != nil {
		return "listing JobSets failed: " + err.Error()
	}
	inUse := 0
	for i := range list.Items {
		inUse += int(parallelism(&list.Items[i]))
	}
	shape, err := r.Translate.Shape(job)
	if err != nil {
		return "" // refused at translation, reported there
	}
	need := int(shape.Pods)
	if inUse+need > limit {
		return fmt.Sprintf("waiting for %d dynamic node slot(s): %d of slurmctld's %d are taken (slurmd.maxDynamicNodes)", need, inUse, limit)
	}
	return ""
}

// ensureJobSet creates the JobSet unless one of ours already exists for
// the same incarnation of the job.
func (r *JobReconciler) ensureJobSet(ctx context.Context, desired *jobsetv1alpha2.JobSet, job *slurm.Job) (bool, error) {
	err := r.Create(ctx, desired)
	if err == nil {
		return true, nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return false, err
	}
	var existing jobsetv1alpha2.JobSet
	if gerr := r.Get(ctx, client.ObjectKeyFromObject(desired), &existing); gerr != nil {
		return false, client.IgnoreNotFound(gerr)
	}
	if !jobSetIdentityMatches(&existing, job) {
		return false, fmt.Errorf("JobSet %s exists for a different incarnation of job %d", desired.Name, job.JobID)
	}
	return false, nil
}

// jobSetIdentityMatches: the JobSet's submit-time anchor is the job's, or
// the job was requeued since (Slurm resets submit_time on requeue and
// bumps restart_cnt).
func jobSetIdentityMatches(js *jobsetv1alpha2.JobSet, job *slurm.Job) bool {
	got, present := js.Annotations[translate.SubmitTimeAnnotation]
	if !present || !job.SubmitTime.Set || job.SubmitTime.Number == 0 {
		return true
	}
	want := strconv.FormatUint(job.SubmitTime.Number, 10)
	if got == want {
		return true
	}
	if job.RestartCnt > 0 {
		g, err := strconv.ParseUint(got, 10, 64)
		return err == nil && g < job.SubmitTime.Number
	}
	return false
}

func (r *JobReconciler) reanchor(ctx context.Context, js *jobsetv1alpha2.JobSet, job *slurm.Job) {
	if !job.SubmitTime.Set || job.SubmitTime.Number == 0 {
		return
	}
	if _, present := js.Annotations[translate.SubmitTimeAnnotation]; !present {
		return
	}
	_ = patchAnnotation(ctx, r.Client, js, translate.SubmitTimeAnnotation, strconv.FormatUint(job.SubmitTime.Number, 10))
}

// unregistered explains a dynamic node with no record. A cleared pod's
// slurmd normally registers within seconds; only past half the
// registration timeout (after which the node reconciler replaces the pod)
// is the wait reported as a problem, with the likely cause.
func (r *JobReconciler) unregistered(name string, views []ownership.PodView) string {
	timeout := r.Cfg.Slurmd.RegistrationTimeout.Duration
	why := fmt.Sprintf("dynamic node %s has not registered yet", name)
	for _, v := range views {
		if v.Gone || !v.Running || v.ClearedAt.IsZero() {
			continue
		}
		since := v.ClearedAt.UTC().Format(time.RFC3339)
		if waited := r.now().Sub(v.ClearedAt); waited < timeout/2 {
			why = fmt.Sprintf("waiting for dynamic node %s to register (pod %s started slurmd at %s)", name, v.Name, since)
		} else {
			why = fmt.Sprintf("dynamic node %s has not registered although pod %s has run slurmd since %s; the pod is replaced after %s (if slurmctld logs 'node table is full', raise MaxNodeCount or set slurmd.maxDynamicNodes)", name, v.Name, since, timeout)
		}
	}
	return why
}

// verifyNodes: every dynamic node of the job has a usable record owned by
// a live pod of that index at the recorded endpoint (and, with host
// resources advertised, exactly the pod's CPUs).
func (r *JobReconciler) verifyNodes(ctx context.Context, snap *Snapshot, job *slurm.Job, js *jobsetv1alpha2.JobSet) (bool, string, error) {
	pods, err := r.podsOf(ctx, job.PinID())
	if err != nil {
		return false, "", err
	}
	wantCPUs := int64(-1)
	if r.Cfg.Slurmd.AdvertiseHostResources && r.Cfg.Slurmd.NodePorts.Enabled() {
		if c := slurmdContainer(js); c != nil {
			wantCPUs = c.Resources.Requests.Cpu().Value()
		}
	}
	for idx, name := range r.naming().NodeNames(job.PinID(), parallelism(js)) {
		rec, found := snap.Node(name)
		views := viewsOf(pods, int32(idx))
		if !found {
			if job.IsArray() {
				continue // the pool runs on whichever workers exist
			}
			return false, r.unregistered(name, views), nil
		}
		if bad := unusableNodeState(rec.State); bad != "" {
			return false, fmt.Sprintf("dynamic node %s is %s (%s)", name, bad, rec.Reason), nil
		}
		if wantCPUs >= 0 && rec.EffectiveCPUs != wantCPUs {
			return false, fmt.Sprintf("dynamic node %s registered %d effective CPUs but its pod requests %d (CPU Manager static policy and TaskPluginParam=SlurmdSpecOverride are required)", name, rec.EffectiveCPUs, wantCPUs), nil
		}
		if ok, why := ownership.Served(rec, views); !ok {
			return false, fmt.Sprintf("dynamic node %s is not served by a live pod of the job: %s", name, why), nil
		}
	}
	return true, "", nil
}

// fenced: no host of a vanished pod of this job is still waiting to be
// fenced. Hosts the fencer now clears are removed from the annotation.
func (r *JobReconciler) fenced(ctx context.Context, js *jobsetv1alpha2.JobSet) (bool, string, error) {
	pending := js.Annotations[FencePendingAnnotation]
	if pending == "" {
		return true, "", nil
	}
	for _, host := range splitCSV(pending) {
		v, err := r.Fencer.Confirm(ctx, fence.Incident{JobID: idOf(js), Host: host, Owner: js})
		if err != nil {
			return false, "", err
		}
		if !v.Safe {
			metrics.FencingWaitsTotal.Inc()
			return false, "fencing required before the job may run again: " + v.Reason, nil
		}
		if err := patchAnnotation(ctx, r.Client, js, FencePendingAnnotation, removeFromCSV(js.Annotations[FencePendingAnnotation], host)); err != nil {
			return false, "", err
		}
		r.event(js, corev1.EventTypeNormal, "FencingCleared", "host %s: %s", host, v.Reason)
	}
	return true, "", nil
}

// requeued: a pending, pinned, unheld job slurmctld will re-allocate onto
// its records after requeue_delay. Held again unless every record is a
// live pod's (and the JobSet exists to be verified against).
func (r *JobReconciler) requeued(ctx context.Context, snap *Snapshot, job *slurm.Job, js *jobsetv1alpha2.JobSet, log Logger) (ctrl.Result, error) {
	if js == nil {
		if r.Cfg.Recovery.CancelOnJobSetLoss {
			return r.cancelOrphan(ctx, job, log)
		}
		return r.rehold(ctx, job, "requeued, its JobSet is gone; recreating it", "jobset-gone", log)
	}
	if ok, why, err := r.fenced(ctx, js); err != nil {
		return ctrl.Result{}, err
	} else if !ok {
		return r.rehold(ctx, job, why, "fencing", log)
	}
	if ok, why, err := r.verifyNodes(ctx, snap, job, js); err != nil {
		return ctrl.Result{}, err
	} else if !ok {
		return r.rehold(ctx, job, "requeued, waiting: "+why, "requeue", log)
	}
	return ctrl.Result{}, nil
}

func (r *JobReconciler) rehold(ctx context.Context, job *slurm.Job, why, reason string, log Logger) (ctrl.Result, error) {
	if err := r.Slurm.HoldJob(ctx, job.JobID); err != nil {
		if slurm.IsPinUnavailable(err) {
			log.Info("hold refused while no node carries the pin, will retry", "slurmJobID", job.JobID)
			return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("holding job %d: %w", job.JobID, err)
	}
	metrics.JobsReheldTotal.WithLabelValues(reason).Inc()
	r.noteWrite(job.JobID)
	r.explain(ctx, job, commentHeld+why)
	log.Warn("held the job again", "slurmJobID", job.JobID, "why", why)
	r.Poller.Nudge()
	return ctrl.Result{}, nil
}

func (r *JobReconciler) cancelOrphan(ctx context.Context, job *slurm.Job, log Logger) (ctrl.Result, error) {
	if err := r.Slurm.CancelJob(ctx, job.JobID); err != nil {
		return ctrl.Result{}, err
	}
	metrics.JobsCancelledTotal.WithLabelValues("orphan").Inc()
	log.Warn("cancelled a job whose JobSet is gone (recovery.cancelOnJobSetLoss)", "slurmJobID", job.JobID)
	return ctrl.Result{}, nil
}

// failForDeadJobSet: Kubernetes lost the job's pods for good; Slurm is
// told so the job ends with the reason instead of waiting on nodes that
// will never come.
func (r *JobReconciler) failForDeadJobSet(ctx context.Context, job *slurm.Job, js *jobsetv1alpha2.JobSet, log Logger) (ctrl.Result, error) {
	reason := "JobSet completed while the job was still running"
	if c := apimeta.FindStatusCondition(js.Status.Conditions, string(jobsetv1alpha2.JobSetFailed)); c != nil && c.Message != "" {
		reason = c.Message
	}
	r.explain(ctx, job, commentJobSetDead+reason)
	if err := r.Slurm.CancelJob(ctx, job.JobID); err != nil {
		return ctrl.Result{}, err
	}
	metrics.JobsCancelledTotal.WithLabelValues("jobset-failed").Inc()
	log.Warn("cancelled the job: its JobSet ended first", "slurmJobID", job.JobID, "reason", reason)
	r.event(js, corev1.EventTypeWarning, "JobSetFailed", "Slurm job %d cancelled: %s", job.JobID, reason)
	return ctrl.Result{}, nil
}

// cleanup after a terminal (or vanished) job: release its node records,
// then delete the JobSet. A record slurmctld still holds a job on is
// retried (DOWN only when no live pod serves it, which cannot be the
// case for a terminal job's own pods — they are simply retried).
func (r *JobReconciler) cleanup(ctx context.Context, snap *Snapshot, id uint64, js *jobsetv1alpha2.JobSet, log Logger) (ctrl.Result, error) {
	if jobSetIs(js, jobsetv1alpha2.JobSetFailed) && r.Cfg.Recovery.KeepFailedJobSetsFor.Duration > 0 {
		if c := apimeta.FindStatusCondition(js.Status.Conditions, string(jobsetv1alpha2.JobSetFailed)); c != nil {
			if keep := c.LastTransitionTime.Add(r.Cfg.Recovery.KeepFailedJobSetsFor.Duration); r.now().Before(keep) {
				return ctrl.Result{RequeueAfter: time.Until(keep)}, nil
			}
		}
	}
	pods, err := r.podsOf(ctx, id)
	if err != nil {
		return ctrl.Result{}, err
	}
	clean := true
	for idx, name := range r.naming().NodeNames(id, parallelism(js)) {
		rec, found := snap.Node(name)
		if !found {
			continue
		}
		st, err := releaseRecord(ctx, r.Slurm, r.naming(), rec, viewsOf(pods, int32(idx)), "slurmkube: job finished", log)
		if err != nil {
			log.Info("releasing a finished job's node record failed, will retry", "node", name, "error", err)
			clean = false
			continue
		}
		if st == recordDowned || st == recordBusy {
			clean = false
		}
	}
	if !clean {
		r.Poller.Nudge()
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err := client.IgnoreNotFound(r.Delete(ctx, js)); err != nil {
		return ctrl.Result{}, err
	}
	metrics.JobSetsDeletedTotal.Inc()
	r.forgetWrite(id)
	log.Info("deleted the JobSet of a finished job", "slurmJobID", id, "jobset", js.Name)
	return ctrl.Result{}, nil
}

func (r *JobReconciler) podsOf(ctx context.Context, id uint64) ([]corev1.Pod, error) {
	var list corev1.PodList
	if err := r.List(ctx, &list, client.InNamespace(r.Cfg.Namespace),
		client.MatchingLabels{translate.ManagedByLabel: translate.ManagedByValue, translate.SlurmJobIDLabel: jobKey(id)}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func viewsOf(pods []corev1.Pod, index int32) []ownership.PodView {
	var out []ownership.PodView
	for i := range pods {
		if podIndex(&pods[i]) == index {
			out = append(out, ownership.FromPod(&pods[i], podKeys))
		}
	}
	return out
}

// explain writes the job's comment when it changed.
func (r *JobReconciler) explain(ctx context.Context, job *slurm.Job, comment string) {
	if job.Comment == comment {
		return
	}
	if err := r.Slurm.SetJobComment(ctx, job.JobID, comment); err != nil && r.Log != nil {
		r.Log.Debug("writing the job's comment failed", "slurmJobID", job.JobID, "error", err)
		return
	}
	job.Comment = comment
}

// event records on the JobSet; *JobSet rather than client.Object so that
// a nil one is caught here (see NodeRecordReconciler.event).
func (r *JobReconciler) event(js *jobsetv1alpha2.JobSet, kind, reason, format string, args ...any) {
	if r.Recorder == nil || js == nil {
		return
	}
	r.Recorder.Eventf(js, kind, reason, format, args...)
}

// parallelism is the JobSet's slurmd pod count; Kubernetes defaults an
// unset Job parallelism to 1.
func parallelism(js *jobsetv1alpha2.JobSet) int32 {
	var p *int32
	if js != nil && len(js.Spec.ReplicatedJobs) > 0 {
		p = js.Spec.ReplicatedJobs[0].Template.Spec.Parallelism
	}
	if p == nil {
		return 1
	}
	return *p
}

func slurmdContainer(js *jobsetv1alpha2.JobSet) *corev1.Container {
	if js == nil || len(js.Spec.ReplicatedJobs) == 0 {
		return nil
	}
	cs := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers
	if len(cs) == 0 {
		return nil
	}
	return &cs[0]
}

// jobSetIs reports the JobSet condition as True; false for a nil JobSet.
func jobSetIs(js *jobsetv1alpha2.JobSet, cond jobsetv1alpha2.JobSetConditionType) bool {
	return js != nil && apimeta.IsStatusConditionTrue(js.Status.Conditions, string(cond))
}

// SetupWithManager registers the reconciler. One worker: JobSet creation
// order follows Slurm's job order and the slot gate counts JobSets.
func (r *JobReconciler) SetupWithManager(mgr ctrl.Manager, events chan eventOf) error {
	if r.Translate == nil {
		r.Translate = translate.New(r.Cfg)
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("slurm-job").
		WatchesRawSource(channelSource(events)).
		Watches(&jobsetv1alpha2.JobSet{}, jobSetToJob(), ownJobSetsOnly(r.Cfg.Instance)).
		Watches(&corev1.Pod{}, podToJob(), ownPodsOnly()).
		Watches(workloadObject(), workloadToJob(r.Client, r.Cfg.Namespace)).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

var _ = unstructured.Unstructured{}
