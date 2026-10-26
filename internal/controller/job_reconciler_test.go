// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/metrics"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// The P0 vertical slice as the controller sees it: a held job gets a
// JobSet, waits for Kueue, is pinned once its pod registered a record it
// owns, is released, and is cleaned up when it ends.
func TestHeldJobRunsOnItsOwnNodeAndIsCleanedUp(t *testing.T) {
	fs := newFakeSlurm(heldJob(10))
	h := newHarness(t, fs)
	n0 := nodeName(10, 0)

	// 1. claim → JobSet.
	h.job(10)
	js := h.jobSet(10)
	if js == nil {
		t.Fatal("JobSet not created")
	}
	if fs.job(10).Comment != commentClaimed {
		t.Errorf("comment = %q, want claimed", fs.job(10).Comment)
	}
	if got := js.Labels[translate.ManagedByLabel]; got != translate.ManagedByValue {
		t.Errorf("managed-by = %q", got)
	}

	// 2. no admission yet → waits, not pinned, not released.
	h.job(10)
	if len(fs.released) != 0 || fs.features[10] != "" || !strings.HasPrefix(fs.job(10).Comment, commentWaitQuota) {
		t.Fatalf("before admission: released=%v features=%q comment=%q", fs.released, fs.features[10], fs.job(10).Comment)
	}

	// 3. admitted, the pod appears; the node reconciler clears it (no
	// record under the name), slurmd registers with the pod's UID.
	h.create(admittedWorkload(js.Name))
	pod := slurmdPod("w0", 10, 0, "10.0.0.2", 63000, corev1.PodRunning)
	h.create(pod)
	h.job(10)
	if len(fs.released) != 0 {
		t.Fatal("released before the node registered")
	}
	h.node(n0)
	if h.pod("w0").Annotations[translate.NodeRecordClearAnnotation] == "" {
		t.Fatal("pod must be cleared: no record under its name")
	}
	fs.register(n0, pod, "10.0.0.2", 63000)

	// 4. pinned and released.
	h.job(10)
	if fs.features[10] != testNaming.PinFeature(10) {
		t.Errorf("pin = %q", fs.features[10])
	}
	if !contains(fs.released, uint64(10)) {
		t.Fatalf("job not released: comment=%q", fs.job(10).Comment)
	}
	// A reconcile an event triggers before the next poll sees the same
	// snapshot; it must not release (or pin) again.
	for i := 0; i < 3; i++ {
		if _, err := h.jobs.Reconcile(h.ctx(), ctrlRequest(jobKey(10))); err != nil {
			t.Fatal(err)
		}
	}
	if len(fs.released) != 1 {
		t.Errorf("released %d times before the next poll, want 1", len(fs.released))
	}

	// 5. the job ends: record and JobSet go.
	fs.set(10, func(j *slurm.Job) { j.JobState = []string{"COMPLETED"}; j.Hold = false })
	h.job(10)
	if !contains(fs.deleted, n0) {
		t.Errorf("record not released: %v", fs.deleted)
	}
	if h.jobSet(10) != nil {
		t.Error("JobSet not deleted after the job ended")
	}
}

// Invariant: the job is released only onto a record its live pod owns —
// not onto a record a gone pod left, not onto a record at another port.
func TestReleaseWaitsForARecordTheLivePodOwns(t *testing.T) {
	fs := newFakeSlurm(heldJob(11))
	h := newHarness(t, fs)
	h.job(11)
	js := h.jobSet(11)
	h.create(admittedWorkload(js.Name))
	old := slurmdPod("w0-old", 11, 0, "10.0.0.2", 63001, corev1.PodFailed)
	cur := slurmdPod("w0-new", 11, 0, "10.0.0.2", 63000, corev1.PodRunning)
	cur.Annotations[translate.NodeRecordClearAnnotation] = "2026-10-24T00:00:00Z"
	h.create(old, cur)
	n0 := nodeName(11, 0)

	fs.register(n0, old, "10.0.0.2", 63001) // the dead pod's record
	h.job(11)
	if len(fs.released) != 0 || !strings.Contains(fs.job(11).Comment, "gone") {
		t.Fatalf("released onto a gone pod's record: released=%v comment=%q", fs.released, fs.job(11).Comment)
	}
	fs.register(n0, cur, "10.0.0.2", 63001) // our UID, but the stale port
	h.job(11)
	if len(fs.released) != 0 || !strings.Contains(fs.job(11).Comment, "listens on") {
		t.Fatalf("released onto a stale endpoint: released=%v comment=%q", fs.released, fs.job(11).Comment)
	}
	fs.register(n0, cur, "10.0.0.2", 63000)
	h.job(11)
	if !contains(fs.released, uint64(11)) {
		t.Fatalf("not released onto the live pod's record: comment=%q", fs.job(11).Comment)
	}
}

// A cleared pod whose slurmd has not registered yet is an ordinary wait
// until half the registration timeout; only then does the comment report
// it as a problem, with the likely cause. Registration releases the job.
func TestRegistrationWaitIsNotReportedAsAProblemAtFirst(t *testing.T) {
	fs := newFakeSlurm(heldJob(42))
	h := newHarness(t, fs)
	h.job(42)
	js := h.jobSet(42)
	h.create(admittedWorkload(js.Name))
	cur := slurmdPod("w0", 42, 0, "10.0.0.2", 63000, corev1.PodRunning)
	cur.Annotations[translate.NodeRecordClearAnnotation] = h.now.UTC().Format(time.RFC3339)
	h.create(cur)
	n0 := nodeName(42, 0)
	timeout := h.cfg.Slurmd.RegistrationTimeout.Duration

	h.now = h.now.Add(5 * time.Second)
	h.job(42)
	if c := fs.job(42).Comment; !strings.Contains(c, "waiting for dynamic node "+n0+" to register") || strings.Contains(c, "MaxNodeCount") {
		t.Fatalf("a fresh registration wait reads as a problem: %q", c)
	}

	h.now = h.now.Add(timeout / 2)
	h.job(42)
	if c := fs.job(42).Comment; !strings.Contains(c, "has not registered although pod w0") || !strings.Contains(c, "MaxNodeCount") {
		t.Fatalf("a slow registration is not reported with its likely cause: %q", c)
	}

	fs.register(n0, cur, "10.0.0.2", 63000)
	h.job(42)
	if !contains(fs.released, uint64(42)) || fs.job(42).Comment != commentReleased {
		t.Fatalf("not released once registered: released=%v comment=%q", fs.released, fs.job(42).Comment)
	}
}

// Another manager's job is never claimed: a comment that is neither the
// hold comment nor SlurmKube's, or a JobSet with the other manager's
// labels, leaves the job alone.
func TestJobsOfAnotherManagerAreNotClaimed(t *testing.T) {
	other := heldJob(12)
	other.Comment = "other: held for admission"
	fs := newFakeSlurm(other)
	h := newHarness(t, fs)
	h.job(12)
	if h.jobSet(12) != nil || len(fs.comments) != 0 {
		t.Error("a job with another manager's comment must not be touched")
	}

	fs2 := newFakeSlurm(heldJob(13))
	foreign := &jobsetv1alpha2JobSetFixture{name: "other-13", labels: map[string]string{"other.example.com/managed-by": "other", "other.example.com/slurm-job-id": "13"}}
	h2 := newHarness(t, fs2, foreign.object())
	h2.cfg.Claim.OtherManagers = []config.OtherManager{{Name: "other", Labels: map[string]string{"other.example.com/managed-by": "other"}, JobIDLabel: "other.example.com/slurm-job-id"}}
	h2.job(13)
	if h2.jobSet(13) != nil || len(fs2.comments) != 0 {
		t.Error("a job with another manager's JobSet must not be claimed")
	}
}

// A running job's Jobs carry a deadline derived from Slurm's end_time:
// `scontrol update TimeLimit` (extension, shortening, UNLIMITED) moves it,
// an unchanged limit writes nothing, and a Job the JobSet recreates is
// corrected too. Array pools never get one.
func TestDeadlineFollowsSlurmsTimeLimit(t *testing.T) {
	job := runningJob(19)
	kubeStart := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	slurmStart := kubeStart.Add(30 * time.Second)
	job.TimeLimit = slurm.OptionalUint64{Set: true, Number: 10}
	job.StartTime = slurm.OptionalUint64{Set: true, Number: uint64(slurmStart.Unix())}
	job.EndTime = slurm.OptionalUint64{Set: true, Number: uint64(slurmStart.Add(10 * time.Minute).Unix())}
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	initial := int64(600 + 300)
	child := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: js.Name + "-workers-0", Namespace: "slurm-jobs",
		Labels: map[string]string{jobSetNameLabel: js.Name, translate.ManagedByLabel: translate.ManagedByValue}},
		Spec: batchv1.JobSpec{ActiveDeadlineSeconds: &initial}}
	unstarted := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: js.Name + "-workers-1", Namespace: "slurm-jobs",
		Labels: map[string]string{jobSetNameLabel: js.Name}}, Spec: batchv1.JobSpec{ActiveDeadlineSeconds: &initial}}
	h := newHarness(t, fs, js, child, unstarted)
	child.Status.StartTime = &metav1.Time{Time: kubeStart}
	if err := h.kube.Status().Update(h.ctx(), child); err != nil {
		t.Fatal(err)
	}
	deadline := func(name string) string {
		var j batchv1.Job
		if err := h.kube.Get(h.ctx(), client.ObjectKey{Namespace: "slurm-jobs", Name: name}, &j); err != nil {
			t.Fatal(err)
		}
		return deadlineString(j.Spec.ActiveDeadlineSeconds) + "@" + j.ResourceVersion
	}
	// Running with the original limit: end + 5 min from the Job's start.
	h.job(19)
	if got := deadline(child.Name); !strings.HasPrefix(got, "930s@") {
		t.Fatalf("deadline after start = %s, want 930s", got)
	}
	if got := deadline(unstarted.Name); !strings.HasPrefix(got, "900s@") {
		t.Errorf("a Job that has not started keeps the template's value, got %s", got)
	}
	// Nothing changes: no write (resourceVersion stays).
	before := deadline(child.Name)
	h.job(19)
	h.job(19)
	if after := deadline(child.Name); after != before {
		t.Errorf("unchanged limit rewrote the Job: %s -> %s", before, after)
	}
	// Extended to 240 min: end moves by 230 min, buffer becomes 24 min.
	fs.set(19, func(j *slurm.Job) {
		j.TimeLimit.Number = 240
		j.EndTime.Number = uint64(slurmStart.Add(240 * time.Minute).Unix())
	})
	h.job(19)
	if got := deadline(child.Name); !strings.HasPrefix(got, "15870s@") {
		t.Errorf("after extension = %s, want 15870s (30 + 14400 + 1440)", got)
	}
	// Shortened to 2 min.
	fs.set(19, func(j *slurm.Job) {
		j.TimeLimit.Number = 2
		j.EndTime.Number = uint64(slurmStart.Add(2 * time.Minute).Unix())
	})
	h.job(19)
	if got := deadline(child.Name); !strings.HasPrefix(got, "450s@") {
		t.Errorf("after shortening = %s, want 450s", got)
	}
	// UNLIMITED: the deadline is removed.
	fs.set(19, func(j *slurm.Job) { j.TimeLimit = slurm.OptionalUint64{Set: true, Infinite: true} })
	h.job(19)
	if got := deadline(child.Name); !strings.HasPrefix(got, "none@") {
		t.Errorf("after UNLIMITED = %s, want none", got)
	}
	// Back to a limit: set again (a restarted controller converges the same way).
	fs.set(19, func(j *slurm.Job) {
		j.TimeLimit = slurm.OptionalUint64{Set: true, Number: 10}
		j.EndTime.Number = uint64(slurmStart.Add(10 * time.Minute).Unix())
	})
	h.job(19)
	if got := deadline(child.Name); !strings.HasPrefix(got, "930s@") {
		t.Errorf("after restoring the limit = %s, want 930s", got)
	}
	// A Slurm outage keeps the previous snapshot: still no write.
	before = deadline(child.Name)
	fs.fail("ListJobs", errBoom)
	h.job(19)
	fs.fail("ListJobs", nil)
	if after := deadline(child.Name); after != before {
		t.Errorf("slurmrestd outage rewrote the Job: %s -> %s", before, after)
	}
	// A Kubernetes API failure surfaces as a reconcile error (retried with
	// backoff) and leaves the Job as it was; the next success converges.
	fs.set(19, func(j *slurm.Job) {
		j.TimeLimit.Number = 20
		j.EndTime.Number = uint64(slurmStart.Add(20 * time.Minute).Unix())
	})
	h.poll()
	h.jobs.Client = &failingPatch{Client: h.kube}
	if _, err := h.jobs.Reconcile(h.ctx(), ctrl.Request{NamespacedName: key(jobKey(19))}); err == nil {
		t.Error("a failed Job patch must be returned for retry")
	}
	if after := deadline(child.Name); after != before {
		t.Errorf("failed patch changed the Job: %s -> %s", before, after)
	}
	h.jobs.Client = h.kube
	h.job(19)
	if got := deadline(child.Name); !strings.HasPrefix(got, "1530s@") {
		t.Errorf("after the API recovered = %s, want 1530s (30 + 1200 + 300)", got)
	}

	// Array pool: the task's limit never becomes a deadline.
	root := runningJob(20)
	root.ArrayJobID = slurm.OptionalUint64{Set: true, Number: 20}
	root.ArrayTaskString = "0-3"
	root.TimeLimit = slurm.OptionalUint64{Set: true, Number: 10}
	root.StartTime, root.EndTime = job.StartTime, slurm.OptionalUint64{Set: true, Number: uint64(slurmStart.Add(10 * time.Minute).Unix())}
	fs2 := newFakeSlurm(root)
	pool, err := translate.New(testConfig()).JobSet(&root)
	if err != nil {
		t.Fatal(err)
	}
	if pool.Spec.ReplicatedJobs[0].Template.Spec.ActiveDeadlineSeconds != nil {
		t.Error("array pool JobSet must have no deadline")
	}
	poolJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: pool.Name + "-workers-0", Namespace: "slurm-jobs",
		Labels: map[string]string{jobSetNameLabel: pool.Name}}}
	h2 := newHarness(t, fs2, pool, poolJob)
	poolJob.Status.StartTime = &metav1.Time{Time: kubeStart}
	_ = h2.kube.Status().Update(h2.ctx(), poolJob)
	h2.job(20)
	var got batchv1.Job
	_ = h2.kube.Get(h2.ctx(), client.ObjectKeyFromObject(poolJob), &got)
	if got.Spec.ActiveDeadlineSeconds != nil {
		t.Errorf("array pool Job got a deadline: %d", *got.Spec.ActiveDeadlineSeconds)
	}
}

// An array's worker pool outlives the tasks that ended while another task
// may still run. Reproduced: a task requeued with `scontrol requeue`
// waited out requeue_delay while the others completed; the ended tasks'
// reconciles found the pool's record idle, deleted the pool, and the
// requeued task stayed pending with no node to run on.
func TestArrayPoolOutlivesEndedTasksWhileARequeuedTaskWaits(t *testing.T) {
	task := func(id uint64, idx uint64, state string) slurm.Job {
		j := runningJob(id)
		j.ArrayJobID = slurm.OptionalUint64{Set: true, Number: 60}
		j.ArrayTaskID = slurm.OptionalUint64{Set: true, Number: idx}
		j.Features = testNaming.PinFeature(60)
		j.JobState = []string{state}
		return j
	}
	root := task(60, 3, "COMPLETED")
	requeued := task(62, 1, "PENDING")
	requeued.RestartCnt = 1
	requeued.StateReason = "BeginTime"
	fs := newFakeSlurm(root, task(61, 0, "COMPLETED"), requeued)
	poolRoot := root
	poolRoot.JobState, poolRoot.ArrayTaskString = []string{"PENDING"}, "0-3%2"
	pool, err := translate.New(testConfig()).JobSet(&poolRoot)
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, fs, pool)
	n0 := nodeName(60, 0)
	worker := slurmdPod("w0", 60, 0, "10.0.0.2", 63000, corev1.PodRunning)
	worker.Annotations[translate.NodeRecordClearAnnotation] = "t"
	h.create(worker)
	fs.register(n0, worker, "10.0.0.2", 63000) // idle: no task on it now

	for _, id := range []uint64{60, 61, 62} {
		h.job(id)
	}
	if h.jobSet(60) == nil || contains(fs.deleted, n0) {
		t.Fatalf("pool torn down while task 1 waits to run again: jobset=%v deleted=%v", h.jobSet(60) != nil, fs.deleted)
	}

	fs.set(62, func(j *slurm.Job) { j.JobState = []string{"COMPLETED"} })
	h.job(62)
	if !contains(fs.deleted, n0) || h.jobSet(60) != nil {
		t.Fatalf("pool not cleaned up once every task ended: jobset=%v deleted=%v", h.jobSet(60) != nil, fs.deleted)
	}
}

// A refused job's lifecycle: the comment marks it, later reconciles and a
// restarted controller leave it alone, a release by hand is held again and
// refused again, fixing the option and releasing lets it through, and the
// gauge counts the refused jobs of each poll.
func TestRefusedJobLifecycle(t *testing.T) {
	j := heldJob(31)
	j.HetJobID, j.HetJobIDSet = slurm.OptionalUint64{Set: true, Number: 31}, "31-32"
	fs := newFakeSlurm(j)
	h := newHarness(t, fs)
	h.job(31)
	if c := fs.job(31).Comment; !strings.HasPrefix(c, commentRefused) {
		t.Fatalf("comment = %q", c)
	}
	h.poll() // the gauge is set from each poll's snapshot
	if got := testutil.ToFloat64(metrics.JobsRefused); got != 1 {
		t.Errorf("slurmkube_jobs_refused = %v, want 1", got)
	}
	// Steady state and a restarted controller: same comment, no JobSet, no cancel.
	before := fs.job(31).Comment
	h.job(31)
	pol := &Poller{Slurm: fs, Interval: time.Hour, Naming: h.pol.Naming, Claims: h.pol.Claims}
	h.pol, h.jobs.Poller = pol, pol
	h.job(31)
	if c := fs.job(31).Comment; c != before || h.jobSet(31) != nil || len(fs.cancels) != 0 {
		t.Errorf("refused job touched again: comment %q->%q jobset=%v cancels=%v", before, c, h.jobSet(31) != nil, fs.cancels)
	}
	// scontrol release by the user: held again, then refused again.
	fs.set(31, func(j *slurm.Job) { j.Hold = false })
	h.job(31)
	if c := fs.job(31).Comment; !strings.HasPrefix(c, commentHeld) {
		t.Errorf("after release: comment = %q, want held again", c)
	}
	h.job(31)
	if c := fs.job(31).Comment; !strings.HasPrefix(c, commentRefused) || h.jobSet(31) != nil {
		t.Errorf("after re-hold: comment = %q jobset=%v", c, h.jobSet(31) != nil)
	}
	// The option is fixed (scontrol update) and the job released: it runs.
	fs.set(31, func(j *slurm.Job) {
		j.HetJobID, j.HetJobIDSet = slurm.OptionalUint64{Set: true, Number: 0}, ""
		j.Hold = false
	})
	h.job(31)
	h.job(31)
	if h.jobSet(31) == nil {
		t.Errorf("fixed and released job got no JobSet; comment %q", fs.job(31).Comment)
	}
	// Cancelled while refused: nothing to clean up, the gauge drops.
	k := heldJob(33)
	k.HetJobID, k.HetJobIDSet = slurm.OptionalUint64{Set: true, Number: 33}, "33-34"
	fs.add(k)
	h.job(33)
	fs.set(33, func(j *slurm.Job) { j.JobState = []string{"CANCELLED"}; j.Hold = false })
	h.job(33)
	h.poll()
	if got := testutil.ToFloat64(metrics.JobsRefused); got != 0 {
		t.Errorf("slurmkube_jobs_refused after cancel = %v, want 0", got)
	}
}

// failingPatch rejects every Patch, as an unreachable API server would.
type failingPatch struct{ client.Client }

func (failingPatch) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return errBoom
}

// The deadline sync under the failure scenarios that matter: a Kueue
// suspend clears the Job's start (nothing is written until it is resumed
// with a new start), several quick TimeLimit changes end at the last one
// with one write each, a controller restarted between a change and its
// sync converges without an extra write, and the Kubernetes deadline is
// never earlier than Slurm's end plus the buffer.
func TestDeadlineSyncUnderSuspendRapidUpdatesAndRestart(t *testing.T) {
	job := runningJob(41)
	kubeStart := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	slurmStart := kubeStart.Add(20 * time.Second)
	job.TimeLimit = slurm.OptionalUint64{Set: true, Number: 10}
	job.StartTime = slurm.OptionalUint64{Set: true, Number: uint64(slurmStart.Unix())}
	job.EndTime = slurm.OptionalUint64{Set: true, Number: uint64(slurmStart.Add(10 * time.Minute).Unix())}
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	initial := int64(900)
	child := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: js.Name + "-workers-0", Namespace: "slurm-jobs",
		Labels: map[string]string{jobSetNameLabel: js.Name}}, Spec: batchv1.JobSpec{ActiveDeadlineSeconds: &initial}}
	h := newHarness(t, fs, js, child)
	get := func() (*int64, string) {
		var j batchv1.Job
		if err := h.kube.Get(h.ctx(), client.ObjectKeyFromObject(child), &j); err != nil {
			t.Fatal(err)
		}
		return j.Spec.ActiveDeadlineSeconds, j.ResourceVersion
	}
	setStart := func(at *time.Time) {
		var j batchv1.Job
		_ = h.kube.Get(h.ctx(), client.ObjectKeyFromObject(child), &j)
		j.Status.StartTime = nil
		if at != nil {
			j.Status.StartTime = &metav1.Time{Time: *at}
		}
		if err := h.kube.Status().Update(h.ctx(), &j); err != nil {
			t.Fatal(err)
		}
	}
	// Invariant: Kubernetes never ends the job before Slurm's end + buffer.
	check := func(when string, startedAt time.Time) {
		t.Helper()
		got, _ := get()
		j := fs.job(41)
		if j.TimeLimitSeconds() <= 0 {
			if got != nil {
				t.Errorf("%s: UNLIMITED job carries a deadline %d", when, *got)
			}
			return
		}
		least := int64(j.RunEnd().Add(translate.DeadlineBuffer(j.TimeLimitSeconds())).Sub(startedAt) / time.Second)
		if got == nil || *got < least {
			t.Errorf("%s: deadline %v is earlier than Slurm's end + buffer (%ds from the Job's start)", when, got, least)
		}
	}
	// Kueue suspended the Job (startTime cleared): the sync leaves it alone.
	setStart(nil)
	_, rv0 := get()
	h.job(41)
	if _, rv := get(); rv != rv0 {
		t.Error("a suspended Job (no startTime) was written")
	}
	// Resumed with a new start (the requeued Slurm job has new times too).
	resumed := kubeStart.Add(time.Hour)
	setStart(&resumed)
	fs.set(41, func(j *slurm.Job) {
		j.RestartCnt = 1
		j.StartTime.Number = uint64(resumed.Add(25 * time.Second).Unix())
		j.EndTime.Number = uint64(resumed.Add(25*time.Second + 10*time.Minute).Unix())
	})
	h.job(41)
	if got, _ := get(); got == nil || *got != 25+600+300 {
		t.Errorf("after resume: deadline %v, want 925 (from the new Job start)", got)
	}
	check("after resume", resumed)
	// Several TimeLimit changes in a row: one write each, the last wins.
	_, rvBefore := get()
	writes := 0
	for _, limit := range []uint64{30, 5, 60} {
		fs.set(41, func(j *slurm.Job) {
			j.TimeLimit.Number = limit
			j.EndTime.Number = j.StartTime.Number + limit*60
		})
		h.job(41)
		if _, rv := get(); rv != rvBefore {
			writes++
			rvBefore = rv
		}
		check("limit "+strconv.FormatUint(limit, 10), resumed)
	}
	got, _ := get()
	if writes != 3 || got == nil || *got != 25+3600+360 {
		t.Errorf("rapid updates: writes=%d deadline=%v, want 3 and 3985", writes, got)
	}
	// Restart between a change and its sync: the new controller converges
	// and a second restart writes nothing more.
	fs.set(41, func(j *slurm.Job) {
		j.TimeLimit.Number = 120
		j.EndTime.Number = j.StartTime.Number + 120*60
	})
	restart := func() {
		pol := &Poller{Slurm: fs, Interval: time.Hour, Naming: h.pol.Naming, Claims: h.pol.Claims}
		h.pol, h.jobs.Poller = pol, pol
	}
	restart()
	h.job(41)
	got, rvAfter := get()
	if got == nil || *got != 25+7200+720 {
		t.Errorf("after restart: deadline %v, want 7945", got)
	}
	restart()
	h.job(41)
	if _, rv := get(); rv != rvAfter {
		t.Error("a restarted controller rewrote an already correct deadline")
	}
	check("after restart", resumed)
	// Extension right before expiry: Slurm's end is a minute away, the old
	// Kubernetes deadline five minutes further; the extension lands first.
	fs.set(41, func(j *slurm.Job) {
		j.TimeLimit.Number = 2
		j.EndTime.Number = j.StartTime.Number + 120
	})
	h.job(41)
	nearEnd, _ := get()
	fs.set(41, func(j *slurm.Job) {
		j.TimeLimit.Number = 240
		j.EndTime.Number = j.StartTime.Number + 240*60
	})
	h.job(41)
	extended, _ := get()
	if nearEnd == nil || extended == nil || *extended <= *nearEnd || *extended != 25+14400+1440 {
		t.Errorf("extension near expiry: %v -> %v, want 15865", nearEnd, extended)
	}
	check("extended near expiry", resumed)
}

// A heterogeneous job's components are refused like any unsupported
// option: held with the reason, no JobSet; with recovery.cancelRefusedJobs
// they are cancelled instead. Later reconciles do nothing.
func TestHeterogeneousJobIsRefusedAndOptionallyCancelled(t *testing.T) {
	leader, member := heldJob(21), heldJob(22)
	for _, j := range []*slurm.Job{&leader, &member} {
		j.HetJobID = slurm.OptionalUint64{Set: true, Number: 21}
		j.HetJobIDSet = "21-22"
	}
	member.HetJobOffset = slurm.OptionalUint64{Set: true, Number: 1}
	fs := newFakeSlurm(leader, member)
	h := newHarness(t, fs)
	h.job(21)
	h.job(22)
	for _, id := range []uint64{21, 22} {
		if h.jobSet(id) != nil || len(fs.cancels) != 0 {
			t.Fatalf("job %d: JobSet=%v cancels=%v", id, h.jobSet(id) != nil, fs.cancels)
		}
		if c := fs.job(id).Comment; !strings.HasPrefix(c, commentRefused) || !strings.Contains(c, "heterogeneous") {
			t.Errorf("job %d comment = %q", id, c)
		}
	}
	n := len(fs.comments)
	h.job(21)
	h.job(21)
	if len(fs.comments) != n || h.jobSet(21) != nil {
		t.Error("a refused job must be left alone on later reconciles")
	}

	fs2 := newFakeSlurm(leader, member)
	h2 := newHarness(t, fs2)
	h2.cfg.Recovery.CancelRefusedJobs = true
	h2.job(21)
	h2.job(22)
	if !contains(fs2.cancels, uint64(21)) || !contains(fs2.cancels, uint64(22)) {
		t.Errorf("cancelRefusedJobs: cancels = %v", fs2.cancels)
	}
	if !strings.Contains(fs2.job(21).Comment, "heterogeneous") {
		t.Errorf("the reason is written before the cancel: %q", fs2.job(21).Comment)
	}
}

// Options the nodes cannot satisfy are refused with the reason in the
// job's comment; no JobSet is created.
func TestUnsupportedOptionIsRefusedWithAReason(t *testing.T) {
	job := heldJob(14)
	job.TresPerNode = "gres/gpu:h100:1"
	fs := newFakeSlurm(job)
	h := newHarness(t, fs)
	h.job(14)
	if h.jobSet(14) != nil {
		t.Error("JobSet created for a refused job")
	}
	if c := fs.job(14).Comment; !strings.HasPrefix(c, commentRefused) || !strings.Contains(c, "untyped") {
		t.Errorf("comment = %q", c)
	}
}

// A requeued job (pending, pinned, not held) whose record is a gone pod's
// is held again; once the replacement registered it is released again.
// Slurm's submit-time reset on requeue does not make the JobSet foreign.
func TestRequeuedJobIsHeldUntilItsNodeIsServedAgain(t *testing.T) {
	fs := newFakeSlurm(heldJob(15))
	h := newHarness(t, fs)
	h.job(15)
	js := h.jobSet(15)
	h.create(admittedWorkload(js.Name))
	old := slurmdPod("w0-old", 15, 0, "10.0.0.2", 63001, corev1.PodRunning)
	old.Annotations[translate.NodeRecordClearAnnotation] = "t"
	h.create(old)
	n0 := nodeName(15, 0)
	fs.register(n0, old, "10.0.0.2", 63001)
	h.job(15)
	if !contains(fs.released, uint64(15)) {
		t.Fatalf("not released: %q", fs.job(15).Comment)
	}
	// The pod dies; slurmctld requeues (new submit time, restart_cnt 1).
	h.setPhase("w0-old", corev1.PodFailed)
	fs.set(15, func(j *slurm.Job) {
		j.JobState = []string{"PENDING"}
		j.Hold = false
		j.StateReason = "BeginTime"
		j.RestartCnt = 1
		j.SubmitTime.Number = 2000
	})
	h.job(15)
	if !contains(fs.held, uint64(15)) || !strings.Contains(fs.job(15).Comment, "requeued") {
		t.Fatalf("requeued job not held: held=%v comment=%q", fs.held, fs.job(15).Comment)
	}
	// Event-triggered reconciles before the next poll do not hold again.
	for i := 0; i < 3; i++ {
		if _, err := h.jobs.Reconcile(h.ctx(), ctrlRequest(jobKey(15))); err != nil {
			t.Fatal(err)
		}
	}
	if len(fs.held) != 1 {
		t.Errorf("held %d times before the next poll, want 1", len(fs.held))
	}
	// Node reconciler releases the dead pod's record; the replacement is
	// cleared and registers; the held job goes through admission again.
	h.node(n0)
	if !contains(fs.deleted, n0) {
		t.Fatalf("dead pod's record not released: deleted=%v downed=%v", fs.deleted, fs.downed)
	}
	cur := slurmdPod("w0-new", 15, 0, "10.0.0.2", 63000, corev1.PodRunning)
	h.create(cur)
	h.node(n0)
	if h.pod("w0-new").Annotations[translate.NodeRecordClearAnnotation] == "" {
		t.Fatal("replacement not cleared")
	}
	fs.register(n0, cur, "10.0.0.2", 63000)
	fs.set(15, func(j *slurm.Job) { j.StateReason = "JobHeldAdmin" })
	h.job(15)
	if n := len(fs.released); n != 2 {
		t.Fatalf("released = %v (want twice): comment=%q", fs.released, fs.job(15).Comment)
	}
	if got := h.jobSet(15).Annotations[translate.SubmitTimeAnnotation]; got != "2000" {
		t.Errorf("JobSet not re-anchored to the new submit time: %q", got)
	}
}

// A requeued job whose JobSet is gone is held so admission recreates it.
func TestRequeuedJobWithoutAJobSetIsHeldForRecreation(t *testing.T) {
	job := heldJob(16)
	job.Hold = false
	job.StateReason = "BeginTime"
	job.RestartCnt = 1
	job.Features = testNaming.PinFeature(16)
	job.Comment = commentAdmitted
	fs := newFakeSlurm(job)
	h := newHarness(t, fs)
	h.job(16)
	if !contains(fs.held, uint64(16)) || !strings.Contains(fs.job(16).Comment, "JobSet is gone") {
		t.Fatalf("held=%v comment=%q", fs.held, fs.job(16).Comment)
	}
	h.job(16)
	if h.jobSet(16) == nil {
		t.Error("JobSet not recreated for the held job")
	}
}

// With fencing manual, a pod gone on a host that is not Ready blocks the
// re-run until the operator lists the host as fenced.
func TestReRunWaitsForFencingOnAnUnreachableHost(t *testing.T) {
	fs := newFakeSlurm(heldJob(17))
	lost := &corev1.Node{}
	lost.Name = "host-10.0.0.2"
	lost.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionUnknown}}
	h := newHarness(t, fs, lost)
	h.job(17)
	js := h.jobSet(17)
	h.create(admittedWorkload(js.Name))
	old := slurmdPod("w0-old", 17, 0, "10.0.0.2", 63001, corev1.PodRunning)
	old.Annotations[translate.NodeRecordClearAnnotation] = "t"
	h.create(old)
	n0 := nodeName(17, 0)
	fs.register(n0, old, "10.0.0.2", 63001)
	h.job(17)
	// The pod is marked for deletion without the kubelet reporting an end
	// (partitioned host): Gone, containers not terminated.
	h.markDeleting("w0-old")
	fs.busy[n0] = true
	h.node(n0)
	if _, downed := fs.downed[n0]; !downed {
		t.Fatalf("the record must be DOWN'd so the job ends: %v", fs.downed)
	}
	if got := h.jobSet(17).Annotations[FencePendingAnnotation]; got != "host-10.0.0.2" {
		t.Fatalf("fence-pending = %q", got)
	}
	// Requeued; replacement registers; still held: fencing required.
	fs.set(17, func(j *slurm.Job) {
		j.JobState = []string{"PENDING"}
		j.Hold = false
		j.RestartCnt = 1
		j.SubmitTime.Number = 3000
	})
	h.node(n0)
	cur := slurmdPod("w0-new", 17, 0, "10.0.0.3", 63000, corev1.PodRunning)
	cur.Annotations[translate.NodeRecordClearAnnotation] = "t"
	h.create(cur)
	fs.register(n0, cur, "10.0.0.3", 63000)
	h.job(17)
	if !contains(fs.held, uint64(17)) || !strings.Contains(fs.job(17).Comment, "fencing required") {
		t.Fatalf("held=%v comment=%q", fs.held, fs.job(17).Comment)
	}
	fs.set(17, func(j *slurm.Job) { j.StateReason = "JobHeldAdmin" })
	h.job(17)
	if len(fs.released) != 1 {
		t.Fatalf("released without fencing: %v", fs.released)
	}
	// The operator fences the host.
	js = h.jobSet(17)
	js.Annotations[h.cfg.Fencing.ConfirmAnnotation] = "host-10.0.0.2"
	if err := h.kube.Update(h.ctx(), js); err != nil {
		t.Fatal(err)
	}
	h.job(17)
	if len(fs.released) != 2 {
		t.Fatalf("not released after fencing: comment=%q", fs.job(17).Comment)
	}
	if h.jobSet(17).Annotations[FencePendingAnnotation] != "" {
		t.Error("fence-pending must be cleared")
	}
}

// A JobSet that fails before the job ends makes the job end with the
// reason, rather than wait for nodes that will never come.
func TestJobSetFailureCancelsTheJob(t *testing.T) {
	job := heldJob(18)
	job.Hold = false
	job.JobState = []string{"RUNNING"}
	job.Features = testNaming.PinFeature(18)
	fs := newFakeSlurm(job)
	h := newHarness(t, fs)
	js, _ := translate.New(h.cfg).JobSet(&job)
	h.create(js)
	js = h.jobSet(18)
	js.Status.Conditions = []metav1Condition{{Type: "Failed", Status: "True", Reason: "ReachedMaxRestarts", Message: "jobset failed due to reaching max number of restarts"}}
	if err := h.kube.Status().Update(h.ctx(), js); err != nil {
		t.Fatal(err)
	}
	h.job(18)
	if !contains(fs.cancels, uint64(18)) || !strings.Contains(fs.job(18).Comment, "max number of restarts") {
		t.Errorf("cancels=%v comment=%q", fs.cancels, fs.job(18).Comment)
	}
}

// With slurmd.maxDynamicNodes set, JobSets are created only while the
// pods fit; the rest wait with the count in their comment.
func TestDynamicNodeSlots(t *testing.T) {
	fs := newFakeSlurm(heldJob(19), heldJob(20))
	h := newHarness(t, fs)
	h.cfg.Slurmd.MaxDynamicNodes = 1
	h.job(19)
	h.job(20)
	if h.jobSet(19) == nil || h.jobSet(20) != nil {
		t.Fatalf("slots: js19=%v js20=%v", h.jobSet(19) != nil, h.jobSet(20) != nil)
	}
	if !strings.Contains(fs.job(20).Comment, "dynamic node slot") {
		t.Errorf("comment = %q", fs.job(20).Comment)
	}
	fs.set(19, func(j *slurm.Job) { j.JobState = []string{"COMPLETED"} })
	h.job(19)
	h.job(20)
	if h.jobSet(20) == nil {
		t.Error("job 20 must get its JobSet once the slot is free")
	}
}

// A slurmrestd outage changes nothing: the poll keeps the previous
// snapshot and the reconcile does no writes it cannot justify.
func TestSlurmOutageIsInert(t *testing.T) {
	fs := newFakeSlurm(heldJob(21))
	h := newHarness(t, fs)
	h.job(21)
	before := len(fs.comments)
	fs.fail("ListJobs", errBoom)
	snap := h.pol.Poll(h.ctx())
	if snap.Err == nil || len(snap.Jobs) != 1 {
		t.Fatalf("snapshot after a failed poll = err %v jobs %d; want the previous jobs with the error", snap.Err, len(snap.Jobs))
	}
	if _, err := h.jobs.Reconcile(h.ctx(), ctrlRequest(jobKey(21))); err != nil {
		t.Fatal(err)
	}
	if len(fs.comments) != before || len(fs.released) != 0 || len(fs.held) != 0 {
		t.Error("writes during an outage")
	}
	fs.fail("ListJobs", nil)
	if snap := h.pol.Poll(h.ctx()); snap.Err != nil {
		t.Error("recovered poll still errors")
	}
}

var _ = time.Second
