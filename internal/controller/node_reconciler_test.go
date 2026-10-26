// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

func runningJob(id uint64) slurm.Job {
	j := heldJob(id)
	j.Hold = false
	j.JobState = []string{"RUNNING"}
	j.Features = testNaming.PinFeature(id)
	j.Comment = commentAdmitted
	return j
}

// Old pod Terminating while the new pod is created, on the same host: the
// old record is released (DOWN first, the job is on it), the old pod's
// port stays reserved until then, the new pod is cleared only afterwards,
// and the new pod's own record is never touched by the old pod's lingering
// object.
func TestReplacementSequenceOnASharedHost(t *testing.T) {
	job := runningJob(30)
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	h := newHarness(t, fs, js)
	n0 := nodeName(30, 0)
	old := slurmdPod("w0-old", 30, 0, "10.0.0.2", 63001, corev1.PodRunning)
	old.Annotations[translate.NodeRecordClearAnnotation] = "t"
	h.create(old)
	fs.register(n0, old, "10.0.0.2", 63001)
	fs.busy[n0] = true

	// Old pod marked for deletion (kubelet reported its containers ended).
	h.markDeleting("w0-old")
	p := h.pod("w0-old")
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "slurmd", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
	if err := h.kube.Status().Update(h.ctx(), p); err != nil {
		t.Fatal(err)
	}
	// The replacement gets the SAME port once the old pod is released.
	cur := slurmdPod("w0-new", 30, 0, "10.0.0.2", 63001, corev1.PodPending)
	h.create(cur)

	h.node(n0)
	if _, downed := fs.downed[n0]; !downed {
		t.Fatalf("a busy record of a gone pod must be DOWN'd first: downed=%v deleted=%v", fs.downed, fs.deleted)
	}
	if h.pod("w0-old").Annotations[translate.NodeRecordReleasedAnnotation] != "" {
		t.Error("the old pod's port must stay reserved while its record exists")
	}
	if h.pod("w0-new").Annotations[translate.NodeRecordClearAnnotation] != "" {
		t.Error("the new pod must not be cleared while the old record exists")
	}
	h.node(n0) // the delete goes through now
	if !contains(fs.deleted, n0) {
		t.Fatalf("record not deleted: %v", fs.deleted)
	}
	if h.pod("w0-old").Annotations[translate.NodeRecordReleasedAnnotation] == "" {
		t.Error("the old pod must be stamped released once its record is gone")
	}
	h.node(n0)
	if h.pod("w0-new").Annotations[translate.NodeRecordClearAnnotation] == "" {
		t.Fatal("the new pod must be cleared once no record exists")
	}
	// New pod registers with the SAME host and port as the old one had.
	h.setPhase("w0-new", corev1.PodRunning)
	fs.register(n0, h.pod("w0-new"), "10.0.0.2", 63001)
	before := len(fs.deleted)
	for i := 0; i < 3; i++ {
		h.node(n0)
	}
	if len(fs.deleted) != before || len(fs.downed) != 1 {
		t.Errorf("the new pod's record was released on the old pod's account: deleted=%v downed=%v", fs.deleted, fs.downed)
	}
}

// A finished pod's record is released whether or not its JobSet still
// exists. The JobSet is deleted at the job's end while the pod and its
// record linger; with no JobSet there is nothing to record the event on
// (a nil *JobSet in client.Object once panicked here, which also skipped
// the nudge and repeated the release until the next poll).
func TestReleaseWithAndWithoutTheJobSet(t *testing.T) {
	for _, tc := range []struct {
		name   string
		jobSet bool
	}{{"jobset present", true}, {"jobset gone", false}} {
		t.Run(tc.name, func(t *testing.T) {
			job := runningJob(34)
			job.JobState = []string{"COMPLETED"}
			fs := newFakeSlurm(job)
			h := newHarness(t, fs)
			if tc.jobSet {
				js, _ := translate.New(testConfig()).JobSet(&job)
				h.create(js)
			}
			n0 := nodeName(34, 0)
			done := slurmdPod("w0", 34, 0, "10.0.0.2", 63000, corev1.PodSucceeded)
			done.Annotations[translate.NodeRecordClearAnnotation] = "t"
			done.Annotations[FenceVerdictAnnotation] = "safe"
			h.create(done)
			fs.register(n0, done, "10.0.0.2", 63000)
			h.pol.Nudge()
			<-h.pol.nudge // start with no nudge pending

			h.node(n0)
			if !contains(fs.deleted, n0) {
				t.Fatalf("record of a finished pod not released: deleted=%v", fs.deleted)
			}
			if h.pod("w0").Annotations[translate.NodeRecordReleasedAnnotation] == "" {
				t.Error("the finished pod must be stamped released")
			}
			if got := h.rec.has("NodeRecordReleased"); got != tc.jobSet {
				t.Errorf("NodeRecordReleased event recorded=%v, want %v", got, tc.jobSet)
			}
			if len(h.pol.nudge) != 1 {
				t.Error("the release must nudge the poller so the next snapshot drops the record")
			}

			deleted := len(fs.deleted)
			for i := 0; i < 3; i++ {
				h.node(n0)
			}
			if len(fs.deleted) != deleted {
				t.Errorf("record released again after it was gone: %v", fs.deleted)
			}
		})
	}
}

// A record whose owner the cache does not know is released only after
// the grace (time-based), and the waiting replacement is cleared then.
func TestVanishedOwnerGraceIsTimeBased(t *testing.T) {
	job := runningJob(31)
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	h := newHarness(t, fs, js)
	n0 := nodeName(31, 0)
	ghost := slurmdPod("ghost", 31, 0, "10.0.0.2", 63001, corev1.PodRunning)
	fs.register(n0, ghost, "10.0.0.2", 63001) // registered by a pod that no longer exists
	cur := slurmdPod("w0-new", 31, 0, "10.0.0.2", 63000, corev1.PodPending)
	h.create(cur)
	for i := 0; i < 5; i++ {
		h.node(n0)
		if len(fs.deleted) != 0 {
			t.Fatalf("released inside the grace (pass %d)", i)
		}
		if h.pod("w0-new").Annotations[translate.NodeRecordClearAnnotation] != "" {
			t.Fatal("cleared while a foreign record exists")
		}
	}
	h.now = h.now.Add(h.cfg.Recovery.VanishedOwnerGrace.Duration + time.Second)
	h.node(n0)
	if !contains(fs.deleted, n0) {
		t.Fatalf("not released after the grace: %v", fs.deleted)
	}
	h.node(n0)
	if h.pod("w0-new").Annotations[translate.NodeRecordClearAnnotation] == "" {
		t.Error("replacement not cleared after the release")
	}
}

// A live owner whose record points at another port (pod network, or a
// legacy record) is released together with its pod.
func TestStaleEndpointReleasesRecordAndPod(t *testing.T) {
	job := runningJob(32)
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	h := newHarness(t, fs, js)
	n0 := nodeName(32, 0)
	cur := slurmdPod("w0", 32, 0, "10.0.0.2", 63000, corev1.PodRunning)
	cur.Annotations[translate.NodeRecordClearAnnotation] = "t"
	h.create(cur)
	fs.register(n0, cur, "10.0.0.2", 63001)
	h.node(n0)
	if !contains(fs.deleted, n0) || h.pod("w0") != nil {
		t.Errorf("stale record: deleted=%v podExists=%v", fs.deleted, h.pod("w0") != nil)
	}
}

// A cleared pod that runs without a record past the registration timeout
// is replaced; a terminal job's pod is never cleared.
func TestRegistrationTimeoutAndTerminalJob(t *testing.T) {
	job := runningJob(33)
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	h := newHarness(t, fs, js)
	n0 := nodeName(33, 0)
	cur := slurmdPod("w0", 33, 0, "10.0.0.2", 63000, corev1.PodRunning)
	cur.Annotations[translate.NodeRecordClearAnnotation] = h.now.Add(-time.Minute).UTC().Format(time.RFC3339)
	h.create(cur)
	h.node(n0)
	if h.pod("w0") == nil {
		t.Fatal("deleted inside the registration timeout")
	}
	h.now = h.now.Add(h.cfg.Slurmd.RegistrationTimeout.Duration)
	h.node(n0)
	if h.pod("w0") != nil {
		t.Error("a pod without a record past the timeout must be replaced")
	}

	done := runningJob(34)
	done.JobState = []string{"COMPLETED"}
	fs2 := newFakeSlurm(done)
	js2, _ := translate.New(testConfig()).JobSet(&done)
	h2 := newHarness(t, fs2, js2)
	p := slurmdPod("w0", 34, 0, "10.0.0.2", 63000, corev1.PodPending)
	h2.create(p)
	h2.node(nodeName(34, 0))
	if h2.pod("w0").Annotations[translate.NodeRecordClearAnnotation] != "" {
		t.Error("a terminal job's pod must not be cleared")
	}
}

// Orphan sweep: a record with neither JobSet nor pods is released after
// the grace when its job is gone or terminal, or live with nothing
// serving it — but never when the cache shows no JobSet of ours at all.
func TestOrphanedRecordsAreSweptAfterTheGrace(t *testing.T) {
	gone := runningJob(35) // JobSet and pods deleted while the controller was down
	fs := newFakeSlurm(gone)
	other := runningJob(99)
	otherJS, _ := translate.New(testConfig()).JobSet(&other)
	h := newHarness(t, fs, otherJS)
	n0 := nodeName(35, 0)
	ghost := slurmdPod("ghost", 35, 0, "10.0.0.2", 63001, corev1.PodRunning)
	fs.register(n0, ghost, "10.0.0.2", 63001)
	fs.busy[n0] = true
	for i := 1; i < h.cfg.Recovery.OrphanPolls; i++ {
		h.node(n0)
		if len(fs.downed) != 0 {
			t.Fatalf("acted before the grace (pass %d)", i)
		}
	}
	h.node(n0)
	if _, downed := fs.downed[n0]; !downed {
		t.Fatalf("live job with no JobSet and no pods: record must be DOWN'd: %v", fs.downed)
	}
	h.node(n0)
	if !contains(fs.deleted, n0) {
		t.Errorf("record not deleted after the DOWN: %v", fs.deleted)
	}

	// No JobSet of ours anywhere: a mis-scoped cache looks the same, so
	// a live job's record is left alone.
	fs2 := newFakeSlurm(runningJob(36))
	h2 := newHarness(t, fs2)
	n := nodeName(36, 0)
	fs2.register(n, slurmdPod("g", 36, 0, "10.0.0.2", 63001, corev1.PodRunning), "10.0.0.2", 63001)
	for i := 0; i < 5; i++ {
		h2.node(n)
	}
	if len(fs2.deleted) != 0 || len(fs2.downed) != 0 {
		t.Error("released a live job's record while no JobSet was visible")
	}
	// A terminal job's leftover record is swept regardless.
	fs3 := newFakeSlurm(func() slurm.Job { j := runningJob(37); j.JobState = []string{"COMPLETED"}; return j }())
	h3 := newHarness(t, fs3)
	n = nodeName(37, 0)
	fs3.register(n, slurmdPod("g", 37, 0, "10.0.0.2", 63001, corev1.PodRunning), "10.0.0.2", 63001)
	for i := 0; i < h3.cfg.Recovery.OrphanPolls; i++ {
		h3.node(n)
	}
	if !contains(fs3.deleted, n) {
		t.Errorf("terminal job's record not swept: %v", fs3.deleted)
	}
}

// A record under one of our names that carries another manager's pin
// feature (a shared node name prefix) is never swept or released, and a
// pod of ours under that name is not cleared while it exists.
func TestForeignRecordUnderOurNameIsNeverTouched(t *testing.T) {
	job := runningJob(39)
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	h := newHarness(t, fs, js)
	n0 := nodeName(39, 0)
	foreign := func() {
		fs.mu.Lock()
		fs.nodes[n0] = &slurm.Node{Name: n0, Address: "10.0.0.2", Port: 63000, Extra: "uid-elsewhere",
			State: []string{"IDLE", "DYNAMIC_NORM"}, Features: []string{"other-pin-39", "other"}}
		fs.mu.Unlock()
	}
	foreign()
	cur := slurmdPod("w0", 39, 0, "10.0.0.2", 63001, corev1.PodRunning)
	h.create(cur)
	for i := 0; i < 2*h.cfg.Recovery.OrphanPolls; i++ {
		h.node(n0)
	}
	if len(fs.deleted) != 0 || len(fs.downed) != 0 {
		t.Errorf("another manager's record was touched: deleted %v downed %v", fs.deleted, fs.downed)
	}
	if h.pod("w0").Annotations[translate.NodeRecordClearAnnotation] != "" {
		t.Error("the pod must not be cleared while a foreign record holds its name")
	}

	// No JobSet and no pods of ours either: the orphan sweep skips it too.
	fs2 := newFakeSlurm(runningJob(40))
	h2 := newHarness(t, fs2, js)
	n := nodeName(40, 0)
	fs2.mu.Lock()
	fs2.nodes[n] = &slurm.Node{Name: n, Features: []string{"other-pin-40"}, State: []string{"IDLE"}}
	fs2.mu.Unlock()
	for i := 0; i < 2*h2.cfg.Recovery.OrphanPolls; i++ {
		h2.node(n)
	}
	if len(fs2.deleted) != 0 || len(fs2.downed) != 0 {
		t.Errorf("another manager's orphan-looking record was swept: %v %v", fs2.deleted, fs2.downed)
	}
}

// Repeating reconciles on unchanged state changes nothing; a slurmrestd
// error leaves everything as it was.
func TestNodeReconcileIsIdempotentAndInertOnErrors(t *testing.T) {
	job := runningJob(38)
	fs := newFakeSlurm(job)
	js, _ := translate.New(testConfig()).JobSet(&job)
	h := newHarness(t, fs, js)
	n0 := nodeName(38, 0)
	cur := slurmdPod("w0", 38, 0, "10.0.0.2", 63000, corev1.PodRunning)
	cur.Annotations[translate.NodeRecordClearAnnotation] = "t"
	h.create(cur)
	fs.register(n0, cur, "10.0.0.2", 63000)
	for i := 0; i < 5; i++ {
		h.node(n0)
	}
	if len(fs.deleted) != 0 || len(fs.downed) != 0 {
		t.Errorf("healthy record touched: %v %v", fs.deleted, fs.downed)
	}
	h.setPhase("w0", corev1.PodFailed)
	fs.fail("DeleteNode", errBoom)
	h.node(n0)
	if len(fs.downed) != 0 || h.pod("w0").Annotations[translate.NodeRecordReleasedAnnotation] != "" {
		t.Error("a failed delete must change nothing")
	}
	fs.fail("DeleteNode", nil)
	h.node(n0)
	if !contains(fs.deleted, n0) || h.pod("w0").Annotations[translate.NodeRecordReleasedAnnotation] == "" {
		t.Error("recovered: record released and pod stamped")
	}
}
