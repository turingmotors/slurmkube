// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/fence"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// fakeSlurm is slurmctld as the controllers see it: jobs and node records
// that the test mutates, plus every write recorded. Node deletes behave
// like slurmctld: refused while a job is on the node (busy), gone after.
type fakeSlurm struct {
	mu       sync.Mutex
	jobs     map[uint64]*slurm.Job
	nodes    map[string]*slurm.Node
	busy     map[string]bool // node has a job on it: delete refused
	released []uint64
	held     []uint64
	cancels  []uint64
	deleted  []string
	downed   map[string]string
	comments map[uint64]string
	features map[uint64]string
	errs     map[string]error // per method injected error
}

func newFakeSlurm(jobs ...slurm.Job) *fakeSlurm {
	f := &fakeSlurm{jobs: map[uint64]*slurm.Job{}, nodes: map[string]*slurm.Node{}, busy: map[string]bool{},
		downed: map[string]string{}, comments: map[uint64]string{}, features: map[uint64]string{}, errs: map[string]error{}}
	for i := range jobs {
		j := jobs[i]
		f.jobs[j.JobID] = &j
	}
	return f
}

func (f *fakeSlurm) fail(method string, err error) { f.mu.Lock(); f.errs[method] = err; f.mu.Unlock() }

func (f *fakeSlurm) ListJobs(_ context.Context) ([]slurm.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["ListJobs"]; err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, len(f.jobs))
	for id := range f.jobs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	out := make([]slurm.Job, 0, len(ids))
	for _, id := range ids {
		out = append(out, *f.jobs[id])
	}
	return out, nil
}

func (f *fakeSlurm) GetJob(_ context.Context, id uint64) (*slurm.Job, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[id]
	if !ok {
		return nil, false, nil
	}
	c := *j
	return &c, true, nil
}

func (f *fakeSlurm) ListNodes(context.Context) ([]slurm.Node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["ListNodes"]; err != nil {
		return nil, err
	}
	out := make([]slurm.Node, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, *n)
	}
	return out, nil
}

func (f *fakeSlurm) SetJobFeatures(_ context.Context, id uint64, features string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["SetJobFeatures"]; err != nil {
		return err
	}
	f.features[id] = features
	if j, ok := f.jobs[id]; ok {
		j.Features = features
	}
	return nil
}

func (f *fakeSlurm) ReleaseJob(_ context.Context, id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["ReleaseJob"]; err != nil {
		return err
	}
	f.released = append(f.released, id)
	if j, ok := f.jobs[id]; ok {
		j.Hold = false
		j.StateReason = "None"
	}
	return nil
}

func (f *fakeSlurm) HoldJob(_ context.Context, id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["HoldJob"]; err != nil {
		return err
	}
	f.held = append(f.held, id)
	if j, ok := f.jobs[id]; ok {
		j.Hold = true
	}
	return nil
}

func (f *fakeSlurm) SetJobComment(_ context.Context, id uint64, comment string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments[id] = comment
	if j, ok := f.jobs[id]; ok {
		j.Comment = comment
	}
	return nil
}

func (f *fakeSlurm) CancelJob(_ context.Context, id uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, id)
	if j, ok := f.jobs[id]; ok {
		j.JobState = []string{"CANCELLED"}
	}
	return nil
}

var errBusy = &slurm.Error{Status: http.StatusUnprocessableEntity, Method: "DELETE", Path: "/node",
	Entries: []slurm.ErrorEntry{{Number: 2016, Message: "Requested nodes are busy"}}}
var errNoNode = &slurm.Error{Status: http.StatusUnprocessableEntity, Method: "DELETE", Path: "/node",
	Entries: []slurm.ErrorEntry{{Number: 2018, Message: "Invalid node name specified"}}}

func (f *fakeSlurm) DeleteNode(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.errs["DeleteNode"]; err != nil {
		return err
	}
	if _, ok := f.nodes[name]; !ok {
		return errNoNode
	}
	if f.busy[name] {
		return errBusy
	}
	delete(f.nodes, name)
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeSlurm) SetNodeState(_ context.Context, name, state, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downed[name] = state + ": " + reason
	if n, ok := f.nodes[name]; ok && state == "DOWN" {
		n.State = []string{"DOWN"}
		delete(f.busy, name) // the job ends; the next delete succeeds
	}
	return nil
}

// register is slurmd registering: a fresh record owned by the pod.
func (f *fakeSlurm) register(name string, pod *corev1.Pod, addr string, port int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, _, _ := testNaming.ParseNodeName(name)
	f.nodes[name] = &slurm.Node{Name: name, Address: addr, Port: port, Extra: string(pod.UID),
		State: []string{"IDLE", "DYNAMIC_NORM"}, Features: []string{testNaming.PinFeature(id), "slurmkube"}, CPUs: 2, EffectiveCPUs: 2}
}

func (f *fakeSlurm) job(id uint64) *slurm.Job { f.mu.Lock(); defer f.mu.Unlock(); return f.jobs[id] }

// add submits another job to the fake slurmctld.
func (f *fakeSlurm) add(j slurm.Job) { f.mu.Lock(); defer f.mu.Unlock(); f.jobs[j.JobID] = &j }

func (f *fakeSlurm) set(id uint64, mod func(j *slurm.Job)) {
	f.mu.Lock()
	mod(f.jobs[id])
	f.mu.Unlock()
}

// --- fixtures -----------------------------------------------------------

// testNaming is the default naming every fixture uses.
var testNaming = translate.NewNaming("")

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := jobsetv1alpha2.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func testConfig() *config.Config {
	cfg := &config.Config{
		Slurm:      config.Slurm{RestURL: "https://fake"},
		Namespace:  "slurm-jobs",
		LocalQueue: "main",
		Partitions: []config.Partition{{Name: "k8s", PriorityClass: "normal"}},
		Slurmd:     config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "slurm-auth"},
	}
	cfg.ApplyDefaults()
	return cfg
}

func heldJob(id uint64) slurm.Job {
	return slurm.Job{
		JobID: id, Partition: "k8s", JobState: []string{"PENDING"}, StateReason: "JobHeldUser", Hold: true,
		Comment:     config.DefaultHeldComment,
		Tasks:       slurm.OptionalUint64{Set: true, Number: 1},
		CPUsPerTask: slurm.OptionalUint64{Set: true, Number: 2},
		SubmitTime:  slurm.OptionalUint64{Set: true, Number: 1000},
	}
}

func admittedWorkload(jobset string) *unstructured.Unstructured {
	wl := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "Workload",
		"metadata": map[string]any{"name": "wl-" + jobset, "namespace": "slurm-jobs",
			"ownerReferences": []any{map[string]any{"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "name": jobset, "uid": "u-" + jobset}}},
		"status": map[string]any{"conditions": []any{map[string]any{"type": "Admitted", "status": "True", "reason": "Admitted", "message": "admitted", "lastTransitionTime": "2026-10-24T00:00:00Z"}}},
	}}
	return wl
}

// slurmdPod is a pod the JobSet created for job id, index idx, on host
// hostIP with a stamped port, record-gated.
func slurmdPod(name string, id uint64, idx int, hostIP string, port int32, phase corev1.PodPhase) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "slurm-jobs", UID: types.UID("uid-" + name),
			Labels: map[string]string{translate.ManagedByLabel: translate.ManagedByValue, translate.SlurmJobIDLabel: strconv.FormatUint(id, 10),
				jobSetNameLabel: testNaming.JobSetName(id)},
			Annotations: map[string]string{batchv1.JobCompletionIndexAnnotation: strconv.Itoa(idx),
				translate.NodePortRangeAnnotation: "63000-64999"},
		},
		Spec:   corev1.PodSpec{NodeName: "host-" + hostIP, HostNetwork: true, Containers: []corev1.Container{{Name: "slurmd", Image: "x"}}},
		Status: corev1.PodStatus{Phase: phase, HostIP: hostIP, PodIP: hostIP},
	}
	if port > 0 {
		p.Annotations[translate.NodePortAnnotation] = strconv.Itoa(int(port))
	}
	return p
}

type harness struct {
	t     *testing.T
	kube  client.Client
	fs    *fakeSlurm
	cfg   *config.Config
	pol   *Poller
	jobs  *JobReconciler
	nodes *NodeRecordReconciler
	rec   *fakeRecorder
	now   time.Time
}

// fakeRecorder keeps the reasons of the events recorded. Like the real
// recorder it reads the object's kind, so an event on a nil object panics
// here as it does in production.
type fakeRecorder struct {
	mu      sync.Mutex
	reasons []string
}

func (r *fakeRecorder) Event(obj runtime.Object, _, reason, _ string) {
	_ = obj.GetObjectKind().GroupVersionKind()
	r.mu.Lock()
	r.reasons = append(r.reasons, reason)
	r.mu.Unlock()
}

func (r *fakeRecorder) Eventf(obj runtime.Object, kind, reason, _ string, _ ...any) {
	r.Event(obj, kind, reason, "")
}

func (r *fakeRecorder) AnnotatedEventf(obj runtime.Object, _ map[string]string, kind, reason, _ string, _ ...any) {
	r.Event(obj, kind, reason, "")
}

func (r *fakeRecorder) has(reason string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return contains(r.reasons, reason)
}

func newHarness(t *testing.T, fs *fakeSlurm, objs ...client.Object) *harness {
	t.Helper()
	cfg := testConfig()
	kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(objs...).WithStatusSubresource(&corev1.Pod{}, &batchv1.Job{}, &jobsetv1alpha2.JobSet{}).Build()
	h := &harness{t: t, kube: kube, fs: fs, cfg: cfg, rec: &fakeRecorder{}, now: time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)}
	tr := translate.New(cfg)
	h.pol = &Poller{Slurm: fs, Interval: time.Hour, Naming: tr.Naming, Claims: func(j *slurm.Job) bool { _, ok := cfg.PriorityClassFor(j.Partition); return ok }}
	clock := func() time.Time { return h.now }
	fencer := fence.Manual{Reader: kube, ConfirmAnnotation: cfg.Fencing.ConfirmAnnotation}
	h.jobs = &JobReconciler{Client: kube, Slurm: fs, Poller: h.pol, Cfg: cfg, Translate: tr, Fencer: fencer, Clock: clock, Recorder: h.rec}
	h.nodes = &NodeRecordReconciler{Client: kube, Slurm: fs, Poller: h.pol, Cfg: cfg, Naming: tr.Naming, Fencer: fencer, Clock: clock, Recorder: h.rec,
		vanishedSince: map[string]time.Time{}, orphanSeen: map[string]int{}}
	return h
}

// poll reads the fake into a fresh snapshot.
func (h *harness) poll() { h.pol.Poll(context.Background()) }

// job reconciles one Slurm job after a poll.
func (h *harness) job(id uint64) ctrl.Result {
	h.t.Helper()
	h.poll()
	res, err := h.jobs.Reconcile(context.Background(), ctrl.Request{NamespacedName: key(jobKey(id))})
	if err != nil {
		h.t.Fatalf("job %d reconcile: %v", id, err)
	}
	return res
}

// node reconciles one dynamic node name after a poll.
func (h *harness) node(name string) ctrl.Result {
	h.t.Helper()
	h.poll()
	res, err := h.nodes.Reconcile(context.Background(), ctrl.Request{NamespacedName: key(name)})
	if err != nil {
		h.t.Fatalf("node %s reconcile: %v", name, err)
	}
	return res
}

func (h *harness) jobSet(id uint64) *jobsetv1alpha2.JobSet {
	h.t.Helper()
	var js jobsetv1alpha2.JobSet
	if err := h.kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: testNaming.JobSetName(id)}, &js); err != nil {
		return nil
	}
	return &js
}

func (h *harness) pod(name string) *corev1.Pod {
	h.t.Helper()
	var p corev1.Pod
	if err := h.kube.Get(context.Background(), client.ObjectKey{Namespace: "slurm-jobs", Name: name}, &p); err != nil {
		return nil
	}
	return &p
}

func (h *harness) create(objs ...client.Object) {
	h.t.Helper()
	for _, o := range objs {
		if p, ok := o.(*corev1.Pod); ok {
			status := p.Status
			if err := h.kube.Create(context.Background(), p); err != nil {
				h.t.Fatal(err)
			}
			p.Status = status
			if err := h.kube.Status().Update(context.Background(), p); err != nil {
				h.t.Fatal(err)
			}
			continue
		}
		if err := h.kube.Create(context.Background(), o); err != nil {
			h.t.Fatal(err)
		}
	}
}

func (h *harness) setPhase(name string, phase corev1.PodPhase) {
	h.t.Helper()
	p := h.pod(name)
	p.Status.Phase = phase
	if err := h.kube.Status().Update(context.Background(), p); err != nil {
		h.t.Fatal(err)
	}
}

func contains[T comparable](list []T, v T) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

var errBoom = errors.New("slurmrestd: boom")

func nodeName(id uint64, idx int) string { return testNaming.NodeName(id, int32(idx)) }

func (h *harness) ctx() context.Context { return context.Background() }

func metav1Now() metav1.Time { return metav1.Now() }

type metav1Condition = metav1.Condition

func ctrlRequest(name string) ctrl.Request { return ctrl.Request{NamespacedName: key(name)} }

// jobsetv1alpha2JobSetFixture is another manager's JobSet.
type jobsetv1alpha2JobSetFixture struct {
	name   string
	labels map[string]string
}

func (f *jobsetv1alpha2JobSetFixture) object() client.Object {
	return &jobsetv1alpha2.JobSet{ObjectMeta: metav1.ObjectMeta{Name: f.name, Namespace: "slurm-jobs", Labels: f.labels}}
}

// markDeleting gives the pod a finalizer and deletes it, so it carries a
// deletionTimestamp (Terminating) but stays in the API.
func (h *harness) markDeleting(name string) {
	h.t.Helper()
	p := h.pod(name)
	p.Finalizers = []string{"test/keep"}
	if err := h.kube.Update(h.ctx(), p); err != nil {
		h.t.Fatal(err)
	}
	if err := h.kube.Delete(h.ctx(), p); err != nil {
		h.t.Fatal(err)
	}
}
