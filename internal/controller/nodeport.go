// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// PodNodeNameIndex indexes pods by the host they are scheduled on.
const PodNodeNameIndex = "spec.nodeName"

func PodNodeNameIndexer(o client.Object) []string {
	if p, ok := o.(*corev1.Pod); ok && p.Spec.NodeName != "" {
		return []string{p.Spec.NodeName}
	}
	return nil
}

// NodePortReconciler gives each gated slurmd pod a port no other pod on its
// host holds and no slurmctld record still names there. Allocation is
// sequential (one worker) and reads pods uncached, so a restart or leader
// change starts from the stamps on the pods, never from memory.
type NodePortReconciler struct {
	client.Client
	// Reader lists pods without the cache (nil: Client).
	Reader client.Reader
	// Records lists slurmctld's node records (nil: not consulted).
	Records nodeLister
	// Naming recognizes SlurmKube's records.
	Naming   translate.Naming
	Recorder record.EventRecorder
	Log      Logger
}

func (r *NodePortReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log
	if log == nil {
		log = nopLogger{}
	}
	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	rangeSpec, gated := pod.Annotations[translate.NodePortRangeAnnotation]
	if !gated || !IsOwnPod(&pod) || pod.Spec.NodeName == "" || podFinished(&pod) {
		return ctrl.Result{}, nil
	}
	lo, hi, err := translate.ParseNodePortRange(rangeSpec)
	if err != nil {
		r.event(&pod, corev1.EventTypeWarning, "NodePortRangeInvalid", "%v", err)
		return ctrl.Result{}, nil
	}
	if p, ok := stampedPort(&pod); ok && p >= lo && p <= hi {
		return ctrl.Result{}, nil // a valid stamp is final for the pod's life
	}
	taken, err := r.portsTaken(ctx, &pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if r.Records != nil {
		if pod.Status.HostIP == "" {
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil // host IP not reported yet
		}
		recorded, err := r.portsInRecords(ctx, pod.Status.HostIP)
		if err != nil {
			log.Warn("listing slurmctld's records before assigning a port failed, will retry", "pod", pod.Name, "error", err)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		for p := range recorded {
			taken[p] = true
		}
	}
	port, ok := choosePort(lo, hi, taken)
	if !ok {
		r.event(&pod, corev1.EventTypeWarning, "NodePortsExhausted", "no free slurmd port in %s on %s; raise slurmd.nodePorts.size or run fewer pods per host", rangeSpec, pod.Spec.NodeName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := patchAnnotation(ctx, r.Client, &pod, translate.NodePortAnnotation, strconv.Itoa(int(port))); err != nil {
		return ctrl.Result{}, err
	}
	// A second allocator (a leader handover) may have stamped the same
	// port on a pod created around the same time: the younger withdraws.
	again, err := r.portsHeldBy(ctx, &pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if other, dup := again[port]; dup && olderThan(other, &pod) {
		r.event(&pod, corev1.EventTypeWarning, "NodePortConflict", "port %d on %s is also %s's; withdrawing and retrying", port, pod.Spec.NodeName, other.Name)
		if err := patchAnnotation(ctx, r.Client, &pod, translate.NodePortAnnotation, ""); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}
	r.event(&pod, corev1.EventTypeNormal, "NodePortAssigned", "slurmd listens on port %d on %s", port, pod.Spec.NodeName)
	log.Info("assigned slurmd port", "pod", pod.Name, "host", pod.Spec.NodeName, "port", port)
	return ctrl.Result{}, nil
}

// choosePort is the lowest port of the range not taken.
func choosePort(lo, hi int32, taken map[int32]bool) (int32, bool) {
	for p := lo; p <= hi; p++ {
		if !taken[p] {
			return p, true
		}
	}
	return 0, false
}

// portsTaken: ports of the other pods on the host whose stamps are still
// in force — live pods, and finished pods whose record has not been
// released yet (portReusable).
func (r *NodePortReconciler) portsTaken(ctx context.Context, pod *corev1.Pod) (map[int32]bool, error) {
	held, err := r.portsHeldBy(ctx, pod)
	if err != nil {
		return nil, err
	}
	taken := map[int32]bool{}
	for p := range held {
		taken[p] = true
	}
	return taken, nil
}

func (r *NodePortReconciler) portsHeldBy(ctx context.Context, pod *corev1.Pod) (map[int32]*corev1.Pod, error) {
	reader := r.Reader
	if reader == nil {
		reader = r.Client
	}
	var peers corev1.PodList
	if err := reader.List(ctx, &peers, client.InNamespace(pod.Namespace),
		client.MatchingLabels{translate.ManagedByLabel: translate.ManagedByValue},
		client.MatchingFields{PodNodeNameIndex: pod.Spec.NodeName}); err != nil {
		return nil, fmt.Errorf("listing pods on %s: %w", pod.Spec.NodeName, err)
	}
	held := map[int32]*corev1.Pod{}
	for i := range peers.Items {
		peer := &peers.Items[i]
		if peer.UID == pod.UID || portReusable(peer) {
			continue
		}
		if p, ok := stampedPort(peer); ok {
			if prev, dup := held[p]; !dup || olderThan(peer, prev) {
				held[p] = peer
			}
		}
	}
	return held, nil
}

// portsInRecords: ports slurmctld's records of ours name at the host.
func (r *NodePortReconciler) portsInRecords(ctx context.Context, hostIP string) (map[int32]string, error) {
	nodes, err := r.Records.ListNodes(ctx)
	if err != nil {
		return nil, err
	}
	out := map[int32]string{}
	for _, n := range nodes {
		if _, _, ours := r.Naming.ParseNodeName(n.Name); ours && n.Port > 0 && n.Address == hostIP {
			out[n.Port] = n.Name
		}
	}
	return out, nil
}

func stampedPort(pod *corev1.Pod) (int32, bool) {
	s, ok := pod.Annotations[translate.NodePortAnnotation]
	if !ok {
		return 0, false
	}
	p, err := strconv.ParseInt(s, 10, 32)
	if err != nil || p < 1 || p > 65535 {
		return 0, false
	}
	return int32(p), true
}

func podFinished(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

// portReusable: a finished pod frees its port only once the controller
// stamped that no record points at it; a pod that was never gated frees
// it at once.
func portReusable(pod *corev1.Pod) bool {
	if !podFinished(pod) {
		return false
	}
	if !translate.RecordGated(pod) {
		return true
	}
	return pod.Annotations[translate.NodeRecordReleasedAnnotation] != ""
}

func olderThan(a, b *corev1.Pod) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.UID < b.UID
}

// IsOwnPod reports whether the object is one of SlurmKube's slurmd pods.
func IsOwnPod(o client.Object) bool {
	return o.GetLabels()[translate.ManagedByLabel] == translate.ManagedByValue
}

func (r *NodePortReconciler) event(pod *corev1.Pod, kind, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(pod, kind, reason, format, args...)
	}
}

// SetupWithManager registers the allocator with one worker.
func (r *NodePortReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexer); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("node-port").
		For(&corev1.Pod{}, ownPodsOnly()).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

var _ = slurm.Node{}
