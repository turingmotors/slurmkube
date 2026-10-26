// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package fence decides whether a job whose pod is gone may run again.
//
// A Pod object disappearing is not proof that its processes stopped: on a
// host whose kubelet is partitioned, Kubernetes marks the pod for deletion
// and eventually removes it while slurmstepd keeps running. Re-running the
// job elsewhere would then write to the same shared storage twice. The
// controller therefore ends the job (the record is DOWN'd: NODE_FAIL,
// requeue for --requeue) but keeps the requeued job HELD until a Fencer
// confirms that the old pod's host cannot still run it.
//
// Hardware fencing is a site matter; SlurmKube only asks. Two policies
// ship: Manual (default) trusts a Ready host and asks an operator for
// the rest; TrustKubernetes treats every disappearance as a stop.
package fence

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Incident is a pod of a job that is gone while the job may be re-run.
type Incident struct {
	JobID   uint64
	Node    string // Slurm node name
	PodName string
	PodUID  string
	Host    string // Kubernetes node the pod ran on ("" if never scheduled)
	// PodTerminated: the pod's status says its containers terminated
	// (the kubelet reported the end), as opposed to the pod vanishing or
	// being marked for deletion without a report.
	PodTerminated bool
	// Owner is the object an operator confirms on (the JobSet).
	Owner client.Object
}

// Verdict is a Fencer's answer.
type Verdict struct {
	// Safe: the old processes cannot be running; the job may run again.
	Safe bool
	// Reason explains, for the job's comment and the event.
	Reason string
}

// Fencer decides whether re-running is safe.
type Fencer interface {
	Confirm(ctx context.Context, in Incident) (Verdict, error)
}

// TrustKubernetes treats a pod's disappearance as a stop. Suitable where
// a NotReady host is powered off by something else, or where double
// execution is acceptable.
type TrustKubernetes struct{}

func (TrustKubernetes) Confirm(context.Context, Incident) (Verdict, error) {
	return Verdict{Safe: true, Reason: "fencing.mode trustKubernetes: a pod that is gone is taken as stopped"}, nil
}

// Manual trusts the kubelet when the host is Ready (it has killed the
// pod's containers, or reported their end) and otherwise requires an
// operator to list the host in the owner's ConfirmAnnotation after fencing
// it by other means.
type Manual struct {
	// Reader reads Nodes.
	Reader client.Reader
	// ConfirmAnnotation on the owner lists fenced hosts, comma-separated.
	ConfirmAnnotation string
}

func (m Manual) Confirm(ctx context.Context, in Incident) (Verdict, error) {
	if in.Host == "" {
		return Verdict{Safe: true, Reason: "the pod was never scheduled; nothing ran"}, nil
	}
	if in.PodTerminated {
		return Verdict{Safe: true, Reason: "the kubelet reported the pod's containers terminated"}, nil
	}
	if in.Owner != nil {
		if fenced := in.Owner.GetAnnotations()[m.ConfirmAnnotation]; listed(fenced, in.Host) {
			return Verdict{Safe: true, Reason: fmt.Sprintf("host %s is listed as fenced (%s)", in.Host, m.ConfirmAnnotation)}, nil
		}
	}
	var node corev1.Node
	if err := m.Reader.Get(ctx, client.ObjectKey{Name: in.Host}, &node); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return Verdict{Safe: true, Reason: fmt.Sprintf("host %s no longer exists in the cluster", in.Host)}, nil
		}
		return Verdict{}, fmt.Errorf("reading host %s: %w", in.Host, err)
	}
	if ready(&node) {
		return Verdict{Safe: true, Reason: fmt.Sprintf("host %s is Ready: its kubelet has stopped the pod", in.Host)}, nil
	}
	pod := "the job's pod"
	if in.PodName != "" {
		pod = "pod " + in.PodName
	}
	return Verdict{Safe: false, Reason: fmt.Sprintf("host %s is not Ready and %s may still be running there; fence the host and add it to the %s annotation of %s",
		in.Host, pod, m.ConfirmAnnotation, ownerName(in.Owner))}, nil
}

func ready(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func listed(csv, host string) bool {
	for _, h := range strings.Split(csv, ",") {
		if strings.TrimSpace(h) == host && host != "" {
			return true
		}
	}
	return false
}

func ownerName(o client.Object) string {
	if o == nil {
		return "the JobSet"
	}
	return o.GetName()
}
