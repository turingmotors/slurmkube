// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/turingmotors/slurmkube/internal/ownership"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// Comments SlurmKube writes on Slurm jobs. The JobSubmit plugin's hold
// comment (config.Claim.HeldComment) is the hand-over; everything after
// starts with CommentPrefix so another manager can tell the job is taken.
const (
	CommentPrefix     = "slurmkube:"
	commentHeld       = "slurmkube: held: "
	commentClaimed    = "slurmkube: claimed, creating its JobSet"
	commentAdmitted   = "slurmkube: admitted, provisioning nodes"
	commentWaitQuota  = "slurmkube: waiting for Kueue admission"
	commentReleased   = "slurmkube: released onto its dynamic nodes"
	commentRefused    = "slurmkube: refused: "
	commentJobSetDead = "slurmkube: JobSet failed: "
)

// Annotations on JobSets and pods beyond translate's.
const (
	// FencePendingAnnotation lists hosts whose pod of this job vanished
	// without the kubelet confirming its end; the job is not run again
	// until the fencer clears them.
	FencePendingAnnotation = "slurmkube.io/fence-pending"
	// FenceVerdictAnnotation on a gone pod records that the fencer found
	// it safe, so it is not asked again.
	FenceVerdictAnnotation = "slurmkube.io/fence-verdict"
)

const jobSetNameLabel = "jobset.sigs.k8s.io/jobset-name"

// key turns a bare name into the NamespacedName reconcilers are keyed by
// (Slurm job IDs and node names are not namespaced).
func key(name string) types.NamespacedName { return types.NamespacedName{Name: name} }

func jobKey(id uint64) string { return strconv.FormatUint(id, 10) }

// podIndex is the pod's completion index, -1 when absent.
func podIndex(pod *corev1.Pod) int32 {
	i, err := strconv.ParseInt(pod.Annotations[batchv1.JobCompletionIndexAnnotation], 10, 32)
	if err != nil {
		return -1
	}
	return int32(i)
}

// podKeys are the annotation names ownership reads pods with.
var podKeys = ownership.Keys{
	PortRange:   translate.NodePortRangeAnnotation,
	Port:        translate.NodePortAnnotation,
	Clear:       translate.NodeRecordClearAnnotation,
	Released:    translate.NodeRecordReleasedAnnotation,
	PortEnv:     translate.NodePortEnv,
	DefaultPort: translate.DefaultSlurmdPort,
}

// nodeLister lists slurmctld's node records (the node-port allocator).
type nodeLister interface {
	ListNodes(ctx context.Context) ([]slurm.Node, error)
}

// patchAnnotation sets one annotation on an object (no-op if unchanged).
func patchAnnotation(ctx context.Context, c client.Client, obj client.Object, k, v string) error {
	if obj.GetAnnotations()[k] == v {
		return nil
	}
	patched := obj.DeepCopyObject().(client.Object)
	ann := patched.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	if v == "" {
		delete(ann, k)
	} else {
		ann[k] = v
	}
	patched.SetAnnotations(ann)
	if err := client.IgnoreNotFound(c.Patch(ctx, patched, client.MergeFrom(obj))); err != nil {
		return fmt.Errorf("annotating %s: %w", obj.GetName(), err)
	}
	obj.SetAnnotations(ann)
	return nil
}

// addToCSV adds host to a comma-separated list, returning the new list.
func addToCSV(csv, item string) string {
	parts := splitCSV(csv)
	for _, p := range parts {
		if p == item {
			return csv
		}
	}
	parts = append(parts, item)
	return strings.Join(parts, ",")
}

func removeFromCSV(csv, item string) string {
	var out []string
	for _, p := range splitCSV(csv) {
		if p != item {
			out = append(out, p)
		}
	}
	return strings.Join(out, ",")
}

// splitCSV is the list's items, trimmed, blanks dropped.
func splitCSV(csv string) []string {
	items := strings.Split(csv, ",")
	for i := range items {
		items[i] = strings.TrimSpace(items[i])
	}
	return slices.DeleteFunc(items, func(s string) bool { return s == "" })
}
