// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package kueue reads the Kueue Workload that admits a JobSet. Workloads
// are handled unstructured: SlurmKube needs two conditions and a message.
package kueue

import (
	"context"
	"slices"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkloadGVK and WorkloadListGVK are Kueue's Workload kinds.
var (
	WorkloadGVK     = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	WorkloadListGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "WorkloadList"}
)

// Conditions reads the Workload's status conditions.
func Conditions(wl *unstructured.Unstructured) []metav1.Condition {
	if wl == nil {
		return nil
	}
	raw, found, err := unstructured.NestedSlice(wl.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}
	var out []metav1.Condition
	for _, r := range raw {
		m, ok := r.(map[string]any)
		if !ok {
			continue
		}
		var c metav1.Condition
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &c); err == nil {
			out = append(out, c)
		}
	}
	return out
}

// IsAdmitted: Kueue granted the Workload its quota.
func IsAdmitted(wl *unstructured.Unstructured) bool {
	return apimeta.IsStatusConditionTrue(Conditions(wl), "Admitted")
}

// IsEvicted: Kueue took the quota back (preemption, waitForPodsReady).
func IsEvicted(wl *unstructured.Unstructured) bool {
	return apimeta.IsStatusConditionTrue(Conditions(wl), "Evicted")
}

// QuotaReason is why the Workload is not admitted, for the job's comment.
func QuotaReason(wl *unstructured.Unstructured) string {
	for _, c := range Conditions(wl) {
		if c.Type == "QuotaReserved" && c.Status == metav1.ConditionFalse && c.Message != "" {
			return c.Message
		}
	}
	return ""
}

// WorkloadForOwner finds the Workload Kueue created for a JobSet.
func WorkloadForOwner(ctx context.Context, c client.Client, namespace, jobSetName string) (*unstructured.Unstructured, error) {
	var workloads unstructured.UnstructuredList
	workloads.SetGroupVersionKind(WorkloadListGVK)
	if err := c.List(ctx, &workloads, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	ownedBy := func(ref metav1.OwnerReference) bool { return ref.Kind == "JobSet" && ref.Name == jobSetName }
	i := slices.IndexFunc(workloads.Items, func(wl unstructured.Unstructured) bool {
		return slices.ContainsFunc(wl.GetOwnerReferences(), ownedBy)
	})
	if i < 0 {
		return nil, nil
	}
	return &workloads.Items[i], nil
}
