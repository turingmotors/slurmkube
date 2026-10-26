// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package kueue

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func workload(owner string, conds ...map[string]any) *unstructured.Unstructured {
	wl := &unstructured.Unstructured{}
	wl.SetGroupVersionKind(WorkloadGVK)
	wl.SetNamespace("slurm-jobs")
	wl.SetName("wl-" + owner)
	if owner != "" {
		wl.SetOwnerReferences([]metav1.OwnerReference{{Kind: "JobSet", Name: owner}})
	}
	if len(conds) > 0 {
		items := make([]any, len(conds))
		for i, c := range conds {
			items[i] = c
		}
		_ = unstructured.SetNestedSlice(wl.Object, items, "status", "conditions")
	}
	return wl
}

func TestConditions(t *testing.T) {
	cases := []struct {
		name              string
		wl                *unstructured.Unstructured
		admitted, evicted bool
		reason            string
	}{
		{"nil", nil, false, false, ""},
		{"no conditions", workload("a"), false, false, ""},
		{"admitted", workload("a", map[string]any{"type": "Admitted", "status": "True"}), true, false, ""},
		{"not admitted", workload("a", map[string]any{"type": "Admitted", "status": "False"}), false, false, ""},
		{"waiting for quota", workload("a",
			map[string]any{"type": "QuotaReserved", "status": "False", "message": "insufficient cpu"},
			map[string]any{"type": "Admitted", "status": "False"}), false, false, "insufficient cpu"},
		{"quota reserved", workload("a", map[string]any{"type": "QuotaReserved", "status": "True", "message": "ok"}), false, false, ""},
		{"evicted", workload("a",
			map[string]any{"type": "Admitted", "status": "False"},
			map[string]any{"type": "Evicted", "status": "True", "reason": "Preempted"}), false, true, ""},
		{"malformed entry skipped", workload("a", map[string]any{"type": "Admitted", "status": "True"}, map[string]any{"status": int64(5)}), true, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if IsAdmitted(tc.wl) != tc.admitted || IsEvicted(tc.wl) != tc.evicted || QuotaReason(tc.wl) != tc.reason {
				t.Errorf("admitted %v evicted %v reason %q", IsAdmitted(tc.wl), IsEvicted(tc.wl), QuotaReason(tc.wl))
			}
		})
	}
}

func TestWorkloadForOwner(t *testing.T) {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(WorkloadGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(WorkloadListGVK, &unstructured.UnstructuredList{})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(workload("slurmkube-7"), workload("slurmkube-8")).Build()

	cases := []struct {
		namespace, jobSet, want string
	}{
		{"slurm-jobs", "slurmkube-7", "wl-slurmkube-7"},
		{"slurm-jobs", "slurmkube-8", "wl-slurmkube-8"},
		{"slurm-jobs", "slurmkube-9", ""},
		{"elsewhere", "slurmkube-7", ""},
	}
	for _, tc := range cases {
		wl, err := WorkloadForOwner(context.Background(), c, tc.namespace, tc.jobSet)
		got := ""
		if wl != nil {
			got = wl.GetName()
		}
		if err != nil || got != tc.want {
			t.Errorf("%s/%s: got %q, %v; want %q", tc.namespace, tc.jobSet, got, err, tc.want)
		}
	}
}
