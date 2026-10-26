// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/slurm"
)

type slurmJobAlias = slurm.Job

type jobSetAlias = jobsetv1alpha2.JobSet

func nsObject(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func jobSetKey(ns string, id uint64) client.ObjectKey {
	return client.ObjectKey{Namespace: ns, Name: testNaming.JobSetName(id)}
}
