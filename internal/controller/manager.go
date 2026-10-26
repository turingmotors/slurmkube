// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/translate"
)

// Scheme is the manager's scheme: core, batch and JobSet types. Kueue
// Workloads are read unstructured.
func Scheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	if err := jobsetv1alpha2.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

// CacheOptions confines the informer cache to the workload namespace (the
// chart's RBAC is namespaced) and to SlurmKube's own pods. Nodes are
// cluster-scoped and read uncached by the fencer, so they are not listed
// here: a cluster-scoped type with a namespace setting is rejected by
// controller-runtime.
func CacheOptions(namespace string) cache.Options {
	return cache.Options{
		DefaultNamespaces: map[string]cache.Config{namespace: {}},
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}: {Label: labels.SelectorFromSet(labels.Set{translate.ManagedByLabel: translate.ManagedByValue})},
		},
	}
}
