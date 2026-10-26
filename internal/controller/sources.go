// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/translate"
)

// eventOf is a reconcile request carried on a channel source.
type eventOf = event.TypedGenericEvent[reconcile.Request]

// Event is the request type carried on the poller's channels.
type Event = eventOf

// channelSource turns the poller's channel into a controller source.
func channelSource(ch chan eventOf) source.TypedSource[reconcile.Request] {
	return source.TypedChannel(ch, handler.TypedEnqueueRequestsFromMapFunc(
		func(_ context.Context, req reconcile.Request) []reconcile.Request { return []reconcile.Request{req} }))
}

// ownPodsOnly admits only SlurmKube's slurmd pods.
func ownPodsOnly() builder.Predicates {
	return builder.WithPredicates(predicate.NewPredicateFuncs(IsOwnPod))
}

// podToNodeName maps a pod event to its dynamic node name.
func podToNodeName(naming translate.Naming) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		pod, ok := o.(*corev1.Pod)
		if !ok {
			return nil
		}
		id, err := strconv.ParseUint(pod.Labels[translate.SlurmJobIDLabel], 10, 64)
		if err != nil {
			return nil
		}
		idx := podIndex(pod)
		if idx < 0 {
			return nil
		}
		return []reconcile.Request{{NamespacedName: key(naming.NodeName(id, idx))}}
	})
}

// podToJob maps a pod event to its Slurm job.
func podToJob() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		id := o.GetLabels()[translate.SlurmJobIDLabel]
		if id == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: key(id)}}
	})
}

// jobSetToJob maps a JobSet event to its Slurm job.
func jobSetToJob() handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []reconcile.Request {
		if _, ok := o.(*jobsetv1alpha2.JobSet); !ok {
			return nil
		}
		id := o.GetLabels()[translate.SlurmJobIDLabel]
		if id == "" {
			return nil
		}
		return []reconcile.Request{{NamespacedName: key(id)}}
	})
}

// workloadToJob maps a Kueue Workload (owned by a JobSet) to the job.
func workloadToJob(c client.Client, namespace string) handler.EventHandler {
	return handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
		for _, ref := range o.GetOwnerReferences() {
			if ref.Kind != "JobSet" {
				continue
			}
			var js jobsetv1alpha2.JobSet
			if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &js); err != nil {
				continue
			}
			if id := js.Labels[translate.SlurmJobIDLabel]; id != "" {
				return []reconcile.Request{{NamespacedName: key(id)}}
			}
		}
		return nil
	})
}

// ownJobSetsOnly admits only SlurmKube's JobSets (and this instance's).
func ownJobSetsOnly(instance string) builder.Predicates {
	return builder.WithPredicates(predicate.NewPredicateFuncs(func(o client.Object) bool {
		l := o.GetLabels()
		return l[translate.ManagedByLabel] == translate.ManagedByValue && (instance == "" || l[translate.InstanceLabel] == instance)
	}))
}

// workloadObject is the unstructured Kueue Workload for watches.
func workloadObject() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("kueue.x-k8s.io/v1beta2")
	u.SetKind("Workload")
	return u
}
