// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package fence

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

func node(name string, ready corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}}}
}

func reader(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = jobsetv1alpha2.AddToScheme(s)
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

// A pod object disappearing is not proof its processes stopped. Manual
// trusts a Ready kubelet, a reported termination, an operator's
// confirmation, or a host that left the cluster — and nothing else.
func TestManualFencerIsFailClosed(t *testing.T) {
	js := &jobsetv1alpha2.JobSet{ObjectMeta: metav1.ObjectMeta{Name: "slurmkube-1", Namespace: "ns"}}
	m := Manual{Reader: reader(t, node("ready", corev1.ConditionTrue), node("lost", corev1.ConditionUnknown)), ConfirmAnnotation: "slurmkube.io/fenced-hosts"}
	ctx := context.Background()
	cases := []struct {
		name string
		in   Incident
		safe bool
	}{
		{"ready host", Incident{Host: "ready", PodName: "p", Owner: js}, true},
		{"not ready host", Incident{Host: "lost", PodName: "p", Owner: js}, false},
		{"not ready but kubelet reported the end", Incident{Host: "lost", PodName: "p", PodTerminated: true, Owner: js}, true},
		{"never scheduled", Incident{Host: "", PodName: "p", Owner: js}, true},
		{"host gone from the cluster", Incident{Host: "vanished", PodName: "p", Owner: js}, true},
	}
	for _, c := range cases {
		v, err := m.Confirm(ctx, c.in)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if v.Safe != c.safe {
			t.Errorf("%s: safe=%v (%s), want %v", c.name, v.Safe, v.Reason, c.safe)
		}
	}
	// The operator confirms by listing the host on the owner.
	fencedJS := js.DeepCopy()
	fencedJS.Annotations = map[string]string{"slurmkube.io/fenced-hosts": "other,lost"}
	if v, _ := m.Confirm(ctx, Incident{Host: "lost", PodName: "p", Owner: fencedJS}); !v.Safe {
		t.Errorf("operator-confirmed host must be safe: %s", v.Reason)
	}
	if v, _ := (TrustKubernetes{}).Confirm(ctx, Incident{Host: "lost"}); !v.Safe {
		t.Error("trustKubernetes is always safe")
	}
}
