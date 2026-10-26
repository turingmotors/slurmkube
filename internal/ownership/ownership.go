// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package ownership answers one question without touching any API: given
// a Slurm dynamic node record and the pods that could be behind it, which
// pod owns the record, is slurmctld reaching that pod, and what should the
// controller do about it. Every function is deterministic in its inputs
// so the invariants can be tested exhaustively.
//
// Why identity matters (slurmctld 26.05, read in its source and
// reproduced): a registration under an EXISTING node name updates the record's
// address but not its port, and slurmd runs a batch launch without
// checking that its own name is in the job's hostlist. A record that
// outlives its pod therefore sends the job's next launch to whatever
// slurmd holds the old port on that host — another job's pod.
//
// Identity: slurmd registers with --extra <pod UID>; slurmctld takes the
// value on every registration (slurmd -b reports a boot), so the record's
// Extra is the UID of the pod that last registered under the name — its
// owner. Records without Extra (older pods) fall back to the endpoint.
package ownership

import (
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/turingmotors/slurmkube/internal/slurm"
)

// PodView is what ownership needs to know about a pod. Build it with
// FromPod; tests may construct it directly.
type PodView struct {
	Name string
	UID  string
	// Gone: finished (Succeeded/Failed) or being deleted. Its slurmd is
	// not, or soon not, serving.
	Gone bool
	// Running: the pod's containers run (slurmd may be up).
	Running bool
	// Address and Port are where its slurmd listens; Port 0 when not yet
	// known (no stamp).
	Address string
	Port    int32
	// Gated: slurmd waits for the controller's clearance before starting.
	Gated bool
	// ClearedAt is when the clearance was given (zero: not cleared).
	ClearedAt time.Time
	// Released: the controller recorded that no record points at it.
	Released bool
	// Host is the Kubernetes node the pod runs on (fencing).
	Host string
}

// FromPod reads a PodView off a pod using the given annotation keys.
func FromPod(pod *corev1.Pod, k Keys) PodView {
	v := PodView{
		Name:    pod.Name,
		UID:     string(pod.UID),
		Gone:    pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed,
		Running: pod.Status.Phase == corev1.PodRunning,
		Host:    pod.Spec.NodeName,
	}
	v.Address, v.Port = endpoint(pod, k)
	_, v.Gated = pod.Annotations[k.PortRange]
	if s := pod.Annotations[k.Clear]; s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			v.ClearedAt = t
		} else {
			v.ClearedAt = time.Unix(1, 0) // cleared, when unknown: never times out
		}
	}
	v.Released = pod.Annotations[k.Released] != ""
	return v
}

// Keys are the annotation and env names a PodView is read from.
type Keys struct {
	PortRange   string // range annotation: present means the pod is gated
	Port        string // stamped port annotation
	Clear       string // clearance annotation
	Released    string // released annotation
	PortEnv     string // literal port env (pod network)
	DefaultPort int32  // slurmd's port with nodePorts off
}

func endpoint(pod *corev1.Pod, k Keys) (string, int32) {
	addr := pod.Status.PodIP
	if pod.Spec.HostNetwork {
		addr = pod.Status.HostIP
	}
	if s, ok := pod.Annotations[k.Port]; ok {
		if p, err := strconv.ParseInt(s, 10, 32); err == nil {
			return addr, int32(p)
		}
		return addr, 0
	}
	for _, c := range pod.Spec.Containers {
		for _, e := range c.Env {
			if e.Name != k.PortEnv {
				continue
			}
			if e.ValueFrom != nil {
				return addr, 0
			}
			if p, err := strconv.ParseInt(e.Value, 10, 32); err == nil {
				return addr, int32(p)
			}
			return addr, 0
		}
	}
	return addr, k.DefaultPort
}

// Owner is the pod a record belongs to: the pod whose UID the record
// carries; for a record without identity, the pod at its endpoint (a live
// one before a gone one). nil when no known pod matches.
func Owner(node *slurm.Node, pods []PodView) *PodView {
	if node.Extra != "" {
		for i := range pods {
			if pods[i].UID == node.Extra {
				return &pods[i]
			}
		}
		return nil
	}
	var gone *PodView
	for i := range pods {
		p := &pods[i]
		if p.Address == "" || p.Port == 0 || p.Address != node.Address || p.Port != node.Port {
			continue
		}
		if !p.Gone {
			return p
		}
		if gone == nil {
			gone = p
		}
	}
	return gone
}

// Reaches reports whether slurmctld's RPCs for the record arrive at the
// pod: the record's endpoint is the pod's.
func Reaches(node *slurm.Node, p *PodView) bool {
	return p != nil && p.Address != "" && p.Port != 0 && node.Address == p.Address && node.Port == p.Port
}

// Live returns the pods that are not gone.
func Live(pods []PodView) []PodView {
	var out []PodView
	for _, p := range pods {
		if !p.Gone {
			out = append(out, p)
		}
	}
	return out
}

// Action is what the controller should do for one node name.
type Action int

const (
	// Nothing: healthy, or waiting.
	Nothing Action = iota
	// ClearPod: let the single live gated pod start slurmd.
	ClearPod
	// ReleaseRecord: the record's owner is gone; delete the record (DOWN
	// first if slurmctld still holds the job on it) and mark the owner
	// released once it is gone.
	ReleaseRecord
	// ReleaseVanished: the record's owner is unknown to the cache and has
	// been for the grace; release it.
	ReleaseVanished
	// ReleaseStale: a live owner is not reachable at the record's
	// endpoint; release the record and delete the pod.
	ReleaseStale
	// ReplacePod: a cleared pod ran slurmd for the registration timeout
	// without a record; delete it so the JobSet replaces it.
	ReplacePod
	// WaitVanished: the owner is unknown but the grace has not passed.
	WaitVanished
	// Conflict: two live pods claim the name; clear neither.
	Conflict
)

func (a Action) String() string {
	switch a {
	case Nothing:
		return "nothing"
	case ClearPod:
		return "clear-pod"
	case ReleaseRecord:
		return "release-record"
	case ReleaseVanished:
		return "release-vanished"
	case ReleaseStale:
		return "release-stale"
	case ReplacePod:
		return "replace-pod"
	case WaitVanished:
		return "wait-vanished"
	case Conflict:
		return "conflict"
	}
	return fmt.Sprintf("action(%d)", int(a))
}

// Input is everything Decide looks at.
type Input struct {
	// Record is slurmctld's record for the name, nil when absent.
	Record *slurm.Node
	// Pods are the pods of this node name (any state).
	Pods []PodView
	// JobLive: the Slurm job is known and not terminal.
	JobLive bool
	// Now is the clock.
	Now time.Time
	// VanishedSince is when the owner was first found unknown (zero: not
	// before). The controller keeps it between decisions.
	VanishedSince time.Time
	// VanishedGrace is how long an unknown owner is tolerated.
	VanishedGrace time.Duration
	// RegistrationTimeout is how long a cleared, running pod may have no
	// record.
	RegistrationTimeout time.Duration
}

// Decision is Decide's result: the action and the pod it concerns.
type Decision struct {
	Action Action
	// Pod is the owner (Release*), the pod to clear or replace, or nil.
	Pod *PodView
	// ReleasePods are the gone pods no record points at: their ports may
	// be reused (mark released).
	ReleasePods []PodView
	Reason      string
}

// Decide applies the ownership rules to one node name.
func Decide(in Input) Decision {
	if in.Record == nil {
		return decideAbsent(in)
	}
	owner := Owner(in.Record, in.Pods)
	d := Decision{Pod: owner}
	for _, p := range in.Pods {
		if p.Gone && (owner == nil || p.UID != owner.UID) {
			d.ReleasePods = append(d.ReleasePods, p)
		}
	}
	switch {
	case owner == nil:
		if in.VanishedSince.IsZero() || in.Now.Sub(in.VanishedSince) < in.VanishedGrace {
			d.Action = WaitVanished
			d.Reason = fmt.Sprintf("record (%s:%d, extra %q) belongs to no known pod; waiting out the grace", in.Record.Address, in.Record.Port, in.Record.Extra)
			return d
		}
		d.Action = ReleaseVanished
		d.Reason = fmt.Sprintf("record (%s:%d, extra %q) belongs to no known pod", in.Record.Address, in.Record.Port, in.Record.Extra)
	case owner.Gone:
		d.Action = ReleaseRecord
		d.Reason = "owner pod " + owner.Name + " is gone"
	case !Reaches(in.Record, owner):
		if !owner.Running {
			d.Action = Nothing // no endpoint yet
			return d
		}
		d.Action = ReleaseStale
		d.Reason = fmt.Sprintf("record points at %s:%d but owner pod %s listens on %s:%d", in.Record.Address, in.Record.Port, owner.Name, owner.Address, owner.Port)
	default:
		d.Action = Nothing
		if owner.Gated && owner.ClearedAt.IsZero() {
			d.Action = ClearPod // registered before the gate existed (upgrade)
		}
	}
	return d
}

func decideAbsent(in Input) Decision {
	d := Decision{}
	for _, p := range in.Pods {
		if p.Gone {
			d.ReleasePods = append(d.ReleasePods, p)
		}
	}
	live := Live(in.Pods)
	switch {
	case len(live) > 1:
		d.Action = Conflict
		d.Reason = fmt.Sprintf("%d live pods claim the name", len(live))
		return d
	case len(live) == 0:
		return d
	}
	p := live[0]
	d.Pod = &live[0]
	if !p.Gated {
		return d
	}
	if !p.ClearedAt.IsZero() {
		if p.Running && in.RegistrationTimeout > 0 && in.Now.Sub(p.ClearedAt) > in.RegistrationTimeout {
			d.Action = ReplacePod
			d.Reason = fmt.Sprintf("pod %s has run slurmd since %s without a record", p.Name, p.ClearedAt.UTC().Format(time.RFC3339))
		}
		return d
	}
	if !in.JobLive {
		d.Reason = "job is over or unknown; not clearing"
		return d
	}
	d.Action = ClearPod
	return d
}

// Served reports whether a job may be released onto the node: the record
// is a live pod's at the recorded endpoint. A record without identity and
// no pods to check against passes (legacy). The string explains a false.
func Served(node *slurm.Node, pods []PodView) (bool, string) {
	if len(pods) == 0 {
		if node.Extra == "" {
			return true, ""
		}
		return false, "no pod of the job is known for it"
	}
	owner := Owner(node, pods)
	switch {
	case owner == nil:
		return false, fmt.Sprintf("its record (%s:%d, pod %q) belongs to no existing pod", node.Address, node.Port, node.Extra)
	case owner.Gone:
		return false, fmt.Sprintf("its record belongs to pod %s, which is gone; waiting for the replacement to register", owner.Name)
	case !Reaches(node, owner):
		return false, fmt.Sprintf("its record points at %s:%d but pod %s listens on %s:%d", node.Address, node.Port, owner.Name, owner.Address, owner.Port)
	}
	return true, ""
}

// MayDown reports whether a record may be marked DOWN: never while a live
// pod serves it at the recorded endpoint.
func MayDown(node *slurm.Node, pods []PodView) bool {
	owner := Owner(node, pods)
	return owner == nil || owner.Gone || !Reaches(node, owner)
}
