// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package ownership

import (
	"testing"
	"time"

	"github.com/turingmotors/slurmkube/internal/slurm"
)

func rec(extra, addr string, port int32) *slurm.Node {
	return &slurm.Node{Name: "slurmkube-w0-1", Extra: extra, Address: addr, Port: port, Features: []string{"slurmkube-pin-1"}}
}

func pod(name, addr string, port int32, gone, running bool) PodView {
	return PodView{Name: name, UID: "uid-" + name, Address: addr, Port: port, Gone: gone, Running: running, Gated: true, Host: "host-a"}
}

var (
	t0    = time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	grace = 15 * time.Second
	reg   = 3 * time.Minute
)

func decide(record *slurm.Node, pods []PodView, mod func(*Input)) Decision {
	in := Input{Record: record, Pods: pods, JobLive: true, Now: t0, VanishedGrace: grace, RegistrationTimeout: reg}
	if mod != nil {
		mod(&in)
	}
	return Decide(in)
}

// Invariant 2: NodeName is not an identity. The record names its owner by
// UID; on a shared host and a reused port, old and new pod differ only by
// UID and the record follows the UID.
func TestOwnerIsTheUIDNotTheEndpoint(t *testing.T) {
	old := pod("old", "10.0.0.2", 63001, true, false)
	cur := pod("new", "10.0.0.2", 63001, false, true)
	pods := []PodView{old, cur}
	if o := Owner(rec("uid-new", "10.0.0.2", 63001), pods); o == nil || o.Name != "new" {
		t.Errorf("owner = %v, want new", o)
	}
	if o := Owner(rec("uid-old", "10.0.0.2", 63001), pods); o == nil || o.Name != "old" {
		t.Errorf("owner = %v, want old", o)
	}
	if o := Owner(rec("uid-never-seen", "10.0.0.2", 63001), pods); o != nil {
		t.Errorf("record of an unknown UID owned by %s, want nobody (never by endpoint)", o.Name)
	}
	// Legacy record (no identity): the live pod at the endpoint, then a gone one.
	if o := Owner(rec("", "10.0.0.2", 63001), pods); o == nil || o.Name != "new" {
		t.Errorf("legacy owner = %v, want the live pod", o)
	}
	if o := Owner(rec("", "10.0.0.2", 63001), []PodView{old}); o == nil || o.Name != "old" {
		t.Errorf("legacy owner with only a gone pod = %v, want old", o)
	}
	if o := Owner(rec("", "10.0.0.9", 63001), pods); o != nil {
		t.Errorf("legacy record at an unknown endpoint owned by %s", o.Name)
	}
}

// Invariant 3: an old pod's event does not release the new pod's record.
func TestLateOldPodDoesNotReleaseTheNewRecord(t *testing.T) {
	old := pod("old", "10.0.0.2", 63001, true, false)
	cur := pod("new", "10.0.0.2", 63001, false, true)
	cur.ClearedAt = t0.Add(-time.Minute)
	d := decide(rec("uid-new", "10.0.0.2", 63001), []PodView{old, cur}, nil)
	if d.Action != Nothing {
		t.Errorf("action = %s (%s), want nothing", d.Action, d.Reason)
	}
	if len(d.ReleasePods) != 1 || d.ReleasePods[0].Name != "old" {
		t.Errorf("release pods = %v, want the old pod (no record points at it)", d.ReleasePods)
	}
}

// Invariant 1/4: the owner is gone → release; the port stays the owner's
// until then (the owner is not in ReleasePods).
func TestGoneOwnerIsReleasedAndKeepsItsPortUntilThen(t *testing.T) {
	old := pod("old", "10.0.0.2", 63001, true, false)
	cur := pod("new", "10.0.0.2", 63000, false, false)
	d := decide(rec("uid-old", "10.0.0.2", 63001), []PodView{old, cur}, nil)
	if d.Action != ReleaseRecord || d.Pod == nil || d.Pod.Name != "old" {
		t.Errorf("action = %s pod=%v, want release of old", d.Action, d.Pod)
	}
	if len(d.ReleasePods) != 0 {
		t.Errorf("the owner's port must not be freed before its record is gone: %v", d.ReleasePods)
	}
}

// A replacement is cleared only when no record exists under the name;
// never for a terminal job; never when two live pods claim the name.
func TestClearanceRequiresAnEmptyName(t *testing.T) {
	old := pod("old", "10.0.0.2", 63001, true, false)
	cur := pod("new", "10.0.0.2", 63000, false, false)
	if d := decide(rec("uid-old", "10.0.0.2", 63001), []PodView{old, cur}, nil); d.Action == ClearPod {
		t.Error("a pod must not be cleared while a record exists under its name")
	}
	d := decide(nil, []PodView{old, cur}, nil)
	if d.Action != ClearPod || d.Pod.Name != "new" {
		t.Errorf("no record: action = %s pod=%v, want clear new", d.Action, d.Pod)
	}
	if len(d.ReleasePods) != 1 || d.ReleasePods[0].Name != "old" {
		t.Errorf("no record: gone pods free their ports: %v", d.ReleasePods)
	}
	if d := decide(nil, []PodView{cur}, func(in *Input) { in.JobLive = false }); d.Action != Nothing {
		t.Errorf("terminal job: action = %s, want nothing", d.Action)
	}
	other := pod("other", "10.0.0.3", 63000, false, false)
	if d := decide(nil, []PodView{cur, other}, nil); d.Action != Conflict {
		t.Errorf("two live pods: action = %s, want conflict", d.Action)
	}
	ungated := cur
	ungated.Gated = false
	if d := decide(nil, []PodView{ungated}, nil); d.Action != Nothing {
		t.Errorf("ungated pod needs no clearance: %s", d.Action)
	}
}

// Invariant 6: an owner the cache does not know is tolerated for the
// grace, then released; the grace is time, not ticks.
func TestVanishedOwnerWaitsOutTheGrace(t *testing.T) {
	cur := pod("new", "10.0.0.2", 63000, false, false)
	r := rec("uid-vanished", "10.0.0.2", 63001)
	if d := decide(r, []PodView{cur}, nil); d.Action != WaitVanished {
		t.Errorf("first sighting: %s", d.Action)
	}
	if d := decide(r, []PodView{cur}, func(in *Input) { in.VanishedSince = t0.Add(-grace / 2) }); d.Action != WaitVanished {
		t.Errorf("inside the grace: %s", d.Action)
	}
	if d := decide(r, []PodView{cur}, func(in *Input) { in.VanishedSince = t0.Add(-grace) }); d.Action != ReleaseVanished {
		t.Errorf("after the grace: %s", d.Action)
	}
}

// Invariant 4: a live owner the record does not reach (stale port) is
// released with its pod; a pending owner without an endpoint is waited on.
func TestStaleEndpointReleasesRecordAndPod(t *testing.T) {
	cur := pod("new", "10.0.0.2", 63000, false, true)
	if d := decide(rec("uid-new", "10.0.0.2", 63001), []PodView{cur}, nil); d.Action != ReleaseStale {
		t.Errorf("stale: %s", d.Action)
	}
	pending := cur
	pending.Running = false
	pending.Port = 0
	if d := decide(rec("uid-new", "10.0.0.2", 63001), []PodView{pending}, nil); d.Action != Nothing {
		t.Errorf("pending owner: %s", d.Action)
	}
}

// A cleared pod that runs without a record for the registration timeout
// is replaced; before the timeout it is left alone.
func TestRegistrationTimeout(t *testing.T) {
	cur := pod("new", "10.0.0.2", 63000, false, true)
	cur.ClearedAt = t0.Add(-reg + time.Second)
	if d := decide(nil, []PodView{cur}, nil); d.Action != Nothing {
		t.Errorf("inside the timeout: %s", d.Action)
	}
	cur.ClearedAt = t0.Add(-reg - time.Second)
	if d := decide(nil, []PodView{cur}, nil); d.Action != ReplacePod {
		t.Errorf("past the timeout: %s", d.Action)
	}
}

// Invariant 5: a job is served only by a live owner at the recorded
// endpoint; a legacy record with no pods passes.
func TestServedAndMayDown(t *testing.T) {
	old := pod("old", "10.0.0.2", 63001, true, false)
	cur := pod("new", "10.0.0.2", 63000, false, true)
	if ok, _ := Served(rec("uid-old", "10.0.0.2", 63001), []PodView{old, cur}); ok {
		t.Error("gone owner must not serve")
	}
	if ok, _ := Served(rec("uid-new", "10.0.0.2", 63001), []PodView{old, cur}); ok {
		t.Error("stale endpoint must not serve")
	}
	if ok, why := Served(rec("uid-new", "10.0.0.2", 63000), []PodView{old, cur}); !ok {
		t.Errorf("live owner at the endpoint must serve: %s", why)
	}
	if ok, _ := Served(rec("", "10.0.0.9", 6818), nil); !ok {
		t.Error("legacy record with no pods passes")
	}
	if ok, _ := Served(rec("uid-x", "10.0.0.9", 6818), nil); ok {
		t.Error("identity record with no pods does not pass")
	}
	if MayDown(rec("uid-new", "10.0.0.2", 63000), []PodView{cur}) {
		t.Error("a record a live pod serves must never be marked DOWN")
	}
	if !MayDown(rec("uid-old", "10.0.0.2", 63001), []PodView{old, cur}) || !MayDown(rec("uid-x", "10.0.0.2", 63001), nil) {
		t.Error("a record with a gone or unknown owner may be marked DOWN")
	}
}

// Decide is a pure function: the same input gives the same decision.
func TestDecideIsDeterministic(t *testing.T) {
	old := pod("old", "10.0.0.2", 63001, true, false)
	cur := pod("new", "10.0.0.2", 63000, false, true)
	a := decide(rec("uid-old", "10.0.0.2", 63001), []PodView{old, cur}, nil)
	for i := 0; i < 10; i++ {
		b := decide(rec("uid-old", "10.0.0.2", 63001), []PodView{cur, old}, nil)
		if a.Action != b.Action || a.Pod.Name != b.Pod.Name {
			t.Fatalf("decision changed with pod order: %v vs %v", a, b)
		}
	}
}
