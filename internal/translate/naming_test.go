// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import "testing"

func TestNamingRoundTrips(t *testing.T) {
	for _, n := range []Naming{{}, NewNaming(""), NewNaming("hpc-w")} {
		name := n.NodeName(4041, 3)
		id, idx, ok := n.ParseNodeName(name)
		if !ok || id != 4041 || idx != 3 {
			t.Errorf("%q: ParseNodeName(%q) = %d,%d,%v", n.NodeNamePrefix, name, id, idx, ok)
		}
		if got := n.NodeNames(7, 2); len(got) != 2 || got[0] != n.NodeName(7, 0) || got[1] != n.NodeName(7, 1) {
			t.Errorf("NodeNames = %v", got)
		}
	}
	def := NewNaming("")
	if def.NodeName(5, 0) != "slurmkube-w0-5" || def.JobSetName(5) != "slurmkube-5" || def.PinFeature(5) != "slurmkube-pin-5" {
		t.Errorf("default names: %s %s %s", def.NodeName(5, 0), def.JobSetName(5), def.PinFeature(5))
	}
	if got := def.NodeNameEnvValue(5); got != "slurmkube-w$("+NodeIndexEnv+")-5" {
		t.Errorf("NodeNameEnvValue = %q", got)
	}
	custom := NewNaming("hpc-w")
	if custom.NodeName(5, 1) != "hpc-w1-5" {
		t.Errorf("custom prefix: %s", custom.NodeName(5, 1))
	}
	if _, _, ok := custom.ParseNodeName("slurmkube-w0-5"); ok {
		t.Error("a custom naming must not claim the default prefix's records")
	}
	for _, bad := range []string{"gpu-node-1", "slurmkube-w0-", "slurmkube-wx-5", "slurmkube-w0-5-6", "slurmkube-w0-5 "} {
		if _, _, ok := def.ParseNodeName(bad); ok {
			t.Errorf("ParseNodeName(%q) must fail", bad)
		}
	}
}

func TestIsPinFeature(t *testing.T) {
	n := NewNaming("")
	for s, want := range map[string]bool{"slurmkube-pin-12": true, " slurmkube-pin-12 ": true, "slurmkube-pin-": false,
		"slurmkube-pin-x": false, "a100": false, "": false, "slurmkube-pin-1&a100": false} {
		if got := n.IsPinFeature(s); got != want {
			t.Errorf("IsPinFeature(%q) = %v, want %v", s, got, want)
		}
	}
}
