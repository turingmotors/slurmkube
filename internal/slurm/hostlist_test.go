// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"slices"
	"testing"
)

func TestExpandHostlist(t *testing.T) {
	cases := map[string][]string{
		"gpu-1":               {"gpu-1"},
		"gpu-[1-3,7]":         {"gpu-1", "gpu-2", "gpu-3", "gpu-7"},
		"a,b[01-02]c":         {"a", "b01c", "b02c"},
		"n[1-2],m[3]":         {"n1", "n2", "m3"},
		" x , y ":             {"x", "y"},
		"":                    nil,
		"rack[1-2]-node[1-2]": nil, // nested groups are not supported
	}
	for spec, want := range cases {
		got, err := ExpandHostlist(spec)
		if want == nil && spec != "" {
			if err == nil {
				t.Errorf("%q: expected an error, got %v", spec, got)
			}
			continue
		}
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("%q = %v, %v; want %v", spec, got, err, want)
		}
	}
	for _, bad := range []string{"a]", "a[1", "a[2-1]", "a[x]", "a[1-]"} {
		if _, err := ExpandHostlist(bad); err == nil {
			t.Errorf("%q must fail", bad)
		}
	}
}
