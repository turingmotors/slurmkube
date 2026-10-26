// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"strconv"
	"strings"
)

// GPUSpec is one GPU TRES entry: "gres/gpu:2", "gres/gpu:h100:2",
// "gres/gpu=1" (26.05 writes "=" for --gpus-per-task), "gpu:26"
// (cpus_per_tres), "gpu:250880" (memory_per_tres).
type GPUSpec struct {
	Type  string
	Count int64
}

// parseGPUSpec reads one entry; ok is false for non-GPU entries.
func parseGPUSpec(entry string) (GPUSpec, bool) {
	s := strings.TrimSpace(entry)
	s = strings.TrimPrefix(s, "gres/")
	rest, ok := strings.CutPrefix(s, "gpu")
	if !ok || rest == "" || (rest[0] != ':' && rest[0] != '=') {
		return GPUSpec{}, false
	}
	parts := strings.Split(rest[1:], ":")
	n, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil || n < 0 {
		return GPUSpec{}, false
	}
	g := GPUSpec{Count: n}
	if len(parts) >= 2 {
		if _, numeric := strconv.ParseInt(parts[0], 10, 64); numeric != nil {
			g.Type = parts[0]
		}
	}
	return g, true
}

// findGPU finds the GPU entry in a comma-separated TRES list.
func findGPU(list string) (GPUSpec, bool) {
	for _, entry := range strings.Split(list, ",") {
		if g, ok := parseGPUSpec(entry); ok {
			return g, true
		}
	}
	return GPUSpec{}, false
}
