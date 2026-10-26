// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"fmt"
	"strings"
	"time"

	"github.com/turingmotors/slurmkube/internal/slurm"
)

// waitReason says why a held job should not get a JobSet yet: its
// dependency is unsatisfied (or can never be), or its begin time is in
// the future. Pods created earlier would hold Kueue quota and burn the
// Job's deadline while Slurm waits. slurmctld keeps evaluating the
// dependency of a held job: "afterany:X(unfulfilled)" empties once X
// ends; "afterok:X(failed)" never clears.
func waitReason(job *slurm.Job, now time.Time) string {
	if dep := strings.TrimSpace(job.Dependency); dep != "" {
		if strings.Contains(dep, "(failed)") {
			return "the dependency can never be satisfied (" + dep + ")"
		}
		return "held until the dependency is satisfied (" + dep + ")"
	}
	if job.EligibleTime.Set && !job.EligibleTime.Infinite && job.EligibleTime.Number > 0 {
		begin := time.Unix(int64(job.EligibleTime.Number), 0)
		if begin.After(now.Add(5 * time.Second)) {
			return fmt.Sprintf("held until the begin time %s", begin.UTC().Format(time.RFC3339))
		}
	}
	return ""
}

// unusableNodeState returns the state flag that makes a freshly registered
// dynamic node unable to run its job, or "".
func unusableNodeState(states []string) string {
	for _, s := range states {
		switch strings.ToUpper(s) {
		case "INVALID_REG", "DRAIN", "DOWN", "FAIL", "INVAL":
			return strings.ToUpper(s)
		}
	}
	return ""
}
