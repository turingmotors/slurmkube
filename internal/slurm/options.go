// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

// State ---------------------------------------------------------------

func (j *Job) IsPending() bool { return slices.Contains(j.JobState, "PENDING") }

// IsHeld: slurmctld reports a hold as hold=true, or as priority 0 with a
// JobHeld* reason.
func (j *Job) IsHeld() bool {
	switch {
	case j.Hold:
		return true
	case !j.Priority.Set || j.Priority.Number != 0:
		return false
	}
	return strings.HasPrefix(j.StateReason, "JobHeld")
}

var terminalStates = []string{"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY"}

// IsTerminal: the job is over in Slurm's eyes.
func (j *Job) IsTerminal() bool {
	for _, s := range j.JobState {
		if slices.Contains(terminalStates, s) {
			return true
		}
	}
	return false
}

// Arrays ---------------------------------------------------------------

func (j *Job) IsArray() bool { return j.ArrayJobID.Set && j.ArrayJobID.Number != 0 }

// IsArrayRoot: the record that holds the not yet started tasks.
func (j *Job) IsArrayRoot() bool { return j.IsArray() && j.JobID == j.ArrayJobID.Number }

// PinID is the ID the job's JobSet and nodes are named after: the array's
// root ID for a task, the job ID otherwise.
func (j *Job) PinID() uint64 {
	if j.IsArray() {
		return j.ArrayJobID.Number
	}
	return j.JobID
}

// ArrayPendingTasks counts the tasks still in the root ("0-3,7%2" → 5).
func (j *Job) ArrayPendingTasks() int64 {
	spec, _, _ := strings.Cut(j.ArrayTaskString, "%")
	if spec == "" {
		return 1
	}
	var n int64
	for _, part := range strings.Split(spec, ",") {
		part, stepStr, hasStep := strings.Cut(part, ":")
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.ParseInt(lo, 10, 64)
		if err != nil {
			continue
		}
		if !isRange {
			n++
			continue
		}
		b, err := strconv.ParseInt(hi, 10, 64)
		if err != nil || b < a {
			continue
		}
		step := int64(1)
		if hasStep {
			if s, err := strconv.ParseInt(stepStr, 10, 64); err == nil && s > 0 {
				step = s
			}
		}
		n += (b-a)/step + 1
	}
	if n == 0 {
		return 1
	}
	return n
}

// ArrayThrottle is the %N limit, 0 for none.
func (j *Job) ArrayThrottle() int64 {
	_, lim, ok := strings.Cut(j.ArrayTaskString, "%")
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(lim, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// IsHeterogeneous: the job is a component of a heterogeneous job (sbatch
// with ":" between component specifications). Each component is its own
// job record; HetJobID names the leader.
func (j *Job) IsHeterogeneous() bool { return j.HetJobID.Set && j.HetJobID.Number != 0 }

// Times ---------------------------------------------------------------

// RunStart is when slurmctld started the job; zero before it runs.
func (j *Job) RunStart() time.Time { return epoch(j.StartTime) }

// RunEnd is when the time limit expires for a running job (start plus the
// limit, moved by `scontrol update TimeLimit`); zero before it runs or
// without a limit.
func (j *Job) RunEnd() time.Time {
	if j.TimeLimitSeconds() <= 0 {
		return time.Time{}
	}
	return epoch(j.EndTime)
}

func epoch(v OptionalUint64) time.Time {
	if n := v.Value(); n > 0 {
		return time.Unix(int64(n), 0)
	}
	return time.Time{}
}

// Shape ---------------------------------------------------------------

// NTasks is -n (1 when unset).
func (j *Job) NTasks() int64 { return max(int64(j.Tasks.Value()), 1) }

// CPUs is -c (1 when unset).
func (j *Job) CPUs() int64 { return max(int64(j.CPUsPerTask.Value()), 1) }

// Nodes is -N (0 when not requested).
func (j *Job) Nodes() int64 { return int64(j.NodeCount.Value()) }

// TasksPerNode is --ntasks-per-node (0 when unset).
func (j *Job) TasksPerNode() int64 { return int64(j.TasksPerNodeSpec.Value()) }

// MemPerNodeMB is --mem (0 when unset).
func (j *Job) MemPerNodeMB() int64 { return int64(j.MemoryPerNode.Value()) }

// DefaultMemPerCPUMB applies when a job names no memory at all.
const DefaultMemPerCPUMB = 1024

// MemPerCPUMB is --mem-per-cpu, DefaultMemPerCPUMB when unset.
func (j *Job) MemPerCPUMB() int64 {
	if v := j.MemoryPerCPU.Value(); v > 0 {
		return int64(v)
	}
	return DefaultMemPerCPUMB
}

// MemPerGPUMB is --mem-per-gpu or the partition's DefMemPerGPU as recorded
// in memory_per_tres ("gpu:250880", "gres/gpu:250880"); 0 when unset.
func (j *Job) MemPerGPUMB() int64 {
	for _, t := range strings.Split(j.MemoryPerTres, ",") {
		if g, ok := parseGPUSpec(t); ok {
			return g.Count
		}
	}
	return 0
}

// GPUsPerNode is --gres=gpu:N / --gpus-per-node, else --gpus spread over
// the requested nodes (rounded up); 0 without GPUs.
func (j *Job) GPUsPerNode() int64 {
	if g, ok := findGPU(j.TresPerNode); ok {
		return g.Count
	}
	if g, ok := findGPU(j.TresPerJob); ok {
		nodes := max(j.Nodes(), 1)
		return (g.Count + nodes - 1) / nodes
	}
	return 0
}

// GPUsPerTask is --gpus-per-task (0 when unset).
func (j *Job) GPUsPerTask() int64 {
	if g, ok := findGPU(j.TresPerTask); ok {
		return g.Count
	}
	return 0
}

// GPUsPerSocket is --gpus-per-socket (unsupported; 0 when unset).
func (j *Job) GPUsPerSocket() int64 {
	if g, ok := findGPU(j.TresPerSocket); ok {
		return g.Count
	}
	return 0
}

// CPUsPerGPU is --cpus-per-gpu or the partition's DefCpuPerGPU as slurmctld
// records it in cpus_per_tres ("gres/gpu:8", "gpu:26"); 0 when unset.
func (j *Job) CPUsPerGPU() int64 {
	for _, t := range strings.Split(j.CPUsPerTres, ",") {
		if g, ok := parseGPUSpec(t); ok {
			return g.Count
		}
	}
	return 0
}

// GPUType is the GRES type a request names ("h100" in gpu:h100:2), "" for
// untyped requests.
func (j *Job) GPUType() string {
	for _, spec := range []string{j.TresPerNode, j.TresPerJob, j.TresPerTask, j.TresPerSocket} {
		if g, ok := findGPU(spec); ok && g.Type != "" {
			return g.Type
		}
	}
	return ""
}

// exclusiveForms are the values slurmrestd reports for --exclusive,
// --exclusive=user, --exclusive=mcs and --exclusive=topo.
var exclusiveForms = []string{"true", "user", "mcs", "topo"}

// IsExclusive is --exclusive in any of its forms.
func (j *Job) IsExclusive() bool {
	return slices.ContainsFunc(j.Exclusive, func(f string) bool {
		return slices.Contains(exclusiveForms, strings.ToLower(strings.TrimSpace(f)))
	})
}

// WantsSwitchLocality is --switches.
func (j *Job) WantsSwitchLocality() bool { return j.RequiredSwitches > 0 }

// TimeLimitSeconds is --time in seconds (0 for none/unlimited).
func (j *Job) TimeLimitSeconds() int64 { return int64(j.TimeLimit.Value()) * 60 }

// Pinning ---------------------------------------------------------------

// PinnedTo reports whether the job's constraint names the feature.
func (j *Job) PinnedTo(feature string) bool {
	return slices.Contains(strings.FieldsFunc(j.Features, func(r rune) bool {
		return strings.ContainsRune("&|,()[]*: ", r)
	}), feature)
}

// Host selection ---------------------------------------------------------

// Keys the submit plugin writes into the job's extra field for -w and -x
// (the constraint field itself is taken by the pin).
const (
	NodeListKey    = "slurmkube.nodelist="
	ExcludeListKey = "slurmkube.exclude="
)

// HostSelection returns the hosts named by -w (include) and -x (exclude),
// expanded from Slurm hostlists in the extra field.
func (j *Job) HostSelection() (include, exclude []string, err error) {
	for _, tok := range strings.FieldsFunc(j.Extra, func(r rune) bool { return r == ';' || r == ' ' || r == '\n' }) {
		switch {
		case strings.HasPrefix(tok, NodeListKey):
			include, err = ExpandHostlist(strings.TrimPrefix(tok, NodeListKey))
		case strings.HasPrefix(tok, ExcludeListKey):
			exclude, err = ExpandHostlist(strings.TrimPrefix(tok, ExcludeListKey))
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return include, exclude, nil
}
