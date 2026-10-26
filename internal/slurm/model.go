// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package slurm talks to slurmrestd (OpenAPI v0.0.44) and gives the rest of
// SlurmKube a small, typed view of jobs and dynamic node records.
package slurm

import (
	"slices"
	"strings"
)

// OptionalUint64 is slurmrestd's {set, infinite, number} triple.
type OptionalUint64 struct {
	Set      bool   `json:"set"`
	Infinite bool   `json:"infinite"`
	Number   uint64 `json:"number"`
}

// Value is the number when set and finite, else 0.
func (o OptionalUint64) Value() uint64 {
	if !o.Set || o.Infinite {
		return 0
	}
	return o.Number
}

// Job is the part of a slurmctld job record SlurmKube reads.
type Job struct {
	JobID       uint64         `json:"job_id"`
	Name        string         `json:"name"`
	Partition   string         `json:"partition"`
	JobState    []string       `json:"job_state"`
	StateReason string         `json:"state_reason"`
	Hold        bool           `json:"hold"`
	Priority    OptionalUint64 `json:"priority"`
	Comment     string         `json:"comment"`
	Features    string         `json:"features"`
	Dependency  string         `json:"dependency"`
	// Extra carries the submit plugin's host selection (see HostSelection).
	Extra        string         `json:"extra"`
	SubmitTime   OptionalUint64 `json:"submit_time"`
	EligibleTime OptionalUint64 `json:"eligible_time"`
	// StartTime and EndTime are set once the job runs; EndTime is
	// StartTime plus the time limit and moves with `scontrol update
	// TimeLimit`.
	StartTime  OptionalUint64 `json:"start_time"`
	EndTime    OptionalUint64 `json:"end_time"`
	RestartCnt uint32         `json:"restart_cnt"`
	TimeLimit  OptionalUint64 `json:"time_limit"`

	Tasks            OptionalUint64 `json:"tasks"`
	CPUsPerTask      OptionalUint64 `json:"cpus_per_task"`
	TasksPerNodeSpec OptionalUint64 `json:"tasks_per_node"`
	NodeCount        OptionalUint64 `json:"node_count"`
	MemoryPerCPU     OptionalUint64 `json:"memory_per_cpu"`
	MemoryPerNode    OptionalUint64 `json:"memory_per_node"`
	MemoryPerTres    string         `json:"memory_per_tres"`
	TresPerNode      string         `json:"tres_per_node"`
	TresPerJob       string         `json:"tres_per_job"`
	TresPerTask      string         `json:"tres_per_task"`
	TresPerSocket    string         `json:"tres_per_socket"`
	CPUsPerTres      string         `json:"cpus_per_tres"`
	Exclusive        []string       `json:"exclusive"`
	RequiredSwitches uint64         `json:"required_switches"`

	ArrayJobID      OptionalUint64 `json:"array_job_id"`
	ArrayTaskID     OptionalUint64 `json:"array_task_id"`
	ArrayTaskString string         `json:"array_task_string"`
	// HetJobID is the leader's ID for a heterogeneous job's components
	// (0 otherwise); HetJobOffset is the component index (slurmctld
	// reports it as set, 0 for ordinary jobs too, so HetJobID decides).
	HetJobID     OptionalUint64 `json:"het_job_id"`
	HetJobOffset OptionalUint64 `json:"het_job_offset"`
	HetJobIDSet  string         `json:"het_job_id_set"`
}

// Node is the part of a slurmctld node record SlurmKube reads.
type Node struct {
	Name     string   `json:"name"`
	Features []string `json:"features"`
	State    []string `json:"state"`
	Reason   string   `json:"reason"`
	// CPUs is the configured count; EffectiveCPUs what is left after the
	// node's specialized CPUs (for a pinned pod, the pod's CPUs).
	CPUs          int64  `json:"cpus"`
	EffectiveCPUs int64  `json:"effective_cpus"`
	Gres          string `json:"gres"`
	// Address and Port are where slurmctld sends the node's RPCs.
	Address string `json:"address"`
	Port    int32  `json:"port"`
	// Extra is the node's arbitrary string: SlurmKube's slurmd registers
	// with --extra <pod UID>, so Extra names the pod that owns the record.
	Extra string `json:"extra"`
}

// HasState reports whether the node carries the state flag.
func (n *Node) HasState(flag string) bool {
	for _, s := range n.State {
		if strings.EqualFold(s, flag) {
			return true
		}
	}
	return false
}

// HasFeature reports whether the node advertises the feature.
func (n *Node) HasFeature(f string) bool { return slices.Contains(n.Features, f) }
