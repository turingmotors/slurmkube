// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"slices"
	"testing"
)

func set(n uint64) OptionalUint64 { return OptionalUint64{Set: true, Number: n} }

func TestOptionalUint64Value(t *testing.T) {
	if (OptionalUint64{}).Value() != 0 || (OptionalUint64{Set: true, Infinite: true, Number: 5}).Value() != 0 || set(7).Value() != 7 {
		t.Error("Value must be the number only when set and finite")
	}
}

func TestJobState(t *testing.T) {
	held := &Job{JobState: []string{"PENDING"}, Hold: true}
	heldByPriority := &Job{JobState: []string{"PENDING"}, Priority: set(0), StateReason: "JobHeldUser"}
	pending := &Job{JobState: []string{"PENDING"}, Priority: set(1000), StateReason: "Priority"}
	running := &Job{JobState: []string{"RUNNING"}}
	done := &Job{JobState: []string{"COMPLETED"}}
	cancelled := &Job{JobState: []string{"CANCELLED", "REQUEUE_HOLD"}}
	for _, tc := range []struct {
		j                       *Job
		pending, held, terminal bool
	}{
		{held, true, true, false}, {heldByPriority, true, true, false}, {pending, true, false, false},
		{running, false, false, false}, {done, false, false, true}, {cancelled, false, false, true},
	} {
		if tc.j.IsPending() != tc.pending || tc.j.IsHeld() != tc.held || tc.j.IsTerminal() != tc.terminal {
			t.Errorf("%v: pending %v held %v terminal %v", tc.j.JobState, tc.j.IsPending(), tc.j.IsHeld(), tc.j.IsTerminal())
		}
	}
}

func TestArrays(t *testing.T) {
	root := &Job{JobID: 100, ArrayJobID: set(100), ArrayTaskString: "0-3,7%2"}
	task := &Job{JobID: 101, ArrayJobID: set(100), ArrayTaskID: set(1)}
	plain := &Job{JobID: 5}
	if !root.IsArray() || !root.IsArrayRoot() || root.PinID() != 100 {
		t.Error("root")
	}
	if !task.IsArray() || task.IsArrayRoot() || task.PinID() != 100 {
		t.Error("task")
	}
	if plain.IsArray() || plain.IsArrayRoot() || plain.PinID() != 5 {
		t.Error("plain")
	}
	for spec, want := range map[string][2]int64{"0-3,7%2": {5, 2}, "0-9:2": {5, 0}, "": {1, 0}, "3": {1, 0}, "0-99%10": {100, 10}, "5-2": {1, 0}} {
		j := &Job{ArrayTaskString: spec}
		if j.ArrayPendingTasks() != want[0] || j.ArrayThrottle() != want[1] {
			t.Errorf("%q: tasks %d throttle %d, want %v", spec, j.ArrayPendingTasks(), j.ArrayThrottle(), want)
		}
	}
}

func TestGPUOptions(t *testing.T) {
	cases := []struct {
		name                              string
		job                               Job
		perNode, perTask, perSocket, cpus int64
		memPerGPU                         int64
		gpuType                           string
	}{
		{"gres=gpu:2", Job{TresPerNode: "gres/gpu:2"}, 2, 0, 0, 0, 0, ""},
		{"typed", Job{TresPerNode: "gres/gpu:h100:2"}, 2, 0, 0, 0, 0, "h100"},
		{"gpus=4 -N3", Job{TresPerJob: "gres/gpu:4", NodeCount: set(3)}, 2, 0, 0, 0, 0, ""},
		{"gpus=4 no -N", Job{TresPerJob: "gres/gpu:4"}, 4, 0, 0, 0, 0, ""},
		{"gpus-per-task (26.05 '=')", Job{TresPerTask: "gres/gpu=1"}, 0, 1, 0, 0, 0, ""},
		{"gpus-per-socket", Job{TresPerSocket: "gres/gpu:1"}, 0, 0, 1, 0, 0, ""},
		{"partition defaults", Job{TresPerNode: "gres/gpu:1", CPUsPerTres: "gres/gpu:26", MemoryPerTres: "gres/gpu:250880"}, 1, 0, 0, 26, 250880, ""},
		{"short forms", Job{TresPerNode: "gpu:1", CPUsPerTres: "gpu:8", MemoryPerTres: "gpu:1000"}, 1, 0, 0, 8, 1000, ""},
		{"other TRES ignored", Job{TresPerNode: "gres/fpga:2,gres/gpu:3", CPUsPerTres: "gres/fpga:1"}, 3, 0, 0, 0, 0, ""},
		{"none", Job{}, 0, 0, 0, 0, 0, ""},
	}
	for _, tc := range cases {
		j := tc.job
		if j.GPUsPerNode() != tc.perNode || j.GPUsPerTask() != tc.perTask || j.GPUsPerSocket() != tc.perSocket ||
			j.CPUsPerGPU() != tc.cpus || j.MemPerGPUMB() != tc.memPerGPU || j.GPUType() != tc.gpuType {
			t.Errorf("%s: node %d task %d socket %d cpus/gpu %d mem/gpu %d type %q", tc.name,
				j.GPUsPerNode(), j.GPUsPerTask(), j.GPUsPerSocket(), j.CPUsPerGPU(), j.MemPerGPUMB(), j.GPUType())
		}
	}
}

func TestScalarOptions(t *testing.T) {
	j := &Job{Tasks: set(3), CPUsPerTask: set(2), NodeCount: set(2), TasksPerNodeSpec: set(2), MemoryPerNode: set(4000), MemoryPerCPU: set(512), TimeLimit: set(90)}
	if j.NTasks() != 3 || j.CPUs() != 2 || j.Nodes() != 2 || j.TasksPerNode() != 2 || j.MemPerNodeMB() != 4000 || j.MemPerCPUMB() != 512 || j.TimeLimitSeconds() != 5400 {
		t.Errorf("options: %d %d %d %d %d %d %d", j.NTasks(), j.CPUs(), j.Nodes(), j.TasksPerNode(), j.MemPerNodeMB(), j.MemPerCPUMB(), j.TimeLimitSeconds())
	}
	empty := &Job{}
	if empty.NTasks() != 1 || empty.CPUs() != 1 || empty.Nodes() != 0 || empty.MemPerCPUMB() != DefaultMemPerCPUMB || empty.TimeLimitSeconds() != 0 {
		t.Error("defaults when unset")
	}
	if (&Job{TimeLimit: OptionalUint64{Set: true, Infinite: true}}).TimeLimitSeconds() != 0 {
		t.Error("UNLIMITED is no limit")
	}
}

func TestExclusiveAndSwitches(t *testing.T) {
	for _, v := range [][]string{{"true"}, {"user"}, {"mcs"}, {"TOPO"}} {
		if !(&Job{Exclusive: v}).IsExclusive() {
			t.Errorf("%v must be exclusive", v)
		}
	}
	for _, v := range [][]string{nil, {"false"}, {"oversubscribe"}} {
		if (&Job{Exclusive: v}).IsExclusive() {
			t.Errorf("%v must not be exclusive", v)
		}
	}
	if !(&Job{RequiredSwitches: 1}).WantsSwitchLocality() || (&Job{}).WantsSwitchLocality() {
		t.Error("--switches")
	}
}

func TestPinnedTo(t *testing.T) {
	for features, want := range map[string]bool{"slurmkube-pin-5": true, "a100&slurmkube-pin-5": true, "(slurmkube-pin-5|x)": true,
		"slurmkube-pin-55": false, "slurmkube-pin-5x": false, "": false} {
		if got := (&Job{Features: features}).PinnedTo("slurmkube-pin-5"); got != want {
			t.Errorf("PinnedTo(%q) = %v", features, got)
		}
	}
}

func TestHostSelection(t *testing.T) {
	j := &Job{Extra: "foo=bar;" + NodeListKey + "gpu-[1-2],cpu-7;" + ExcludeListKey + "gpu-9"}
	inc, exc, err := j.HostSelection()
	if err != nil || !slices.Equal(inc, []string{"gpu-1", "gpu-2", "cpu-7"}) || !slices.Equal(exc, []string{"gpu-9"}) {
		t.Errorf("HostSelection = %v %v %v", inc, exc, err)
	}
	if inc, exc, err := (&Job{Extra: "unrelated"}).HostSelection(); err != nil || inc != nil || exc != nil {
		t.Errorf("no selection: %v %v %v", inc, exc, err)
	}
	if _, _, err := (&Job{Extra: NodeListKey + "gpu-[2-1]"}).HostSelection(); err == nil {
		t.Error("an invalid hostlist must be reported")
	}
}
