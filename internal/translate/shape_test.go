// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/slurm"
)

func opt(n uint64) slurm.OptionalUint64 { return slurm.OptionalUint64{Set: true, Number: n} }

// testConfig is a site with two hardware threads per core and GPU hosts.
func testConfig() *config.Config {
	cfg := &config.Config{Namespace: "slurm-jobs", LocalQueue: "main",
		Partitions: []config.Partition{{Name: "k8s", PriorityClass: "normal"}},
		Slurmd: config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "slurm-auth",
			ThreadsPerCore: 2, AdvertiseHostResources: true, NodeCapacity: config.NodeCapacity{CPU: 214, MemoryMB: 2055628, GPUs: 8}},
		Array: config.Array{MaxPods: 16, PodCapacity: config.NodeCapacity{CPU: 26, MemoryMB: 256953, GPUs: 1}}}
	cfg.ApplyDefaults()
	return cfg
}

// job is a held job of the k8s partition with the given sbatch options.
func job(id uint64, mod ...func(j *slurm.Job)) *slurm.Job {
	j := &slurm.Job{JobID: id, Partition: "k8s", JobState: []string{"PENDING"}, Hold: true,
		Tasks: opt(1), CPUsPerTask: opt(1), NodeCount: opt(1), MemoryPerCPU: opt(1024), SubmitTime: opt(1700000000)}
	for _, m := range mod {
		if m != nil {
			m(j)
		}
	}
	return j
}

func TestShapeOfSizesPodsLikeSlurmctld(t *testing.T) {
	cfg := testConfig()
	cases := []struct {
		name string
		job  *slurm.Job
		want Shape
	}{
		{"-c2", job(1, func(j *slurm.Job) { j.CPUsPerTask = opt(2) }),
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 2, MemoryMB: 2048}},
		{"-n4 lands on one node", job(2, func(j *slurm.Job) { j.Tasks = opt(4) }),
			Shape{Pods: 1, TasksPerPod: 4, CPUs: 4, MemoryMB: 4096}},
		{"-N2 -n2 --ntasks-per-node=1 -c100", job(3, func(j *slurm.Job) {
			j.NodeCount, j.Tasks, j.TasksPerNodeSpec, j.CPUsPerTask = opt(2), opt(2), opt(1), opt(100)
		}), Shape{Pods: 2, TasksPerPod: 1, CPUs: 100, MemoryMB: 102400}},
		{"-N2 -n3 sizes for the fuller node", job(4, func(j *slurm.Job) { j.NodeCount, j.Tasks = opt(2), opt(3) }),
			Shape{Pods: 2, TasksPerPod: 2, CPUs: 2, MemoryMB: 2048}},
		{"--ntasks-per-node without -N derives the nodes", job(5, func(j *slurm.Job) {
			j.NodeCount, j.Tasks, j.TasksPerNodeSpec = slurm.OptionalUint64{}, opt(6), opt(2)
		}), Shape{Pods: 3, TasksPerPod: 2, CPUs: 2, MemoryMB: 2048}},
		{"-c3 rounds up to whole cores", job(6, func(j *slurm.Job) { j.CPUsPerTask = opt(3) }),
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 4, MemoryMB: 4096}},
		{"--mem=4000 wins over per-CPU memory", job(7, func(j *slurm.Job) { j.MemoryPerNode = opt(4000); j.MemoryPerCPU = slurm.OptionalUint64{} }),
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 2, MemoryMB: 4000}},
		{"-c1 is still a whole core", job(15, nil),
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 2, MemoryMB: 2048}},
		{"no memory at all: the default per CPU", job(8, func(j *slurm.Job) { j.MemoryPerCPU = slurm.OptionalUint64{}; j.CPUsPerTask = opt(2) }),
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 2, MemoryMB: 2 * slurm.DefaultMemPerCPUMB}},
		{"--gres=gpu:1 with the partition's per-GPU defaults", job(9, func(j *slurm.Job) {
			j.TresPerNode, j.CPUsPerTres, j.MemoryPerTres = "gres/gpu:1", "gres/gpu:26", "gres/gpu:250880"
		}), Shape{Pods: 1, TasksPerPod: 1, CPUs: 26, MemoryMB: 250880, GPUs: 1}},
		{"--gpus=2 on one node", job(10, func(j *slurm.Job) {
			j.TresPerJob, j.CPUsPerTres, j.MemoryPerTres = "gres/gpu:2", "gres/gpu:26", "gres/gpu:250880"
		}),
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 52, MemoryMB: 501760, GPUs: 2}},
		{"--gpus=4 -N3 spreads rounded up", job(11, func(j *slurm.Job) { j.NodeCount, j.Tasks, j.TresPerJob = opt(3), opt(3), "gres/gpu:4" }),
			Shape{Pods: 3, TasksPerPod: 1, CPUs: 2, MemoryMB: 2048, GPUs: 2}},
		{"--gpus-per-task=1 -n2", job(12, func(j *slurm.Job) { j.Tasks, j.TresPerTask = opt(2), "gres/gpu=1" }),
			Shape{Pods: 1, TasksPerPod: 2, CPUs: 2, MemoryMB: 2048, GPUs: 2}},
		{"--mem=32G is overridden by per-GPU memory", job(13, func(j *slurm.Job) {
			j.TresPerNode, j.MemoryPerTres, j.MemoryPerNode, j.CPUsPerTask = "gres/gpu:1", "gres/gpu:250880", opt(32768), opt(4)
		}), Shape{Pods: 1, TasksPerPod: 1, CPUs: 4, MemoryMB: 250880, GPUs: 1}},
		{"--cpus-per-gpu=8 --gres=gpu:1", job(14, func(j *slurm.Job) { j.TresPerNode, j.CPUsPerTres = "gres/gpu:1", "gres/gpu:8" }),
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 8, MemoryMB: 8192, GPUs: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ShapeOf(tc.job, cfg, NewNaming(""))
			if err != nil {
				t.Fatalf("ShapeOf: %v", err)
			}
			if got != tc.want {
				t.Errorf("ShapeOf = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestShapeOfRefusesWhatPodsCannotHonor(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.GresDeviceLinks = config.GresDeviceLinks{Dir: "/run/gres", Count: 8, DevicePrefix: "/dev/nvidia"}
	cases := []struct {
		name   string
		job    *slurm.Job
		option string
	}{
		{"typed GPU", job(1, func(j *slurm.Job) { j.TresPerNode = "gres/gpu:h100:1" }), "--gres=gpu:h100"},
		{"gpus per socket", job(2, func(j *slurm.Job) { j.TresPerSocket = "gres/gpu:1" }), "--gpus-per-socket"},
		{"foreign constraint", job(3, func(j *slurm.Job) { j.Features = "a100&nvme" }), "--constraint a100&nvme"},
		{"more GPUs than device links", job(4, func(j *slurm.Job) { j.TresPerNode = "gres/gpu:9" }), "9 GPUs per node"},
		{"array over several nodes", job(5, func(j *slurm.Job) {
			j.ArrayJobID, j.ArrayTaskString, j.NodeCount = opt(5), "0-3", opt(2)
		}), "--array with -N > 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ShapeOf(tc.job, cfg, NewNaming(""))
			var u *Unsupported
			if !errors.As(err, &u) {
				t.Fatalf("expected an Unsupported error, got %v", err)
			}
			if u.Option != tc.option || u.Reason == "" {
				t.Errorf("Unsupported = %+v, want option %q with a reason", u, tc.option)
			}
			if !strings.Contains(err.Error(), tc.option) {
				t.Errorf("error %q does not name the option", err)
			}
		})
	}
	// Our own pin is not a foreign constraint.
	if _, err := ShapeOf(job(6, func(j *slurm.Job) { j.Features = "slurmkube-pin-6" }), cfg, NewNaming("")); err != nil {
		t.Errorf("the pin feature must be accepted: %v", err)
	}
}

func TestShapeOfArrayPool(t *testing.T) {
	cfg := testConfig()
	cases := []struct {
		name string
		mod  func(*config.Config, *slurm.Job)
		want Shape
	}{
		{"0-3%2 -c2: two slots in one worker", func(_ *config.Config, j *slurm.Job) { j.ArrayTaskString = "0-3%2" },
			Shape{Pods: 1, TasksPerPod: 2, CPUs: 4, MemoryMB: 4096, Array: true, Slots: 2}},
		{"0-99 -c2: 13 slots per worker by capacity, 8 workers", func(_ *config.Config, j *slurm.Job) { j.ArrayTaskString = "0-99" },
			Shape{Pods: 8, TasksPerPod: 13, CPUs: 26, MemoryMB: 26624, Array: true, Slots: 13}},
		{"maxPods caps the pool", func(c *config.Config, j *slurm.Job) { c.Array.MaxPods = 2; j.ArrayTaskString = "0-99" },
			Shape{Pods: 2, TasksPerPod: 13, CPUs: 26, MemoryMB: 26624, Array: true, Slots: 13}},
		{"tasksPerPod overrides capacity", func(c *config.Config, j *slurm.Job) { c.Array.TasksPerPod = 4; j.ArrayTaskString = "0-9" },
			Shape{Pods: 3, TasksPerPod: 4, CPUs: 8, MemoryMB: 8192, Array: true, Slots: 4}},
		{"GPU tasks: one per worker (podCapacity.gpus 1)", func(_ *config.Config, j *slurm.Job) { j.ArrayTaskString = "0-3"; j.TresPerNode = "gres/gpu:1" },
			Shape{Pods: 4, TasksPerPod: 1, CPUs: 2, MemoryMB: 2048, GPUs: 1, Array: true, Slots: 1}},
		{"an array task (not the root) is one plain job", func(_ *config.Config, j *slurm.Job) { j.JobID = 101; j.ArrayTaskID = opt(1) },
			Shape{Pods: 1, TasksPerPod: 1, CPUs: 2, MemoryMB: 2048}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := *cfg
			j := job(100, func(j *slurm.Job) { j.ArrayJobID = opt(100); j.CPUsPerTask = opt(2) })
			tc.mod(&c, j)
			got, err := ShapeOf(j, &c, NewNaming(""))
			if err != nil {
				t.Fatalf("ShapeOf: %v", err)
			}
			if got != tc.want {
				t.Errorf("ShapeOf = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestActiveDeadlineSeconds(t *testing.T) {
	cases := []struct {
		minutes uint64
		want    int64
	}{{0, 0}, {10, 900}, {60, 3960}, {600, 39600}}
	for _, tc := range cases {
		j := job(1, func(j *slurm.Job) { j.TimeLimit = opt(tc.minutes) })
		got := ActiveDeadlineSeconds(j)
		switch {
		case tc.want == 0 && got != nil:
			t.Errorf("--time %d: want no deadline, got %d", tc.minutes, *got)
		case tc.want != 0 && (got == nil || *got != tc.want):
			t.Errorf("--time %d: deadline = %v, want %d", tc.minutes, got, tc.want)
		}
	}
	// UNLIMITED and array pools carry none.
	if got := ActiveDeadlineSeconds(job(2, func(j *slurm.Job) { j.TimeLimit = slurm.OptionalUint64{Set: true, Infinite: true} })); got != nil {
		t.Errorf("UNLIMITED: deadline = %d", *got)
	}
	if got := ActiveDeadlineSeconds(job(3, func(j *slurm.Job) { j.TimeLimit = opt(10); j.ArrayJobID = opt(3); j.ArrayTaskString = "0-9" })); got != nil {
		t.Errorf("array pool: deadline = %d", *got)
	}
}

// The running deadline is Slurm's end_time plus the buffer, measured from
// the Kubernetes Job's own start, so it tracks `scontrol update TimeLimit`.
func TestRunningDeadlineSecondsFollowsSlurmsEndTime(t *testing.T) {
	kubeStart := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	slurmStart := kubeStart.Add(40 * time.Second) // pods start before Slurm allocates
	running := func(limitMin uint64, end time.Time) *slurm.Job {
		return job(1, func(j *slurm.Job) {
			j.JobState = []string{"RUNNING"}
			j.Hold = false
			j.TimeLimit = opt(limitMin)
			j.StartTime = opt(uint64(slurmStart.Unix()))
			j.EndTime = opt(uint64(end.Unix()))
		})
	}
	cases := []struct {
		name string
		job  *slurm.Job
		want int64
		ok   bool
	}{
		{"10 min: end + 5 min buffer from the Job's start", running(10, slurmStart.Add(10*time.Minute)), 40 + 600 + 300, true},
		{"extended to 240 min: end moved, buffer 24 min", running(240, slurmStart.Add(240*time.Minute)), 40 + 14400 + 1440, true},
		{"shortened to 1 min", running(1, slurmStart.Add(time.Minute)), 40 + 60 + 300, true},
		{"shortened below what has elapsed never goes below 1", running(1, kubeStart.Add(-time.Hour)), 1, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := RunningDeadlineSeconds(tc.job, kubeStart)
			if ok != tc.ok || got == nil || *got != tc.want {
				t.Errorf("RunningDeadlineSeconds = %v,%v want %d,%v", got, ok, tc.want, tc.ok)
			}
		})
	}
	// UNLIMITED: no deadline (clears one set earlier).
	unlimited := running(10, slurmStart.Add(10*time.Minute))
	unlimited.TimeLimit = slurm.OptionalUint64{Set: true, Infinite: true}
	if got, ok := RunningDeadlineSeconds(unlimited, kubeStart); got != nil || !ok {
		t.Errorf("UNLIMITED = %v,%v want nil,true", got, ok)
	}
	// Not running yet (no end_time): leave the Job alone.
	pending := job(2, func(j *slurm.Job) { j.TimeLimit = opt(10) })
	if got, ok := RunningDeadlineSeconds(pending, kubeStart); ok || got != nil {
		t.Errorf("pending = %v,%v want nil,false", got, ok)
	}
	// Array pools: none, regardless of the task's limit.
	task := running(10, slurmStart.Add(10*time.Minute))
	task.ArrayJobID, task.ArrayTaskID = opt(1), opt(3)
	if got, ok := RunningDeadlineSeconds(task, kubeStart); got != nil || !ok {
		t.Errorf("array = %v,%v want nil,true", got, ok)
	}
	// Same inputs, same answer: the sync writes nothing on repeat.
	a, _ := RunningDeadlineSeconds(cases[0].job, kubeStart)
	b, _ := RunningDeadlineSeconds(cases[0].job, kubeStart)
	if *a != *b {
		t.Error("not deterministic")
	}
}

func TestHeterogeneousJobsAreRefused(t *testing.T) {
	cfg := testConfig()
	// slurmctld reports het_job_offset as set (0) for ordinary jobs too;
	// only het_job_id != 0 marks a component.
	plain := job(1, func(j *slurm.Job) { j.HetJobID = opt(0); j.HetJobOffset = opt(0) })
	if _, err := ShapeOf(plain, cfg, NewNaming("")); err != nil {
		t.Fatalf("ordinary job refused: %v", err)
	}
	for _, offset := range []uint64{0, 1} {
		c := job(100+offset, func(j *slurm.Job) { j.HetJobID = opt(100); j.HetJobOffset = opt(offset); j.HetJobIDSet = "100-101" })
		_, err := ShapeOf(c, cfg, NewNaming(""))
		var u *Unsupported
		if !errors.As(err, &u) || u.Option != "heterogeneous jobs" || !strings.Contains(u.Reason, "100-101") {
			t.Errorf("component %d: %v", offset, err)
		}
	}
}
