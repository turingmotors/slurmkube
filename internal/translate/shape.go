// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"
	"time"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/slurm"
)

// Unsupported is a job option the pods cannot honor. It is reported to the
// user through the job's comment and never silently reinterpreted.
type Unsupported struct {
	Option string
	Reason string
}

func (u *Unsupported) Error() string { return u.Option + ": " + u.Reason }

func unsupported(option, format string, args ...any) error {
	return &Unsupported{Option: option, Reason: fmt.Sprintf(format, args...)}
}

// Shape is what the job needs from Kubernetes: how many pods (one per
// Slurm node), and each pod's CPUs, memory and GPUs — what slurmctld will
// allocate on the node the pod registers.
type Shape struct {
	Pods        int32
	TasksPerPod int64
	CPUs        int64 // per pod, whole cores
	MemoryMB    int64 // per pod, what Slurm requires of the node
	GPUs        int64 // per pod
	// Array: the pods are a worker pool for a job array; Slots is how
	// many tasks one worker holds.
	Array bool
	Slots int64
}

// ShapeOf sizes a job. The rules follow slurmctld's own allocation:
//
//   - pods = -N (1 by default); tasks land tasks-nodes+1 on one node unless
//     --ntasks-per-node says otherwise, so a pod is sized for the worst
//     case;
//   - CPUs per pod = -c × tasks, or --cpus-per-gpu × GPUs when larger,
//     rounded up to whole cores (threadsPerCore);
//   - GPUs per pod = --gres/--gpus-per-node, --gpus spread over the nodes,
//     or --gpus-per-task × tasks, whichever is larger;
//   - memory per pod = per-GPU memory × GPUs when the job has GPUs (cons_tres
//     sets that at allocation even over --mem), else --mem, else
//     --mem-per-cpu × CPUs.
func ShapeOf(job *slurm.Job, cfg *config.Config, naming Naming) (Shape, error) {
	if job.IsHeterogeneous() {
		// Components must be allocated together and each is its own job
		// record; a JobSet per component would run them independently.
		return Shape{}, unsupported("heterogeneous jobs", "not supported (component %d of %s); submit the components as separate jobs, e.g. with --dependency",
			job.HetJobOffset.Value(), job.HetJobIDSet)
	}
	if t := job.GPUType(); t != "" {
		return Shape{}, unsupported("--gres=gpu:"+t, "the dynamic nodes register untyped GPUs; request --gres=gpu:N or --gpus without the type")
	}
	if job.GPUsPerSocket() > 0 {
		return Shape{}, unsupported("--gpus-per-socket", "the dynamic nodes advertise no GPU socket layout; use --gres=gpu:N, --gpus or --gpus-per-task")
	}
	if f := job.Features; f != "" && !naming.IsPinFeature(f) {
		return Shape{}, unsupported("--constraint "+f, "SlurmKube pins the job to its own nodes by feature and would drop the constraint; remove it")
	}
	pods, tasksPerPod := podLayout(job)
	gpus := max(job.GPUsPerNode(), job.GPUsPerTask()*tasksPerPod)
	cpus := wholeCores(job.CPUs()*tasksPerPod, cfg.Slurmd.ThreadsPerCore)
	if per := job.CPUsPerGPU(); per > 0 && gpus > 0 {
		cpus = max(cpus, wholeCores(per*gpus, cfg.Slurmd.ThreadsPerCore))
	}
	if g := cfg.Slurmd.GresDeviceLinks; g.Enabled() && gpus > int64(g.Count) {
		return Shape{}, unsupported(fmt.Sprintf("%d GPUs per node", gpus), "slurmd.gresDeviceLinks.count allows at most %d", g.Count)
	}
	task := Shape{Pods: pods, TasksPerPod: tasksPerPod, CPUs: cpus, GPUs: gpus, MemoryMB: memoryMB(job, cpus, gpus)}
	if !job.IsArrayRoot() {
		return task, nil
	}
	if pods > 1 {
		return Shape{}, unsupported("--array with -N > 1", "array tasks run on single-node worker pods")
	}
	return arrayPool(task, job, cfg), nil
}

// podLayout: pods and the tasks the fullest pod holds.
func podLayout(job *slurm.Job) (int32, int64) {
	tasks := job.NTasks()
	pods := max(job.Nodes(), 1)
	if tpn := job.TasksPerNode(); tpn > 0 {
		if job.Nodes() == 0 {
			pods = (tasks + tpn - 1) / tpn
		}
		return int32(pods), tpn
	}
	return int32(pods), max(tasks-pods+1, 1)
}

func wholeCores(cpus, threadsPerCore int64) int64 {
	if threadsPerCore > 1 {
		return (cpus + threadsPerCore - 1) / threadsPerCore * threadsPerCore
	}
	return cpus
}

func memoryMB(job *slurm.Job, cpus, gpus int64) int64 {
	if per := job.MemPerGPUMB(); per > 0 && gpus > 0 {
		return per * gpus
	}
	if mem := job.MemPerNodeMB(); mem > 0 {
		return mem
	}
	return job.MemPerCPUMB() * cpus
}

// arrayPool sizes the worker pool of a job array: each worker holds as many
// tasks as fit the configured capacity (or array.tasksPerPod), the pool
// holds no more tasks than may run at once (%N, pending tasks) and at most
// array.maxPods workers.
func arrayPool(task Shape, job *slurm.Job, cfg *config.Config) Shape {
	slots := cfg.Array.TasksPerPod
	if slots <= 0 {
		slots = slotsByCapacity(task, cfg)
	}
	concurrent := job.ArrayPendingTasks()
	if lim := job.ArrayThrottle(); lim > 0 {
		concurrent = min(concurrent, lim)
	}
	maxPods := int64(max(cfg.Array.MaxPods, 1))
	concurrent = min(concurrent, maxPods*slots)
	slots = max(min(slots, concurrent), 1)
	return Shape{
		Pods: int32((concurrent + slots - 1) / slots), TasksPerPod: slots,
		CPUs: slots * task.CPUs, MemoryMB: slots * task.MemoryMB, GPUs: slots * task.GPUs,
		Array: true, Slots: slots,
	}
}

func slotsByCapacity(task Shape, cfg *config.Config) int64 {
	cap := cfg.Array.PodCapacity
	if cap.CPU == 0 && cap.MemoryMB == 0 && cap.GPUs == 0 {
		cap = cfg.Slurmd.NodeCapacity
	}
	slots := int64(1 << 30)
	fit := func(have, need int64) {
		if have > 0 && need > 0 {
			slots = min(slots, have/need)
		}
	}
	fit(cap.CPU, task.CPUs)
	fit(cap.MemoryMB, task.MemoryMB)
	fit(cap.GPUs, task.GPUs)
	if slots == 1<<30 {
		slots = 1
	}
	return max(slots, 1)
}

// The Kubernetes deadline.
//
// Slurm is the source of truth for the time limit: slurmctld ends the job
// (TIMEOUT) at its end_time. The Job's activeDeadlineSeconds is a backstop
// that fires DeadlineBuffer later, so pods cannot outlive a job Slurm lost
// track of. The two clocks differ: Slurm's starts when the job is
// allocated to the registered nodes, Kubernetes' (Job.status.startTime)
// when the Job is unsuspended, earlier by the pod start and registration.
//
// Array pools carry no deadline: the pool runs tasks one after another,
// each under its own Slurm limit, and is deleted when the array ends.

// DeadlineBuffer is how long after Slurm's end_time the Kubernetes
// deadline fires: the larger of 10 % of the limit and 5 minutes.
func DeadlineBuffer(limitSeconds int64) time.Duration {
	return time.Duration(max(limitSeconds/10, 300)) * time.Second
}

// ActiveDeadlineSeconds is the deadline a new Job is created with: the
// limit plus the buffer, measured from the Job's start. nil for no limit
// and for array pools.
func ActiveDeadlineSeconds(job *slurm.Job) *int64 {
	limit := job.TimeLimitSeconds()
	if limit <= 0 || job.IsArray() {
		return nil
	}
	v := limit + int64(DeadlineBuffer(limit).Seconds())
	return &v
}

// RunningDeadlineSeconds is the deadline a Job that started at startedAt
// must carry while its Slurm job runs: Slurm's end_time plus the buffer,
// expressed from the Job's own start. It follows `scontrol update
// TimeLimit` in either direction. nil means no deadline (no limit, array
// pool); ok is false when Slurm reports no end time yet (the job is not
// running), in which case the Job's current value is left alone.
func RunningDeadlineSeconds(job *slurm.Job, startedAt time.Time) (want *int64, ok bool) {
	if job.IsArray() {
		return nil, true
	}
	limit := job.TimeLimitSeconds()
	if limit <= 0 {
		return nil, true
	}
	end := job.RunEnd()
	if end.IsZero() || startedAt.IsZero() {
		return nil, false
	}
	v := int64(end.Add(DeadlineBuffer(limit)).Sub(startedAt).Round(time.Second) / time.Second)
	if v < 1 {
		v = 1
	}
	return &v, true
}
