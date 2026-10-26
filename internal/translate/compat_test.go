// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/slurm"
)

// test/compat holds Slurm job records as slurmrestd returns them and, in
// expected.json, the shape each must translate to under the reference
// configuration below: pod count, CPUs, memory, GPUs and the slurmd
// --conf line (test/compat/README.md). The numbers are what slurmctld
// allocates for the same job on a static node, so a change here is a
// change in what users get.
func TestTranslationGolden(t *testing.T) {
	dir := filepath.Join("..", "..", "test", "compat")
	raw, err := os.ReadFile(filepath.Join(dir, "expected.json"))
	if err != nil {
		t.Fatal(err)
	}
	type shape struct {
		Pods     int32       `json:"pods"`
		CPU      int64       `json:"cpu"`
		MemoryMi int64       `json:"memoryMi"`
		GPUs     json.Number `json:"gpus"`
		Conf     string      `json:"conf"`
		Error    string      `json:"error"`
	}
	var expected map[string]shape
	if err := json.Unmarshal(raw, &expected); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Namespace: "slurm-jobs", LocalQueue: "main",
		Partitions: []config.Partition{{Name: "k8s", PriorityClass: "normal-priority"}},
		Slurmd: config.Slurmd{Image: "slurmd:test", ConfServer: "ctl:6817", AuthSecretName: "slurm-auth", ThreadsPerCore: 2, AdvertiseHostResources: true,
			GresDeviceLinks: config.GresDeviceLinks{Dir: "/run/gres", Count: 8}, NodeCapacity: config.NodeCapacity{CPU: 214, MemoryMB: 2055628, GPUs: 8}},
		Array: config.Array{MaxPods: 16, PodCapacity: config.NodeCapacity{CPU: 26, MemoryMB: 256953, GPUs: 1}}}
	cfg.ApplyDefaults()
	tr := New(cfg)
	names := make([]string, 0, len(expected))
	for n := range expected {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		want := expected[name]
		raw, err := os.ReadFile(filepath.Join(dir, name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var job slurm.Job
		if err := json.Unmarshal(raw, &job); err != nil {
			t.Fatal(err)
		}
		js, err := tr.JobSet(&job)
		if want.Error != "" {
			if err == nil {
				t.Errorf("%s: expected a refusal (%s), got a JobSet", name, want.Error)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		c := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers[0]
		got := shape{
			Pods: *js.Spec.ReplicatedJobs[0].Template.Spec.Parallelism, CPU: c.Resources.Requests.Cpu().Value(),
			MemoryMi: c.Resources.Limits.Memory().Value() >> 20, Conf: c.Args[2],
		}
		if q, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
			got.GPUs = json.Number(q.String())
		} else {
			got.GPUs = "0"
		}
		if got.Pods != want.Pods || got.CPU != want.CPU || got.MemoryMi != want.MemoryMi || got.GPUs != want.GPUs || got.Conf != want.Conf {
			t.Errorf("%s:\n  want: pods=%d cpu=%d mem=%dMi gpus=%s conf=%s\n  got:  pods=%d cpu=%d mem=%dMi gpus=%s conf=%s",
				name, want.Pods, want.CPU, want.MemoryMi, want.GPUs, want.Conf, got.Pods, got.CPU, got.MemoryMi, got.GPUs, got.Conf)
		}
	}
}
