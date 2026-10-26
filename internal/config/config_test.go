// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

func TestDuration(t *testing.T) {
	var v struct {
		D Duration `json:"d"`
	}
	if err := yaml.Unmarshal([]byte("d: 1m30s"), &v); err != nil || v.D.Duration != 90*time.Second {
		t.Fatalf("1m30s: %v, %v", v.D, err)
	}
	for _, bad := range []string{"d: 10", "d: soon", "d: [1s]"} {
		if err := yaml.Unmarshal([]byte(bad), &v); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	out, err := yaml.Marshal(v)
	if err != nil || strings.TrimSpace(string(out)) != "d: 1m30s" {
		t.Errorf("marshal: %q, %v", out, err)
	}
}

func TestPartitionLookups(t *testing.T) {
	c := &Config{
		LocalQueue: "main",
		Partitions: []Partition{
			{Name: "k8s", PriorityClass: "normal"},
			{Name: "urgent", PriorityClass: "high", LocalQueue: "fast"},
		},
	}
	cases := []struct {
		partition, class, queue string
		managed                 bool
	}{
		{"k8s", "normal", "main", true},
		{"urgent", "high", "fast", true},
		{"other", "", "main", false},
	}
	for _, tc := range cases {
		class, managed := c.PriorityClassFor(tc.partition)
		if class != tc.class || managed != tc.managed || c.LocalQueueFor(tc.partition) != tc.queue {
			t.Errorf("%s: class %q managed %v queue %q", tc.partition, class, managed, c.LocalQueueFor(tc.partition))
		}
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	valid := `slurm:
  restURL: https://slurmrestd:6820
  requestTimeout: 5s
namespace: slurm-jobs
localQueue: main
partitions:
- name: k8s
  priorityClass: normal
slurmd:
  image: slurmd:test
  confServer: ctl:6817
  authSecretName: slurm-auth
`
	cfg, err := Load(write("valid.yaml", valid))
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if cfg.Slurm.RequestTimeout.Duration != 5*time.Second || cfg.Claim.HeldComment != DefaultHeldComment {
		t.Errorf("not read or not defaulted: %+v", cfg)
	}

	if _, err := Load(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Error("missing file accepted")
	}
	if _, err := Load(write("typo.yaml", valid+"namespcae: x\n")); err == nil {
		t.Error("unknown key accepted")
	}
	if _, err := Load(write("invalid.yaml", strings.Replace(valid, "namespace: slurm-jobs\n", "", 1))); err == nil {
		t.Error("config without a namespace accepted")
	}
}
