// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package config is SlurmKube's configuration: one YAML file, defaults
// applied once (ApplyDefaults), checked once (Validate). Everything that
// is site-specific lives here, nothing in the controllers.
package config

import (
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// Duration is time.Duration that reads "10s" from YAML/JSON. encoding/json
// hands a string value to UnmarshalText and rejects any other kind.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("duration %q: %w", text, err)
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// Config is the whole configuration.
type Config struct {
	// Slurm is how to reach slurmrestd.
	Slurm Slurm `json:"slurm"`
	// Namespace holds the JobSets and slurmd pods.
	Namespace string `json:"namespace"`
	// LocalQueue is the Kueue LocalQueue JobSets are submitted to.
	LocalQueue string `json:"localQueue"`
	// Partitions lists the Slurm partitions this instance manages and the
	// Kueue WorkloadPriorityClass each maps to. Jobs of other partitions
	// are never touched.
	Partitions []Partition `json:"partitions"`
	// Claim says which held jobs are this instance's to run and how to
	// stay out of another manager's way.
	Claim Claim `json:"claim,omitempty"`
	// Slurmd describes the pods that register as dynamic Slurm nodes.
	Slurmd Slurmd `json:"slurmd"`
	// Array sizes the worker pool a job array runs on.
	Array Array `json:"array,omitempty"`
	// Topology maps Slurm locality hints onto Kueue topology-aware
	// scheduling.
	Topology Topology `json:"topology,omitempty"`
	// Recovery tunes polling, graces and orphan handling.
	Recovery Recovery `json:"recovery,omitempty"`
	// Fencing decides when a job whose pod may still be running elsewhere
	// is allowed to run again (docs/fencing.md).
	Fencing Fencing `json:"fencing,omitempty"`
	// Instance names this SlurmKube instance when several share a
	// namespace; it is stamped on every JobSet (translate.InstanceLabel)
	// and selects this instance's objects. Empty for a single instance.
	Instance string `json:"instance,omitempty"`
}

// Slurm is the slurmrestd endpoint and credentials.
type Slurm struct {
	RestURL string `json:"restURL"`
	// User is sent as X-SLURM-USER-NAME (the account slurmrestd acts as).
	User string `json:"user,omitempty"`
	// TokenFile holds a long-lived JWT (X-SLURM-USER-TOKEN).
	TokenFile string `json:"tokenFile,omitempty"`
	// JWTKeyFile is slurmctld's HS256 signing key; the client mints
	// short-lived tokens for JWTUser itself instead of reading TokenFile.
	JWTKeyFile  string   `json:"jwtKeyFile,omitempty"`
	JWTUser     string   `json:"jwtUser,omitempty"`
	JWTLifetime Duration `json:"jwtLifetime,omitempty"`
	// AllowInsecureHTTP permits a plain http:// RestURL (in-cluster only).
	AllowInsecureHTTP     bool     `json:"allowInsecureHTTP,omitempty"`
	CACertFile            string   `json:"caCertFile,omitempty"`
	InsecureSkipTLSVerify bool     `json:"insecureSkipTLSVerify,omitempty"`
	RequestTimeout        Duration `json:"requestTimeout,omitempty"`
	RequestsPerSecond     float64  `json:"requestsPerSecond,omitempty"`
}

// Partition maps one Slurm partition to a Kueue priority class.
type Partition struct {
	Name          string `json:"name"`
	PriorityClass string `json:"priorityClass"`
	// LocalQueue overrides Config.LocalQueue for this partition.
	LocalQueue string `json:"localQueue,omitempty"`
}

// Claim is how SlurmKube recognizes the jobs it should run.
//
// A JobSubmit plugin on the Slurm controller holds every job of a managed
// partition at submission and stamps HeldComment on it; SlurmKube takes
// such a job over (its comment then starts with CommentPrefix) and runs
// it. A job whose comment is neither is another manager's or an
// operator's and is left alone — the one mechanism that keeps two
// controllers from running the same job. OtherManagers additionally
// names labels of JobSets another controller created so a job that
// already has one is skipped even if its comment was reset.
type Claim struct {
	HeldComment   string         `json:"heldComment,omitempty"`
	OtherManagers []OtherManager `json:"otherManagers,omitempty"`
}

// OtherManager describes another controller's JobSets: their labels and
// the label carrying the Slurm job ID.
type OtherManager struct {
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels"`
	JobIDLabel string            `json:"jobIDLabel"`
}

// Slurmd describes the pods that will register as dynamic Slurm nodes.
type Slurmd struct {
	Image          string `json:"image"`
	ConfServer     string `json:"confServer"`
	AuthSecretName string `json:"authSecretName"`
	// GPUResourceName is the extended resource for GPUs (nvidia.com/gpu).
	GPUResourceName string `json:"gpuResourceName,omitempty"`
	// Privileged runs slurmd privileged (default) or with the capability
	// set slurmd needs.
	Privileged *bool     `json:"privileged,omitempty"`
	NFS        *NFSMount `json:"nfs,omitempty"`
	// NodeFeatures are extra Slurm features every dynamic node advertises.
	NodeFeatures []string `json:"nodeFeatures,omitempty"`
	// PodTemplate is merged onto the generated pod (strategic merge by
	// name for containers, volumes, env).
	PodTemplate *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
	// AdvertiseHostResources makes slurmd advertise the host's topology
	// and lets slurmctld derive the pod's share (SlurmdSpecOverride);
	// off, the node advertises the pod's size (docs/cpu-topology.md).
	AdvertiseHostResources bool `json:"advertiseHostResources,omitempty"`
	// GPUDevicesFromDevicePlugin leaves GPU selection to the device
	// plugin instead of NVIDIA_VISIBLE_DEVICES=all.
	GPUDevicesFromDevicePlugin bool `json:"gpuDevicesFromDevicePlugin,omitempty"`
	// NodeNamePrefix precedes "<index>-<jobID>" in dynamic node names
	// (default "slurmkube-w"); the site's gres.conf must match it.
	NodeNamePrefix string `json:"nodeNamePrefix,omitempty"`
	// ThreadsPerCore rounds the pod's CPUs to whole cores like slurmctld.
	ThreadsPerCore int64 `json:"threadsPerCore,omitempty"`
	// MemoryOverheadMB is headroom above the job's memory for slurmd
	// itself so Slurm's job cgroup, not the kubelet, ends an overrun.
	MemoryOverheadMB *int64 `json:"memoryOverheadMB,omitempty"`
	// NodeIdentity registers slurmd with --extra <pod UID> so a node
	// record names the pod that owns it (default true).
	NodeIdentity *bool `json:"nodeIdentity,omitempty"`
	// MaxDynamicNodes is slurmctld's MaxNodeCount minus its static nodes;
	// 0 means unknown (no gate).
	MaxDynamicNodes int `json:"maxDynamicNodes,omitempty"`
	// RegistrationTimeout replaces a pod whose slurmd has no record after
	// this long (default 3m).
	RegistrationTimeout Duration `json:"registrationTimeout,omitempty"`
	// NodeCapacity is what one host may offer slurmd pods.
	NodeCapacity NodeCapacity `json:"nodeCapacity,omitempty"`
	// NodePorts gives each slurmd pod on a host its own listen port.
	NodePorts NodePorts `json:"nodePorts,omitempty"`
	// GresDeviceLinks publishes a pod's GPU device nodes under stable
	// names for a static gres.conf (docs/gres-device-links.md).
	GresDeviceLinks GresDeviceLinks `json:"gresDeviceLinks,omitempty"`
}

func (s *Slurmd) PrivilegedOrDefault() bool { return s.Privileged == nil || *s.Privileged }

func (s *Slurmd) NodeIdentityEnabled() bool { return s.NodeIdentity == nil || *s.NodeIdentity }

// NFSMount mounts an NFS export into every slurmd pod.
type NFSMount struct {
	Server    string `json:"server"`
	Path      string `json:"path"`
	MountPath string `json:"mountPath"`
}

// NodeCapacity is a resource envelope (one host, or one array worker).
type NodeCapacity struct {
	CPU      int64 `json:"cpu,omitempty"`
	MemoryMB int64 `json:"memoryMB,omitempty"`
	GPUs     int64 `json:"gpus,omitempty"`
}

// NodePorts carves a port range for slurmd pods sharing a host.
type NodePorts struct {
	// Base is the first port; nil means the default (63000), 0 disables.
	Base   *int32 `json:"base,omitempty"`
	Size   int32  `json:"size,omitempty"`
	PerJob int32  `json:"perJob,omitempty"`
}

func (n NodePorts) Enabled() bool { return n.Base != nil && *n.Base > 0 }

// GresDeviceLinks is the device-link contract with the site's gres.conf.
type GresDeviceLinks struct {
	Dir          string `json:"dir,omitempty"`
	Count        int    `json:"count,omitempty"`
	DevicePrefix string `json:"devicePrefix,omitempty"`
}

func (g GresDeviceLinks) Enabled() bool { return g.Dir != "" }

// Array sizes the worker pool of a job array: at most MaxPods pods (0: one),
// each holding TasksPerPod tasks (0: as many as fit PodCapacity, or the
// host's NodeCapacity).
type Array struct {
	MaxPods     int32        `json:"maxPods,omitempty"`
	PodCapacity NodeCapacity `json:"podCapacity,omitempty"`
	TasksPerPod int64        `json:"tasksPerPod,omitempty"`
}

// Topology maps --switches and -w onto Kueue TAS levels.
type Topology struct {
	// SwitchesLevel is the level a --switches job must fit in one domain of.
	SwitchesLevel string `json:"switchesLevel,omitempty"`
	// PackLevel is the level other jobs are packed into when possible.
	PackLevel string `json:"packLevel,omitempty"`
}

// Recovery tunes the loops that bring Slurm and Kubernetes back together.
type Recovery struct {
	// PollInterval is how often slurmctld is read (default 10s).
	PollInterval Duration `json:"pollInterval,omitempty"`
	// VanishedOwnerGrace is how long a node record whose pod is not in
	// the cache is left alone before release (default 15s).
	VanishedOwnerGrace Duration `json:"vanishedOwnerGrace,omitempty"`
	// OrphanPolls is how many consecutive polls a record or job must
	// look orphaned before it is acted on (default 3).
	OrphanPolls int `json:"orphanPolls,omitempty"`
	// SweepOrphanedNodes deletes node records whose job and JobSet are
	// gone (default true).
	SweepOrphanedNodes *bool `json:"sweepOrphanedNodes,omitempty"`
	// CancelOnJobSetLoss cancels a running job whose JobSet vanished
	// instead of recreating it (default false).
	CancelOnJobSetLoss bool `json:"cancelOnJobSetLoss,omitempty"`
	// CancelRefusedJobs cancels a job the translation refuses (typed GPU,
	// heterogeneous job, ...) instead of leaving it held with the reason
	// in its comment (default false: the comment stays visible in squeue;
	// a cancelled job keeps it only with AccountingStoreFlags=job_comment).
	CancelRefusedJobs bool `json:"cancelRefusedJobs,omitempty"`
	// KeepFailedJobSetsFor keeps a JobSet that failed before its job did
	// for inspection (default 1h).
	KeepFailedJobSetsFor Duration `json:"keepFailedJobSetsFor,omitempty"`
}

// Fencing is the policy for re-running a job whose pod may still be alive
// on an unreachable host (docs/fencing.md).
type Fencing struct {
	// Mode: "manual" (default) requires an operator's confirmation when
	// the pod's host is not Ready; "trustKubernetes" treats a pod's
	// disappearance as proof that its processes stopped.
	Mode string `json:"mode,omitempty"`
	// ConfirmAnnotation on a JobSet lists the hosts an operator fenced.
	ConfirmAnnotation string `json:"confirmAnnotation,omitempty"`
}

const (
	FencingManual          = "manual"
	FencingTrustKubernetes = "trustKubernetes"
)

const (
	DefaultHeldComment          = "slurmkube: held for admission"
	DefaultNodeNamePrefix       = "slurmkube-w"
	defaultNodePortBase         = int32(63000)
	defaultNodePortsSize        = int32(2000)
	defaultNodePortsPerJob      = int32(32)
	defaultGresDevicePrefix     = "/dev/nvidia"
	defaultMemoryOverheadMB     = int64(256)
	defaultRegistrationTimeout  = 3 * time.Minute
	defaultGresDeviceLinkCount  = 8
	defaultFencingConfirmAnnot  = "slurmkube.io/fenced-hosts"
	defaultKeepFailedJobSetsFor = time.Hour
)

// partitionEntry is the partition's entry (Validate keeps names unique),
// nil if this instance does not manage the partition.
func (c *Config) partitionEntry(partition string) *Partition {
	i := slices.IndexFunc(c.Partitions, func(p Partition) bool { return p.Name == partition })
	if i < 0 {
		return nil
	}
	return &c.Partitions[i]
}

// PriorityClassFor maps a partition to its Kueue priority class.
func (c *Config) PriorityClassFor(partition string) (string, bool) {
	m := c.partitionEntry(partition)
	if m == nil {
		return "", false
	}
	return m.PriorityClass, true
}

// LocalQueueFor is the LocalQueue a partition's JobSets go to.
func (c *Config) LocalQueueFor(partition string) string {
	if m := c.partitionEntry(partition); m != nil && m.LocalQueue != "" {
		return m.LocalQueue
	}
	return c.LocalQueue
}

// ApplyDefaults fills what the file left out. Idempotent.
func (c *Config) ApplyDefaults() {
	if c.Slurm.RequestTimeout.Duration == 0 {
		c.Slurm.RequestTimeout.Duration = 30 * time.Second
	}
	if c.Slurm.JWTKeyFile != "" && c.Slurm.JWTLifetime.Duration == 0 {
		c.Slurm.JWTLifetime.Duration = 10 * time.Minute
	}
	if c.Claim.HeldComment == "" {
		c.Claim.HeldComment = DefaultHeldComment
	}
	if c.Slurmd.GPUResourceName == "" {
		c.Slurmd.GPUResourceName = "nvidia.com/gpu"
	}
	if c.Slurmd.NodeNamePrefix == "" {
		c.Slurmd.NodeNamePrefix = DefaultNodeNamePrefix
	}
	if c.Slurmd.NodePorts.Base == nil {
		b := defaultNodePortBase
		c.Slurmd.NodePorts.Base = &b
	}
	if c.Slurmd.NodePorts.Size == 0 {
		c.Slurmd.NodePorts.Size = defaultNodePortsSize
	}
	if c.Slurmd.NodePorts.PerJob == 0 {
		c.Slurmd.NodePorts.PerJob = defaultNodePortsPerJob
	}
	if c.Slurmd.GresDeviceLinks.Enabled() {
		if c.Slurmd.GresDeviceLinks.Count == 0 {
			c.Slurmd.GresDeviceLinks.Count = defaultGresDeviceLinkCount
		}
		if c.Slurmd.GresDeviceLinks.DevicePrefix == "" {
			c.Slurmd.GresDeviceLinks.DevicePrefix = defaultGresDevicePrefix
		}
	}
	if c.Slurmd.MemoryOverheadMB == nil {
		v := defaultMemoryOverheadMB
		c.Slurmd.MemoryOverheadMB = &v
	}
	if c.Slurmd.RegistrationTimeout.Duration == 0 {
		c.Slurmd.RegistrationTimeout.Duration = defaultRegistrationTimeout
	}
	if c.Recovery.PollInterval.Duration == 0 {
		c.Recovery.PollInterval.Duration = 10 * time.Second
	}
	if c.Recovery.VanishedOwnerGrace.Duration == 0 {
		c.Recovery.VanishedOwnerGrace.Duration = 15 * time.Second
	}
	if c.Recovery.OrphanPolls == 0 {
		c.Recovery.OrphanPolls = 3
	}
	if c.Recovery.SweepOrphanedNodes == nil {
		t := true
		c.Recovery.SweepOrphanedNodes = &t
	}
	if c.Recovery.KeepFailedJobSetsFor.Duration == 0 {
		c.Recovery.KeepFailedJobSetsFor.Duration = defaultKeepFailedJobSetsFor
	}
	if c.Fencing.Mode == "" {
		c.Fencing.Mode = FencingManual
	}
	if c.Fencing.ConfirmAnnotation == "" {
		c.Fencing.ConfirmAnnotation = defaultFencingConfirmAnnot
	}
}

// Validate rejects a configuration that cannot work.
func (c *Config) Validate() error {
	if c.Slurm.RestURL == "" || c.Namespace == "" || c.LocalQueue == "" {
		return fmt.Errorf("slurm.restURL, namespace and localQueue are required")
	}
	u, err := url.Parse(c.Slurm.RestURL)
	if err != nil {
		return fmt.Errorf("invalid slurm.restURL %q: %w", c.Slurm.RestURL, err)
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !c.Slurm.AllowInsecureHTTP {
			return fmt.Errorf("slurm.restURL uses plain http; set slurm.allowInsecureHTTP for an in-cluster endpoint")
		}
	default:
		return fmt.Errorf("slurm.restURL must be http(s)://, got %q", u.Scheme)
	}
	if c.Slurm.JWTKeyFile != "" {
		if c.Slurm.TokenFile != "" {
			return fmt.Errorf("slurm.tokenFile and slurm.jwtKeyFile are mutually exclusive")
		}
		if c.Slurm.JWTUser == "" {
			return fmt.Errorf("slurm.jwtUser is required with slurm.jwtKeyFile")
		}
		if d := c.Slurm.JWTLifetime.Duration; d < time.Minute || d > 24*time.Hour {
			return fmt.Errorf("slurm.jwtLifetime must be between 1m and 24h, got %s", d)
		}
	}
	if len(c.Partitions) == 0 {
		return fmt.Errorf("at least one partition mapping is required")
	}
	seen := map[string]bool{}
	for i, m := range c.Partitions {
		if strings.TrimSpace(m.Name) == "" || strings.TrimSpace(m.PriorityClass) == "" {
			return fmt.Errorf("partitions[%d]: name and priorityClass are required", i)
		}
		if seen[m.Name] {
			return fmt.Errorf("partitions[%d]: duplicate partition %q", i, m.Name)
		}
		seen[m.Name] = true
	}
	if c.Slurmd.Image == "" || c.Slurmd.ConfServer == "" || c.Slurmd.AuthSecretName == "" {
		return fmt.Errorf("slurmd.image, slurmd.confServer and slurmd.authSecretName are required")
	}
	if c.Slurmd.ThreadsPerCore < 0 || c.Slurmd.MaxDynamicNodes < 0 {
		return fmt.Errorf("slurmd.threadsPerCore and slurmd.maxDynamicNodes must not be negative")
	}
	if !validNodeNamePrefix(c.Slurmd.NodeNamePrefix) {
		return fmt.Errorf("slurmd.nodeNamePrefix %q must be lowercase letters, digits and '-', starting with a letter", c.Slurmd.NodeNamePrefix)
	}
	if n := c.Slurmd.NodePorts; n.Enabled() {
		if n.Size <= 0 || n.PerJob <= 0 || n.PerJob > n.Size {
			return fmt.Errorf("slurmd.nodePorts: size and perJob must be positive and perJob <= size")
		}
		if int64(*n.Base)+int64(n.Size)-1 > 65535 {
			return fmt.Errorf("slurmd.nodePorts: base+size exceeds 65535")
		}
	}
	if g := c.Slurmd.GresDeviceLinks; g.Enabled() && g.Count <= 0 {
		return fmt.Errorf("slurmd.gresDeviceLinks.count must be positive when dir is set")
	}
	for i, m := range c.Claim.OtherManagers {
		if m.Name == "" || len(m.Labels) == 0 || m.JobIDLabel == "" {
			return fmt.Errorf("claim.otherManagers[%d]: name, labels and jobIDLabel are required", i)
		}
	}
	switch c.Fencing.Mode {
	case FencingManual, FencingTrustKubernetes:
	default:
		return fmt.Errorf("fencing.mode must be %q or %q", FencingManual, FencingTrustKubernetes)
	}
	return nil
}

func validNodeNamePrefix(p string) bool {
	if p == "" || p[0] < 'a' || p[0] > 'z' {
		return false
	}
	for _, r := range p {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

// Load reads, defaults and validates a configuration file.
// Unknown keys are errors: a misspelt key would otherwise fall back to its
// default without a word.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	err = yaml.UnmarshalStrict(raw, &cfg)
	if err == nil {
		cfg.ApplyDefaults()
		err = cfg.Validate()
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
}
