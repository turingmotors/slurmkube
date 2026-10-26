// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/slurm"
)

func mustJobSet(t *testing.T, tr *Translator, j *slurm.Job) *jobsetv1alpha2.JobSet {
	t.Helper()
	js, err := tr.JobSet(j)
	if err != nil {
		t.Fatalf("JobSet: %v", err)
	}
	return js
}

func podOf(js *jobsetv1alpha2.JobSet) *corev1.PodTemplateSpec {
	return &js.Spec.ReplicatedJobs[0].Template.Spec.Template
}

func envValue(c *corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

func hostNetworkTemplate(cfg *config.Config) {
	cfg.Slurmd.PodTemplate = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{HostNetwork: true, DNSPolicy: corev1.DNSClusterFirstWithHostNet}}
}

func TestJobSetShape(t *testing.T) {
	cfg := testConfig()
	cfg.Instance = "blue"
	tr := New(cfg)
	js := mustJobSet(t, tr, job(42, func(j *slurm.Job) { j.CPUsPerTask = opt(2); j.TimeLimit = opt(60) }))

	if js.Name != "slurmkube-42" || js.Namespace != "slurm-jobs" {
		t.Errorf("name/namespace = %s/%s", js.Namespace, js.Name)
	}
	wantLabels := map[string]string{ManagedByLabel: ManagedByValue, SlurmJobIDLabel: "42", InstanceLabel: "blue",
		queueNameLabel: "main", priorityClassLabel: "normal"}
	for k, v := range wantLabels {
		if js.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, js.Labels[k], v)
		}
	}
	if js.Annotations[SubmitTimeAnnotation] != "1700000000" {
		t.Errorf("submit-time anchor = %q", js.Annotations[SubmitTimeAnnotation])
	}
	if js.Spec.Suspend == nil || !*js.Spec.Suspend {
		t.Error("the JobSet must start suspended for Kueue")
	}
	if js.Spec.FailurePolicy == nil || js.Spec.FailurePolicy.MaxRestarts != JobSetRestarts {
		t.Errorf("failure policy = %+v", js.Spec.FailurePolicy)
	}
	rj := js.Spec.ReplicatedJobs
	if len(rj) != 1 || rj[0].Name != WorkersJobName || rj[0].Replicas != 1 {
		t.Fatalf("replicated jobs = %+v", rj)
	}
	spec := rj[0].Template.Spec
	if *spec.Parallelism != 1 || *spec.Completions != 1 || *spec.BackoffLimit != 0 || *spec.ActiveDeadlineSeconds != 3960 {
		t.Errorf("job spec = parallelism %d completions %d backoff %d deadline %d", *spec.Parallelism, *spec.Completions, *spec.BackoffLimit, *spec.ActiveDeadlineSeconds)
	}
	pod := podOf(js)
	if pod.Labels[ManagedByLabel] != ManagedByValue || pod.Labels[SlurmJobIDLabel] != "42" {
		t.Errorf("pod labels = %v", pod.Labels)
	}
	if _, gated := pod.Annotations[NodePortRangeAnnotation]; gated {
		t.Error("without hostNetwork the pods are not port-gated")
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restart policy = %s", pod.Spec.RestartPolicy)
	}

	c := pod.Spec.Containers[0]
	if c.Name != "slurmd" || c.Image != "slurmd:test" {
		t.Errorf("container = %s %s", c.Name, c.Image)
	}
	wantArgs := []string{"-Z", "--conf", "'Features=slurmkube-pin-42 Port=$(" + NodePortEnv + ")'", "-N", "$(" + NodeNameEnv + ")",
		"--conf-server", "ctl:6817", "-b", "--extra", "$(" + PodUIDEnv + ")"}
	if !slices.Equal(c.Args, wantArgs) {
		t.Errorf("args = %q\n want %q", c.Args, wantArgs)
	}
	if e, ok := envValue(&c, NodeNameEnv); !ok || e.Value != "slurmkube-w$("+NodeIndexEnv+")-42" {
		t.Errorf("%s = %+v", NodeNameEnv, e)
	}
	if e, ok := envValue(&c, NodeIndexEnv); !ok || e.ValueFrom.FieldRef.FieldPath != "metadata.annotations['batch.kubernetes.io/job-completion-index']" {
		t.Errorf("%s = %+v", NodeIndexEnv, e)
	}
	if e, ok := envValue(&c, PodUIDEnv); !ok || e.ValueFrom.FieldRef.FieldPath != "metadata.uid" {
		t.Errorf("%s = %+v", PodUIDEnv, e)
	}
	// The job's base port: 63000 + (42 % (2000/32)) * 32.
	if e, ok := envValue(&c, NodePortEnv); !ok || e.Value != "64344" {
		t.Errorf("%s = %+v", NodePortEnv, e)
	}
	if c.Resources.Requests.Cpu().Value() != 2 || c.Resources.Limits.Cpu().Value() != 2 ||
		c.Resources.Requests.Memory().Value()>>20 != 2048+256 || c.Resources.Limits.Memory().Value()>>20 != 2048+256 {
		t.Errorf("resources = %v", c.Resources)
	}
	if c.SecurityContext == nil || c.SecurityContext.Privileged == nil || !*c.SecurityContext.Privileged {
		t.Error("slurmd runs privileged by default")
	}
	if c.ReadinessProbe == nil || c.ReadinessProbe.Exec == nil {
		t.Errorf("with node ports the readiness probe checks the pod's own port: %+v", c.ReadinessProbe)
	}
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != "/etc/slurm/slurm.key" || !c.VolumeMounts[0].ReadOnly {
		t.Errorf("mounts = %+v", c.VolumeMounts)
	}
	if len(pod.Spec.Volumes) != 1 || pod.Spec.Volumes[0].Secret == nil || pod.Spec.Volumes[0].Secret.SecretName != "slurm-auth" {
		t.Errorf("volumes = %+v", pod.Spec.Volumes)
	}
}

func TestJobSetRefusesUnmappedPartition(t *testing.T) {
	_, err := New(testConfig()).JobSet(job(1, func(j *slurm.Job) { j.Partition = "other" }))
	if err == nil || !strings.Contains(err.Error(), "other") {
		t.Errorf("expected a partition error, got %v", err)
	}
}

func TestJobSetOfArrayIsNamedAfterTheRoot(t *testing.T) {
	js := mustJobSet(t, New(testConfig()), job(101, func(j *slurm.Job) { j.ArrayJobID = opt(100); j.ArrayTaskID = opt(1) }))
	if js.Name != "slurmkube-100" || js.Labels[SlurmJobIDLabel] != "100" {
		t.Errorf("array task JobSet = %s (%s)", js.Name, js.Labels[SlurmJobIDLabel])
	}
	c := podOf(js).Spec.Containers[0]
	if !strings.Contains(c.Args[2], "Features=slurmkube-pin-100") {
		t.Errorf("array task pins to the root: %s", c.Args[2])
	}
}

func TestNodePortsOffUsesTheDefaultPort(t *testing.T) {
	cfg := testConfig()
	zero := int32(0)
	cfg.Slurmd.NodePorts.Base = &zero
	c := podOf(mustJobSet(t, New(cfg), job(1))).Spec.Containers[0]
	if strings.Contains(c.Args[2], "Port=") {
		t.Errorf("conf names a port without node ports: %s", c.Args[2])
	}
	if _, ok := envValue(&c, NodePortEnv); ok {
		t.Error("no port env without node ports")
	}
	if c.ReadinessProbe.TCPSocket == nil || c.ReadinessProbe.TCPSocket.Port.IntVal != DefaultSlurmdPort {
		t.Errorf("readiness = %+v", c.ReadinessProbe)
	}
}

func TestHostNetworkPodsAreGated(t *testing.T) {
	cfg := testConfig()
	hostNetworkTemplate(cfg)
	js := mustJobSet(t, New(cfg), job(7))
	pod := podOf(js)
	if pod.Annotations[NodePortRangeAnnotation] != "63000-64999" {
		t.Errorf("range annotation = %q", pod.Annotations[NodePortRangeAnnotation])
	}
	if !pod.Spec.HostNetwork || pod.Spec.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Error("the template's hostNetwork and dnsPolicy must survive")
	}
	c := pod.Spec.Containers[0]
	e, ok := envValue(&c, NodePortEnv)
	if !ok || e.ValueFrom == nil || e.ValueFrom.FieldRef.FieldPath != "metadata.annotations['"+NodePortAnnotation+"']" {
		t.Errorf("under hostNetwork the port comes from the stamp: %+v", e)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != gateInitName {
		t.Fatalf("init containers = %+v", pod.Spec.InitContainers)
	}
	gate := pod.Spec.InitContainers[0]
	if gate.Image != c.Image || len(gate.Command) != 3 || !strings.Contains(gate.Command[2], NodeRecordClearAnnotation) {
		t.Errorf("gate = %+v", gate)
	}
	var files []string
	for _, v := range pod.Spec.Volumes {
		if v.DownwardAPI != nil {
			for _, it := range v.DownwardAPI.Items {
				files = append(files, it.Path+"="+it.FieldRef.FieldPath)
			}
		}
	}
	if !slices.Contains(files, gatePortFile+"=metadata.annotations['"+NodePortAnnotation+"']") ||
		!slices.Contains(files, gateClearFile+"=metadata.annotations['"+NodeRecordClearAnnotation+"']") {
		t.Errorf("gate volume files = %v", files)
	}
	if !RecordGated(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: pod.Annotations}}) {
		t.Error("RecordGated must see the range annotation")
	}
}

func TestParseNodePortRange(t *testing.T) {
	lo, hi, err := ParseNodePortRange(NodePortRange(63000, 2000))
	if err != nil || lo != 63000 || hi != 64999 {
		t.Errorf("round trip = %d-%d, %v", lo, hi, err)
	}
	for _, bad := range []string{"", "63000", "64999-63000", "0-10", "1-70000", "a-b"} {
		if _, _, err := ParseNodePortRange(bad); err == nil {
			t.Errorf("ParseNodePortRange(%q) must fail", bad)
		}
	}
}

func TestPodTemplateMergesByName(t *testing.T) {
	cfg := testConfig()
	small := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("32Mi")}
	cfg.Slurmd.PodTemplate = &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"team": "hpc"}, Annotations: map[string]string{"note": "x"}},
		Spec: corev1.PodSpec{
			NodeSelector:     map[string]string{"pool": "slurm"},
			RuntimeClassName: ptr("nvidia"),
			Containers: []corev1.Container{
				{Name: "slurmd", Env: []corev1.EnvVar{{Name: "SLURMD_DEBUG", Value: "3"}}},
				{Name: "sidecar", Image: "busybox", Resources: corev1.ResourceRequirements{Requests: small, Limits: small}},
			},
		},
	}
	js := mustJobSet(t, New(cfg), job(3, func(j *slurm.Job) { j.CPUsPerTask = opt(2) }))
	pod := podOf(js)
	if pod.Labels["team"] != "hpc" || pod.Labels[ManagedByLabel] != ManagedByValue || pod.Annotations["note"] != "x" {
		t.Errorf("pod metadata = %v %v", pod.Labels, pod.Annotations)
	}
	if pod.Spec.NodeSelector["pool"] != "slurm" || pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "nvidia" {
		t.Error("template scalars must be kept")
	}
	if len(pod.Spec.Containers) != 2 || pod.Spec.Containers[0].Name != "slurmd" || pod.Spec.Containers[1].Name != "sidecar" {
		t.Fatalf("containers = %+v", pod.Spec.Containers)
	}
	c := pod.Spec.Containers[0]
	if e, ok := envValue(&c, "SLURMD_DEBUG"); !ok || e.Value != "3" {
		t.Error("the template's env must be merged into slurmd")
	}
	if _, ok := envValue(&c, NodeNameEnv); !ok {
		t.Error("the generated env must survive the merge")
	}
	if c.Image != "slurmd:test" || c.Resources.Requests.Cpu().Value() != 2 {
		t.Error("the generated image and resources must survive the merge")
	}
}

func TestPodTemplateMustKeepPodsGuaranteed(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.PodTemplate = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "slurmd"}, {Name: "sidecar", Image: "busybox"},
	}}}
	_, err := New(cfg).JobSet(job(1))
	var u *Unsupported
	if !errors.As(err, &u) || !strings.Contains(u.Reason, "sidecar") || !strings.Contains(u.Reason, "Guaranteed") {
		t.Errorf("a sidecar without equal requests and limits must be refused, got %v", err)
	}
	cfg.Slurmd.PodTemplate = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "other", Image: "x"}}}}
	if _, err := New(cfg).JobSet(job(1)); err == nil {
		t.Error("a template that displaces the slurmd container must be refused")
	}
}

func TestHostSelection(t *testing.T) {
	cfg := testConfig()
	tr := New(cfg)
	js := mustJobSet(t, tr, job(1, func(j *slurm.Job) {
		j.NodeCount, j.Tasks = opt(2), opt(2)
		j.Extra = slurm.NodeListKey + "gpu-[1-2];" + slurm.ExcludeListKey + "gpu-9"
	}))
	na := podOf(js).Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if na == nil || len(na.NodeSelectorTerms) != 1 {
		t.Fatalf("node affinity = %+v", na)
	}
	exprs := na.NodeSelectorTerms[0].MatchExpressions
	if len(exprs) != 2 || exprs[0].Operator != corev1.NodeSelectorOpIn || !slices.Equal(exprs[0].Values, []string{"gpu-1", "gpu-2"}) ||
		exprs[1].Operator != corev1.NodeSelectorOpNotIn || !slices.Equal(exprs[1].Values, []string{"gpu-9"}) {
		t.Errorf("match expressions = %+v", exprs)
	}
	_, err := tr.JobSet(job(2, func(j *slurm.Job) { j.NodeCount, j.Tasks, j.Extra = opt(3), opt(3), slurm.NodeListKey+"gpu-[1-2]" }))
	var u *Unsupported
	if !errors.As(err, &u) || u.Option != "-w" {
		t.Errorf("-w with fewer hosts than nodes must be refused, got %v", err)
	}
	if _, err := tr.JobSet(job(3, func(j *slurm.Job) { j.Extra = slurm.NodeListKey + "gpu-[2-1]" })); err == nil {
		t.Error("an invalid hostlist must be refused")
	}
	// -w with one pod and the pod network also asks Kueue for that host.
	one := mustJobSet(t, tr, job(4, func(j *slurm.Job) { j.Extra = slurm.NodeListKey + "gpu-1" }))
	zero := int32(0)
	cfg.Slurmd.NodePorts.Base = &zero
	one = mustJobSet(t, tr, job(4, func(j *slurm.Job) { j.Extra = slurm.NodeListKey + "gpu-1" }))
	if podOf(one).Annotations[topologyRequired] != hostnameLabel {
		t.Errorf("topology annotations = %v", podOf(one).Annotations)
	}
}

func TestTopologyAnnotations(t *testing.T) {
	cfg := testConfig()
	cfg.Topology = config.Topology{SwitchesLevel: "rack", PackLevel: "block"}
	tr := New(cfg)
	if a := podOf(mustJobSet(t, tr, job(1, func(j *slurm.Job) { j.RequiredSwitches = 1 }))).Annotations; a[topologyRequired] != "rack" {
		t.Errorf("--switches: %v", a)
	}
	if a := podOf(mustJobSet(t, tr, job(2))).Annotations; a[topologyPreferred] != "block" || a[topologyRequired] != "" {
		t.Errorf("default: %v", a)
	}
}

func TestExclusiveTakesTheWholeHost(t *testing.T) {
	js := mustJobSet(t, New(testConfig()), job(1, func(j *slurm.Job) { j.Exclusive = []string{"true"} }))
	pa := podOf(js).Spec.Affinity.PodAntiAffinity
	if pa == nil || len(pa.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("anti-affinity = %+v", pa)
	}
	term := pa.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if term.TopologyKey != hostnameLabel || term.LabelSelector.MatchLabels[ManagedByLabel] != ManagedByValue {
		t.Errorf("term = %+v", term)
	}
}

func TestTemplateAntiAffinityAgainstOurselvesIsDroppedUnderHostNetwork(t *testing.T) {
	cfg := testConfig()
	hostNetworkTemplate(cfg)
	cfg.Slurmd.PodTemplate.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
			{TopologyKey: hostnameLabel, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{ManagedByLabel: ManagedByValue}}},
			{TopologyKey: hostnameLabel, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "db"}}},
		}}}
	pa := podOf(mustJobSet(t, New(cfg), job(1))).Spec.Affinity.PodAntiAffinity
	if pa == nil || len(pa.RequiredDuringSchedulingIgnoredDuringExecution) != 1 || pa.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchLabels["app"] != "db" {
		t.Errorf("anti-affinity = %+v", pa)
	}
}

func TestGPUPodsGetDeviceLinksAndTolerations(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.GresDeviceLinks = config.GresDeviceLinks{Dir: "/run/gres", Count: 8, DevicePrefix: "/dev/nvidia"}
	js := mustJobSet(t, New(cfg), job(1, func(j *slurm.Job) { j.TresPerNode = "gres/gpu:2" }))
	pod := podOf(js)
	c := pod.Spec.Containers[0]
	if q := c.Resources.Limits["nvidia.com/gpu"]; q.Value() != 2 {
		t.Errorf("GPU limit = %s", q.String())
	}
	if !strings.Contains(c.Args[2], "Gres=gpu:2") {
		t.Errorf("conf = %s", c.Args[2])
	}
	if e, ok := envValue(&c, "NVIDIA_VISIBLE_DEVICES"); !ok || e.Value != "all" {
		t.Error("without the device plugin slurmd sees all devices")
	}
	if len(pod.Spec.Tolerations) != 1 || pod.Spec.Tolerations[0].Key != "nvidia.com/gpu" {
		t.Errorf("tolerations = %+v", pod.Spec.Tolerations)
	}
	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != deviceLinksInit {
		t.Fatalf("init containers = %+v", pod.Spec.InitContainers)
	}
	init := pod.Spec.InitContainers[0]
	if q := init.Resources.Limits["nvidia.com/gpu"]; q.Value() != 2 {
		t.Errorf("the init container must be handed the same GPUs, got %s", q.String())
	}
	if !strings.Contains(init.Command[2], "want=2") || !strings.Contains(init.Command[2], "count=8") {
		t.Errorf("script = %s", init.Command[2])
	}
	if _, ok := envValue(&init, "NVIDIA_VISIBLE_DEVICES"); !ok {
		t.Error("the init container needs the devices too")
	}
	var mounted bool
	for _, m := range c.VolumeMounts {
		mounted = mounted || m.MountPath == "/run/gres"
	}
	if !mounted {
		t.Errorf("slurmd must mount the links dir: %+v", c.VolumeMounts)
	}
	// Without GPUs the links are all /dev/null, and no GPU resource is asked.
	none := podOf(mustJobSet(t, New(cfg), job(2)))
	if _, ok := none.Spec.InitContainers[0].Resources.Limits["nvidia.com/gpu"]; ok || !strings.Contains(none.Spec.InitContainers[0].Command[2], "want=0") {
		t.Errorf("CPU job's device links = %+v", none.Spec.InitContainers[0])
	}
}

func TestDevicePluginModeLeavesDeviceSelectionToKubernetes(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.GPUDevicesFromDevicePlugin = true
	c := podOf(mustJobSet(t, New(cfg), job(1, func(j *slurm.Job) { j.TresPerNode = "gres/gpu:1" }))).Spec.Containers[0]
	if _, ok := envValue(&c, "NVIDIA_VISIBLE_DEVICES"); ok {
		t.Error("NVIDIA_VISIBLE_DEVICES must not be forced with the device plugin")
	}
}

func TestPodSizedNodesAdvertiseTheirOwnTopology(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.AdvertiseHostResources = false
	c := podOf(mustJobSet(t, New(cfg), job(1, func(j *slurm.Job) { j.CPUsPerTask = opt(3) }))).Spec.Containers[0]
	want := "'Features=slurmkube-pin-1 CPUs=4 SocketsPerBoard=1 CoresPerSocket=2 ThreadsPerCore=2 RealMemory=4096 Port=$(" + NodePortEnv + ")'"
	if c.Args[2] != want {
		t.Errorf("conf = %s\n want %s", c.Args[2], want)
	}
}

func TestNodeFeaturesAndCustomPrefix(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.NodeFeatures = []string{"k8s", "dyn"}
	cfg.Slurmd.NodeNamePrefix = "hpc-w"
	tr := New(cfg)
	c := podOf(mustJobSet(t, tr, job(9))).Spec.Containers[0]
	if !strings.HasPrefix(c.Args[2], "'Features=slurmkube-pin-9,k8s,dyn") {
		t.Errorf("conf = %s", c.Args[2])
	}
	if e, _ := envValue(&c, NodeNameEnv); e.Value != "hpc-w$("+NodeIndexEnv+")-9" {
		t.Errorf("node name env = %q", e.Value)
	}
	if id, idx, ok := tr.Naming.ParseNodeName("hpc-w1-9"); !ok || id != 9 || idx != 1 {
		t.Error("the translator's naming must parse its own names")
	}
}

func TestUnprivilegedSlurmdKeepsTheCapabilitiesItNeeds(t *testing.T) {
	cfg := testConfig()
	cfg.Slurmd.Privileged = ptr(false)
	cfg.Slurmd.NodeIdentity = ptr(false)
	cfg.Slurmd.NFS = &config.NFSMount{Server: "nfs", Path: "/export", MountPath: "/home"}
	pod := podOf(mustJobSet(t, New(cfg), job(1)))
	c := pod.Spec.Containers[0]
	sc := c.SecurityContext
	if sc.Privileged == nil || *sc.Privileged || sc.Capabilities == nil || !slices.Contains(sc.Capabilities.Add, "SYS_ADMIN") {
		t.Errorf("security context = %+v", sc)
	}
	if slices.Contains(c.Args, "--extra") {
		t.Error("nodeIdentity off: no --extra")
	}
	var nfs bool
	for _, v := range pod.Spec.Volumes {
		nfs = nfs || (v.NFS != nil && v.NFS.Server == "nfs")
	}
	if !nfs || len(c.VolumeMounts) != 2 {
		t.Errorf("shared storage missing: %+v %+v", pod.Spec.Volumes, c.VolumeMounts)
	}
}
