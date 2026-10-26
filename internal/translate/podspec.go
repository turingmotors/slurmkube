// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/slurm"
)

// podSpec builds the slurmd pod for a job of the given shape: the slurmd
// container, then the site's podTemplate merged in, then the pieces that
// must survive the merge (port stamp gate, identity, host selection), then
// the Guaranteed check.
func (t *Translator) podSpec(job *slurm.Job, shape Shape) (corev1.PodSpec, error) {
	cfg := t.Cfg
	spec := corev1.PodSpec{
		RestartPolicy: corev1.RestartPolicyNever,
		Containers:    []corev1.Container{t.slurmdContainer(job, shape)},
		Volumes: []corev1.Volume{{
			Name: "slurm-key",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName: cfg.Slurmd.AuthSecretName, DefaultMode: ptr[int32](0o600)}},
		}},
	}
	if ss := cfg.Slurmd.NFS; ss != nil {
		spec.Containers[0].VolumeMounts = append(spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "shared-storage", MountPath: ss.MountPath})
		spec.Volumes = append(spec.Volumes, corev1.Volume{Name: "shared-storage",
			VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: ss.Server, Path: ss.Path}}})
	}
	if shape.GPUs > 0 {
		spec.Tolerations = append(spec.Tolerations, corev1.Toleration{
			Key: cfg.Slurmd.GPUResourceName, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
	}
	if cfg.Slurmd.GresDeviceLinks.Enabled() {
		addDeviceLinks(&spec, cfg, shape.GPUs)
	}
	if cfg.Slurmd.PodTemplate != nil {
		merged, err := mergeTemplate(spec, cfg.Slurmd.PodTemplate.Spec)
		if err != nil {
			return corev1.PodSpec{}, fmt.Errorf("applying slurmd.podTemplate: %w", err)
		}
		spec = merged
	}
	if t.stampedPorts(&spec) {
		addPortGate(&spec)
		// Several pods of ours may share a host: a template's anti-affinity
		// on our own label would forbid that.
		dropSelfAntiAffinity(&spec)
	}
	if cfg.Slurmd.NodeIdentityEnabled() {
		addIdentity(&spec)
	}
	if err := t.hostSelection(&spec, job, shape); err != nil {
		return corev1.PodSpec{}, err
	}
	if job.IsExclusive() {
		requireAntiAffinity(&spec)
	}
	if err := requireGuaranteed(&spec); err != nil {
		return corev1.PodSpec{}, err
	}
	return spec, nil
}

// stampedPorts: per-pod ports are needed when pods share the host's
// network namespace; on the pod network every pod has its own IP.
func (t *Translator) stampedPorts(spec *corev1.PodSpec) bool {
	return t.Cfg.Slurmd.NodePorts.Enabled() && spec.HostNetwork
}

func (t *Translator) slurmdContainer(job *slurm.Job, shape Shape) corev1.Container {
	cfg := t.Cfg
	c := corev1.Container{
		Name:            "slurmd",
		Image:           cfg.Slurmd.Image,
		SecurityContext: securityContext(cfg),
		Args: []string{
			"-Z", // dynamic node
			"--conf", t.slurmdConf(job, shape),
			"-N", "$(" + NodeNameEnv + ")",
			"--conf-server", cfg.Slurmd.ConfServer,
			// Report a boot on every start: slurmctld then takes --extra on
			// each registration and requeues or fails a job whose node
			// came back empty instead of waiting on a ghost.
			"-b",
		},
		Env:            t.env(job, shape),
		Resources:      resources(cfg, shape),
		ReadinessProbe: readinessProbe(cfg),
		VolumeMounts: []corev1.VolumeMount{{
			Name: "slurm-key", MountPath: "/etc/slurm/slurm.key", SubPath: "slurm.key", ReadOnly: true}},
	}
	return c
}

// slurmdConf is the dynamic node's configuration line: its pin feature,
// the site's features, its size when the host's topology is not
// advertised, its GPUs, and its port.
func (t *Translator) slurmdConf(job *slurm.Job, shape Shape) string {
	cfg := t.Cfg
	features := append([]string{t.Naming.PinFeature(job.PinID())}, cfg.Slurmd.NodeFeatures...)
	conf := "Features=" + strings.Join(features, ",")
	switch {
	case !cfg.Slurmd.AdvertiseHostResources:
		threads := max(cfg.Slurmd.ThreadsPerCore, 1)
		cores := max((shape.CPUs+threads-1)/threads, 1)
		conf += fmt.Sprintf(" CPUs=%d SocketsPerBoard=1 CoresPerSocket=%d ThreadsPerCore=%d RealMemory=%d", cores*threads, cores, threads, shape.MemoryMB)
	case shape.Array && !cfg.Slurmd.NodePorts.Enabled():
		conf += fmt.Sprintf(" RealMemory=%d", shape.MemoryMB)
	}
	if shape.GPUs > 0 {
		conf += fmt.Sprintf(" Gres=gpu:%d", shape.GPUs)
	}
	if cfg.Slurmd.NodePorts.Enabled() {
		conf += " Port=$(" + NodePortEnv + ")"
	}
	return "'" + conf + "'"
}

func (t *Translator) env(job *slurm.Job, shape Shape) []corev1.EnvVar {
	cfg := t.Cfg
	env := []corev1.EnvVar{
		{Name: NodeIndexEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
			FieldPath: "metadata.annotations['batch.kubernetes.io/job-completion-index']"}}},
		{Name: NodeNameEnv, Value: t.Naming.NodeNameEnvValue(job.PinID())},
	}
	if np := cfg.Slurmd.NodePorts; np.Enabled() {
		// The job's base port; stamped per pod instead under hostNetwork.
		slots := max(int64(np.Size/np.PerJob), 1)
		env = append(env, corev1.EnvVar{Name: NodePortEnv, Value: fmt.Sprint(int64(*np.Base) + int64(job.PinID())%slots*int64(np.PerJob))})
	}
	if shape.GPUs > 0 {
		env = append(env,
			corev1.EnvVar{Name: "PATH", Value: "/usr/local/nvidia/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
			corev1.EnvVar{Name: "LD_LIBRARY_PATH", Value: "/usr/local/nvidia/lib64"},
			corev1.EnvVar{Name: "NVIDIA_DRIVER_CAPABILITIES", Value: "compute,utility"})
		if !cfg.Slurmd.GPUDevicesFromDevicePlugin {
			env = append(env, corev1.EnvVar{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"})
		}
	}
	return env
}

// resources: requests equal limits (Guaranteed, so the kubelet's static
// CPU manager pins whole cores). Memory gets slurmd's own headroom so
// Slurm's job cgroup, not the kubelet, ends an overrun.
func resources(cfg *config.Config, shape Shape) corev1.ResourceRequirements {
	overhead := int64(0)
	if cfg.Slurmd.MemoryOverheadMB != nil {
		overhead = *cfg.Slurmd.MemoryOverheadMB
	}
	rl := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewQuantity(shape.CPUs, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity((shape.MemoryMB+overhead)<<20, resource.BinarySI),
	}
	if shape.GPUs > 0 {
		rl[corev1.ResourceName(cfg.Slurmd.GPUResourceName)] = *resource.NewQuantity(shape.GPUs, resource.DecimalSI)
	}
	return corev1.ResourceRequirements{Requests: rl, Limits: rl.DeepCopy()}
}

// slurmdCapabilities are what an unprivileged slurmd needs: cgroups and
// namespaces, scheduling, networking, and switching to the job's user.
var slurmdCapabilities = []corev1.Capability{"SYS_ADMIN", "SYS_NICE", "NET_ADMIN", "SETUID", "SETGID", "CHOWN"}

func securityContext(cfg *config.Config) *corev1.SecurityContext {
	privileged := cfg.Slurmd.PrivilegedOrDefault()
	sc := &corev1.SecurityContext{Privileged: ptr(privileged)}
	if !privileged {
		sc.AllowPrivilegeEscalation = ptr(true)
		sc.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: slices.Clone(slurmdCapabilities)}
	}
	return sc
}

func readinessProbe(cfg *config.Config) *corev1.Probe {
	if !cfg.Slurmd.NodePorts.Enabled() {
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(DefaultSlurmdPort)}},
			InitialDelaySeconds: 2, PeriodSeconds: 3}
	}
	return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{
		"sh", "-c", `kill -0 1 && nc -z localhost "$` + NodePortEnv + `" >/dev/null 2>&1`}}},
		InitialDelaySeconds: 2, PeriodSeconds: 3}
}

// addIdentity makes slurmd register with --extra <pod UID>: the record
// then names the pod that owns it.
func addIdentity(spec *corev1.PodSpec) {
	c := &spec.Containers[0]
	c.Env = append(c.Env, corev1.EnvVar{Name: PodUIDEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}})
	c.Args = append(c.Args, "--extra", "$("+PodUIDEnv+")")
}

// hostSelection maps -w to a required node affinity and -x to an exclusion.
func (t *Translator) hostSelection(spec *corev1.PodSpec, job *slurm.Job, shape Shape) error {
	include, exclude, err := job.HostSelection()
	if err != nil {
		return unsupported("-w/-x", "%v", err)
	}
	if len(include) == 0 && len(exclude) == 0 {
		return nil
	}
	if len(include) > 0 && int32(len(include)) < shape.Pods {
		return unsupported("-w", "%d hosts named for %d nodes", len(include), shape.Pods)
	}
	var terms []corev1.NodeSelectorRequirement
	if len(include) > 0 {
		terms = append(terms, corev1.NodeSelectorRequirement{Key: hostnameLabel, Operator: corev1.NodeSelectorOpIn, Values: include})
	}
	if len(exclude) > 0 {
		terms = append(terms, corev1.NodeSelectorRequirement{Key: hostnameLabel, Operator: corev1.NodeSelectorOpNotIn, Values: exclude})
	}
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.NodeAffinity == nil {
		spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	sel := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if sel == nil {
		sel = &corev1.NodeSelector{}
	}
	sel.NodeSelectorTerms = append(sel.NodeSelectorTerms, corev1.NodeSelectorTerm{MatchExpressions: terms})
	spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = sel
	return nil
}

// requireAntiAffinity gives --exclusive the whole host: no other SlurmKube
// pod may share it.
func requireAntiAffinity(spec *corev1.PodSpec) {
	if spec.Affinity == nil {
		spec.Affinity = &corev1.Affinity{}
	}
	if spec.Affinity.PodAntiAffinity == nil {
		spec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}
	spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
		spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
		corev1.PodAffinityTerm{TopologyKey: hostnameLabel,
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{ManagedByLabel: ManagedByValue}}})
}

// dropSelfAntiAffinity removes a template's anti-affinity against our own
// pods (one pod per host was the rule before per-pod ports).
func dropSelfAntiAffinity(spec *corev1.PodSpec) {
	if spec.Affinity == nil || spec.Affinity.PodAntiAffinity == nil {
		return
	}
	pa := spec.Affinity.PodAntiAffinity
	var kept []corev1.PodAffinityTerm
	for _, term := range pa.RequiredDuringSchedulingIgnoredDuringExecution {
		if term.LabelSelector != nil && term.LabelSelector.MatchLabels[ManagedByLabel] == ManagedByValue {
			continue
		}
		kept = append(kept, term)
	}
	pa.RequiredDuringSchedulingIgnoredDuringExecution = kept
	if len(kept) == 0 && len(pa.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
		spec.Affinity.PodAntiAffinity = nil
	}
}

// requireGuaranteed: every container must request exactly what it limits
// for CPU and memory, or the kubelet's static CPU manager gives the pod no
// cpuset and slurmd would bind CPUs other pods own.
func requireGuaranteed(spec *corev1.PodSpec) error {
	check := func(c *corev1.Container, kind string) error {
		for _, res := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			req, limit := c.Resources.Requests[res], c.Resources.Limits[res]
			if req.IsZero() || limit.IsZero() || req.Cmp(limit) != 0 {
				return unsupported("slurmd.podTemplate", "%s container %q must request exactly its %s limit (Guaranteed pod); got request %s, limit %s", kind, c.Name, res, req.String(), limit.String())
			}
		}
		return nil
	}
	for i := range spec.InitContainers {
		if err := check(&spec.InitContainers[i], "init"); err != nil {
			return err
		}
	}
	for i := range spec.Containers {
		if err := check(&spec.Containers[i], "app"); err != nil {
			return err
		}
	}
	return nil
}

// mergeTemplate strategic-merge-patches the site's template onto the
// generated spec: containers, volumes, env and mounts merge by name,
// scalars from the template win.
func mergeTemplate(generated, overlay corev1.PodSpec) (corev1.PodSpec, error) {
	base, err := json.Marshal(generated)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	// A template without containers marshals "containers": null, which a
	// strategic patch reads as "delete"; drop the key instead.
	var patch map[string]any
	raw, err := json.Marshal(overlay)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	if err := json.Unmarshal(raw, &patch); err != nil {
		return corev1.PodSpec{}, err
	}
	if v, ok := patch["containers"]; ok && v == nil {
		delete(patch, "containers")
	}
	raw, _ = json.Marshal(patch)
	merged, err := strategicpatch.StrategicMergePatch(base, raw, corev1.PodSpec{})
	if err != nil {
		return corev1.PodSpec{}, err
	}
	var out corev1.PodSpec
	if err := json.Unmarshal(merged, &out); err != nil {
		return corev1.PodSpec{}, err
	}
	if len(out.Containers) == 0 || out.Containers[0].Name != "slurmd" {
		return corev1.PodSpec{}, fmt.Errorf("the template must keep the slurmd container first")
	}
	return out, nil
}

func ptr[T any](v T) *T { return &v }
