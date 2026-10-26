// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// The port and record gate.
//
// Under hostNetwork several slurmd pods share a host and need distinct
// ports; the node-port allocator chooses one per pod after scheduling and
// stamps it as NodePortAnnotation. slurmd must not start before that, nor
// while slurmctld still has a record under the pod's node name (a stale
// record keeps its predecessor's port, and slurmd runs any batch launch
// it is sent). An init container therefore waits for both annotations,
// read through a downward-API volume that follows annotation changes;
// the slurmd container's SLURMKUBE_NODE_PORT env reads the stamp.
const (
	gateInitName      = "slurmkube-gate"
	gateVolume        = "slurmkube-gate"
	gateDir           = "/run/slurmkube/gate"
	gatePortFile      = "port"
	gateClearFile     = "node-record-clear"
	gatePortWaitSec   = 300
	gateRecordWaitSec = 600
)

// NodePortRange formats the configured range for the annotation.
func NodePortRange(base, size int32) string {
	return fmt.Sprintf("%d-%d", base, base+size-1)
}

// ParseNodePortRange reads "lo-hi".
func ParseNodePortRange(s string) (lo, hi int32, err error) {
	a, b, ok := strings.Cut(s, "-")
	if !ok {
		return 0, 0, fmt.Errorf("port range %q is not lo-hi", s)
	}
	l, err1 := strconv.ParseInt(a, 10, 32)
	h, err2 := strconv.ParseInt(b, 10, 32)
	if err1 != nil || err2 != nil || l < 1 || h > 65535 || h < l {
		return 0, 0, fmt.Errorf("port range %q is invalid", s)
	}
	return int32(l), int32(h), nil
}

// RecordGated reports whether a pod's slurmd waits for the clearance.
func RecordGated(pod *corev1.Pod) bool {
	_, ok := pod.Annotations[NodePortRangeAnnotation]
	return ok
}

// addPortGate rewires the slurmd port env to the stamp and prepends the
// gate init container.
func addPortGate(spec *corev1.PodSpec) {
	slurmd := &spec.Containers[0]
	for i := range slurmd.Env {
		if slurmd.Env[i].Name == NodePortEnv {
			slurmd.Env[i] = corev1.EnvVar{Name: NodePortEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.annotations['" + NodePortAnnotation + "']"}}}
		}
	}
	small := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}
	spec.InitContainers = append(spec.InitContainers, corev1.Container{
		Name:         gateInitName,
		Image:        slurmd.Image,
		Command:      []string{"/bin/sh", "-ec", gateScript()},
		Resources:    corev1.ResourceRequirements{Requests: small, Limits: small.DeepCopy()},
		VolumeMounts: []corev1.VolumeMount{{Name: gateVolume, MountPath: gateDir, ReadOnly: true}},
	})
	spec.Volumes = append(spec.Volumes, corev1.Volume{Name: gateVolume, VolumeSource: corev1.VolumeSource{
		DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: []corev1.DownwardAPIVolumeFile{
			{Path: gatePortFile, FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + NodePortAnnotation + "']"}},
			{Path: gateClearFile, FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['" + NodeRecordClearAnnotation + "']"}},
		}}}})
}

// gateScript waits for a valid port, then for the clearance. Anything but
// a port number keeps waiting, so slurmd never starts on a guess; bounds
// turn a stopped controller into a visible init failure.
func gateScript() string {
	return fmt.Sprintf(`port_file=%s/%s; clear_file=%s/%s
i=0
while :; do
  p=$(cat "$port_file" 2>/dev/null || true)
  case "$p" in ''|*[!0-9]*) ;; *) [ "$p" -ge 1 ] && [ "$p" -le 65535 ] && break ;; esac
  i=$((i+1)); [ "$i" -le %d ] || { echo "slurmkube: no slurmd port assigned within %ds (annotation %s is '$p'); is the controller running?" >&2; exit 1; }
  sleep 1
done
echo "slurmkube: slurmd port $p (waited ${i}s)"
j=0
while [ -z "$(cat "$clear_file" 2>/dev/null || true)" ]; do
  j=$((j+1)); [ "$j" -le %d ] || { echo "slurmkube: slurmctld still holds a record under this node name after %ds (annotation %s unset); is the controller running?" >&2; exit 1; }
  sleep 1
done
echo "slurmkube: node record clear, starting slurmd (waited ${j}s)"
`, gateDir, gatePortFile, gateDir, gateClearFile, gatePortWaitSec, gatePortWaitSec, NodePortAnnotation,
		gateRecordWaitSec, gateRecordWaitSec, NodeRecordClearAnnotation)
}
