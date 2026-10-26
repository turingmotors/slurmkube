// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/turingmotors/slurmkube/internal/config"
)

// GPU device links.
//
// slurmd reads gres.conf from the controller (configless) and the site
// keeps one static line for every dynamic node:
// "NodeName=<prefix>* Name=gpu File=<dir>/gpu[0-<count-1>]". An init
// container links the GPU device nodes Kubernetes allocated to the pod
// under those stable names in an emptyDir the slurmd container mounts at
// the same path, the remaining ones to /dev/null; slurmd then registers
// exactly the pod's GPUs without NVML autodetection.
const (
	deviceLinksInit   = "slurmkube-device-links"
	deviceLinksVolume = "slurmkube-device-links"
)

func addDeviceLinks(spec *corev1.PodSpec, cfg *config.Config, gpus int64) {
	g := cfg.Slurmd.GresDeviceLinks
	slurmd := &spec.Containers[0]
	mount := corev1.VolumeMount{Name: deviceLinksVolume, MountPath: g.Dir}
	// Small, equal requests keep the pod Guaranteed without changing its
	// effective request; the GPUs mirror slurmd's so the kubelet hands the
	// init container the same devices.
	rl := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}
	if gpus > 0 {
		rl[corev1.ResourceName(cfg.Slurmd.GPUResourceName)] = *resource.NewQuantity(gpus, resource.DecimalSI)
	}
	init := corev1.Container{
		Name:         deviceLinksInit,
		Image:        slurmd.Image,
		Command:      []string{"/bin/sh", "-ec", deviceLinksScript(g, gpus)},
		Resources:    corev1.ResourceRequirements{Requests: rl, Limits: rl.DeepCopy()},
		VolumeMounts: []corev1.VolumeMount{mount},
	}
	for _, e := range slurmd.Env {
		if e.Name == "NVIDIA_VISIBLE_DEVICES" {
			init.Env = append(init.Env, e)
		}
	}
	spec.InitContainers = append(spec.InitContainers, init)
	slurmd.VolumeMounts = append(slurmd.VolumeMounts, mount)
	spec.Volumes = append(spec.Volumes, corev1.Volume{Name: deviceLinksVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
}

func deviceLinksScript(g config.GresDeviceLinks, gpus int64) string {
	return fmt.Sprintf(`dir=%s; prefix=%s; count=%d; want=%d
i=0
if [ "$want" -gt 0 ]; then
  for n in $(ls "$(dirname "$prefix")" | sed -n "s|^$(basename "$prefix")\([0-9][0-9]*\)$|\1|p" | sort -n); do
    [ "$i" -lt "$count" ] || break
    ln -sfn "$prefix$n" "$dir/gpu$i"; i=$((i+1))
  done
fi
found=$i
while [ "$i" -lt "$count" ]; do ln -sfn /dev/null "$dir/gpu$i"; i=$((i+1)); done
ls -l "$dir"
[ "$found" -eq "$want" ] || { echo "slurmkube: expected $want GPU device node(s) ${prefix}<N>, found $found" >&2; exit 1; }
`, shellQuote(g.Dir), shellQuote(g.DevicePrefix), g.Count, gpus)
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
