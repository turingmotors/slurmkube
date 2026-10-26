// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/turingmotors/slurmkube/internal/config"
	"github.com/turingmotors/slurmkube/internal/slurm"
)

// Translator turns Slurm jobs into JobSets for one configuration.
type Translator struct {
	Cfg    *config.Config
	Naming Naming
}

// New builds a Translator for the configuration.
func New(cfg *config.Config) *Translator {
	return &Translator{Cfg: cfg, Naming: NewNaming(cfg.Slurmd.NodeNamePrefix)}
}

// Shape sizes the job (see ShapeOf).
func (t *Translator) Shape(job *slurm.Job) (Shape, error) { return ShapeOf(job, t.Cfg, t.Naming) }

// JobSetRestarts is how often the JobSet replaces every pod after a pod
// failure before it gives up (then the job is ended with the reason).
const JobSetRestarts = 2

// JobSet builds the JobSet that runs the job: one ReplicatedJob of
// Shape.Pods indexed pods, suspended for Kueue, labeled with the Slurm
// job ID, anchored to the job's submit time.
func (t *Translator) JobSet(job *slurm.Job) (*jobsetv1alpha2.JobSet, error) {
	cfg := t.Cfg
	priorityClass, ok := cfg.PriorityClassFor(job.Partition)
	if !ok {
		return nil, fmt.Errorf("partition %q is not mapped to a priority class", job.Partition)
	}
	shape, err := t.Shape(job)
	if err != nil {
		return nil, err
	}
	spec, err := t.podSpec(job, shape)
	if err != nil {
		return nil, err
	}
	id := job.PinID()
	labels := map[string]string{
		ManagedByLabel:     ManagedByValue,
		SlurmJobIDLabel:    strconv.FormatUint(id, 10),
		queueNameLabel:     cfg.LocalQueueFor(job.Partition),
		priorityClassLabel: priorityClass,
	}
	if cfg.Instance != "" {
		labels[InstanceLabel] = cfg.Instance
	}
	podLabels := map[string]string{ManagedByLabel: ManagedByValue, SlurmJobIDLabel: labels[SlurmJobIDLabel]}
	if cfg.Slurmd.PodTemplate != nil {
		for k, v := range cfg.Slurmd.PodTemplate.Labels {
			podLabels[k] = v
		}
	}
	podAnnotations := map[string]string{}
	if cfg.Slurmd.PodTemplate != nil {
		for k, v := range cfg.Slurmd.PodTemplate.Annotations {
			podAnnotations[k] = v
		}
	}
	if t.stampedPorts(&spec) {
		podAnnotations[NodePortRangeAnnotation] = NodePortRange(*cfg.Slurmd.NodePorts.Base, cfg.Slurmd.NodePorts.Size)
	}
	for k, v := range t.topologyAnnotations(job, shape) {
		podAnnotations[k] = v
	}
	var annotations map[string]string
	if st := job.SubmitTime.Value(); st > 0 {
		annotations = map[string]string{SubmitTimeAnnotation: strconv.FormatUint(st, 10)}
	}
	pods := shape.Pods
	return &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{Name: t.Naming.JobSetName(id), Namespace: cfg.Namespace, Labels: labels, Annotations: annotations},
		Spec: jobsetv1alpha2.JobSetSpec{
			Suspend:       ptr(true),
			FailurePolicy: &jobsetv1alpha2.FailurePolicy{MaxRestarts: JobSetRestarts},
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{{
				Name:     WorkersJobName,
				Replicas: 1,
				Template: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{
					Parallelism:           &pods,
					Completions:           &pods,
					BackoffLimit:          ptr[int32](0),
					ActiveDeadlineSeconds: ActiveDeadlineSeconds(job),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: podLabels, Annotations: emptyToNil(podAnnotations)},
						Spec:       spec,
					},
				}},
			}},
		},
	}, nil
}

// topologyAnnotations maps locality hints onto Kueue's topology-aware
// scheduling: -w with one pod requires that host; --switches requires the
// configured level; otherwise the preferred level, if any.
func (t *Translator) topologyAnnotations(job *slurm.Job, shape Shape) map[string]string {
	include, _, err := job.HostSelection()
	switch {
	case err == nil && len(include) > 0 && shape.Pods == 1 && !t.Cfg.Slurmd.NodePorts.Enabled():
		return map[string]string{topologyRequired: hostnameLabel}
	case job.WantsSwitchLocality() && t.Cfg.Topology.SwitchesLevel != "":
		return map[string]string{topologyRequired: t.Cfg.Topology.SwitchesLevel}
	case t.Cfg.Topology.PackLevel != "":
		return map[string]string{topologyPreferred: t.Cfg.Topology.PackLevel}
	}
	return nil
}

func emptyToNil(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return m
}
