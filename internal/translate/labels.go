// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package translate turns a Slurm job into the Kubernetes objects that run
// it: first a Shape (how many pods, how big — a pure function of the job
// and the configuration), then a PodSpec, then the JobSet. Options the
// pods cannot honor are refused with an Unsupported error naming the
// option and the reason; nothing is silently reinterpreted.
package translate

// Labels and annotations SlurmKube puts on its objects.
const (
	ManagedByLabel  = "slurmkube.io/managed-by"
	ManagedByValue  = "slurmkube"
	SlurmJobIDLabel = "slurmkube.io/slurm-job-id"
	// InstanceLabel names the SlurmKube instance that owns the object when
	// several share a namespace.
	InstanceLabel = "slurmkube.io/instance"

	// SubmitTimeAnnotation anchors a JobSet to one incarnation of a Slurm
	// job ID (IDs wrap after MaxJobId).
	SubmitTimeAnnotation = "slurmkube.io/slurm-submit-time"
	// NodePortRangeAnnotation on a pod means its slurmd port is assigned
	// per pod by the node-port allocator inside this range ("lo-hi"), and
	// that the pod is record-gated.
	NodePortRangeAnnotation = "slurmkube.io/node-port-range"
	// NodePortAnnotation is the assigned port.
	NodePortAnnotation = "slurmkube.io/node-port"
	// NodeRecordClearAnnotation is set by the controller when no slurmctld
	// record exists under the pod's node name; the init gate waits for it.
	NodeRecordClearAnnotation = "slurmkube.io/node-record-clear"
	// NodeRecordReleasedAnnotation is set on a finished pod once no record
	// points at it; its port may then be reused.
	NodeRecordReleasedAnnotation = "slurmkube.io/node-record-released"

	// Kueue's labels.
	queueNameLabel     = "kueue.x-k8s.io/queue-name"
	priorityClassLabel = "kueue.x-k8s.io/priority-class"
	topologyRequired   = "kueue.x-k8s.io/podset-required-topology"
	topologyPreferred  = "kueue.x-k8s.io/podset-preferred-topology"
	hostnameLabel      = "kubernetes.io/hostname"
)

// Environment of the slurmd container.
const (
	NodeNameEnv  = "SLURMKUBE_NODE_NAME"
	NodeIndexEnv = "SLURMKUBE_NODE_INDEX"
	NodePortEnv  = "SLURMKUBE_NODE_PORT"
	PodUIDEnv    = "SLURMKUBE_POD_UID"
)

// DefaultSlurmdPort is slurmd's port when node ports are off.
const DefaultSlurmdPort int32 = 6818

// WorkersJobName is the JobSet's single ReplicatedJob.
const WorkersJobName = "workers"
