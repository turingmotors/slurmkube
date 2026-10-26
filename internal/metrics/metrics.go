// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// Package metrics registers SlurmKube's Prometheus metrics on
// controller-runtime's registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	PollsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_slurm_polls_total", Help: "Reads of slurmctld's job and node lists.",
	})
	PollErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_slurm_poll_errors_total", Help: "Failed reads of slurmctld's job or node list.",
	})
	PollDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "slurmkube_slurm_poll_duration_seconds", Help: "Duration of one read of slurmctld's lists.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	})
	JobsClaimed = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "slurmkube_jobs_claimed", Help: "Slurm jobs this instance manages (non-terminal).",
	})
	SlurmRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "slurmkube_slurm_requests_total", Help: "slurmrestd requests by method and status.",
	}, []string{"method", "code"})
	SlurmRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "slurmkube_slurm_request_duration_seconds", Help: "slurmrestd request duration by method.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 12),
	}, []string{"method"})
	JobSetsCreatedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_jobsets_created_total", Help: "JobSets created for Slurm jobs.",
	})
	JobSetsDeletedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_jobsets_deleted_total", Help: "JobSets deleted after their Slurm job ended.",
	})
	JobsReleasedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_jobs_released_total", Help: "Slurm jobs released onto their verified dynamic nodes.",
	})
	JobsReheldTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "slurmkube_jobs_reheld_total", Help: "Slurm jobs held again, by reason.",
	}, []string{"reason"})
	JobsCancelledTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "slurmkube_jobs_cancelled_total", Help: "Slurm jobs cancelled by SlurmKube, by reason.",
	}, []string{"reason"})
	// TranslationRefusalsTotal counts refusals by the option refused (a
	// small, fixed set of names; never a job or user ID).
	TranslationRefusalsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "slurmkube_translation_refusals_total", Help: "Jobs refused at translation, by the unsupported option.",
	}, []string{"option"})
	// JobsRefused is the number of pending jobs currently held with a
	// refusal comment (set on every poll), as opposed to jobs held while
	// they wait for admission.
	JobsRefused = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "slurmkube_jobs_refused", Help: "Pending Slurm jobs held with a SlurmKube refusal in their comment.",
	})
	NodeRecordActionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "slurmkube_node_record_actions_total", Help: "Ownership actions on dynamic node records, by action.",
	}, []string{"action"})
	NodeRecordsDownedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_node_records_downed_total", Help: "Node records marked DOWN because a job held them while no live pod served them.",
	})
	FencingWaitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_fencing_waits_total", Help: "Requeued jobs held because the fencer did not confirm their old host.",
	})
	DynamicNodeSlotWaitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slurmkube_dynamic_node_slot_waits_total", Help: "Held jobs that waited for a dynamic node slot.",
	})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		PollsTotal, PollErrorsTotal, PollDuration, JobsClaimed,
		SlurmRequestsTotal, SlurmRequestDuration,
		JobSetsCreatedTotal, JobSetsDeletedTotal, JobsReleasedTotal, JobsReheldTotal, JobsCancelledTotal,
		TranslationRefusalsTotal, JobsRefused, NodeRecordActionsTotal, NodeRecordsDownedTotal, FencingWaitsTotal,
		DynamicNodeSlotWaitsTotal,
	)
}
