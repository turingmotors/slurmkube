// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"slices"

	"github.com/turingmotors/slurmkube/internal/metrics"
	"github.com/turingmotors/slurmkube/internal/ownership"
	"github.com/turingmotors/slurmkube/internal/slurm"
	"github.com/turingmotors/slurmkube/internal/translate"
)

// releaseState is releaseRecord's outcome.
type releaseState int

const (
	recordDeleted releaseState = iota // deleted now
	recordAbsent                      // there was none
	recordDowned                      // a job held it; marked DOWN, delete later
	recordBusy                        // a job holds it and a live pod serves it; retry later
)

// releaseRecord deletes a dynamic node record. slurmctld refuses while a
// job is allocated to or completing on the node; then, if no live pod of
// the name serves the record (mayDown) it is marked DOWN — which ends the
// job (NODE_FAIL, or a requeue for --requeue) — and the delete follows on
// a later reconcile. A record that is not SlurmKube's by name and pin
// feature is never marked DOWN.
func releaseRecord(ctx context.Context, api SlurmAPI, naming translate.Naming, rec *slurm.Node, pods []ownership.PodView, reason string, log Logger) (releaseState, error) {
	name := rec.Name
	err := api.DeleteNode(ctx, name)
	if err == nil {
		return recordDeleted, nil
	}
	if slurm.IsNotFound(err) {
		return recordAbsent, nil
	}
	if !slurm.IsNodeBusy(err) {
		return recordBusy, err
	}
	id, _, ours := naming.ParseNodeName(name)
	if !ours || !slices.Contains(rec.Features, naming.PinFeature(id)) {
		return recordBusy, fmt.Errorf("delete refused and the record is not SlurmKube's (features %v); not marking DOWN", rec.Features)
	}
	if !ownership.MayDown(rec, pods) {
		if log != nil {
			log.Info("record could not be deleted yet and a live pod serves it; not marking DOWN", "node", name)
		}
		return recordBusy, nil
	}
	if derr := api.SetNodeState(ctx, name, "DOWN", reason); derr != nil {
		return recordBusy, fmt.Errorf("delete refused (%v); marking DOWN: %w", err, derr)
	}
	metrics.NodeRecordsDownedTotal.Inc()
	if log != nil {
		log.Info("record could not be deleted yet (a job is on it) and no live pod serves it; marked DOWN so the job ends", "node", name, "reason", reason)
	}
	return recordDowned, nil
}
