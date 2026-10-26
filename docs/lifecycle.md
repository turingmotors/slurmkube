# Job lifecycle and sources of truth

## Who decides what

| Question | Source of truth | How the others follow |
|---|---|---|
| Does the job exist, is it pending/running/done, exit code | **Slurm** | the controller creates the JobSet from the job and deletes it when the job is terminal or gone |
| May the job's pods use the cluster's resources | **Kueue** | the JobSet stays suspended until the Workload is Admitted; the job is not pinned or released before |
| Are the pods alive | **Kubernetes** (JobSet/Job/Pod) | a JobSet that fails or completes while the job is non-terminal ends the job (cancelled with the reason) |
| Which pod a dynamic node record means | **slurmctld's record** (`extra` = pod UID) | the node-record reconciler keeps the record pointing at a live pod it owns, or removes it |
| May the job run again after its pod vanished | **the fencer** | a requeued job stays held until the fencer clears the old host |
| How long the job may run | **Slurm** (`time_limit`, `end_time`) | the Jobs' `activeDeadlineSeconds` is a later backstop recomputed from Slurm's end on every reconcile (docs/time-limit.md) |

Slurm and Kubernetes never both decide to re-run a job: Slurm requeues
(or not, `--no-requeue`), and the controller only lets a requeued job go
once its records are honest and its old host is known to be fenced.

## States

| Slurm | Kubernetes | Controller |
|---|---|---|
| PENDING, held with the plugin's comment | nothing | claims: creates the JobSet (`slurmkube: claimed`); refuses with a comment if an option cannot be honored |
| PENDING, held, `slurmkube: refused: ...` | nothing | nothing (also after a restart); the user cancels, or fixes the options and releases (docs/slurm-options.md, "Refused jobs") |
| PENDING, held | JobSet suspended (Kueue) | waits: `slurmkube: waiting for Kueue admission` |
| PENDING, held | Workload Admitted, pods starting | pods are cleared one name at a time (no record under the name); slurmd registers with the pod's UID |
| PENDING, held | every record owned by a live pod at its endpoint | pins (`slurmkube-pin-<id>`), verifies, releases |
| RUNNING | pods Running | nothing; node-record reconciler watches records |
| PENDING, pinned, not held, `restart_cnt > 0` (requeued) | pods present or being replaced | **held again** unless every record is served and no host awaits fencing; released once they are |
| PENDING, not held, not pinned | — | held again (released by someone before the pin) |
| COMPLETED / FAILED / TIMEOUT / CANCELLED / NODE_FAIL | JobSet exists | records released (DOWN only if no live pod serves them), JobSet deleted |
| non-terminal | JobSet Failed / Completed | job cancelled with the JobSet's reason |
| gone from slurmctld (MinJobAge) | JobSet exists | live read confirms, then cleanup |

## Requeue and JobSet restart

A pod's loss reaches Slurm through the node-record reconciler: the record
of a gone pod is deleted, or marked DOWN when a job is still on it. Slurm
then ends the job on that node — NODE_FAIL, which requeues a `--requeue`
job. The JobSet (`failurePolicy.maxRestarts`) replaces the pods. The two
meet in the job reconciler: the requeued job is held until the replacement
pods registered fresh records and the fencer cleared the old host. A
`--no-requeue` job ends; its replacement pods are never cleared (terminal
job) and the JobSet is deleted.

`scontrol requeue` of a running job whose pods survive needs nothing from
the controller: records are honest, the job is left to slurmctld.

Slurm resets a job's `submit_time` on requeue; the JobSet's identity anchor
accepts a later submit time when `restart_cnt > 0` and moves the anchor.

## Cancel

`scancel` makes the job terminal; the next reconcile releases the records
and deletes the JobSet. Pods end with the JobSet. Nothing recreates a
JobSet for a terminal job.

## Controller down

Nothing runs without the controller except Slurm and Kubernetes
themselves. Replacement pods wait at their init gate (slurmd does not
start). On return, the first poll rebuilds the picture: records of gone or
vanished pods are released (the vanished grace absorbs informer lag),
requeued jobs are held and later released, orphaned records swept, jobs
that ended cleaned up. No in-memory state is needed for safety.
