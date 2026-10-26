# Time limit

Slurm is the source of truth for a job's time limit. slurmctld ends the
job (TIMEOUT) when its `end_time` arrives; the Kubernetes deadline is a
backstop that fires later, so pods cannot outlive a job slurmctld lost
track of, and never first.

## Two clocks

| | Slurm | Kubernetes |
|---|---|---|
| Limit | `time_limit` (minutes; `UNLIMITED` = infinite) | Job `spec.activeDeadlineSeconds` |
| Starts | when the job is allocated to its (registered) nodes: `start_time` | when the Job is unsuspended by Kueue: `Job.status.startTime` |
| Ends | `end_time` = `start_time` + limit; moves with `scontrol update TimeLimit` | `startTime` + `activeDeadlineSeconds`, then the Job fails (`DeadlineExceeded`) |

The Kubernetes clock starts earlier: the pods are created, start, and
their slurmd registers before slurmctld allocates the job. Writing
`TimeLimit × 60` into the Job would make Kubernetes end the job before
Slurm. SlurmKube therefore sets the deadline from Slurm's absolute end
time, expressed in the Job's own clock.

## Buffer

The deadline is Slurm's end plus a buffer: the larger of 10 % of the
limit and 5 minutes (`translate.DeadlineBuffer`). Slurm's TIMEOUT comes
first; the buffer absorbs the time slurmctld needs to end the job and
the controller needs to delete the JobSet.

## Creation

A new JobSet's worker template carries `activeDeadlineSeconds` = limit +
buffer (`translate.ActiveDeadlineSeconds`). At this point the job is
held and `end_time` is 0, so the limit itself is the best estimate; the
running sync below corrects it once the job runs. No limit (`UNLIMITED`)
means no deadline.

## While the job runs

On every reconcile of a non-pending job that has a JobSet, the controller
lists the JobSet's child Jobs and, for each Job that has started
(`status.startTime` set), computes

    want = end_time + buffer − Job.status.startTime

(`translate.RunningDeadlineSeconds`, rounded to seconds, never below 1)
and patches `spec.activeDeadlineSeconds` when it differs
(`syncDeadline`). The reconcile runs on each poll (`recovery.pollInterval`)
and on JobSet events, so `scontrol update TimeLimit` is followed within
one poll.

| Change in Slurm | Effect on the Job |
|---|---|
| `TimeLimit` extended | `end_time` moves later; the deadline grows by the same amount plus the buffer difference. A job past its old deadline keeps running |
| `TimeLimit` shortened | the deadline shrinks; if Slurm's new end is already past, Slurm ends the job (TIMEOUT) and the Job is deleted with the JobSet |
| `TimeLimit=UNLIMITED` | `activeDeadlineSeconds` is removed (merge patch to null) |
| no change | the computed value equals the stored one; nothing is written (`resourceVersion` unchanged) |

The patch is a merge patch of the one field on the child Job, not the
JobSet: JobSet's webhook makes `replicatedJobs` immutable, and Kubernetes
allows `activeDeadlineSeconds` to change on a running Job without
touching its pods. A Job that has not started keeps the template's value
until it does. Each sync writes an event `DeadlineUpdated` on the JobSet
with the old and new values and Slurm's end time.

## Failures

- **slurmrestd unreachable**: the poller keeps the last snapshot; no
  deadline is derived from unknown values, nothing is written.
- **Kubernetes API error on the patch**: the reconcile returns the error
  and controller-runtime retries with backoff; the Job keeps its previous
  deadline meanwhile.
- **Controller restart**: the first poll recomputes the same value from
  `end_time` and `status.startTime`; a matching Job is left alone, a
  stale one is corrected.
- **Extension right before expiry**: the deadline is only ever written
  from Slurm's end; between the `scontrol update` and the next poll the
  old deadline (Slurm's old end plus at least 5 minutes) still holds, so
  an extension made before Slurm's old `end_time` is picked up before the
  Kubernetes deadline can fire.
- **Several changes in quick succession**: each poll writes the value of
  the latest `end_time` once; intermediate values that arrive between
  polls are never written. The result is the last change.
- **Controller restarted between a change and its sync**: the new
  instance's first poll computes the same value from `end_time` and
  `status.startTime` and writes it once; a further restart writes nothing.

Invariants the tests hold (`TestDeadlineFollowsSlurmsTimeLimit`,
`TestDeadlineSyncUnderSuspendRapidUpdatesAndRestart`): the Job's deadline
is never earlier than Slurm's end plus the buffer; no value is derived
from a failed read; the same inputs produce the same value with no write;
a Job without `status.startTime` (suspended, not yet started) is left as it
is.

## Suspend, resume, requeue, arrays

- **Kueue suspend/resume** (preemption): Kueue suspends the JobSet and
  the Jobs; Kubernetes resets `Job.status.startTime` when a Job is
  resumed, and the active-deadline clock restarts from the new start. The
  Slurm job meanwhile lost its nodes (NODE_FAIL → requeue); the requeued
  job gets a new `start_time`/`end_time` when it runs again, and the sync
  derives the deadline from those.
- **Slurm requeue / JobSet restart**: a Job the JobSet recreates starts
  with the template's value (limit + buffer) and is corrected on the next
  reconcile once it has a `startTime` and Slurm reports the run's
  `end_time`. The requeued job's limit is the one at requeue time.
- **Array jobs**: the worker pool runs tasks one after another, each
  under its own Slurm limit; the pool's Jobs carry no deadline (creation
  and sync both skip arrays). Slurm's TIMEOUT per task applies as usual.
- **Multi-node (`-N2`)**: every child Job of the JobSet is synced to the
  same Slurm end; their `startTime`s may differ by seconds, so their
  `activeDeadlineSeconds` may differ by the same.

## Known limits

- The sync waits for the next poll; a shortening that brings Slurm's end
  into the past is enforced by Slurm, not by the deadline.
- `end_time` is read from slurmrestd; if a site's slurmctld reports a
  running job without `end_time` (not observed with 26.05), the Job keeps
  its creation-time value.
- The Kubernetes deadline is a backstop and is not expected to fire on a
  healthy cluster; when it does (`DeadlineExceeded`), the JobSet fails and
  the job is cancelled with that reason (docs/lifecycle.md).
