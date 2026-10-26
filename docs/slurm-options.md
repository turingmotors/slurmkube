# Slurm options

How SlurmKube's translation treats sbatch options. Anything it cannot
honor is refused at submission time with the reason in the job's
comment (`slurmkube: refused: <option>: <reason>`); nothing is silently
reinterpreted. The sizing follows slurmctld's own allocation rules, so a
job gets the same CPUs, memory and GPUs on a dynamic node that it would
on a static node of the same configuration (`test/compat`).

| Option | Treatment |
|---|---|
| `-N` | one pod per node |
| `-n`, `--ntasks-per-node` | tasks land `tasks-nodes+1` on one node unless `--ntasks-per-node` says otherwise; a pod is sized for the fullest node |
| `-c` | per task; rounded up to whole cores (`slurmd.threadsPerCore`) |
| `--gres=gpu:N`, `--gpus-per-node` | GPUs per pod |
| `--gpus` | spread over the nodes, rounded up |
| `--gpus-per-task` | × tasks per pod |
| `--cpus-per-gpu`, partition `DefCpuPerGPU` | CPUs per pod when larger than `-c` × tasks |
| `--mem-per-gpu`, partition `DefMemPerGPU` | memory per pod = per-GPU × GPUs; overrides `--mem` on a GPU job (cons_tres does the same) |
| `--mem` | memory per pod (CPU jobs) |
| `--mem-per-cpu` | × CPUs; 1024 MB per CPU when the job names no memory |
| `--time` | Job `activeDeadlineSeconds` = Slurm's `end_time` + max(10 %, 5 min) in the Job's own clock, so Slurm's TIMEOUT comes first; follows `scontrol update TimeLimit` (extension, shortening, `UNLIMITED`) while the job runs (docs/time-limit.md) |
| `--array` | one worker pool for the array: `array.tasksPerPod` tasks per pod (or as many as fit `array.podCapacity`), at most `%N` or the pending tasks concurrently, at most `array.maxPods` pods; the pool carries no Kubernetes deadline, each task runs under its Slurm limit; the pool is removed only once every task has ended, so a task requeued on its own (waiting out `requeue_delay`) runs again on it |
| `-w`, `-x` | required node affinity on `kubernetes.io/hostname`; the JobSubmit plugin writes them into the job's `extra` as `slurmkube.nodelist=` / `slurmkube.exclude=` because the constraint field carries the pin |
| `--exclusive` | pod anti-affinity against SlurmKube's other pods on the host |
| `--switches` | Kueue topology-aware scheduling at `topology.switchesLevel` |
| `--requeue`, `--dependency`, `--begin` | Slurm-side; no JobSet is created until the dependency is satisfied and the begin time has passed |
| `--nice`, `--qos`, `--priority`, `--output`, `--export`, `--mail-*` | Slurm-side, untouched |

Refused: typed GPUs (`--gres=gpu:h100:N`; dynamic nodes register untyped
GPUs), `--gpus-per-socket`, `--constraint` (the field carries the pin),
more GPUs per node than `slurmd.gresDeviceLinks.count`, `--array` with
`-N` > 1, a `slurmd.podTemplate` that leaves a container non-Guaranteed,
and heterogeneous jobs.

## Heterogeneous jobs

`sbatch ... : ...` submits one job record per component, allocated
together. slurmrestd reports each component with `het_job_id` = the
leader's job ID, `het_job_offset` = the component index and
`het_job_id_set` = the ID range (`"1000-1001"`). An ordinary job,
including `-N2` and array tasks, reports `het_job_id` and
`het_job_offset` as set with the value 0, so only `het_job_id != 0`
marks a component (`Job.IsHeterogeneous`).

SlurmKube refuses every component at translation: no JobSet, no Kueue
Workload, no node. The comment reads

    slurmkube: refused: heterogeneous jobs: not supported (component 1 of 1000-1001); submit the components as separate jobs, e.g. with --dependency

and the job stays held, like every other refusal. Nothing else is
cancelled or held by this path.

## Refused jobs

A refused job is a pending, held job whose comment starts with
`slurmkube: refused: `. That comment is the state: it is written once, the
refusal is logged once (`job refused at translation`, with the option)
and counted once, and every later reconcile, including a restarted
controller's first one, sees the comment and does nothing. A slurmrestd
outage changes nothing either (the poller keeps its last snapshot).

| Who | How |
|---|---|
| the user | `squeue -u $USER -o '%i %T %r %k'` shows the reason in the comment; `scontrol show job <id>` likewise |
| the user, to give up | `scancel <id>`; the gauge below drops on the next poll |
| the user, to fix the job | `scontrol update JobId=<id> <option>` (e.g. `Gres=gpu:1` without the type), then `scontrol release <id>`: the job is held again and translated again; if the options are now supported a JobSet is created. A site's JobSubmit plugin may forbid `scontrol update` / `release` of these jobs by users (the example plugin does), in which case the way out is `scancel` and resubmitting with supported options |
| the user, releasing without a fix | the job is held again (`slurmkube: held: released before SlurmKube pinned it ...`) and refused again with the same comment |
| the operator | gauge `slurmkube_jobs_refused` (refused jobs in the last poll), counter `slurmkube_translation_refusals_total{option=...}` (by option name, never by job or user) |
| the operator, site policy | `recovery.cancelRefusedJobs: true` cancels a refused job right after writing the comment (counted in `slurmkube_jobs_cancelled_total{reason="refused"}`); the reason then survives only in `sacct` with `AccountingStoreFlags=job_comment`, so the default keeps the job visible in the queue instead |

Only jobs the controller claims (partition mapped, comment written by the
plugin or by SlurmKube) reach this path; jobs of other managers and jobs
held by an operator for another reason are never translated.

## Role of the JobSubmit plugin

The controller is the authority on what it can run: every check above is
made from slurmrestd's job record, so a site without the JobSubmit plugin
gets the same refusals, after submission, in the job's comment. The
plugin (`hack/job_submit.lua`) is the early, friendlier copy of a subset
of those rules: it holds the job with the claim comment, rejects
heterogeneous jobs and `--constraint` at `sbatch` with the reason on the
user's terminal, and keeps users from releasing a held job by hand. A
site's plugin may add more (typed-GPU stripping, `-w`/`-x` into `extra`,
memory defaults). Keep both: the plugin is optional, the controller's
checks are not.
