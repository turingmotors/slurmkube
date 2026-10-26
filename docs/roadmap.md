# Roadmap

## Done (0.1)

- P0: claim → JobSet → Kueue admission → gated slurmd start → verified
  pin and release → cleanup; node-record ownership; node ports; device
  links; requeue hold; JobSet-gone recovery; orphan sweep; registration
  timeout; `maxDynamicNodes`; fencing policy; Helm chart; unit, race and
  envtest suites; golden translation fixtures (test/compat).
- Time-limit sync: a running job's Jobs follow `scontrol update
  TimeLimit` (docs/time-limit.md); heterogeneous jobs refused by the
  controller itself (docs/slurm-options.md), opt-in
  `recovery.cancelRefusedJobs`.
- Reproduced end to end, with the JobSubmit plugin writing the
  SlurmKube comment and `gres.conf` naming the dynamic nodes: CPU
  jobs; `--gres=gpu:1` and `--gres=gpu:8`; `-N2`, including the loss
  of one of its pods; `--array` worker pool,
  `scancel` and `scontrol requeue` of one task; `-w` / `-x` via
  `slurmkube.nodelist=` / `slurmkube.exclude=`; pod loss → requeue →
  verified release; fencing
  on a NotReady host; Kueue preemption → requeue → re-admission;
  TimeLimit sync across controller restarts; refused jobs (comment,
  metrics).

## Next (P1), in order

1. **Fault injection in an isolated environment**: Kubernetes API and
   slurmrestd outages, and the controller's own heterogeneous-job
   refusal without a JobSubmit plugin in front of it. These are covered
   by unit and envtest suites only; reproducing them end to end needs
   an environment where outages harm no one.
2. **Node-record reconciler on a stale snapshot**: pod events reconcile
   a record again before the nudged poll lands, so a release is logged
   (and the delete repeated) 2-3 times. Harmless (the delete is
   idempotent); skip like the job reconciler does after its own writes.
3. **Kueue eviction drain**: release records on eviction so Slurm
   requeues at once (opt-in).
4. **Compatibility fixtures**: record more job shapes as slurmrestd
   reports them (GPU 2/4, arrays, `-N2`) into `test/compat`.
5. **Chart**: a `jobSubmit.lua` example and a `gres.conf` snippet;
   ServiceMonitor option, so that `controller_runtime_reconcile_panics_total`
   and `_errors_total` can be alerted on.

## Later (P2)

- Slurm control plane on Kubernetes: deploy `slurmctld`, `slurmrestd`
  and `slurmdbd` alongside the controller, so a site without an existing
  Slurm installation can start from SlurmKube alone.
- Slurm option coverage beyond the current matrix (`--mem` semantics on
  GPU jobs stays Slurm's).
- Priority sync / user priority directive.
- Several Slurm clusters per controller: a `SlurmCluster` configuration
  CRD (configuration only; never job state).
- DRA for GPUs instead of device links, once the device plugin path is
  no longer needed.
- Scale: per-job reconciles already parallelize; measure 200+ pods, tune
  the poll (watch-driven Slurm events are not available over REST).
- Node-port allocator without the per-stamp node list (cache the list
  per poll).
