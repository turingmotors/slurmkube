# Node record ownership

## The hazard

slurmctld 26.05 (read in its source and reproduced):

- a registration under an **existing** dynamic node name updates the
  record's address but not its port (`node_mgr.c set_node_comm_name`);
- `slurmd` runs a **batch launch without checking** that its own name is
  in the job's hostlist (`req.c _rpc_batch_job`; only `srun` step
  launches are checked).

So a record `slurmkube-w0-X → host:port` that outlives its pod sends the
job's next launch to whatever `slurmd` holds that port on the host —
another job's pod, whose `slurmd` runs the batch script unchecked.
Every rule below exists to make that impossible.

## Identity

`slurmd` registers with `--extra $(SLURMKUBE_POD_UID)` (downward API) and
`-b` (reports a boot), so slurmctld takes the value on every registration
and the record's `extra` is the UID of the pod that last registered under
the name: its **owner**. Name alone is reused across replacements and
requeues; address and port cannot tell two pods on one host apart; the
UID is unique and lives in slurmctld's own (persisted) state. Records
without `extra` (older pods) fall back to the endpoint.

## Rules (`internal/ownership`, pure functions)

| Record | Owner | Decision |
|---|---|---|
| absent | — | gone pods of the name are marked released (port reusable); the single live gated pod is cleared to start `slurmd` — unless the job is terminal/unknown, or two live pods claim the name |
| present | gone (finished or Terminating) | release: delete; DOWN first if slurmctld still has the job on it. The owner's port stays reserved until the record is gone. The fencer is consulted (docs/fencing.md). |
| present | unknown to the cache | wait `recovery.vanishedOwnerGrace` (time, not ticks), then release |
| present | live, record points elsewhere | release the record and delete the pod (pod network or legacy only; impossible with the gate) |
| present | live at the endpoint | healthy |
| absent, pod cleared for `slurmd.registrationTimeout` | — | delete the pod so the JobSet replaces it (a refused `slurmd` never registers again) |

Invariants these give:

1. a record always corresponds to the pod that registered it;
2. ownership is never decided by name alone;
3. an old pod's event cannot release the new pod's record (different UID);
4. a port in use by a record is not reused: finished pods keep their port
   until `node-record-released`; the allocator skips ports any record
   names on the host; a replacement cannot register until the old record
   is gone;
5. a job is released only onto records owned by live pods at the recorded
   endpoint (`ownership.Served`), and a requeued job is held until so;
6. a record with an unknown owner is released only after the grace, and
   DOWN is never applied while a live pod serves the record or when the
   record is not SlurmKube's by name and pin feature;
7. a record under one of SlurmKube's node names that does not carry its
   pin feature (`slurmkube-pin-<id>`) belongs to another manager sharing
   the name prefix: it is never released, DOWN'd or swept, and a pod of
   SlurmKube's under that name stays gated until it is gone.

## The gate

With per-pod ports (hostNetwork + `slurmd.nodePorts`) the pod's init
container waits for two annotations the controller stamps:
`slurmkube.io/node-port` (the allocator) and
`slurmkube.io/node-record-clear` (the node-record reconciler, only while
no record exists under the pod's node name). Both are read through a
downward-API volume. A stopped controller shows as an init container that
waits, then fails after `registrationTimeout`-sized bounds — never as a
`slurmd` that started on a guess.

## Capacity

slurmctld refuses registrations past `MaxNodeCount` ("node table is
full") and the refused `slurmd` never retries as a dynamic node. With
`slurmd.maxDynamicNodes` (MaxNodeCount minus static nodes) the job
reconciler creates JobSets only while every owned JobSet's pods plus the
new job's fit; the rest wait with the count in their comment.
