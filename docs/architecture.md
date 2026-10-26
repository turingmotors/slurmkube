# Architecture

SlurmKube runs Slurm jobs on Kubernetes. Slurm stays the scheduler and the
user's interface; Kubernetes (Kueue, JobSet) supplies the compute. Each
Slurm job becomes one JobSet of slurmd pods that register with slurmctld
as dynamic nodes; the job is released onto exactly those nodes once the
controller has verified that every one of them is the pod it created.

## Responsibilities

| Component | Package | Owns | Reads | Writes |
|---|---|---|---|---|
| Slurm client | `internal/slurm` | the REST protocol, JWT minting, rate limiting, job/node models and option accessors | slurmrestd | slurmrestd |
| Job translator | `internal/translate` | Slurm job → JobSet spec: pod shape, CPU/GPU/memory sizing, node names, pin feature, init gates, device links, podTemplate merge; refusals for unsupported options | config, job | — (pure) |
| Ownership rules | `internal/ownership` | which pod owns a dynamic node record and what to do about it | record, pod views | — (pure) |
| Fencing policy | `internal/fence` | whether a job whose pod is gone may run again | Node conditions, owner annotations | — |
| Poller | `internal/controller/snapshot.go` | one consistent read of slurmctld per interval; fan-out of reconcile keys | slurmrestd | channels |
| Job reconciler | `internal/controller/job_reconciler.go` | one Slurm job's lifecycle: claim, JobSet, admission, pin, verification, release, requeue hold, cleanup | snapshot, cache | JobSet, Slurm job |
| Node-record reconciler | `internal/controller/node_reconciler.go` | one dynamic node name: clearance, release, DOWN, stale/vanished records, registration timeout, orphan sweep | snapshot, cache | node record, pod annotations/deletion |
| Node-port allocator | `internal/controller/nodeport.go` | one port per slurmd pod per host | pods (uncached), node records | pod annotation |
| Config | `internal/config` | the YAML schema, defaults, validation | file | — |

Dependencies point one way: `controller → {translate, ownership, fence,
kueue, slurm, config, metrics}`; `translate → {slurm, config}`;
`ownership → slurm`; nothing imports `controller`. The two pure packages
(`translate`, `ownership`) carry the invariants and most of the tests.

## The loop

```
             every pollInterval (and on nudge)
slurmctld ──ListJobs/ListNodes──> Poller ──Snapshot──┐
                                     │                 │ read
                   enqueue job IDs   │  enqueue node   │
                                     v  names          v
                              JobReconciler    NodeRecordReconciler
                                 ^   ^              ^
            JobSet/Workload/Pod  │   │ Pod events   │
            watches (cache) ─────┘   └──────────────┘
```

A reconcile is a function of (snapshot, cache) → a few idempotent writes.
Nothing is remembered between reconciles except two graces (vanished
owner, orphan). Repeating a reconcile on unchanged state is a no-op, so a
restart, a leader change or a duplicate event cannot do harm.

## Why not a CRD

A Slurm job already has an authoritative record (slurmctld) and an
execution environment (the JobSet). A `SlurmJob` CRD would be a third
copy whose status had to be reconciled with both. The JobSet, its Job and
pods carry everything Kubernetes needs: the Slurm job ID as a label, the
submit-time anchor, the port and clearance stamps, the fencing state.
Kubernetes Events on the JobSet are the audit trail; the Slurm job's
comment is the user's. A CRD may come later for multi-cluster
configuration; it would own configuration, never job state.

## Naming and labels

| Thing | Value |
|---|---|
| JobSet | `slurmkube-<jobID>` (an array: its root ID) |
| dynamic node | `<prefix><index>-<jobID>`; the prefix is `slurmd.nodeNamePrefix` (default `slurmkube-w`) |
| pin feature | `slurmkube-pin-<jobID>` |
| labels | `slurmkube.io/managed-by=slurmkube`, `slurmkube.io/slurm-job-id`, `slurmkube.io/instance` |
| pod annotations | `slurmkube.io/node-port`, `…/node-port-range`, `…/node-record-clear`, `…/node-record-released`, `…/fence-verdict` |
| JobSet annotations | `slurmkube.io/slurm-submit-time`, `…/fence-pending`, `…/fenced-hosts` (operator) |
| slurmd env | `SLURMKUBE_NODE_NAME`, `SLURMKUBE_NODE_INDEX`, `SLURMKUBE_NODE_PORT`, `SLURMKUBE_POD_UID` |

With GPU device links, the site's `gres.conf` must name the dynamic
nodes (see the README's Slurm controller requirements for the lines).
A site whose `gres.conf` already lists another prefix can set
`slurmd.nodeNamePrefix` instead of changing the file; SlurmKube then
recognizes only records of that prefix as its own.

Translation is a pipeline in `internal/translate`: `ShapeOf` sizes the
job (pods, CPUs, memory, GPUs; a pure function of the job record and the
configuration, refusing options the pods cannot honor with an
`Unsupported` error that names the option), `podSpec` builds the slurmd
pod (container, site template merge, port and record gate, identity,
host selection, exclusive, Guaranteed check), and `JobSet` wraps it.
`Naming` is the one place names are formed and parsed.

## Coexistence with another manager

Two controllers must never manage the same job or node. SlurmKube claims a
job only when its comment is the submit plugin's hold comment or one of
its own; a job whose comment belongs to another manager is invisible to
it, and `claim.otherManagers` makes a job that already has another
manager's JobSet invisible even if its comment was reset. As long as node
names, pin features, labels and annotations are disjoint, neither
controller's cleanup can touch the other's records or pods. The claim
is not atomic across two controllers reading the same pristine job in
the same second: run one manager per partition, and hand partitions
over one at a time by changing the comment the submit plugin writes.
