# Fencing: when may a job run again

A Pod object disappearing is not proof that its processes stopped. When a
host's kubelet is partitioned, Kubernetes marks the pod for deletion
(and may eventually remove the object) while `slurmstepd` keeps running
on the host. Re-running the job elsewhere would then write to the same
shared storage from two places. SlurmKube therefore separates two
decisions that other systems conflate:

1. **Ending the job in Slurm** is always safe: the record of a gone pod is
   DOWN'd/deleted, the job ends (NODE_FAIL) and, with `--requeue`, is
   requeued. Nothing new runs yet.
2. **Running the job again** requires that the old host cannot still run
   it. That is the fencer's call.

## Conditions

| Situation | Re-run |
|---|---|
| the kubelet reported the pod's containers terminated (Succeeded/Failed, or every container `terminated`) | allowed: the processes are gone |
| the pod is gone and its host is **Ready** | allowed: a Ready kubelet has killed the pod's containers |
| the pod was never scheduled | allowed: nothing ran |
| the host no longer exists in the cluster | allowed |
| the pod is gone (Terminating, or removed) and its host is **NotReady / Unknown** | **held**: `slurmkube: held: fencing required before the job may run again: host … is not Ready …` |
| `fencing.mode: trustKubernetes` | always allowed (sites that power off NotReady hosts by other means, or accept double execution) |

The decision is taken when the node-record reconciler releases the gone
pod's record. A host that is not cleared is written to the JobSet
annotation `slurmkube.io/fence-pending`, which survives the pod object's
removal. The job reconciler consults the fencer again for every pending
host before any release of that job (admission after a requeue, or a
requeued job slurmctld would otherwise re-allocate) and clears hosts as
they become safe.

## Confirming by hand

After fencing the host by other means (power, network, `scontrol update
NodeName=… State=DOWN` on the physical node, …), list it on the JobSet:

```sh
kubectl -n slurm-jobs annotate jobset slurmkube-<jobID> slurmkube.io/fenced-hosts=<host>
```

The next reconcile releases the job. The annotation name is
`fencing.confirmAnnotation`.

## Delegating to an external fencer

`fence.Fencer` is one method:

```go
type Fencer interface {
    Confirm(ctx context.Context, in Incident) (Verdict, error)
}
```

`Incident` carries the Slurm job ID, node name, pod name and UID, the
host, whether the kubelet reported termination, and the owner object. A
site can implement it against a BMC, a cloud API or a ticketing system,
and return `Safe: false` with a reason until the host is fenced. The
`Manual` policy ships as the default; `TrustKubernetes` as the opt-out.

## What is not covered

- Slurm's own requeue of a job on a physical node has the same window;
  SlurmKube only handles its pods.
- A pod whose host is Ready but whose containers hang in termination
  (stuck unmount, D-state) is reported by the kubelet in time; the
  verdict waits for the report (the pod stays Terminating, hence Gone
  but not terminated, and the host is Ready → allowed). If a site needs
  stricter semantics, use a custom Fencer that waits for the pod object
  to be removed.
