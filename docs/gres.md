# GPU resources

How a dynamic node's GPUs reach Slurm, and the `gres.conf` lines this
needs. The examples use the values of `config/example.yaml`: node name
prefix `slurmkube-w`, `slurmd.gresDeviceLinks` `{dir: /run/slurmkube/gres,
count: 8}`, `MaxNodeCount=64`.

## Device links

Kubernetes allocates a pod's GPUs (through the device plugin), not
Slurm. An init container links the GPU device nodes allocated to the pod
as `gpu0` … `gpu<n-1>` in `slurmd.gresDeviceLinks.dir` and the remaining
names up to `count` to `/dev/null`, so every pod has the same `count`
files whatever its GPU count. slurmd starts with `Gres=gpu:<n>` in its
`--conf` line and registers the first `n` files (it logs "Ignoring extra
GRES" once for the rest). Jobs with `--gres=gpu:1` and `--gres=gpu:8`
were reproduced end to end with `count: 8`.

GPU autodetection stays off for the dynamic nodes:

- slurmctld rejects GPU autodetection when it loads `gres.conf` for a
  dynamic or cloud node ("Cannot use AutoDetect on cloud/dynamic node",
  `src/interfaces/gres.c`).
- NVML reports a GPU's CPU affinity across the host's sockets, which a
  pod-sized node does not have; slurmctld then drains the node for a
  topology mismatch. The device files carry no affinity.

Leaving `gresDeviceLinks` unset and letting slurmd find GPUs another way
has not been verified.

## The lines

A dynamic node is named `<prefix><index>-<jobID>`, so `gres.conf` names
them with a hostlist of two brackets. A line without `NodeName=` would
apply to every node that reads the file, static ones included, so the
lines name the dynamic nodes only.

```sh
max=$(scontrol show config | awk '$1 == "MaxJobId" {print $3}')
for lo in $(seq 0 65536 "$max"); do
  hi=$(( lo + 65535 < max ? lo + 65535 : max ))
  echo "NodeName=slurmkube-w[0-63]-[$lo-$hi] AutoDetect=off Name=gpu File=/run/slurmkube/gres/gpu[0-7]"
done
```

- One bracket holds at most 65536 names (`MAX_RANGE`); a larger one is
  rejected ("Too many hosts in range"). The job ID bracket is therefore
  split into one line per 65536 IDs up to `MaxJobId`: 1024 lines for
  the default `MaxJobId` (67,043,328) or the largest local job ID
  (67,108,863).
- Job IDs restart at `FirstJobId` after `MaxJobId`; the lines start at 0
  and cover every ID up to `MaxJobId`, before and after the wrap. An
  array's nodes carry the array's root ID, which is a job ID too.
- The index bracket must cover the largest index a job's nodes get: the
  job's node count minus one, and `array.maxPods - 1` for an array's
  workers. `MaxNodeCount - 1` covers both.
- `File=` must name `count` files in `dir`, as configured in
  `slurmd.gresDeviceLinks`. Fewer files than a pod's GPUs register the
  node with fewer GPUs than its `Gres=`, and slurmctld rejects the
  registration ("gres/gpu count reported lower than configured").

Regenerate the lines when `MaxJobId`, `MaxNodeCount`, the node name
prefix or the device links change.

## Cost

slurmd reads `gres.conf` when it starts, once per pod. For each line
with `NodeName=` it builds a hostlist, looks its own name up and frees
it (`_parse_gres_config_node`). The hostlist expands the index bracket
into one prefix per index (`slurmkube-w0-` … `slurmkube-w63-`) and keeps
the job ID bracket as a single range per prefix: 64 ranges per line, and
only one line's hostlist exists at a time. The roughly 4.3 billion names
the lines describe are never materialized.

Measured with libslurm 26.05.5 (x86_64, one core), building, searching
and freeing all 1024 lines for one name (`hack/gres-bench.c`):

| Lines × indexes | Time | Peak RSS of the process |
|---|---|---|
| 1 × 1 (baseline) | — | 2.8–2.9 MiB |
| 1024 × 64 | 11–12 ms | 2.7–2.8 MiB |
| 1024 × 1024 | 175 ms | 2.9–3.0 MiB |

Time grows with lines × indexes; memory stays at the baseline.

Names at the bracket edges (job IDs 65535 and 65536, `MaxJobId`,
index 63) matched the right line; index 64, `MaxJobId + 1` and another
prefix matched none.
