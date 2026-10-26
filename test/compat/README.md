# Translation fixtures

`*.json` are Slurm job records in slurmrestd's v0.0.44 shape, holding the
fields the translation reads: `tasks`, `cpus_per_task`, `node_count`,
`tasks_per_node`, `memory_per_*`, `tres_per_*`, `cpus_per_tres`,
`array_*`. `expected.json` is the shape each must translate to under the
reference configuration in `internal/translate/compat_test.go`: pod
count, CPUs, memory (Mi, including `memoryOverheadMB`), GPUs and the
slurmd `--conf` line.

The expected numbers are what slurmctld allocates for the same job on a
static node of that configuration (whole cores with `ThreadsPerCore=2`,
`DefCpuPerGPU` and `DefMemPerGPU` through `cpus_per_tres` and
`memory_per_tres`), so a change in `expected.json` is a change in what
users get and needs a reason in the commit message.

## Derivation

The reference configuration: `ThreadsPerCore=2`, `memoryOverheadMB` 256,
an array worker of 26 CPUs and 256953 MB. Memory is in MB in the job
record and Mi in `expected.json`; the `--conf` line is
`Features=slurmkube-pin-<job ID>`, then `Gres=gpu:<GPUs>` with GPUs,
then the port.

| Fixture | Request | Pods | CPUs per pod | Memory per pod (Mi) | GPUs |
|---|---|---|---|---|---|
| `cpu-c2` | `-c 2` | 1 | 2 | 2 × 1024 + 256 = 2304 | 0 |
| `cpu-n4` | `-n 4` | 1 | 4 tasks × 1 = 4 | 4 × 1024 + 256 = 4352 | 0 |
| `cpu-N2-tpn1-c100` | `-N 2 --ntasks-per-node=1 -c 100` | 2 | 100 | 100 × 1024 + 256 = 102656 | 0 |
| `gpu1-default` | `--gres=gpu:1`, `DefCpuPerGPU=26` | 1 | 26 | 250880 per GPU + 256 = 251136 | 1 |
| `gpu1-mem32G` | `--gres=gpu:1 -c 4 --mem=32G` | 1 | 4 | per-GPU memory wins over `--mem`: 251136 | 1 |
| `cpus-per-gpu8-gpu1` | `--gres=gpu:1 --cpus-per-gpu=8` | 1 | max(1, 8 × 1) = 8 | 251136 | 1 |
| `gpu2-gpus` | `--gpus=2` | 1 | max(52, 26 × 2) = 52 | 2 × 250880 + 256 = 502016 | 2 |
| `gpus-per-task1-n2` | `-n 2 --gpus-per-task=1` | 1 | 2 tasks × 26 = 52 | 502016 | 2 |
| `gpu8` | `--gres=gpu:8` | 1 | max(208, 26 × 8) = 208 | 8 × 250880 + 256 = 2007296 | 8 |
| `array-0-3-c2` | `--array=0-3%2 -c 2` | 1 worker holding min(13 that fit, 2 at once) = 2 tasks | 2 × 2 = 4 | 2 × 2048 + 256 = 4352 | 0 |

To add a fixture: `scontrol show job --json <id>` on the Slurm
controller, keep the `jobs[0]` object, strip site data (`user_name`,
`command`, `work_dir`, `standard_*`, account, cluster), save it under a
descriptive name (`gpu2-gpus.json`: `--gpus=2`), and add its expected
shape, derived by hand from the Slurm rules above.
