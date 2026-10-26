// Copyright 2026 The SlurmKube Authors
// SPDX-License-Identifier: Apache-2.0

// gres.conf cost for SlurmKube dynamic nodes (docs/gres.md).
//
//   gcc -O2 -o gres-bench hack/gres-bench.c -lslurm
//   ./gres-bench [MaxJobId] [indexes]
//
// Measures what slurmd does with SlurmKube's gres.conf NodeName lines:
// for every line, hostlist_create + hostlist_find + hostlist_destroy
// (src/interfaces/gres.c _parse_gres_config_node).
#include <slurm/slurm.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

static unsigned long max_job_id = 67043328UL; /* DEFAULT_MAX_JOB_ID */
static int indexes = 64;

/* the line (as in the README generator) that matches name, or -1 */
static long match(const char *name, long *lines)
{
	char buf[256];
	long found = -1, n = 0;
	for (unsigned long lo = 0; lo <= max_job_id; lo += 65536, n++) {
		unsigned long hi = lo + 65535 < max_job_id ? lo + 65535 : max_job_id;
		snprintf(buf, sizeof(buf), "slurmkube-w[0-%d]-[%lu-%lu]", indexes - 1, lo, hi);
		hostlist_t *hl = slurm_hostlist_create(buf);
		if (!hl) {
			fprintf(stderr, "hostlist_create failed: %s\n", buf);
			exit(1);
		}
		if (found < 0 && slurm_hostlist_find(hl, name) >= 0)
			found = n;
		slurm_hostlist_destroy(hl);
	}
	*lines = n;
	return found;
}

int main(int argc, char **argv)
{
	if (argc > 1)
		max_job_id = strtoul(argv[1], NULL, 10);
	if (argc > 2)
		indexes = atoi(argv[2]);
	const char *names[] = {
		"slurmkube-w0-1", "slurmkube-w0-65535", "slurmkube-w0-65536",
		"slurmkube-w63-12345", "slurmkube-w7-67043328", "slurmkube-w0-0",
		"slurmkube-w64-1", "slurmkube-w0-67043329", "slurmkube-x0-1",
	};
	struct timespec t0, t1;
	long lines = 0;
	for (size_t i = 0; i < sizeof(names) / sizeof(*names); i++) {
		clock_gettime(CLOCK_MONOTONIC, &t0);
		long m = match(names[i], &lines);
		clock_gettime(CLOCK_MONOTONIC, &t1);
		printf("%-24s line=%-6ld %.1f ms\n", names[i], m,
		       (t1.tv_sec - t0.tv_sec) * 1e3 + (t1.tv_nsec - t0.tv_nsec) / 1e6);
	}
	struct rusage ru;
	getrusage(RUSAGE_SELF, &ru);
	printf("lines=%ld maxrss=%ld KiB\n", lines, ru.ru_maxrss);

	hostlist_t *big = slurm_hostlist_create("slurmkube-w[0-63]-[0-65536]");
	printf("bracket of 65537 names: %s\n", big ? "accepted" : "rejected");
	if (big)
		slurm_hostlist_destroy(big);
	return 0;
}
