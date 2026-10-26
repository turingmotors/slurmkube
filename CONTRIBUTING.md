# Contributing

## Ground rules

- **Slurm is the source of truth for jobs.** A change that makes
  Kubernetes decide a job's fate (other than reporting a JobSet failure
  to Slurm) needs a design note first.
- **Every write must be idempotent** and derived from the current
  snapshot and cache. No decision may depend on an event's content or on
  in-memory history beyond the documented graces.
- **Ownership before action**: never delete, DOWN or clear anything
  whose owner is not established (`internal/ownership`).
- **Fail closed**: when the controller cannot tell, it waits and says why
  in the job's comment and a Kubernetes Event.
- **No site data in code**: host names, registries, IPs and credentials
  belong in Helm values or the config file.

## Workflow

```sh
make lint            # gofmt + go vet (with the integration tag)
make test-race       # unit tests
make test-integration  # envtest against a real kube-apiserver
make helm-lint
```

Tests prefer observable behavior and invariants over structure: a test
sets up Slurm (the fake) and Kubernetes (fake client or envtest), runs
reconciles, and asserts what slurmctld and the API server would see.
Scenario tests that reproduce an incident go next to the rule they
protect; name them after the behavior, not the function.

Commits follow Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`,
`chore:`). Keep a change to one concern; put the reason in the message.

## License headers

Every source file carries the SlurmKube copyright header and the
`Apache-2.0` SPDX identifier. Third-party code may only be added with its
license recorded in NOTICE.

## Reporting incidents

An incident report should contain: the Slurm job IDs, `scontrol show
job --json` and `scontrol show node` of the dynamic nodes (address,
port, `extra`), the JobSet and pod objects with annotations, the
controller's log for the interval, and the slurmctld log lines for the
job. The ownership rules are decided from exactly that data.
