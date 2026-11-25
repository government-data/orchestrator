# AGENTS.md

Instructions for coding agents working in this repository, the
`orchestrator` module of Quality Data. Adapted from `quality-sdk`'s
`AGENTS.md`: the same way of working, for this repository.

## What this is

`orchestrator` is an application module of Quality Data (quality-sdk
decision 0002): it fires schedules, keeps the state of every run (queue,
leases, attempts, logs) and hands runs out to **engine modules**, which pull
them over the engine port. One installation is one organisation (no
tenants) and there are no licences. The orchestrator's core never names an
engine: an engine is a module of its own, in a folder of its own.

- `crates/`: the Rust workspace. Pure crates (`orchestration-model`,
  `schedule`, `engine-port`) do no I/O; the server, its adapters and
  `engine-runner` build on them, as the decisions add them.
- `modules/`: one folder per module (`orchestrator`, then each engine), each
  with its `module.yaml`, compose and package.
- `tools/verify/`: scripts that check external behaviour, run by hand.
- `docs/`: internal documentation (decisions, implementation, roadmap).
  **Ignored by git, never committed, pushed or published.**

## Before you change anything

1. **Find the decision.** The work here follows quality-sdk decision 0011
   and the records it names. Read the step you are doing and its check.
2. **Follow the decision; do not redesign it in code.** If the code needs
   something the decision does not say, or contradicts it, stop and say so.
   An accepted decision changes only through a new record.
3. **Verify external behaviour first** (PostgreSQL, Docker Compose, an
   engine) with a script in `tools/verify/` before building on it, and
   record the result in the decision (date, version, what was found).

## Architecture patterns

- **Clusters pull.** Nothing pushes to a cluster: an engine's runner claims
  its runs from the orchestrator and renews a lease while it runs them. No
  inbound port on a cluster.
- **The core never names an engine.** An engine is a free name checked
  against the engines registered now; no list in code, no `CHECK` of names
  in SQL. Engine detail lives in the engine module.
- **Ports and adapters.** The scheduler and the engine port work behind
  traits; PostgreSQL, HTTP and files live in adapters. Every use case is
  tested with fakes.
- **Every step survives a crash.** State is written before each step that
  changes something, and every step can run twice.
- **Pure crates stay pure**: no I/O, no clock, no randomness; time comes in
  as an argument.

## Data rules

- **Migrations only add.** Drops, renames, type changes and new required
  columns go in a later version, marked `-- contract: vX.Y.Z no longer uses
  it`; a version that cannot go back is marked `reversible: false`.
- **No personal data** in logs, metrics, test fixtures, examples or
  documentation. Metrics carry only fixed labels (`engine`, `line`, `kind`,
  `result`); technical logs carry run ids, never parameters, values or SQL.
- **Secrets** never go in the repository, a log, a commit or an error.
- **Never real cloud accounts in development or tests.** Tests use fakes.
  Releasing (a tag) is a person's.

## Language

**English only**: code, identifiers, file names, comments, documentation,
messages, logs, commits, branches and pull requests. Code brought from
`knime-orchestador` is translated to English when it comes in. The only
Spanish allowed is the `es` text of a manifest's menu and permissions (text
people read in the product).

## Size limits

Targets, then hard limits that fail CI:

- **Rust file** 400, hard 800 lines. **Function** 60, hard 100 lines
  (Clippy `too_many_lines`). **Test files** 800, no hard limit: split by
  use case.
- **A change**: at most about 400 diff lines without generated files, one
  step of a decision's Implementation at a time.

Above the target, split when you touch it. Never add an exception: split.

## Checks before a commit

```sh
cargo fmt --all -- --check
cargo clippy -p <crate> --all-targets --locked -- -D warnings
cargo test -p <crate> --locked
```

On a shared machine, build one crate at a time and never two builds at
once (see quality-sdk's `AGENTS.md`, "Builds on this machine"): every
`cargo` command through `~/.local/bin/capped` with `-j 2`, never
`--workspace`, no `cargo clean`. Integration tests that start PostgreSQL
run with `--test-threads=2` and remove their containers when they end.

## Do not

- Do not commit, quote or link a decision: code comments may name it by
  number ("decision 0011"), never copy its content.
- Do not bring back licences, plans, tenants or an engine's name in the
  core's crates.
- Do not weaken a test, a lint or a check to make it pass.
- Do not push a tag: tagging publishes to AWS.
