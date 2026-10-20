# Contributing

How to build, test and land a change. `ARCHITECTURE.md` is the map and `GLOSSARY.md` the
vocabulary; read those first if the words here are new.

## Build

The toolchain is pinned in `rust-toolchain.toml`; `rustup` picks it up. Initialize the pinned contract model before building:

    git submodule update --init contract/inference-events
    cargo build --workspace

## Test

The suite has three tiers (`docs/testing.md` says who acts when each is red). The first
runs with plain `cargo test`:

    cargo test --workspace

That is the unit tests, the guards' engine-free cases and the static audit. It needs no
engine binary. Every test that needs the engine is `#[ignore]`d with a reason that says
how to run it, and `cargo test` prints those reasons beside `ignored`, so the output says
what it skipped and why.

The engine-backed battery needs the pinned ClickHouse. One command fetches it,
hash-verified, into `.tools/clickhouse`:

    scripts/fetch-engine.sh
    scripts/test.sh engine pipeline

`CLICKHOUSE_BIN` points the tests at a build of the pinned version somewhere else. A
missing engine is refused with that instruction, not guessed around.

## The gates

Two scripts, judged by exit code:

- `scripts/preflight.sh` while working: workspace formatting, SPDX headers, runner checks,
  clippy and engine-free tests. Pass a crate name (for example `scripts/preflight.sh api`)
  to scope clippy and tests to that crate. This is development feedback, not a push verdict.
- `scripts/test.sh engine pipeline keyed_equivalence` runs one integration target. Omit the target
  to run that crate's engine suite. See [testing](docs/testing.md) for module examples.
- `scripts/prepush.sh` before a push: `preflight.sh --full` (all fast tests, pipeline
  correctness, API and daemon loop integration), then docs, public API, third-party notices
  and OpenAPI checks. Fetch the pinned engine once with `scripts/fetch-engine.sh`.

Push only on prepush's green, then read CI's verdict (`gh run list --limit 1`). "Tests
pass locally" and "the pipeline passes" are different claims; `CLAUDE.md` names the jobs
only CI runs.

## The contributor licence agreement

This repository accepts contributions under the InferTap Contributor License Agreement,
`CLA.md`. It is modelled on the Apache ICLA and it grants InferTap the right to relicense,
which is what lets the project change its licence later without asking every contributor
again. You keep your copyright.

Sign it once: comment on your first pull request with exactly

    I have read the CLA Document and I hereby sign the CLA

The CLA assistant records the signature and checks every later pull request; a pull
request from an unsigned author is held until the comment is there. Contributing on behalf
of an employer is section 7 of the agreement.

The tap repository is different: it is Apache-2.0 and uses the Developer Certificate of
Origin (`Signed-off-by`) instead. This repository and the console use the CLA.

## Landing a change

- One task per commit. A refactor that moves code carries no behaviour change; the
  pins and the battery are the proof, and a golden is never re-blessed to make a diff go
  away — a changed golden is a finding to explain in the commit.
- The static audit (`crates/cli/tests/static_audit.rs`) ratchets the dependency allowlist,
  the SQL-building sites per file and the analytic constants. When it goes red it names the
  entry to edit and the review the change needs; adding a dependency also needs `deny.toml`
  and a regenerated `THIRD-PARTY-NOTICES`.
- The public surface of every library crate is a committed baseline
  (`contract/public-api/`). A changed signature is a diff in the commit that causes it:
  `scripts/public-api.sh update`, then commit the baseline with the change.
- `.github/CODEOWNERS` names who reviews which crate.
