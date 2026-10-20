# Testing

How the suite is run, and the checks that are not part of `cargo test`. `CLAUDE.md` names
the two gates (`scripts/preflight.sh` while working, `scripts/prepush.sh` before a push);
this page is what sits around them.

## Development commands

Use crate and integration-target selection for short feedback loops. These are Cargo's
standard [package/target selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html);
benchmarks use Cargo's [bench targets](https://doc.rust-lang.org/cargo/reference/cargo-targets.html#benchmarks).
No changed-file heuristic silently omits dependents.
All scripts can be called from any working directory and use the locked dependency graph.

| Change | Fast checks | Relevant engine integration |
|---|---|---|
| Pure analysis logic | `scripts/preflight.sh analysis` | `scripts/test.sh engine pipeline classify_guards` (choose the guard for the law changed) |
| Pipeline keyed parity | `scripts/preflight.sh pipeline` | `scripts/test.sh engine pipeline keyed_equivalence` |
| API routes, cursors, responses | `scripts/preflight.sh api` | `scripts/test.sh engine api` |
| Daemon scheduling or retry | `scripts/preflight.sh daemon` | `scripts/test.sh engine daemon` |
| Shared publication boundary | `scripts/preflight.sh pipeline` | Run both API and daemon engine suites |
| Any crate without engine I/O | `scripts/preflight.sh <crate>` | Exercise downstream consumers when behavior changes |

Examples are starting points, not an exhaustive dependency map. A focused pass does not
validate downstream crates. `scripts/prepush.sh` always checks the whole PR scope, and CI
runs the same suites for every PR regardless of which files changed.

For finer iteration, use Cargo directly:

```sh
cargo test -p analyzer-pipeline --locked --lib publication::
cargo test -p analyzer-pipeline --locked --test keyed_equivalence -- --ignored
```

Cargo validates package and integration-target names. A test-name filter can match zero
tests: check the executed count when using filters. Omit the filter for the module gate.
`scripts/test.sh fast <crate>` skips formatting/linting when only test feedback is needed.

## Execution tiers

- **Fast:** `scripts/test.sh fast` runs all engine-free workspace tests and doctests.
  `scripts/preflight.sh [crate]` adds formatting, headers, runner regression checks and linting.
- **Engine:** `scripts/test.sh engine pipeline`, `engine module-example`, `engine api`,
  `engine daemon`, `engine serving-clickhouse` and `engine conformance` use the pinned
  ClickHouse. Daemon defaults to `loop`; `conformance` is the spec's reader corpus
  (`crates/cli/tests/reader_corpus.rs`). Every scope but pipeline runs serially. No
  benchmark runs in this tier.
- **Full PR correctness:** `scripts/test.sh full` combines fast and every engine scope. `scripts/preflight.sh --full` adds linting; prepush adds generated contracts/docs.
- **Release transport:** `scripts/test.sh release` runs all ignored workspace tests,
  including the live-bucket transport, TTL and chaos suites. It needs an S3-API bucket that
  keeps conditional writes and the environment in `.github/workflows/release.yml`, which runs
  SeaweedFS; it is not a bare-laptop gate.
- **Measurements:** `BENCH_EVENTS=300000 cargo bench -p analyzer-pipeline --bench tick`
  explicitly measures the synthetic tick. It uses the pinned engine and runs outside
  correctness gates; timing results are not portable pass/fail thresholds.

The first engine run needs `scripts/fetch-engine.sh` and the checked-out contract submodule.
The wrappers fetch nothing. CI shares these entrypoints, with fast, pipeline and serving
jobs running independently. New integration targets are discovered by Cargo automatically;
new daemon suites needing only the engine should be added to the daemon tier explicitly
(the existing `ttl_gate` requires the release transport).

## The tiers

The suite is three tiers, and the tier says who acts when a test is red and what the
action is. A contributor works in the first; the second is maintainer-owned; the third is
the gate.

### Tier 1: the laws a contributor reasons about

`cargo test --workspace` runs the unit tests and the static audit, and needs no engine:
every engine-backed test is `#[ignore]`d with a reason that says how to run it. The
pipeline's guards need the pinned engine — `scripts/fetch-engine.sh` fetches it once,
hash-verified, into `.tools/clickhouse` (`scripts/preflight.sh` fetches nothing) — and run
with `cargo test -p analyzer-pipeline -- --ignored`. A missing engine is refused with that
instruction.

- **Unit tests** in each crate, beside the code they test.
- **The guards**: `crates/pipeline/tests/*_guards.rs`, one file per law — classification,
  coverage, degradation, the differential audit, intervals, liveness, reuse, the rollups,
  the roster, the block tree. Each constructs a stream, runs the derivation, and asserts a
  property of the result that the law states in its doc comment. When a guard is red, either
  the code broke the law or the law changed; the fix is in the code or in the guard's stated
  law, never in a re-bless.
- **The static audit**, `crates/cli/tests/static_audit.rs`: the structural laws — no
  `unsafe`, no process spawn outside the engine boundary, no network outside the I/O
  crates, every SQL-building site through the quoting helpers (counted per file), the
  dependency allowlist per crate, every analytic constant on the re-derivation list. Every
  failure names the file, what it found, what it expected, and the exact edit to make if
  the change is intended. A red audit on a pure refactor is normal: the SQL-site counts
  follow code between files, and the message says which entry to move.
- **The API's response shapes**, `crates/api/tests/goldens.rs`: asserted in code against a
  database a tick wrote, with the figure walk that checks every figure on every response
  carries its coverage. Red here means a response changed shape; the assertion is the
  contract's reading of it.

### Tier 2: byte-equality goldens, maintainer-owned

A diff in this tier is a semantics change until shown otherwise. Re-blessing is a
deliberate act in its own commit, with the reason stated; a contributor who lands here
should stop and say what changed rather than regenerate.

| golden | what it pins | re-bless |
|---|---|---|
| `crates/pipeline/tests/tap_corpus_conformance.rs` | every figure the product prints over the tap's published telemetry corpora, byte for byte | `BLESS=1 cargo test -p analyzer-pipeline --test tap_corpus_conformance -- --ignored` |
| `crates/pipeline/tests/derived_snapshot.rs` | the derived-facts document per fixture case, over the whole read surface | `UPDATE_DERIVED_SNAPSHOTS=1 cargo test -p analyzer-pipeline --test derived_snapshot -- --ignored` |
| `contract/openapi.json` | the read API's contract, generated from the wire types and their doc comments | `cargo run -p analyzer-api --bin dump-openapi > contract/openapi.json` |
| `contract/public-api/<crate>.txt` | each library crate's public surface | `scripts/public-api.sh update` |
| `fixtures/joins`, `fixtures/gait`, `fixtures/producer` | the interpretation rules, arrival-order gaits, co-residency claims — contract fixtures with stated expectations, run by `fixtures_corpus`, `keyed_equivalence`, `placement_corpus` | edit the expectation with its reason; these are the spec's executable form, not snapshots |

Every golden in this tier has a Tier 1 front line that catches the same class of regression
in words a contributor can act on: the guards and the API figure walk for the derived
figures, the static audit's SQL and allowlist ratchets for the structure, the public-API
check for the surface. Read the Tier 1 failure first; the golden's diff is the evidence,
not the diagnosis.

### Tier 3: the batteries and the gate

The pipeline battery (`--ignored`) runs in `scripts/preflight.sh --full` and CI's `battery` job;
the release workflow runs the whole workspace's battery against real daemons on the
tagged tree. `scripts/prepush.sh` is the gate before a push, and it is judged by exit code.
The rig's measurements are measurements, not tests.

### When it is red

| you see | it is | do |
|---|---|---|
| a `*_guards.rs` test names a law | Tier 1 | fix the code, or change the law in the guard's doc and its assertion together |
| `static_audit` says a count or a list changed | Tier 1 | follow the message: it names the entry to edit and the review the change needs |
| `public-api: the public surface of crates/X CHANGED` | Tier 2 | if intended, `scripts/public-api.sh update` in the same commit; if not, the item wants `pub(crate)` |
| `contract/openapi.json is stale` | Tier 2 | if intended, regenerate in the same commit; a doc comment on a wire type is contract text |
| a corpus or snapshot diff | Tier 2 | stop, state what changed and why; a maintainer re-blesses with the reason |
| a test fails only on some days or under load | any | run `scripts/clock-forward-check.sh`; a fixture may have pinned a date |

## Build cache

The development profile uses `split-debuginfo = "packed"`; the test profile inherits it.
This preserves full debug information and incremental compilation. On macOS the symbols
live in `.dSYM` bundles, and linking pays for `dsymutil`. Release and benchmark profiles
retain their existing settings. See Cargo's [profile documentation](https://doc.rust-lang.org/cargo/reference/profiles.html#split-debuginfo)
and rustc's [symbol formats](https://doc.rust-lang.org/rustc/codegen-options/index.html#split-debuginfo).

Why: Cargo defaults to **unpacked** debug information on macOS. Rebuilding retains loose
`.rcgu.o` files in `target/debug/deps`. The local cache inspected on 2026-09-19 contained
3,243,184 object entries; a bounded sample included artifacts from July through September
and many distinct build hashes per target. These are often hard links, so entry counts
must not be interpreted as equivalent numbers of unique files or disk bytes.

A controlled reproduction with the pinned Rust 1.97.0 toolchain, a tiny binary and
incremental compilation enabled produced these loose-object counts in `debug/deps`:

| Build sequence | Unpacked | Packed |
|---|---:|---:|
| Initial build | 6 | 0 |
| Unchanged rebuild | 6 | 0 |
| Four successive source edits and rebuilds | 30 | 0 |
| Four subsequent package-version changes and rebuilds | 54 | 0 |

To reproduce, create an empty Cargo binary outside this workspace and set
`[profile.dev] split-debuginfo` to each value in separate target directories. Build once,
rebuild unchanged, then change a printed literal and rebuild four times. Count `.o` files
directly in each `target/debug/deps`. Finally repeat with four package-version changes.
The experiment requires macOS; it measures artifact retention, not application speed.

The workspace's first packed preflight passed in 88 seconds including recompilation;
loose-object count remained at its pre-existing 34,116, with 82 symbol bundles produced.
Those old objects are not removed by changing the profile. Likewise packed symbols do not
bound the entire Cargo cache: old package versions, feature combinations and toolchains
can still leave artifacts. Retire or clean an unused target directory when needed; avoid
routine per-build cleaning, which discards useful incremental work. Never clean or move
a target directory while a build is using it.

## The clock

Every fixture is rebased from the wall clock — the `analyzer-harness` crate (`crates/harness`)
provides `rebase_offset()` (the current UTC day minus three days) and `now_ms()`, and the
other crates' suites build their instants from `SystemTime::now()` the same way. No test
pins an absolute date, and a test must not: a fixture written against a fixed day is
correct until the retention horizon or the calendar moves past it, and then it fails on a
date nobody changed.

`scripts/clock-forward-check.sh` runs the workspace tests and the pipeline battery with the
machine's clock a month ahead (only its `date` call is elevated; the tests run as the
invoking user). It moves the system clock, so it is for a disposable machine:
CI's `clock-forward` job runs it on a fresh runner every week and on a manual dispatch
(`gh workflow run ci.yml`), and that job's verdict is the check. It is not a check to run on
a development box, and `libfaketime` cannot stand in for it: the pinned engine is a static
binary that no preloaded library reaches, so under `faketime` the tests run a month ahead of
the engine and fail for that split rather than for a fixture (seen 2026-09-14 in
`producer_corpus`). One clock for every process, or no verdict.

The static audit's `every_seeded_timestamp_derives_from_the_clock` is the cheap half of the
same rule, on every push: a seed that carries an absolute epoch fails it before the calendar
can.

## Static module boundary

`scripts/test.sh engine module-example` runs the separate-crate public-API acceptance proof:
nonempty coverage-gap output, partial projection and completion failures, a replacement
executor with a fresh source cache and no arrivals, orphan-key removal, module standing,
disable/removal refusal and explicit backfill across an automatic-output gap. It is included
in `scripts/test.sh full`. Workspace fast tests cover pure arithmetic and registration
validation. No example runtime dependency reaches private pipeline interfaces.
