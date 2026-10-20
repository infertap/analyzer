# Changelog

All notable changes to the InferTap analyzer. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Release tags are `vX.Y.Z`;
the version on `main` is the next tag.

## [Unreleased]

## [1.0.10] - 2026-10-20

### Changed: the log compacts an hour a run of sections at a time
Compaction read every object of an hour whole and held all of its records at once, about 540
million at 500 GPUs, and cut them again under the current layout. It now reads runs of
consecutive sections, one ranged read of each tick object a run and at most 256 MiB of them, and
writes each section as it is merged, so it holds a run and one section's records. An hour keeps
the partition count it was written under; a changed layout cuts later hours. Each compaction of
an hour names its objects by its generation, so recompacting never writes over an object an
older manifest a rollback can reach still names. The day boundary's carried frontiers are
encoded into their sections as each key is read, never collected first.

### Added: a silent holder's residencies close
A residency now holds its block for as long as it is open, so a holder whose producer stopped
reporting (a decommissioned instance, a dead tap) would have held its blocks forever. Where an
instance's heartbeats stop past its liveness bound (at least the 30-minute staleness grace), the
completeness model draws a silence, between two segments of any of its runs or past its last
segment once the read reaches it, and every residency on the instance closes there with reason
`silent`. A store elsewhere afterwards is not `RE` against those copies; the holder's own next
store of such a block is `indeterminate`. A late segment that lands inside a drawn silence moves
it, and re-taint re-derives every block the instance held, its classes and its duplication
cells alike. Placement applies the silences in its own fold, so a silent holder's copies stop
counting as duplicates there; its re-derivation starts from the residencies open in the
frontiers carried at the window's day start, no longer from the live seeds open before the read.

### Fixed: a block with late records lost the earlier stretch re-taint reached
Re-taint re-derives the blocks a changed drop window or a moved silence reaches, and leaves to
the late-record path any block that path re-derived in the same tick; that path started at the
block's first late record, so a stretch the changed model reached before it was never restated.
The tick now takes re-taint's reach first, and a block's correction starts at the earlier of its
first late record and that reach, for its classes and its placement.

### Fixed: an evict that closed nothing counted as holding in the duplication cells
An interval whose open was not observed counted as held from the start of whatever window folded
it, so a tick's own width decided how much duplication a store-less evict added, and a correction
folding a narrower window stated less than the tick had. It now states the close and no holding,
as it already did for classification.

### Fixed: a tick with no records crossed day boundaries without carrying the frontier
A tick with nothing pending set its edge and returned, so an idle tick across midnight wrote no
carried frontiers for re-derivations to start from. A tick now folds from the classified edge
whether or not records wait, under a completeness model that starts from the earliest of the
records still to fold and both operators' edges; it was empty when no record waited.

### Changed: a residency holds its block for as long as it is open
An open interval counted for a store elsewhere only while its open was within the examined
history before the store, so a block resident on one holder for longer (a system prompt, a block
in an offload tier) stopped counting as held there, and a store of it elsewhere read `cold`, not
`RE`. An open interval whose store was observed now counts however long ago it opened; one whose
open was not observed (an evict that closed nothing) states the close only. The examined history
still bounds the prior-close lookback and the taint lookback.

Re-derivations (the drill, the sampled audit, corrections and re-taint) read the log only over a
bounded stretch, so they now start from what repeat compute carried: at each UTC day boundary
every key's frontier is written to the log partitioned by block, a carry section per partition,
and a re-derivation starts from its window's day start. A correction that re-derives a block
across a boundary writes its carry again; the latest naming a key wins. A re-derivation reads the
log from its window's day start, no longer from an examined history before the window.

### Changed: repeat compute holds its frontiers compact
Each lane held a key's frontiers as classifier rows, every row spelling its holder and block as
strings. A lane now holds each block's string once, its holders and workload classes once, and
an entry of 64 bytes per holder and block. A scope close reads the lane's entries and copies
only the open ones on the holders it covers, where it copied every row; a correction replaces a
block by one lookup where it scanned the lane. `RepeatCompute` and `Driver` lose their state type parameter,
and the `Frontiers` alias is gone.

### Changed: the examined history is an hour, exact, and the bands end there
The default `examined_history_ms` was 72 h, with a Bloom-filter tier past the first hour. It is
now an hour (`EXAMINED_HISTORY_MS`), held exact, and `Params::validate` refuses a longer one; the
tier is gone (`keyed_closes` leaves the working state). A close decides a store only while it is
less than the history back, whether it was carried in the frontier or read in the same pass;
before, the line sat at the slice's start minus the history, so the same store could classify
differently by where a tick began. The frontier drops each closed entry once it is older than the
history. At 500 GPUs a day of closes was hundreds of gigabytes of memory; an hour is a
twenty-fourth of it.

The time-since-eviction bands are `<10s`, `10s-1m`, `1-10m`, `10-60m` and `none`: `<1m` is split
where cache thrash and slower reuse part, and `1h-1d` and `>1d` are gone, since no gap reaches
them. The discriminator token is `s1000x3600000`, so the change re-derives every lineage. Joins
fixtures E2, E7, E8 and E11 state their closes and drop windows within the hour, and E8 adds a
close exactly an hour back and one past it.

How late a record may arrive and still be corrected is no longer the examined history: a day stays
open until every edge is `CORRECTION_HORIZON_DAYS` (1) past it, so a shipper backlog of hours is
still corrected, and the input floor (`LEDGER_RETENTION_FLOOR_DAYS`) is that horizon, the history
and the margin: 3 days. A close a silence made stays in the frontier for the correction horizon,
so a late segment that takes the silence back still finds the blocks it closed. The evict audit
keeps a sampled store until its evict, up to the log's retention, and a closed one for the
examined history, so a residency longer than the history is still graded.

### Changed: placement costs what moved, and whales are a settled-day register
A tick folded every resident block over every minute of its window, so its cost grew with
resident blocks times minutes, and it rewrote a whale row for every resident block. A block
whose holders hold still now adds a constant rate per cell key: a lane sums the still blocks'
rates and states their cells from the sum, and folds one by one only the blocks with records,
those a clear or reset reaches, and those whose holders still change. Each block accrues its own
whale day totals in its state. `replication_identity_day` is gone: once a day settles, one pass
publishes its top 1,000 identities by duplicated token-time (`replication_whale_day`) and the
day's exact totals over every identity, with the cut's floor (`replication_whale_day_total`).
`/v1/residual/whales` reads the window's settled days: `days_stated`, `cut`,
`duplicated_identity_days`, and per row `days_in_cut` and `dup_token_ms_bound`, the most the row
falls short by; `emitted_to_ms` and `duplicated_identities` leave the response.

### Fixed: a drop window is the span between two consecutive heartbeats
`docs/classification-semantics.md` §5 defines it so; the segment rows kept the counters only at
a segment's last heartbeat, so a drop inside a segment marked the whole quiet stretch before it,
and every other instance's stores in that stretch read tainted. Each row now states the
counters at its first heartbeat too (`dropped_at_hb_first`, `oversized_at_hb_first` on
`input_segments`), and the gap and the segment are two spans.

### Changed: the release binary runs on glibc 2.28 and later
It is built in PyPA's manylinux_2_28 image (AlmaLinux 8, GCC 14), so RHEL 8 joins Debian 10+,
Ubuntu 18.10+, RHEL 9 and Amazon Linux 2023. Debian 11's compilers cannot build the C++20
RocksDB the working state embeds. The release job asserts the glibc floor and RHEL 8's C++
library (GLIBCXX 3.4.25).

### Added: the evict audit runs
Each tick grades the evicts on a sample of engine ids against their latest prior store: an
evict naming another identity turns the `evict_resolution` check critical, one naming none where
the same run's store named one warns. Metrics: `infertap_analyzer_evict_audit_audited_total`,
`_mismatched_total`, `_underresolved_total`. Until now the audit ran only in tests.

### Added: the sampled audit
Each tick re-derives a few of the blocks it classified from the log partitioned by block and
compares them with what it stated. The stamp's `tick` carries `audited_to_ms`, the edge before
which every sample agreed. A disagreement is logged with its block and store, holds the edge,
and turns the `classification_audit` check critical until the derived state is derived afresh.
Metrics: `infertap_analyzer_audit_blocks_total`, `infertap_analyzer_audit_disagreed`.

### Added: published state retires at the rollups' horizon
The lake's published state keeps its days for `--rollup-retention-years` and no longer: the
daemon's maintenance drops older days, manifests first.

### Removed: the shed rung
The disk ladder is Normal, Reclaim and Refuse. Shedding lowered only the input horizon and freed
no local disk.

### Added: what a duplication zero means (spec 5.7)
`/v1/residual/replication` carries `zero_basis`: whether a cross-engine figure of zero is a
measured absence or one of `single_holder`, `not_comparable`, `split_key_space`,
`partly_comparable`, with the counts behind it. Ingest counts each instance's own and
comparable stores into `facts_rollup_daily`, and each run's key-space canary is kept in
`input_key_spaces` for as long as the rollups. The reader corpus grades the `residual_zero`
family again.

### Added: preflight proves the bucket fences writers
`preflight` adds `bucket_fences_writers`: on the deployment's own bucket, a create of an
existing key and an update at a superseded version must both be refused. The analyzer's lease
and heads rest on those refusals, and an S3-compatible store that ignores `If-None-Match` or
`If-Match` would let two writers commit with nothing downstream able to tell.

### Fixed: year horizons past 2106
Every table whose horizon is `rollup_retention_years` now ages by date. Seven of them aged by
a `DateTime`, which a long enough horizon overflowed, deleting every row.

### Changed: the keyed pipeline (semantics `js-7`)
The daily classification pass is gone. Every ingest cycle runs one keyed tick: repeat compute,
placement, coverage, the roster and the tag cells advance from the input log on block-keyed
lanes, and their changed cells are published as one batch under the journal's intent. A day's
day-grain outputs (the generation markers and the public modules) are stated when it settles.

The `classified_stores` table is gone. A block's holders are re-derived from the input log
partitioned by block, a holder's timeline is read from its ring of recent classified stores,
and the blocks by stored mass are the served `block_mass_daily` and `block_mass_day` cells.
The `derivation_status` facts `last_pass_at_ms` and `audited_through_day` are replaced by
`last_tick_at_ms`, `classified_to_ms`, `emitted_to_ms`, `covered_to_ms` and
`settled_through_day`.

#### Self-health
- Renamed: `infertap_analyzer_passes_total` to `infertap_analyzer_ticks_total`,
  `infertap_analyzer_pass_failures_total` to `infertap_analyzer_tick_failures_total`,
  `infertap_analyzer_pass_duration_seconds` to `infertap_analyzer_tick_duration_seconds`,
  `infertap_analyzer_pass_lag_seconds` to `infertap_analyzer_tick_lag_seconds`, and the
  check `pass_cadence_kept` to `tick_cadence_kept`. Lag is judged against the ingest cadence.
- Removed: `infertap_analyzer_deltas_bytes`, `infertap_analyzer_deltas_bytes_per_day`,
  `infertap_analyzer_projected_steady_state_bytes` and the check `retention_fits_disk`,
  which projected `classified_stores`; and the five `infertap_analyzer_differential_*`
  series with the check `differential_audit`, which the pass's audit cadence fed.

#### Command line
- Added: `reset-derived`, which deletes a deployment's derived state and keeps its input log,
  so the next `run` derives afresh from the log; `--ingest-interval <SECONDS>`, the cadence of
  the ingest cycle and the tick it runs (a minute by default); and `--maintain-interval
  <SECONDS>`, the cadence of the input log's upkeep (hourly by default).
- Removed: `--pass-interval` (the tick runs every ingest cycle), `--drill-cache-bytes` (no
  local copy of classified rows is kept) and the `INFERTAP_INVARIANT_EVERY_TICK` switch.

#### Read API
- The stamp's `pass` object is now `tick`: `at_ms`, `classified_to_ms`, `classified_day`,
  `emitted_to_ms`, `covered_to_ms` and `settled_through_day`. There is no audit edge until
  the sampled audit lands.
- `/v1/blocks` serves each UTC day's blocks by stored mass from a bounded summary: per day the
  tokens, the summary's capacity, the cut and the error bound, and per block lower and upper
  bounds, whether it is certainly in the day's top, and the holders that stored it at least.
- `/v1/evidence` takes `block` or `holder` and a `window`. The `day` selector is removed.
  Drills say where their rows came from: `read_from` is `log` or `ring`.
- `/v1/health` states each public module's span, `stated_from_day` to `stated_through_day`,
  in place of per-day scopes.
- The SSE event `pass_completed` is now `tick_applied`.
- Removed `stamped_events` from figure buckets and `coverage_path` from evidence records. Both
  were constant: the producers' clocks are compared directly, so there is one coverage path.
- Added `/v1/tags`: the declared tag intervals grouped by label over the window, with each
  label's exposure in identity-minutes, refetch tokens per identity-minute of exposure
  (primary) and per stored token (secondary, biased upward under a policy that cuts refetch),
  each with a 90 % interval over identities, the indeterminate tokens apart, and the window's
  coverage.

#### Semantics `js-7`
`docs/classification-semantics.md` section 13 records what moved: coverage of an open day is
stated through the covered edge, late heartbeats restate it, late records correct classes and
placement by re-deriving the identities they reach, and a changed parameter derives the whole
log afresh.

### Changed
- **Contract 1.11 (`v1-draft.24`).** A headerless recovery is no longer a shipped
  `orphan-<ms>` file: the producer discards it and declares the discard second in its own
  first segment. This reader already claimed no span for that shape; the declaration is
  banked as a lifecycle row and nothing more, the span before an incarnation's start being
  uncovered already (§5.3). The marker-first orphan form stays admitted under §4.1's
  reader tolerance and lands in `orphan_batches` as before. The delivery corpus's
  generator caught up with its hand-edited fixture upstream, so the pinned segment is
  unchanged in content and carries `contract_version` 1.11.
- **Licence.** The analyzer is licensed under `AGPL-3.0-only`; contributions are accepted
  under `CLA.md`. Every source file carries its SPDX identifier.
- **The pass works per stripe.** The fold and the tick run one stripe of identities at a
  time, on their own lanes, under a memory budget; a day's arrivals are chunked into a
  lake spine and merged in key order; memory is a resource with a ladder and a cap, like
  disk.
- **The lifecycle bank.** Lifecycle rows are banked at ingest, so the ledger read that
  anchors a window takes a fraction of a second instead of minutes.
- **The library shape.** The pass and the daemon are decomposed into files one concern
  each; every crate's public surface is curated and pinned as a committed baseline;
  every crate has a typed error; `ARCHITECTURE.md` and `GLOSSARY.md` are the map.
- **The suite.** `cargo test` runs without the engine; `scripts/fetch-engine.sh` fetches
  the pinned engine hash-verified; CI runs the lint, the tests and the engine-backed
  battery in parallel, and the suite a month ahead on a schedule.

### Added
- `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `SECURITY.md`, `GOVERNANCE.md`,
  `MAINTAINERS.md`, `ROADMAP.md`, issue and pull-request templates.

## [1.0.9] - 2026-08-22
## [1.0.8] - 2026-08-22
## [1.0.7] - 2026-08-21
## [1.0.6] - 2026-08-21
## [1.0.5] - 2026-08-20
## [1.0.4] - 2026-08-19
## [1.0.3] - 2026-08-18
## [1.0.2] - 2026-08-17
## [1.0.1] - 2026-08-16
## [1.0.0] - 2026-08-15
The first release: the analyzer binary with the pinned engine, the embedded console, the
read API, the OCI bundle, the container image and the Helm chart. The releases from 1.0.1
to 1.0.9 are the install and release-pipeline fixes found by installing on clean machines;
their notes are on the tags.

[Unreleased]: https://github.com/infertap/analyzer/compare/v1.0.10...HEAD
[1.0.10]: https://github.com/infertap/analyzer/compare/v1.0.9...v1.0.10
[1.0.9]: https://github.com/infertap/analyzer/compare/v1.0.8...v1.0.9
[1.0.8]: https://github.com/infertap/analyzer/compare/v1.0.7...v1.0.8
[1.0.7]: https://github.com/infertap/analyzer/compare/v1.0.6...v1.0.7
[1.0.6]: https://github.com/infertap/analyzer/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/infertap/analyzer/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/infertap/analyzer/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/infertap/analyzer/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/infertap/analyzer/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/infertap/analyzer/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/infertap/analyzer/releases/tag/v1.0.0
