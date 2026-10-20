# Architecture

The analyzer is the aggregator-side half of InferTap. Taps beside each inference engine ship
sealed Parquet segments of KV-cache events (stores, evictions, clears, lifecycle) to an
S3-API bucket. The analyzer reads those segments, folds them into figures — duplication,
hit and residency figures, the recompute bands, coverage — and serves them to the console
and to Grafana. This document is the map: the shape, the crates, the line between the
library and the application, the fence every figure obeys, and the grain ladder the storage
follows. It says where to start reading; the documents it links say why each boundary is
where it is. `GLOSSARY.md` defines the words this document and the source use
without explaining them.

## The shape in one paragraph

The analyzer combines pure Rust derivation with a durable lake and a read service. Taps'
sealed segments are the original input; ingest records each one in the lineage's input log.
The `analysis` crate computes figures without I/O. `pipeline::Executor` runs the keyed driver:
one tick reads what the log committed, advances its operators (repeat compute, placement,
coverage, the roster, the tag cells) on block-keyed lanes, saves their state in the lake's
working state, and publishes the cells that moved as one batch under the journal's intent.
ClickHouse serves the published cells and the input tables; it holds no state the derivation
reads back. The object store holds the input, the log partitioned by block, the journal, the
working state's checkpoints and the published state.

Restart recovery needs the same lineage: its journal, its input log and its working state's
checkpoints in the object store. A replacement worker restores the working state from the
newest checkpoint and publishes an unapplied tick's batch again. One logical whole-fleet
partition is supported per
lineage, and its writer is fenced by the journal's leased epoch. Lanes parallelize local
work; they do not establish independent distributed ownership. Standalone callers and the
daemon invoke the same executor.

## The crates, and the two lines between them

One Cargo workspace, nine production library crates, a separate module-example library, one binary and a test harness every suite shares, a layered DAG with no cycles:

| crate | role | depends on |
|---|---|---|
| `analysis` | the fold: the Ordered Event IR, intervals, classification, coverage, the residual, staleness, the tree | nothing |
| `segments` | the tap's record model and its decode (Parquet shipped, JSON Lines write-ahead) | nothing |
| `objectstore` | the object-storage port, the workspace's only outbound network client | nothing |
| `lake` | the bucket's layout and the lake: the input log, the execution journal's head, the working state and the published state | objectstore |
| `ingest` | segment to input log: the content-hash batch, the arrivals, annotations | lake, objectstore, segments |
| `pipeline` | the engine-free core: the source, the keyed driver and its executor, the journal, the serving port | analysis, lake, objectstore, segments |
| `serving-clickhouse` | the ClickHouse serving store: the engine, its migrations, the port's implementation, the engine's health, and every read the product performs | analysis, ingest, lake, pipeline |
| `api` | the versioned read seam: HTTP, OpenAPI, the embedded console | analysis, pipeline, serving-clickhouse |
| `daemon` | the run loop: ingest, tick, maintain, pressure, shed | analysis, ingest, lake, objectstore, pipeline, serving-clickhouse |
| `module-example` | separate-crate static coverage-gap derivation and public registration proof | analysis, pipeline |
| `cli` | `infertap-analyzer`, the one binary that composes the rest | everything |
| `harness` | the suites' shared fixture: the pinned engine, planted streams, journal faults; a dev-dependency only | analysis, ingest, lake, objectstore, pipeline, segments, serving-clickhouse |

**The bright lines.** `analysis` depends on nothing, and the dependency allowlist pins it. The
fold is a pure function of `(segments, parameters)`; everything that does I/O lives in
`pipeline` or above. And `pipeline`, the core, depends on no serving store: it states its
output through the serving port (`pipeline::publish::Serving`) and never reads it back, and
`static_audit::the_core_depends_on_no_serving_store` pins that
(`docs/lake.md`).

**The library seam.** A partner who wants the derivation over their own segments links
`analysis`, `segments`, `objectstore`, `lake` and `pipeline::Executor`, with
`pipeline::parquet_source` and `pipeline::spine` for input — the analyzer as a library.
`serving-clickhouse`, `daemon`, `api` and `cli` are the application built on it. Each library crate's root states its
supported surface in a `pub use` block; everything else is `pub(crate)`.

## How an event becomes a figure

Read the code in this order and each step is one crate or one module.

1. **A segment is decoded** — `segments`: sealed bytes in, the record model out, no store.
2. **A window's events are assembled** — `pipeline::parquet_source::ParquetSource`: dated
   discovery under the tap's key layout (`lake::dated`), a fetch-once content cache over
   `objectstore`, the projected columnar decode, and the Ordered Event IR
   (`analysis::events`) grouped by identity.
3. **The fold** — `analysis`: residency intervals (`intervals`), the six-class
   classification of every store (`classify`), coverage and the completeness clock
   (`completeness`), the prefix-semantic residual (`residual`), staleness (`staleness`), the
   block tree (`tree`). One identity group at a time, nothing shared.
4. **Execution** — `pipeline::Executor`, opened over a spine and a serving store, runs one keyed
   tick (`Executor::advance`) under the journal's intent: `keyed::Driver::tick` reads the input
   log through `keyed::SegmentSource`, routes each record to its block's lane, and advances the
   operators; `Driver::save` writes their state and the batch to the working state; the batch
   is published, the settled-day hook (`keyed::Hook`) states the day-grain outputs, and the
   intent is applied. The module doc of `pipeline/src/keyed/mod.rs` is the map of those files,
   and `docs/pipeline.md` is the design.
5. **The store** — `serving-clickhouse`: `engine` (the pinned ClickHouse, its migrations,
   the retention knobs, and the only two paths a string may take into SQL, `quote_ident` and
   `quote_literal`), and `serving::ClickHouse`, the serving port's implementation.
6. **The reads** — `serving-clickhouse`'s read layer: one module per query shape, every
   figure coverage-carrying.
7. **The surface** — `api` serves the read layer over HTTP under `docs/read-api.md`; `daemon` runs
   the loop (`boot`, `run`, `ingest`, `tick`, `settle`, `shed`, `sample`); `cli` composes them
   into the binary.

## The fence

Every figure the analyzer emits obeys one criterion:

> A stamped dimension is an event fact iff it is a pure function of one identity's own event
> prefix at the moment of emission, emitted at the timestamp of the latest event in that
> prefix.

Two lanes follow from it. A dimension that passes the fence (the recompute band, the
workload class) is data before any merge happens, so everything downstream is a grouped sum
and a late segment repairs its own identity and nothing else. A measure read from state
outside one identity's line (a copies-at-emission count, anything the fleet's frontier
shapes) fails the fence and is admitted only under placement's discipline: a watermark,
emission trailing it, and late records corrected by re-deriving the identities they reach
(`pipeline::keyed::PlacementCompute`).

The fence identifies arithmetic that can be evaluated per identity when its complete
context is supplied. Classification still needs fleet uncertainty, holder-scoped closers
and roster context; duplication also needs prefix state and fleet watermark coordination.
Only the whole-fleet execution partition is supported.

## The grain ladder, as a reader meets it

- **One grain vocabulary.** `minute`, `hour`, `day`, on every endpoint. One window grammar
  and one anchor (wall now), resolved once by the HTTP request constructor, with every response stating its own
  freshness in-band.
- **Retention per grain, every horizon a knob.** `serving-clickhouse::engine` owns the ladder: minute days,
  hour days, rollup years, the ledger's horizon; the daemon may shed toward a floor under
  disk pressure and never below it (`daemon::shed`, `daemon::pressure`).
- **Cells are whole values.** Each operator holds its cells keyed, and a tick publishes the
  ones that moved, each key's whole value under the tick's publication; reads take the latest
  version per key under `FINAL`. A late record's correction moves the held cell, and the next
  batch carries it.
- **The block is the unit.** An identity's hash selects its lane (`keyed::repeat::lane_of`),
  and every operator's state for an identity lives in its lane. Scope closers, which name no
  identity, reach every lane. This is local parallelism, not a claim that independent workers
  can publish without coordination.
- **The log partitioned by block.** The driver writes each batch's records once, on the tick
  that first reads it, into hourly objects partitioned by block, compacted once both
  operators have passed the hour (`keyed::spine`). A drill re-derives one block from it
  (`keyed::refold_block`), and the correction path re-derives the identities a late record
  reaches.
- **What lives where.** The bucket holds everything per event: the tap's segments and the log
  partitioned by block. The lake holds the working state's checkpoints and the published
  state; the box holds the working state itself and the serving tables, which are per fleet.
  Nothing on the box scales with the events of a day.
- **The tick.** Classification rides the ingest cycle (`daemon::tick`): every cycle runs one
  keyed tick. A day settles once every edge is past it by the settle delay, and the settled-day
  hook states its day-grain outputs.
- **Memory and disk are resources.** The working state is bounded by a share of the box's
  ceiling (`daemon::memory`); disk has a ladder of rungs at which intake is refused and
  merges never are (`daemon::pressure`), and a shed never gives up input the driver has not
  read (`daemon::shed`).

## Where the proof is

- **The pins.** `crates/pipeline/tests` run the fold and the keyed driver against the pinned
  engine over `fixtures/joins`, `fixtures/gait` and `fixtures/producer`; a golden asserts
  bytes, a guard asserts a law. `keyed_equivalence` holds the driver to what the
  classification pass published (`fixtures/keyed-parity/`), and `placement_corpus` holds
  placement to one global fold of the same records. `crates/analysis` carries the fold's unit
  tests.
- **The static audit.** `crates/cli/tests/static_audit.rs` ratchets what review cannot hold:
  every SQL-building site routes through the quoting helpers (counted per file), every
  analytic constant is on the re-derivation list, the network and the process spawn stay
  inside their crates, and the dependency allowlist holds the bright line.
- **The gates.** `scripts/preflight.sh` while working, `scripts/prepush.sh` before a push,
  both judged by exit code; CI runs the same battery plus parity, audit, deny and SBOM.

## Where to read next

- `docs/pipeline.md` — the keyed driver: its operators, edges and correction path.
- `docs/read-api.md` — the contract between the analyzer and the console.
- `docs/conformance.md` — the query inventory and every analytic constant's derivation.
- `docs/quickstart.md` — installing it.

Derivation operations hold the lineage's local `execution::Writer`, keyed by the lake's
identity (its store's location and its root); ingest commits the input log under the log's own
lease and needs no writer. A tick holds the writer from its intent through its apply. Input
progress is a durable commit position, with exact committed carrier/day membership;
timestamps do not order input. [The lake](docs/lake.md) describes the input log.

## Module ownership

The core registers (classes, placement, coverage, the roster) are the keyed driver's
operators: always on, never modules. Public modules are static registrations through
`pipeline::adapter`, run by the settled-day hook over each settled day, with the day's
coverage built from the summed coverage cells as their declared dependency. A module's rows
replace its day's; a replayed tick states them again under a fresh revision, so a replace
retires whatever a failed attempt wrote. The head records each enabled module's first settled
day, and `Executor::availability` reports the span a module's output is stated over.

The public day/window adapter boundary is proven by the separate `module-example` crate. The
[contributor guide](docs/contributing-a-module.md#current-extension-boundary) states the
fixed-schema, static-registration boundary. No general plugin framework is part of this
foundation.


## Process-scoped state

Four facts are properties of a process, and four registries hold them, each a `LazyLock` in
the module that owns the fact. They are the only process-global state in the library crates.

| registry | the fact it holds | why it is process-scoped |
|---|---|---|
| `lake::head::HELD` | the leases this process holds, by lease key and instance | one operation opens several heads over one document; they share one lease rather than each re-acquiring it |
| `lake::working::OPEN` | the working states open, by directory | RocksDB admits one handle per directory per process; a second open with other relations is refused |
| `pipeline::execution::WRITERS` | the lineages this process is mutating | the mutation capability is non-reentrant within a process; across processes the lineage's lease fences the writer |
| `objectstore::runtime` | the one object-store runtime | the S3 client is asynchronous and the rest of the analyzer is not; one runtime of two workers serves every handle |

A library user sees them as the refusals they produce: a second writer, a second working
state with other relations, a lease another instance holds.

## HTTP operation ownership

The [HTTP contributor guide](docs/contributing-an-http-endpoint.md) traces a complete operation.
`api::routes` declares paths and middleware; `extract` translates transport failures and checks
query names against OpenAPI; private `requests` constructors establish immutable operation
inputs before I/O. Resource families coordinate DAO reads and map typed wire responses.
`pagination` alone owns token binding, the private before/after revision read and completion
metadata. Malformed input is an error; unavailable pages are a separate typed outcome.
Authentication, bounded blocking admission, deadlines and SSE replay remain separate owners.
[The read API](docs/read-api.md) states the wire conventions.
