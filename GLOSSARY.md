# Glossary

The words this codebase uses for its own concepts, in plain terms, with the place each one
lives. Comments in the source use these words without explaining them; this is where they
resolve. `ARCHITECTURE.md` is the map; this is the vocabulary.

## The data

**Tap.** The agent beside each inference engine. It records KV-cache events (stores,
evictions, clears, lifecycle) and ships them as sealed segments to a bucket.

**Segment.** One sealed file the tap ships: Parquet in the shipped form, JSON Lines in the
write-ahead form. `crates/segments` decodes it. A **batch** is one ingested segment, named
by its content hash, so a replay of the same bytes is a no-op.

**Event.** One typed record after decode: a store, an eviction, a clear, a holder reset, or a
lifecycle fact. The **Ordered Event IR** (`analysis::events`) is the sequence of events for a
window, grouped by identity, that every derivation folds over.

**Identity.** The key a fold works on: the block id together with its epoch, held by a
**holder** (an instance, its data-parallel rank and its group index). An **identity
group** is every event of one identity in order. The fold takes one group at a time.

**Meaning pair.** The two stamps every derived row carries: the semantics version (which
interpretation rules produced it) and the discriminator (which parameters). Rows with
different meaning pairs are never summed together.

**Fleet.** Everything one analyzer watches: the instances the taps report from. The
**roster** is the fleet's instances and their incarnations, derived from the lifecycle
stream.

## The derivation

**The fold.** The pure function from events to figures, in `crates/analysis`. It depends
on nothing and holds no store. Residency intervals, the six-class classification of every
store, coverage, the residual, staleness and the block tree are all folds.

**The fence.** The one criterion every emitted figure obeys: a stamped dimension is an
event fact if and only if it is a pure function of one identity's own event prefix at the
moment of emission. What passes the fence is data before any merge, so downstream is a sum
and a late segment repairs only its own identity. What fails it (anything read from the
fleet's state outside one identity) travels the second lane, under placement's watermark and
its correction path. `ARCHITECTURE.md` states it in full.

**Classification.** Each store falls in one of six classes by what its identity's own
history says about it (`analysis::classify`). The **register** is the set of stores that
count toward a figure.

**Residency interval.** The span during which one holder held one identity, built from the
events (`analysis::intervals`). Coverage, classification and the residual all read
intervals, never raw events.

**Residual.** The prefix-semantic join (`analysis::residual`): what remains duplicated once
co-residency is judged by verified prefix structure rather than by bag-of-blocks equality.

**Coverage.** How much of a window the analyzer actually observed, stated beside every
figure. No figure leaves without it (`analysis::completeness`).

**Degradation ledger.** The record of what the reader could not trust and why (a gap, a
truncated tail, a producer restart), held against the figures it affects
(`analysis::degradation`).

**The differential audit.** Evict resolution re-derived from the raw stream and diffed
against what the tap emitted (`analysis::differential`, `pipeline::read::audit`). The
library computes it; no cadence runs it.

## The keyed pipeline

**Tick.** One step of the keyed driver on the ingest cadence (`daemon::tick`,
`pipeline::Executor::advance`): read what the input log committed, advance every operator,
save their state, publish the cells that moved, and apply the journal's intent.

**Operator.** One derivation the driver advances: repeat compute (the classes and bands),
placement (duplication), coverage, the roster and the tag cells (`pipeline::keyed`).

**Lane.** One of an operator's partitions, chosen by the block's hash; each lane folds its
identities on a thread of its own. Scope closers, which name no identity, reach every lane.

**Watermark and edges.** The watermark is how far the input log is proven complete. Each
operator's edge is how far it has read and emitted: the classified, emitted and covered edges,
which the stamp states.

**Frontier.** The live cache state per identity: its newest close and its open state, held in
the working state, with a long tier of filters per hour past the exact frontier
(`keyed::tier`). The **seeds** are placement's per-identity state.

**Batch.** The cells a tick moved, every family, published under one publication. An unapplied
batch is published again by the next tick.

**Correction path.** A record that arrives behind an operator's edge re-derives the identities
it reaches from the log partitioned by block, once without it and once with it; the held cells
take the difference.

**Settled day.** A day every edge has passed by the settle delay. The settled-day hook
(`keyed::Hook`) states its day-grain outputs: the generation markers and the public modules.

**Ring.** A holder's newest stores as repeat compute classified them, kept in the working state
and read by the holder drills (`keyed::ring`).

## The storage shape

**Grain.** The resolution of a figure over time: `minute`, `hour`, `day`. One vocabulary on
every endpoint; the **window** grammar and its **anchor** (wall now) are resolved once by the HTTP request constructor
in `crates/api/src/requests.rs`; the DAO selects a serving grain from the resolved range.

**The grain ladder.** The storage design: one grain vocabulary, one window grammar,
retention tiered by grain with every horizon a knob. `ARCHITECTURE.md` walks it.

**The lake.** The lineage's objects in the bucket: the input log, the journal, the log
partitioned by block, the working state's checkpoints and the published state. Nothing on
the box scales with the events of a day.

**Log partitioned by block.** The input's records by block, hourly, written as the driver
first reads each batch and compacted once both operators have passed the hour
(`keyed::spine`). Drills and the correction path re-derive from it.

**Box.** A standalone deployment with its serving database. Serving topology and Rust
execution ownership are separate; a distributed ClickHouse cluster does not distribute
derivation work.

**Input position.** A durable, monotonic commit-cycle sequence within the database owner.
Only committed carrier membership counts; wall time serves retention and observation.
It is separate from event-time progress and output revision.

**Edges.** The driver's progress through event time, per operator, as the stamp states it.
They are distinct from the input position and from a publication's revision.

## The engine

**Engine.** The serving store: the pinned ClickHouse this process owns, one connection and
one runtime per `Engine` value (`crates/serving-clickhouse`). Raw events never enter it; it
holds published cells and input tables and answers reads. The **bright line**: `crates/analysis` does not depend on
it, and the dependency allowlist pins that.

**Migration.** The schema, as the ordered SQL files under `migrations/`.

**Retention knob.** A configured horizon per grain and per tier; the daemon may shed toward
a floor under disk pressure and never below it (`daemon::shed`, `daemon::pressure`).

## The proof

**Pin.** A test that holds a law: a property, an invariant, or a byte-equality golden over
a fixture corpus. The pipeline's pins in `crates/pipeline/tests` run against the pinned
engine.

**Golden.** A pinned expected output, compared byte for byte. Changing one is a deliberate
re-bless by a maintainer, never a side effect.

**Ratchet.** A static check that only tightens: the count of SQL-building sites per file,
the analytic constants on the re-derivation list, the dependency allowlist
(`crates/cli/tests/static_audit.rs`).

**Corpus.** A fixture set with known answers: `fixtures/joins` (interpretation rules),
`fixtures/gait` (slice against whole day), `fixtures/producer` (the streamed producer).

**Rig.** The measurement fleet the analyzer is measured against: real taps, a real bucket,
one box. Its numbers are the behaviour baseline the pins hold.

## Execution and recovery

**Execution namespace.** A stable source ID and fleet, with one logical partition: the whole
fleet. One namespace owns one serving database; the database binding and the object keys
enforce isolation independently of workers and cache paths (`pipeline::execution`).

**Lineage.** One namespace's derivation: its input log, journal, working state and published
state live under the lineage's root in the bucket. A data directory names its lineage.

**Lease.** The lineage's writer lease. A commit stamps the lease's epoch into the journal's
head, so a writer that lost the lease cannot commit; within one process, `execution::Writer`
keeps two operations from mutating a lineage at once.

**Revision.** The journal's monotonic clock for publications. Each tick's batch goes out
under one; a replay of an unapplied batch publishes it again under a fresh one, so a replace
retires whatever the failed attempt wrote. It is distinct from the semantics generation.

**Module span.** The days a public module has stated: from the first day that settled while
it was enabled through the newest (`Executor::availability`). A module enabled later states
only the days that settle after; `reset-derived` and a fresh derivation are the backfill.

**Initialization.** What a module needs before it can state an output day: the output window
alone, the examined history before it, or its prefix state (`analysis::module::Initialization`).
