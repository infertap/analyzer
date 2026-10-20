# The keyed pipeline

How the analyzer turns the tap's segments into the cells the console reads. `ARCHITECTURE.md`
is the map of the crates; [the lake](lake.md) describes what the pipeline keeps in the bucket;
this is the pipeline itself (`crates/pipeline/src/keyed`).

Every figure the console renders is a sum, a count or a top-N over time buckets, cut by class,
band, workload, model and holder. So the pipeline keeps no restatable event grain: it folds each
record once, on lanes keyed by what the fold needs, into cells that sum, and corrects a late
record by re-deriving the identities it reaches.

## 1. The shape

A durable log, operators keyed by what they fold with local state, and summed cells as output:
the shape Flink and Kafka Streams run, inside the one binary.

```
segments (bucket) ──► Source ──► router by block hash ──► lanes, each keyed by block ──► cells ─► serving
  per producer run,   ordered per     (a holder's order      frontier: (holder, block) → open
  gap-free seq        run             survives in each lane)  interval, last close and its reason
                                                             placement: block → open holders
```

- **The log** is the tap's segments, committed to the input log as they arrive. Each producer
  run numbers its segments without gaps (`segment_seq`), and one run covers one engine, so each
  holder's records are delivered in the engine's own order.
- **One key: the block.** The classifier's unit is the identity group, one block with every
  holder's intervals for it: `re_admit` reads this holder's open interval, `RE` another
  holder's open interval over the skew-widened instant, `ETR` and `post_clear` the reason of
  this holder's last close, `cold` and `indeterminate` the absence of one and the declared drop
  windows. All of it lives under the block, so both states are keyed by block and nothing
  crosses a lane per event. Routing by block hash keeps each holder's records in order within a
  lane.
- **Repeat compute** classifies a store once the watermark passes its instant plus the skew
  allowance (`delta_skew_ms`, 1 s by default), since `RE` needs the other holders' state that
  far ahead. What waits for the watermark is a reference to its batch, never its records: the
  driver keeps each delivered batch whose newest record is past the classified edge and reads
  its records again on the tick that classifies them, so memory past the watermark is a few
  batch references however far the watermark lags. It adds to the recompute cells (minute ×
  class × band × workload × model × holder) and updates the frontier. Nothing is classified
  before its predecessors, so nothing is cancelled. Beside the records' own `clear` and
  `holder_reset`, a slice's scope closers hold the silences the completeness model draws in it
  (`docs/classification-semantics.md` §3), so a holder that went unobserved stops holding its
  blocks. A tick folds from the classified edge whether or not records wait, so an idle stretch
  still crosses its silences and its day boundaries.
- **Placement** is the duplication fold on the same lanes, routed by the same hash, so an
  identity's placement seeds sit beside its frontiers: the open holders of each block, the
  replication and pair-day cells, and the whale register (each settled day's heaviest
  duplicated identities). It folds up to the watermark and emits half the skew behind it, since
  a closer just past an instant can shorten the residency before it. **A tick costs what moved,
  not what is resident.** A block whose holders hold still adds the same token-time per
  millisecond to the same cells (`dup_cells::Rates`), so a lane keeps the still blocks' rates
  summed and states their cells for a window from the sum, one addition per key and minute. A
  tick folds one by one only the blocks with records in it, those a `clear` or `holder_reset`
  reaches, and those whose holders still change, each only up to where its holders stop changing
  inside the window, joining the sum there; the delay is what makes the sum exact, since a
  record's effect starts no earlier than the window it is folded in. Each block accrues its own
  whale day totals in its state, its rates covering the stretch it holds still.
- **Coverage** states each source's covered and stated milliseconds per minute and per day, as
  cells that sum. A stretch is stated once it is settled: a source's tail after its last
  heartbeat is uncertain until a later heartbeat attests it, so a tick states coverage up to the
  earliest tail among the sources heard from within the staleness grace. A source is stated
  from the start of the UTC day of its first evidence, read from the segment arrivals on their
  own clock (`keyed::CoverageFeed`), so the stretch before its first heartbeat is covered where
  an `agent_start` attests it. Coverage is what the producer observed, not when its segments
  arrived: a source whose arrivals reach behind the stated edge (a shipper backlog) is stated
  again from its heartbeats over the days still open. Only a source that produced no heartbeat
  stays uncovered past the grace.
- **The roster** is who spoke, one run per producer incarnation, read from every commit on its
  own clock and never gated by the watermark. A run's posture (active, or inactive past the
  staleness grace) is judged against the real now when it is read.
- **Blocks by stored mass.** Each day's heaviest blocks come from a weighted Space-Saving
  summary fed by the stores repeat compute classifies: at most `capacity` blocks a day, so
  memory does not grow with the day. A block's count is an upper bound and its count less its
  error a lower bound; the top `cut` are published, each marked certain where its lower bound
  clears the upper bound of the first block below the cut. A day with fewer distinct blocks
  than the capacity is exact. Stored mass is its own register, never joined with placement's
  held mass.
- **Lanes.** Blocks share nothing per event, so block-hash lanes across the cores need no
  coordination.

## 2. The seams

Each is a trait or a parameter, so a second implementation needs no change to the operators.

| Seam | Now | Contract |
|---|---|---|
| `Source` | `SegmentSource` over the input log, its records read from the tap's segments (`FillReader`) or the log partitioned by block (`SpineReader`) | ordered per-holder delivery with sequence numbers; a position to checkpoint and resume from; the policy for a missing segment. The operators share one `Intake`: the position and the batch references either has yet to finish, each batch read once per tick for both |
| the fold's state store | RAM on the per-event path; `RocksDB` for checkpoints | the pure fold code does not assume `RocksDB` |
| emission | watermarked (`keyed::repeat::Emission`): emit only what the provable watermark has settled | each operator resolves it against the delay its own output needs: repeat compute the skew, placement half of it |
| the cell sink | ClickHouse, through the serving port | cells are monoids keyed by their dimensions |

**A missing segment.** `SegmentSource` holds a run's delivery at a gap until the segment
arrives, a drop is declared, or the run passes the roster's staleness grace; then the run's
later segments are delivered, and the gap's records, if they come, take the correction path
(section 5).

**The watermark** is the provable one where any run has delivered a sequenced segment: the
minimum over live runs of each run's last heartbeat, the maximum over stopped ones when none is
live. A log whose carriers claim no sequence (a corpus, a harness) is settled through its newest
record.

## 3. The frontier

The one large state: (holder, block) → open interval, or last close and its reason. An open
interval is held for as long as it is open, however long that is; a close is held for the
examined history (an hour) and then dropped. It is exact: a store's class and gap are what the
whole history says, nothing is estimated, and a block that comes back after its close left the
history reads as cold, which understates. A residency counts from an observed store: an evict
that closes nothing states a close and no residency.

Every fact is a function of its own block's records and the frontier carried to the day. At each
UTC day boundary repeat compute writes every key's frontier as it stands there to the log
partitioned by block (section 6), so a re-fold of one block starts from the boundary before its
window and states what the tick stated, a residency opened days before included.

Its size is the fleet's distinct (holder, block) pairs closed within the hour, and its open
residencies, which the fleet's cache capacity bounds; the rig measures both. An hour is the
width at which every band an operator acts on stays exact. A longer reach for the offload
question (a block back after hours) is a sample of identities, chosen by their hash so each fact
stays a function of its own block, should a fleet show it matters.

## 4. The tick

`Executor::advance` runs one tick on the ingest cadence, under the journal's intent. Each tick's
cells, every family (class and band, placement, coverage, the roster's rows, the blocks by
stored mass, the tag cells and declarations, and the restatements), are one **emission batch**,
stated exactly once:

1. **Intent.** The journal records the tick's intent under a new revision, its publication.
   Reads are withheld while an intent is unapplied.
2. **Fold.** The operators fold the tick's records, and coverage is stated to its settled edge.
   The partial cells add to the **held cells**: the whole value of every cell of the days still
   open to change, kept in the working state beside the operators' state.
3. **Unit.** The working state writes, in one unit of work, what the tick changed: frontiers,
   tier, seeds, held cells, the head (the tick, its publication and each operator's edges) and
   the **outbox** (the keys of the held cells the tick changed).
4. **Publish.** After the unit ends, the outbox's cells are published whole under the tick's
   publication: the lake's state log, then serving. The settled-day hook runs for the days the
   batch settled (section 7).
5. **Apply.** The journal marks the intent applied, with the edges readers may now see.

A tick that finds an intent unapplied whose publication the working state names publishes that
outbox first, under that publication, and its days' hook under a fresh revision, so a replace
retires whatever the failed attempt wrote; then it applies that intent and records its own.

## 5. Recovery and corrections

**Recovery.** Each operator's state names the input position it holds, so recovery restores the
state and replays the log from there. A clean stop closes the local state so a restart on the box
reopens it without a restore. A unit that does not end rolls back to the newest checkpoint; the
head then names an older tick than memory holds, and the driver reloads from the store and
replays the log from the head's intake. A crash after the unit ends leaves the intent unapplied:
the next start publishes the head's outbox again under the head's publication (whole values, so
a second copy is the first), then applies it. The held cells are restored with the operators'
state, never read back from serving, so a replay never adds a tick to cells that already hold it.

**A rollback past published ticks restates its open days.** A rollback can return the state to a
checkpoint older than ticks whose cells serving already holds as applied. The replay re-derives
those cells, but a key only a lost tick published is one the replay never names, so a keyed
change cannot retract it. When the store names an older publication than the journal's newest
applied one, the replaying tick therefore replaces every open day of every day-held family
whole (`restate_open_days`), and serving drops whatever else those days held.

**The correction path.** A record that arrives behind an operator's edge (a silent run past the
staleness grace, a shipper backlog) re-derives each identity it reaches from the log partitioned
by block over the span it touches, once without the late records and once with them
(`refold_block`). The held class and band cells take the difference, so the correction rides the
next emission batch like any tick's; the re-derivation's end state replaces the identity's
frontiers, and the late stores' mass joins the blocks by stored mass. Placement does the same on
its own late set (`refold_placement`), from the residencies open in the frontiers carried at the
window's day start, under the silences of the model before the tick's batches and of the model
now. A late scope closer touches every block its holder held
and so no one identity's re-derivation: late closers are counted, not corrected.

**Late lifecycle evidence re-taints.** A late segment's heartbeat span opens at its source's
previous heartbeat, so it can open, or split, a drop window behind the classified edge. Each
tick compares the drop windows of the model as the input log stood before its batches with
the model now; where they differ, the stores the changed window reaches (its own source's
through the examined history after it, every other source's within the skew band) are read
from their logged segments, and each identity is re-derived under both models, the held cells
taking the difference (`retaint`). The silences are compared the same way: where one moved,
every block its instance held is re-derived from where it was drawn, its classes and its
placement both. Every correction's old side is re-derived under the model
before the tick's batches, since that is what the emitted cells were derived under. Coverage
and the pair basis reach back the same way: an arrival restates its source from its previous
heartbeat, where the span it closes opens.

**A restated window says so.** Every late arrival counts in `restated_hourly`, in the hour of its
own instant and under the family it restated: corrected, too late, or a scope closer left
uncorrected. A late record of a day no longer open to change (behind the edges by more than the
examined history, so its cells are no longer held) is counted and not corrected, because a
correction there would publish a difference as the cell's whole value.

**What the pipeline does not re-price.** A block whose stored token mass rises after placement
has emitted it keeps the mass it was emitted with: duplication is a lower bound there
(`docs/classification-semantics.md`).

## 6. Drills

Drills read the event grain from the **log partitioned by block**, which the driver writes as it
reads: each batch's records once, on the tick that first reads the batch, late ones included. A
tick writes one object per hour its records fall in, every partition a compressed section indexed
by a footer; once both operators have passed an hour, maintenance compacts it into one object per
partition, so a sealed hour is one read per block. Compaction reads a run of consecutive sections
at a time, one ranged read of each tick object a run and at most a fixed budget of bytes, so it
holds a bounded share of the hour whatever the fleet's size; an hour keeps the partition count
it was written under, and each compaction names its objects anew, so none a rollback can reach is
written over. The scope closers are a partition of their own, since every block's classification
reads them. The partitions per hour and the days kept
are knobs (`SpineLayout`). The working state's manifest names each hour's live objects inside
the operators' unit of work, so a rolled-back tick's objects are named nowhere and never read.

Each UTC day boundary's frontiers are carry sections of the boundary's first hour, one per
partition, written a lane at a time. A correction that re-derives a block across a boundary
writes the block's carry again in a later object, the latest naming a key winning; compaction
keeps only the latest.

A block drill re-derives the block (`refold_block`): from the frontiers carried at the window's
day start, its history from there and the scope closers, folded by the same operator. The holder
timeline and the evidence by holder read each holder's **ring**: its newest stores as repeat
compute classified them (`RING_CAPACITY`, 100), restated whenever a correction restates a store,
so the ring never shows a class the cells no longer hold. The ring says how far back it reaches.

## 7. Settled days and modules

A day is **settled** once every edge (repeat compute's, placement's, coverage's) is past it by the
settle delay (the staleness grace); a correction that restates a settled day hands it to the hook
again. The settled-day hook (`keyed::Hook`) states the day-grain outputs:

- the **generation seams** (`markers`), derived from the head's generations;
- the **whale register** (`whale_days`): one pass over the blocks' accrued day totals publishes
  the day's top `WHALE_CUT` by duplicated token-time and the day's exact totals over every block,
  the cut's floor among them, so a read over several days can bound what a block left out of a
  day's cut held;
- each enabled **public module**'s output, reconstructed with the day's coverage from the summed
  coverage cells, so a module's dependency agrees with the published `coverage_daily`. Its rows
  replace the day's, so a second statement is the first. The head records each enabled module's
  span, from the first day that settled while it was enabled: a day settled before is never
  claimed for it. `reset-derived` and a fresh derivation are the backfill.

Coverage, the roster and the tags never wait for a day to settle.

## 8. Tags

A declared input: identity X carried the opaque label T from t1 to t2, stated by a declarer in its
own sealed segments. Ingest records each `tag_interval` statement in the input log; the analyzer
stores the intervals and groups by them, and never interprets a label.

- **On repeat compute's clock.** The tag feed reads every commit from its own position before
  repeat compute classifies, so a store is attributed against every statement committed by then.
  A declaration's interval is its statement with the greatest `at_ms`.
- **Attribution, conservative across clocks.** A store counts toward an interval only when its
  instant, widened by the skew allowance on both sides, lies wholly inside it; a store crossing
  either end counts toward neither and is counted as ambiguous.
- **The cells**, per `(tag, identity, minute)`: the stores attributed, their tokens (prefill),
  the recompute register's tokens among them (refetch: `ETR` and `post_clear`), the
  indeterminate tokens apart, and the ambiguous stores. The served intervals give each
  identity's exposure in identity-minutes.
- **Late statements and late records.** A statement changing an interval behind the classified
  edge re-derives the identity's stores over the span and moves them to the new interval; a late
  record's correction restates its tag cells with the rest.

## 9. Maintenance, meanings and a fresh derivation

**Maintenance** runs between ticks on its own cadence (`--maintain-interval`): every hour both
operators have passed compacts, and the hours older than the log's retention retire. A
compacted hour's tick objects are deleted only once a checkpoint holds the compaction: a
rollback to an older checkpoint returns a manifest that still names them.

**Meanings.** The driver classifies under one meaning (the semantics version and the parameters'
discriminator), which the head records. A start under another meaning begins a fresh keyed state
under it and re-derives from the log within retention; the head keeps each meaning's classified
day range, and the generation seams are stated from those ranges. Figures under two meanings are
never summed.

**A fresh derivation.** `infertap-analyzer reset-derived` deletes the derived state and keeps the
input log, the journal's lease and the served input tables; the next start derives afresh from
the log within input retention. Until a tick states the edges, the daemon treats every day the
log holds as unread, and discovery holds the horizon wide enough to read it.

## 10. What holds it

`keyed_equivalence` holds the driver to what the earlier classification pass published over every
gait fixture (`fixtures/keyed-parity/`); `placement_corpus` holds placement to one global fold of
the same records; `correctness`, the API goldens and the conformance corpora run through the
keyed driver.

**The sampled audit** holds the running deployment to the same claim `keyed_refold` holds a test
run to. Each tick picks a few of the blocks it classified (`SAMPLES_PER_TICK`, by a hash of the
block and the classified edge), re-derives each from the log over the tick's window and compares
the stores' class, gap and tokens with what the tick stated. A block a correction path re-derived
in the same tick is not sampled. While every sample agrees, the audit edge (`audited_to_ms` on
the stamp) follows the classified edge. The first disagreement is logged with its block and
store, the `classification_audit` check goes critical, and the edge holds there: a disagreement
is a defect in the keyed path, and what follows it is not audited by later agreement. A fresh
derivation clears it. Every engine-backed test tick asserts that its audit agreed.

**The evict audit** checks the producer, not the keyed path: an evict must carry the identity
its latest prior store on the same holder and engine id carried. The log is partitioned by the
resolved identity, so a wrongly resolved evict lands apart from its store there; the audit
therefore folds each tick's records as they arrive, on the engine ids whose hash
`EVICT_SAMPLE_MODULUS` divides, keeping the latest store on each for the examined history and in
memory only. The grades (`analyzer_analysis::differential` names the classes) go to the
`evict_resolution` check, critical on a mismatch and a warning on an underresolved evict, and to
the `infertap_analyzer_evict_audit_*` counters; the log names the instances.
