# Data lifecycle

What the analyzer keeps, for how long, and what survives each failure. [The lake](lake.md) maps
where each part lives; this document is about time and loss.

## 1. The stages

```
tap disk ──ship──▶ bucket ──ingest──▶ input log ──tick──▶ working state ──publish──▶ published state ──▶ serving
 (bounded ring)    (customer's)        (lake)              (box + lake)                (lake)                (ClickHouse)
```

- **Tap disk.** Each tap seals segments into a bounded ring and ships them. A segment the ring
  reclaims before it ships is declared, not silent: `segments_dropped` names the incarnation and
  the seq range, and the gap stays visible in `segment_seq`.
- **Bucket.** The customer's dated tree of sealed segments. The analyzer reads it and never writes
  or deletes under it; how long segments stay is the customer's lifecycle policy. A tick reads the
  events from these segments, through the segment cache.
- **Input log.** What arrived, per batch, under the lineage's root (`lake::ledger`). With the
  segments it names, it is the derivation's only input.
- **Working state.** The keyed operators' state: a `RocksDB` on the box, checkpointed to the lake.
- **Published state.** Every published table's days as keyed logs in the lake. The core reads its
  own output back from here.
- **Serving.** ClickHouse tables the read API queries. Each one is a projection of the input log or
  of the published state, so the serving store holds nothing that cannot be rebuilt.

## 2. Horizons

Every horizon is a knob with a floor; the floor guard refuses a value below it before any
migration applies it.

| what | knob | default | floor |
|---|---|---|---|
| input log and the served input tables | `--ledger-retention-days` | 14 days | 4 |
| keyed spine (the log partitioned by block) | `SpineLayout::retention_days` | 7 days | — |
| minute tier | `--minute-retention-days` | 7 days | 3 |
| per-instance detail | `--instance-detail-retention-days` | 14 days | 14 |
| fleet hour tier | `--hour-retention-days` | 365 days | 14 |
| daily rollups, markers, tags, block mass, pairs | `--rollup-retention-years` | 5 years | 1 |
| published state in the lake | `--rollup-retention-years` | 5 years | 1 |
| bucket segments | the customer's lifecycle policy | — | — |

The serving tables expire by `TTL … DELETE` at their next merge, and the daemon re-applies every
knob's horizon at start, so a changed knob takes effect without a migration. The input log retires
whole days past its horizon (`lake::ledger::Log::retire_before`). The published state in the lake
is what serving rebuilds from, so it keeps every day for the longest serving horizon, the
rollups' `--rollup-retention-years`, and maintenance retires the days before it
(`lake::state::StateLogs::retire_before`).

**Re-derivation reaches back as far as the input log and the segments both reach.**
`reset-derived` keeps the input log and deletes everything derived from it, and the next tick
derives afresh from what the log still holds, re-reading its segments from the bucket. A day the
log has retired is not derived again; its serving rows stand until their own TTL. So the
customer's segment retention should be at least the input horizon: a shorter lifecycle policy
narrows every replay to itself.

## 3. What survives each failure

| failure | what is lost | how it recovers |
|---|---|---|
| analyzer process restart | nothing | The working state reopens at its last unit of work, the journal names the last applied tick, and ingest resumes from the input log's committed position. |
| crash mid-tick | nothing | A tick's changes land in one unit of work, and readers see its edges only once the journal applies its intent. The next tick finishes or retires what the failed attempt wrote ([the pipeline](pipeline.md), section 4). |
| crash mid-ingest | nothing | A record object counts only once its position is committed under the writer's epoch; an orphan is never read, and the batch is ingested again. |
| analyzer box loss | nothing durable | A new box on the same lineage restores the newest working-state checkpoint and replays the input log from the position it names, re-reading those segments. Caches and serving rebuild from the lake. |
| a second writer on the lineage | nothing | The lease fences it: a commit under an older epoch is refused. |
| derived state damaged | nothing inside the input horizon | `reset-derived`, then derive afresh (§2). |
| tap node loss | segments not yet shipped, bounded by the ship interval | none — those records existed only there |
| shipping outage past the tap's disk cap | the segments the ring reclaimed | not recoverable, but declared (§1) |
| segments expired by the customer | nothing already derived | A replay or re-derivation that needs them cannot run (§2). |

## 4. Key rotation

The tap pseudonymises block identities under a fleet key, and each segment declares its
`key_epoch` and canary. A rotation is a declared discontinuity (contract spec 3.3): every join key
carries the epoch, so no join crosses a rotation, and there is no cross-epoch mapping.

Rotate the key in the deploy that restarts the **engines**. An engine restart clears the cache, so
every residency interval closes as post-clear and no block spans the cut. Rotating while engines
stay warm is supported; the blocks that span the boundary read as new identities.

Most of what compounds carries no block pseudonym: the class and band rollups, coverage, tags and
replication key on instance, class, band and day, so their trends cross a rotation unchanged.
`block_mass_daily` keys on `block_id`, so a block held across a rotation appears under two ids on
either side of the cut; each day's row is still that day's figure.
