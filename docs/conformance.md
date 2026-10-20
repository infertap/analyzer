# Conformance: clock domains and analytic constants

Two ledgers that hold the analyzer to what it claims: the rule for comparing instants across
clocks, and every number somebody chose that decides what a figure means or how the box
behaves, with what it bounds and when to re-derive it.

## 1. Clock domains

Two clocks reach the analyzer: the engine's, on every cache record (`at_ms` of a store, an
eviction, a clear), and the tap process's, on every lifecycle record (heartbeats, run start
and stop, segment opens). Both run on one host.

- **One sanctioned cross-clock comparison.** A heartbeat's `at_ms` meets a cache record's
  `at_ms` only widened by the skew allowance (`Params::delta_skew_ms`), never bare: a drop
  window's engine-domain extent is its producer-time bracket widened by the skew, and a store
  is tainted when its own `at_ms` falls inside it. The comparison asks whether the box agrees
  with itself, and the widening makes disagreement over-taint toward `indeterminate`, never
  toward a false `cold`.
- **Separate types for separate clocks.** Cache records are `analysis::events::Event` and
  lifecycle records `analysis::events::LifecycleEvent`, so a cross-domain comparison is never a
  field access away.
- **Lifecycle reads are clipped on the wall side.** A lifecycle fetch takes a wall-clock range
  already clipped by the caller (`LIFECYCLE_CLIP_MARGIN_MS`, or `EVIDENCE_CLIP_MARGIN_MS` for
  evidence that reaches emitted rows); the clip is a pre-filter, not a comparison.
- **Staleness is judged on the wall side only**, against the observation horizon, never
  against the reader's real now, per producer run (`analysis::staleness`).
- **Arrival stamps are feed-lag self-health only.** When a segment arrived is the analyzer's
  clock and never decides a class or a coverage figure.
- **Self-health measures the analyzer, never the fleet**, so no clock domain is crossed by it.

## 2. Analytic constants and the re-derivation list

Every `pub const` that decides a figure or a behaviour is listed here, and
`static_audit::every_analytic_constant_is_on_the_re_derivation_list` fails the build on one
that is not; a constant that is a unit, a layout token or a version string is on the audit's
`NON_ANALYTIC` list with its reason instead. Each entry's derivation is in the constant's doc
comment; this ledger says what it bounds and what would move it.

### The classifier's parameters

`analysis::Params` holds the numbers that decide what a classified figure means, and each is in
the discriminator, so a change re-derives the log under a new meaning
(`static_audit::every_params_field_is_on_the_re_derivation_list`).

- **`delta_skew_ms`** = 1 s. The cross-instance clock allowance: which events on different
  holders count as simultaneous, and how far drop windows widen. Re-derive from measured clock
  skew between engine and tap on a deployment's hosts.
- **`examined_history_ms`** = 1 h (`EXAMINED_HISTORY_MS`), and no longer: the bands end at the
  hour; `EXAMINED_HISTORY_DAYS` = 1 is it rounded up to the whole days the input floor counts. How far back classification looks before a store: `cold` means no prior close within
  it, so a shorter horizon undercounts ETR and post-clear. The frontier holds every (holder,
  block) closed within it, so it is the analyzer's memory. Re-derive from the distribution of
  gaps between a block's close and its next store, measured on a fleet.

### Analysis

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `CORRECTION_HORIZON_DAYS` | 1 | how late a record may arrive and still be corrected: a day stays open until every edge is this far past it; `CORRECTION_HORIZON_MS` is the same in milliseconds, and the input floor is built from it | a fleet's shipper backlogs run longer |
| `CONTEXT_MARGIN_DAYS` | 1 | the margin around the examined history; `EVIDENCE_CLIP_MARGIN_MS` and the retention floors are built from it | the examined history moves |
| `EVIDENCE_CLIP_MARGIN_MS` | 1 day | how far past a window evidence reaching emitted rows is fetched | `CONTEXT_MARGIN_DAYS` moves |
| `LIFECYCLE_CLIP_MARGIN_MS` | 7 days | the wall-side pre-filter on lifecycle fetches | a deployment's runs outlive it with no heartbeat in the window |
| `STALENESS_GRACE_MS` | 30 min | the allowance beyond the declared heartbeat bound before silence is `Unexplained`; also the settle delay | the tap's heartbeat cadence moves |
| `HULL_COMPACTION_BOUND` | 32 | same-class degradation entries on one scope before they coalesce to their hull | a ledger's size on a real incident |

### The pipeline

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `BLOCK_CAPACITY` | 65,536 | blocks a day's stored-mass summary monitors; exact below it, each figure within the day's mass over it above | a deployment's distinct blocks per day |
| `BLOCK_CUT` | 1,000 | blocks a day publishes | the console's ranking depth |
| `WHALE_CUT` | 1,000 | identities each settled day's whale register states; the day's totals cover every identity | the console's ranking depth |
| `EVICT_SAMPLE_MODULUS` | 1,024 | one engine id in this many is in the evict audit's sample; the audit keeps the latest store on each sampled id while it is open, up to the log's retention, and for the examined history after its evict | resident blocks per holder times holders |
| `SAMPLES_PER_TICK` | 4 | blocks a tick re-derives from the log for the sampled audit; each reads its examined history, one object an hour | the bucket's read cost or the tick's duration |

### The lake and the working state

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `STRIPES` | 16 | the identity-hash ranges the working state is keyed and compacted by | lanes or stripe size on a large fleet |
| `IO_WIDTH` | 8 | requests in flight from one call with several to make | the bucket's request rate and the box's memory for bodies |
| `LIST_MARGIN_HOURS` | 24 | how far behind the watermark a listing re-lists, the shipper's upload deadline plus skew | the shipper's deadline is derived |
| `TIER_MIN`, `TIER_MAX`, `TIER_RATIO` | 4, 8, 4 | the published state's size-tiered merge (SlateDB's defaults) | merge cost on a real state log |
| `DEFAULT_MEMORY_BYTES` | 768 MiB | the working state's block cache where no ceiling sets it | — (a ceiling sets it in a deployment) |
| `MEMTABLE_SHARE` | 0.25 | the memtables' share of that memory before writers stall | write stalls are measured |
| `BLOOM_BITS` | 10 | bits per key, about 1 % false positives | probe cost on a large state |
| `INGEST_BYTES` | 64 MiB | staged bytes past which a commit ingests sorted files | commit latency on a large tick |
| `BUILD_LANES` | 4 | stripes a large commit builds at once | the box's cores and memory |
| `METADATA_BLOCK_BYTES` | 4 KiB | one partition of a file's index or filter | probe cost |
| `BACKGROUND_JOBS` | 4 | flush and compaction threads | compaction debt |
| `WRITE_BATCH_BYTES` | 32 MiB | bytes per batch of a commit, retirement or drop | commit memory |
| `RETAINED` | 3 | checkpoints the head keeps named | restore duration |
| `GARBAGE_MIN_AGE_MS` | 10 min | how long an unnamed checkpoint file stays | the longest restore |
| `CHECKPOINT_BYTES`, `CHECKPOINT_INTERVAL_MS` | 2 GiB, 30 min | when the next checkpoint is due | replay time after a lost box |

### The object-storage port

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `RETRY_ATTEMPTS` | 10 | attempts per request (object_store's default) | the soak measures real bucket latency tails |
| `CONNECT_TIMEOUT` | 5 s | one connect | as above |
| `REQUEST_TIMEOUT` | 30 s | one attempt; must stay above the largest segment's GET on the slowest link | as above |
| `RETRY_BUDGET` | 1 min | one request across all attempts, so an outage is a decided error | as above |
| `RUNTIME_WORKERS` | 2 | the port's I/O threads | concurrency of the lake's writers and readers |

### Segments

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `STREAM_BATCH_ROWS` | 8,192 | rows per batch of the streaming decode, the decoded-memory bound | segment row width |

### The serving store

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `DEFAULT_LEDGER_RETENTION_DAYS` | 14 | how late a segment may arrive and still be read with full context; the served input facts' horizon | a harness run with a stalled shipper |
| `LEDGER_RETENTION_FLOOR_DAYS` | 4 | the floor that retention may not be lowered below: the examined history plus a day | the examined history moves |
| `DEFAULT_INSTANCE_DETAIL_RETENTION_DAYS` | 14 | the instance-keyed hourly tables | instance-page reach |
| `DEFAULT_MINUTE_RETENTION_DAYS` | 7 | every minute table, by bucket age | the incident zoom's reach |
| `DEFAULT_HOUR_RETENTION_DAYS` | 365 | the fleet-level hourly rollups | the incident mode's reach |
| `EXTERNAL_SORT_SPILL_BYTES`, `EXTERNAL_GROUP_BY_SPILL_BYTES` | 4 GiB | the engine's sort and group-by buffers before they spill | a read's peak memory on a large window |
| `MAX_SERVER_MEMORY_RATIO` | 0.9 | the engine's memory ceiling as a share of the process ceiling: a backstop on the sum | the engine refuses while the analyzer holds the memory |
| `SYSTEM_LOG_TTL_DAYS` | 14 | the engine's own bookkeeping tables | — |
| `AUTO_GRAIN_HOUR_POINT_BUDGET` | 200 | hourly points an `auto` read serves before falling to daily | the chart's render budget |
| `AUTO_GRAIN_MINUTE_POINT_BUDGET` | 240 | minute points an `auto` read serves, a 4-hour zoom | the incident window's size |
| `HISTOGRAM_BANDS` | k2, k34, k58, k9 | the copies histogram's fixed bands | the console's band design |

### The read API

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `PAGE_LIMIT_MAX` | 200 | rows per page, from response size (~339 B a holders row), never a fleet size | the wire shape grows |
| `PLACEMENT_PAIRS_DEFAULT`, `PLACEMENT_PAIRS_MAX` | 2,000, 10,000 | pairs a placement read returns; pairs are quadratic in holders | fleet size |
| `WHALES_DEFAULT`, `WHALES_MAX` | 8, 100 | identities a whale read returns | the panel's design |

### The daemon and its self-health

| constant | value | bounds | re-derive when |
|---|---|---|---|
| `DEFAULT_INGEST_INTERVAL` | 1 min | the ingest cycle and the tick (`--ingest-interval`) | a deployment's segment rotation |
| `DEFAULT_MAINTAIN_INTERVAL` | 1 h | the input log's upkeep (`--maintain-interval`) | — |
| `DEFAULT_HEALTH_INTERVAL` | 1 min | the self-health sample | alerting resolution |
| `STALE_SAMPLE_INTERVALS` | 3 | silent health intervals before the sampler is called dead | — |
| `RECLAIM_COOLDOWN` | 10 min | between forced merges | merge latency on a full disk |
| `CLOCK_SKEW_MAX_SECONDS` | 60 | engine clock against host clock before preflight refuses | `delta_skew_ms` moves |
| `DISK_FREE_FRACTION_WARN`, `DISK_FREE_FRACTION_CRITICAL` | 0.20, 0.10 | the disk check's lines; critical is "refuse rather than wedge" | — |
| `DEAD_PART_RATIO_WARN` | 0.5 | dead bytes over live before reclamation is behind | — |
| `PARTS_PER_PARTITION_WARN`, `PARTS_PER_PARTITION_CRITICAL` | 1,000, 3,000 | the engine's own `parts_to_delay_insert` and `parts_to_throw_insert` | the engine pin moves |
| `RAW_TTL_OVERDUE_FACTOR` | 1.25 | how far past its horizon the oldest input row may sit before TTL enforcement reads broken | merge scheduling slack |
| `ORPHAN_TEMP_AGE_WARN_SECONDS` | 1 day | when a temp file is an orphan, not work in flight | — |
| `TICK_LAG_WARN_CADENCES` | 2 | tick lag, in ingest cadences, before the loop reads behind | — |
| `ENVELOPE_WINDOW_CYCLES` | 10 | ingest cycles the envelope averages before it judges | — |
| `ENVELOPE_UTILISATION_WARN` | 0.30 | the share of its own ingest capacity a deployment may use before the envelope warns | — |

## 3. Stated differences from the reference, on cases the corpus does not assert

The conformance corpora pin what the producer EMITS. Where this reader takes a position on an
input the producer never emits, the corpus is silent by design, and the position is written
here.

- **An empty-string `content_id` reads as ABSENT here; the reference treats it as PRESENT.** A
  record carrying `content_id: ""` falls back to `block_id` for its identity, where the
  reference would take the empty string as the identity itself. The delivery corpus does not
  assert the case: the producer emits absence rather than emptiness. If a producer ever did,
  every such record would collapse to ONE identity under the reference and keep its own engine
  id here. Ours is the safer error: a shared empty identity would manufacture duplication
  across unrelated blocks, the direction this metric must never fail in. Revisit only if the
  spec asserts the case.
