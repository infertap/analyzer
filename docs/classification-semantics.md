# Classification semantics, `js-7`

The rules by which the analyzer turns cache records into figures. The input contract
(`contract/inference-events`, `spec/kv-cache-v2.md`) defines the records; this document
defines what the analyzer concludes from them. Every rule cites the code that implements it.
Where this text and the code disagree, the code is what runs, and the disagreement is a defect
in one of them.

Key words MUST, MUST NOT and MAY are normative in uppercase.

## 1. Versioning

- The semantics version is `SEMANTICS_VERSION = "js-7"` (`crates/analysis/src/lib.rs`).
  A change to any rule below that can change a stored figure MUST change it.
- The discriminator is a token over the parameters that can change stored figures
  (`Discriminator::of`, `lib.rs`). Today these are `delta_skew_ms` and
  `examined_history_ms` (§10).
- Every stored row carries the pair (semantics version, discriminator). Figures under
  different pairs MUST NOT be summed.

## 2. Records, identity and holder

**Kinds** (`events.rs`, `Kind`): `store` opens a residency; `evict`, `clear` and
`holder_reset` close residencies.

**Own copies only** (`events.rs`, `own_copy`). A record enters the substrate only if its
`locality` is absent or `LOCAL`. An unrecognized value is not own. A remote copy MUST NOT be
read as residency on the publishing holder.

**Identity** (`Event::identity`, `events.rs`): `content_id` where present, otherwise
`block_id`. A record without `block_id` has no identity and does not enter the substrate.

**Holder** (`Holder`, `intervals.rs`): `(instance, dp_rank, group_idx)`, one physical cache. On
stores and evicts an absent `dp_rank` or `group_idx` reads as 0 (`Event::holder_dims`).

**Interval key** (`group_by_interval_key`, `events.rs`):
`(instance, dp_rank, group_idx, identity, key_epoch, tier, backend_id)`.

- `key_epoch` is part of the key. No residency, class or join crosses an epoch.
- `tier` is part of the key but not of the holder: the same block on two tiers of one
  holder is two residencies and one cache (§3, §8). An absent tier is its own tier.

## 3. Residency intervals

Per interval key, events are ordered by time, closes before stores at the same instant
(`Event::close_first_rank`). Then (`intervals.rs` module note):

- A `store` with no open interval opens one. A `store` over an open interval is a re-admit;
  the interval stays open.
- An `evict` closes the open interval with reason `evict`. With none open it makes a
  left-open interval `(−∞, t)`.
- A `clear` closes every open interval its declared scope covers, with reason `clear`. With
  none open it does nothing.
- A `holder_reset` closes every open interval of the holder's process, with reason `reset`.
- An interval still open at the window's end is right-open; its duration is censored there.

**Scope of a closer** (`scope_covers`, `events.rs`):

- A `clear` covers keys of the same instance whose `dp_rank`, `group_idx` and `tier` match
  the values it names. A dimension the clear does not name is global over that dimension.
  `backend_id` is not a scope dimension.
- A `holder_reset` covers every key of the instance that matches its `dp_rank` (or every
  rank if unnamed), over all groups and all tiers.
- A **silence** covers every key of the instance. It is not a record: the analyzer draws it
  where the instance's heartbeats stopped, at the liveness bound (`silence_after_ms`, at least
  the 30-minute staleness grace) past its last heartbeat. The bound past a segment's last
  heartbeat is a silence when the instance's next segment, of any run, starts later; past its
  last segment's, once the log read reaches it. Its intervals close with reason `silent`: the
  cache went unobserved, so a store elsewhere afterwards is not `RE` against them. A segment that
  lands inside a silence after it was drawn moves it, and every block the instance held is
  re-derived from where it was drawn.

**Workload class of an interval**: the class the opening store carried. An absent class is
the bucket `undeclared` (`emitted::UNDECLARED`), never a null.

## 4. Store classification

Every store is classified into exactly one of six classes (`classify.rs`, `Class`), in this
precedence (`classify_group_core`, `classify.rs`):

1. **`re_admit`**: the store falls inside an interval its own key already holds open, or
   it is a second or later store at the instant an interval opens. Positive evidence;
   never tainted.
2. **`RE`** (resident elsewhere): another holder has an interval for the same identity and
   epoch that opened at or before `t − δ` and closes after `t + δ`, where
   `δ = delta_skew_ms` (`BlockIndex::held_elsewhere`). A different tier on the same holder is
   not another holder. The whole widened instant MUST be covered.
3. **By the most recent prior close on the store's own key** at or before `t`
   (`KeyIndex::prior_close`), decided by its reason alone, with no gap limit:
   - `evict` → **`ETR`** (evicted, then refetched);
   - `clear` → **`post_clear`**;
   - `reset` → **`cold`**, decided by evidence;
   - `silent` → **`indeterminate`**: the block may have stayed or left while nothing was heard.
   The gap `t − close` is recorded as `gap_ms` for these stores.
4. **No prior close** within the examined history:
   - **`indeterminate`** if any taint source (§5) applies;
   - **`cold`** otherwise.

**What the examined history bounds** (`Params::examined_history_ms`): the prior-close lookback
and the taint lookback. It does not bound an open interval: a residency whose opening store was
observed holds the block for as long as it stays open, however long before the store it opened.
An interval whose open was not observed (an evict that closed nothing) is evidence of the close
only. Every error a finite horizon makes lowers a waste figure: a close outside the horizon reads
as `cold`.

**Registers** (`classify.rs`, `Class::RECOMPUTE`):

- `eviction_waste_tokens = Σ n_tokens` over `ETR` stores; `post_clear_tokens = Σ n_tokens`
  over `post_clear` stores. Together they are the recompute register.
- An absent `n_tokens` counts as 0, so the registers are lower bounds.

## 5. Drop windows and taint

**Drop window** (`UncertaintyModel::drop_windows`, `completeness.rs`): the span between two consecutive heartbeats of
one source whose cumulative `dropped` counter rose, or whose agent-level `oversized` counter
rose. `oversized` is replicated onto every source's spans.

**Taint** of an absence decision for a store at `t` on instance `i` (`classify_group_core`,
`UncertaintyModel`'s taint checks in `completeness.rs`). The store is tainted if any of these holds:

1. `t` lies within a drop window of `i`, widened by `δ` on both sides (closed interval);
2. a drop window of `i` intersects `[t − examined_history, t]`;
3. a drop window of any other instance intersects `[t − δ, t + δ]`;
4. a drop window of `i` intersects `[t − δ, t + δ]`.

Positive evidence (classes 1–3 of §4) is never tainted.

## 6. Time-since-eviction bands

Every classified store carries a band of its `gap_ms` (`emitted/rows.rs`, `Band::of`,
`UPPER_EDGES_MS`): `[0, 10 s)` (including any negative gap), `[10 s, 1 min)`, `[1, 10 min)`,
`[10, 60 min)`, or `Unobserved` when no gap was recorded. `Unobserved` MUST NOT be read as
"longer than an hour" and is not stacked with the observed bands.

A prior close decides a store only while it is less than the examined history back, and the
history is at most an hour (`Params::validate`), so `[10, 60 min)` is the last band any gap
reaches.

`gap_ms` is recorded only in §4 step 3. A `re_admit` or `RE` store is `Unobserved` even when
its own key had a prior close (§11).

## 7. Coverage

Per source, over a window `W` (`observed_coverage.rs`, `ObservedCoverage::fold`), the
uncertain spans are:

- before the first heartbeat, up to its start plus `δ`, unless an `agent_start` at or before
  that heartbeat attests it;
- every drop window, widened by `δ` on both sides;
- a gap between two heartbeats past the run's liveness bound, from the bound after the earlier
  heartbeat to the later one: the larger of the declared `heartbeat_secs` and
  `max_segment_secs`, or `STALENESS_GRACE_MS` where that is longer (`silence_after_ms`). A
  producer that emitted no heartbeat for longer attested nothing over the rest of the gap
  (spec 5.8), whatever its counters say when it resumes;
- after the last heartbeat (from its end minus `δ`), unless an `agent_stop` at or after it
  closes the tail and the stop's span dropped nothing;
- the whole window, for a source with evidence but no heartbeat spans. A source's evidence is
  the input log's records naming it on the window's days, whatever segment carried them.

`covered(s) = |W| − |∪ uncertain(s) ∩ W|`. The window's fraction is
`Σ covered(s) / (|W| × number of sources)`. Per UTC day, spans are cut at midnight and each
day is charged exactly its part (`per_day_coverage`). The weighting is by instance-time, not
by traffic.

**Live edge**: coverage is stated through the covered edge, the instant through which every
source's heartbeats are settled (`UncertaintyModel::coverage_settled_to`, the staleness grace
past the last evidence). A day the edge has not passed is stated through the edge: its
denominator is the stated part of the day. As the edge moves, the rest of the day is stated,
and a source's silence becomes final, uncovered, once it is past the grace, unless a later
heartbeat of its run attests the stretch. A heartbeat delivered after the edge passed the
stretch it reaches restates that stretch while its day is open, and the restatement is
counted in `restated_hourly` under the family `coverage`.

**Evidence reach**: heartbeats are read up to `EVIDENCE_CLIP_MARGIN_MS` (one day) before the
analysis window (`completeness.rs`). A stretch whose anchoring heartbeat lies further
back reads as uncertain.

## 8. Duplication

**Spans** (`HolderSpans`, `residual.rs`): each interval's observed edges are shrunk by `δ / 2`;
censored edges stay unbounded; a span that shrinks away is dropped. Spans are merged per
`(workload class, holder)`.

**Mass**: `n_tokens` of an identity is the largest `n_tokens` among its stores. An identity
with no positive mass produces no duplication cell.

**Co-residency token-time** (`token_time_cells`, `residual.rs`): for two different holders
under the same class, the overlap of their spans within the window, times the mass. Pairs
are formed within one class only. This measure sums.

**Copies histogram** (`replication_histogram`, `residual.rs`): token-time at each exact
concurrent holder count, with the distinct instances and distinct `(instance, dp_rank)`
ranks among those holders. The single-copy bucket is emitted. Per-class cells MUST NOT be
summed into a fleet figure; the fleet figure sweeps the spans with the class flattened
(`fleet_flattened`).

**Copies band** (`copy_band`, `residual.rs`): `1`, `2`, `3`, `4`, `5-8`, `9-16`, `17+`.
The console's histogram bands (`bands_token_ms`, `crates/serving-clickhouse/src/replication_read.rs`): `k2` = 2, `k34` = 3–4,
`k58` = 5–8, `k9` = 9 and above.

**Locality** (`locality`, `residual.rs`), the coarsest relationship among the copies:
`none` (one copy), `cross` (more than one instance), `within` (one instance, more than one
rank), `group` (one rank, more than one KV cache group; architectural, not placement).

## 9. Staleness

A producer run is stale when it has been silent longer than
`max(larger declared bound, STALENESS_GRACE_MS)`, where the declared bounds are the
`heartbeat_secs` and `max_segment_secs` of its `segment_open` records and
`STALENESS_GRACE_MS` is 30 minutes (`staleness.rs`).

## 10. Parameters and constants

| Name | Value | Where | In the discriminator |
|---|---|---|---|
| `delta_skew_ms` | 1000 ms (preset 50 ms for attested NTP) | `lib.rs` | yes |
| `examined_history_ms` | 1 h (`EXAMINED_HISTORY_MS`), at most | `lib.rs` | yes |
| `CONTEXT_MARGIN_DAYS` | 1 | `lib.rs` | no |
| `EVIDENCE_CLIP_MARGIN_MS` | 1 day | `completeness.rs` | no |
| `LIFECYCLE_CLIP_MARGIN_MS` | 7 days (snapshot reads only) | `completeness.rs` | no |
| `STALENESS_GRACE_MS` | 30 min | `staleness.rs` | no |
| band edges | 1 min, 10 min, 60 min, 1 day | `emitted/rows.rs`, `Band::UPPER_EDGES_MS` | no |

## 11. Open questions

1. **Local cause under `RE`.** A store classified `RE` records neither its own key's prior
   close nor its gap (§4, §6), so the recompute register can read 0 where the reuse figure
   reads the same store as a recompute; the E1 goldens show it. Resolve before v1, or state the
   limitation beside the register.
2. **Multi-agent fleets.** `agent_start` and `agent_stop` apply fleet-wide, so one agent's
   graceful stop can close another agent's tail (`completeness.rs` module note). This is
   optimistic for coverage.

## 12. Late records, generations and settled days

The keyed pipeline (`crates/pipeline/src/keyed`, [the pipeline](pipeline.md)) applies the rules
above tick by tick. These rules govern what it states over time:

1. **Days with no input.** Every day of a source's stretch is stated, so a day the fleet was
   silent is stated uncovered.
2. **Late records.** A record that arrives behind an operator's edge is corrected by
   re-deriving the identities it reaches from the input log, for classes and placement alike.
   Placement's re-derivation never reaches before the first instant placement emitted: nothing
   was observed there. Late lifecycle evidence corrects classes too: a late segment that opens
   or splits a drop window behind the edge re-derives the stores whose taint (§5) it changes,
   and one that moves a silence (§3) re-derives the classes and the placement of every block
   its instance held.
   A late scope closer names no identity, so it is counted in `restated_hourly` and the span
   it touches stays uncorrected, as does a record of a day no longer open to change
   (`too_late`).
3. **Generations.** A changed parameter starts the working state again from the input log, so
   the new generation covers every day the log holds and overlaps the old one: a fork, never a
   handoff. Generation markers are stated as days settle.
4. **A rising token mass is not re-priced.** Placement prices a residency by the identity's
   token mass as known when the cells are emitted. A later, larger store of the identity raises
   the mass for what follows, and the minutes already emitted keep the smaller mass: a lower
   bound, under the `>=` register.
5. **Public modules** state their output for each settled day, from the first day that settled
   while the module was enabled (`stated_from_day`). A replayed tick states its days again
   under a fresh revision, so the replace retires whatever the failed attempt wrote.

`crates/pipeline/tests/keyed_equivalence.rs` holds the keyed driver to a whole-window
derivation's recorded output over every gait fixture (`fixtures/keyed-parity/`): the class and
band cells agree, and each source's covered time agrees once the log is settled just past its
last record.
