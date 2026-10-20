# The error direction

Nineteen ways to inflate your own KV-cache waste numbers, the guard against each, and the
fixture that proves it.

**Why this document exists.** The product is not the derivation. Anyone competent can fold
store/evict/clear into residencies. The product is that every one of these mistakes fails in the
direction that makes your cache look *worse* than it is — and that we test for it rather than
assert it.

Every row below is a reading a careful engineer would plausibly choose, and every one of them
moves the headline number in the flattering direction. The provenance column separates what the
event spec makes normative from what this reader derived on its own.

Notation: `[spec §x]` — the contract states the rule. `[ours]` — the analyzer's own discipline,
beyond what the spec requires.

---

## A. Readings that inflate the waste figure

The number a customer checks first.

### A1 — Collapse the storage tiers

*Naive:* a block is a block; pair stores and evicts on `(holder, block)`.

*Produces:* a GPU evict closes a residency the CPU copy still holds. The next GPU store then
reads as a recompute when it was a promotion off the local CPU tier — **a device-to-host copy
counted as a recomputed prefill.**

*Guard:* `tier` is in the interval key and deliberately not in `Holder` — a tier is a location
*within* a cache, not a cache, so admitting it to the holder would invent duplication just as
surely. `analysis::intervals` (module docs). `[ours]`

*Fixtures:* `telemetry/tier_absent_stays_null`, `telemetry/clear_tier_undeclared`,
`reader/audit_offload_and_scope`

### A2 — Read an absent clear scope as holder zero

*Naive:* a `clear` record with no `dp_rank`/`group_idx` clears rank 0.

*Produces:* every other holder's residency stays open past a clear that emptied it. Overstated
durations, and the producer emits exactly that shape whenever the wire named no scope.

*Guard:* a NULL dimension on a clear means the wire named no value, so the clear is global over
it — matched against every holder rather than coalesced onto zero. `analysis::intervals`.
`[spec §2.3]`

*Fixtures:* `telemetry/clear_scope_undeclared`, `telemetry/clear_is_scope_level`,
`telemetry/clear_tier_undeclared`

### A3 — Sum `n_tokens` over stores on an unlabelled stream

*Naive:* stores are insertions; sum their tokens to get inserted tokens.

*Produces:* **the measurement inverts.** Some engines announce a block *reused* from cache with
the same event they use for one newly inserted. Summing counts a block once per announcement, so
the figure rises with how *effective* the cache is — highest on the fleet with the least waste.
Every input correct, the conclusion backwards.

*Guard:* the producer declares `reuse_reporting`; under `unlabelled` an unqualified count is
refused outright rather than caveated. Silence is not `none`, and an unrecognized value reads as
`unlabelled`. `analysis::reuse::Declaration`. `[spec §5.9]`

> A reader MUST NOT assume the safe-sounding case from silence — that is the one reading that
> turns an unknown into a measurement. *(spec §2.3)*

*Fixtures:* `reader/reuse_unlabelled_forbids_an_insertion_count`,
`reader/reuse_unlabelled_forbids_a_first_insertion_figure`,
`reader/reuse_unlabelled_still_permits_a_recompute_figure`, `reader/reuse_undeclared_is_not_none`,
`reader/reuse_unrecognized_reads_as_unlabelled`, `reader/reuse_none_makes_every_store_an_insertion`,
`reader/a_window_takes_the_weakest_reuse_declaration_it_contains`

### A4 — Condition on absent prior residency

*Naive:* no prior residency observed for this block, so this store is a first insertion.

*Produces:* exactly where a reuse announcement and a first insertion are indistinguishable — the
block may have been resident since before observation began. And the bias concentrates: a block
that is never evicted is never re-announced, so **the miscount lands hardest on the hottest shared
prefix in the fleet.**

*Guard:* it is indeterminate, not cold. `analysis::reuse`, `analysis::classify`. `[spec §5.9]`

*Fixture:* `reader/first_insertion_is_countable_where_reuse_is_not_announced`

### A5 — Treat absence during a telemetry gap as evidence

*Naive:* no prior close observed, therefore cold.

*Produces:* a drop window that swallowed the deciding close is read as proof the close never
happened. Cold is a waste class; the store gets counted.

*Guard:* absence-decided classes consult the taint model — drop-window membership, truncated
batch, cross-instance overlap of the simultaneity range — and produce `indeterminate` rather than
`cold` when the deciding evidence cannot be trusted. `analysis::classify`. `[spec §5.4, §5.6]`

*Fixtures:* `reader/absence_during_a_gap_is_a_lower_bound`,
`reader/absence_before_the_producer_started_is_a_lower_bound`,
`reader/absence_outside_observation_is_a_lower_bound`,
`reader/absence_is_exact_within_a_counted_bracket`

---

## B. Readings that invent or erase duplication

### B1 — Treat a REMOTE block as the publisher's own copy

*Naive:* a block record is a block this node holds.

*Produces:* two publishers reporting one shared or offloaded block are most likely describing a
**single copy**. Admitting both as per-holder residency reports duplication that does not exist.

*Guard:* the residency substrate counts only own copies. Absent `locality` counts as own (that is
the entire observed world for deployments without an offload tier); an *unrecognized* value does
not — "declared nothing" and "declared something this build cannot read" are different, and only
the first is safe. `analysis::intervals::Interval`. `[ours]`

*Fixture:* `telemetry/remote_locality_is_carried`

### B2 — Key identity on the bare instance

*Naive:* `instance_id` names the cache.

*Produces:* data-parallel workers hold physically independent caches and KV cache groups hash
independently. Two workers holding one prefix are two blocks; a key without `dp_rank`/`group_idx`
reads that as one block re-admitted — **reporting duplication as zero rather than as a number.**

*Guard:* `Holder` is the full triple, and it is one dimension rather than three so no caller can
group by `dp_rank` alone. `analysis::intervals::Holder`, `serving_clickhouse::Dim::Holder`. `[spec §3.2]`

> Collapsing `dp_rank` or `group_idx` under a bare instance treats independent caches as one and
> under-reports duplication between them. *(spec §3.2)*

*Fixtures:* `telemetry/same_hash_distinct_dp_ranks`, `telemetry/same_hash_distinct_groups`,
`telemetry/same_hash_distinct_instances`

### B3 — Compare engine block IDs across processes

*Naive:* the same hash means the same block.

*Produces:* **vLLM seeds each prefix chain from a per-process random root unless
`PYTHONHASHSEED` is fixed.** An engine ID is comparable only inside the process that computed it,
so cross-node matching on it is either meaningless or accidental.

*Guard:* identity prefers `content_id` where the tap derived one and falls back to `block_id`
otherwise; the two spaces are domain-separated at the tap so they can never collide, and the
`comparable_stores / total_stores` ratio measures how much of the data is fleet-comparable at all.
`analysis::intervals`. `[spec §3.1]`

*Fixtures:* `telemetry/same_content_distinct_engine_ids`,
`telemetry/content_id_rides_beside_the_engine_id`, `telemetry/same_hash_distinct_instances`

### B4 — Read `parent_unknown` as a root

*Naive:* no parent named, so it begins a chain.

*Produces:* R1 counts a root as contributing, because an empty ancestry claim is vacuously
proven. "I cannot determine the parent" is not an empty claim — it is an unproven one, and
collapsing them **manufactures contribution out of ignorance.**

*Guard:* three states, not two — `Root`, `Named`, `Unknown` — and an explicit unknown outranks an
absent `parent_id` when a record carries both. `analysis::tree::Ancestry`. `[spec §2.3]`

> Treating unknown ancestry as a root invents chain structure the producer never claimed and
> inflates any figure built on it. *(spec §2.3)*

*Fixture:* `telemetry/parent_unknown_is_not_a_root`

### B5 — Silently resolve an identity collision

*Naive:* one block, one parent; pick one and move on.

*Produces:* under content-derived identity, a block observed with conflicting parent claims is a
collision announcement. Folding it hides the one event that says the identity space is failing.

*Guard:* counted as `parent_conflicts` and surfaced, never resolved. `analysis::tree::BlockTree`. `[spec §3.1]`

*Fixture:* `reader/identity_collision_claims_two_parents`

---

## C. Readings that turn an unknown into a measurement

### C1 — Report a cross-node zero without checking key spaces

*Naive:* no matches across nodes, so no duplication.

*Produces:* pseudonyms under different keys cannot match **at any overlap**. A fleet mid-rotation
or split across key material reports a confident zero that is an artifact of provisioning, not a
measurement of the cache.

*Guard:* the key spaces of the runs that stored are counted from the canaries they declared
(`input_key_spaces`); above one, a zero reads `split_key_space`, and nothing folds two key
spaces into one. `analysis::residual::ZeroBasis`. `[spec §3.4]`

*Fixture:* `reader/canary_split_key_material_is_not_a_measured_zero`

### C2 — Report a zero with no portable identity

*Naive:* zero is zero.

*Produces:* if no store carried a `content_id`, every block is keyed on a process-local engine ID
and **could not have matched across nodes however much duplication existed.**

*Guard:* the replication read carries `zero_basis`, and its `meaning` is `not_comparable` when
no own store carried a `content_id`, `partly_comparable` when some did not.
`analysis::residual::ZeroMeaning`. `[spec §3.1, §5.7]`

*Fixtures:* `reader/zero_with_no_portable_identity_says_nothing`,
`reader/zero_single_holder_is_not_a_finding`, `reader/zero_across_comparable_holders_is_measured`

### C3 — Report a zero from a single observed holder

*Naive:* zero duplication.

*Produces:* duplication is a claim about a block in two caches at once. Below two holders there is
nothing it could be — the zero is definitional, not empirical.

*Guard:* `zero_basis.instances` counts the instances that stored into their own cache; below
two, a zero reads `single_holder`. `analysis::residual::ZeroBasis`. `[spec §5.7]`

*Fixture:* `reader/zero_single_holder_is_not_a_finding`

### C4 — Treat a producer restart as a gap

*Naive:* the sequence number went backwards; data was lost.

*Produces:* every rolling deploy renders as a fleet-wide incident, and coverage drops for a stream
that lost nothing.

*Guard:* continuity is scoped to `(instance, incarnation)` — a fresh incarnation at seq 0 is a
restart, a missing seq *within* one incarnation is loss. `segments::DELIVERY_FIELDS`,
`analysis::degradation`. `[spec §5.2]`

*Fixtures:* `reader/gap_an_intact_stream_is_not_degraded`,
`reader/seq_across_an_incarnation_boundary_does_not_narrow`,
`reader/incarnation_is_an_opaque_token`

### C5 — Narrow a degraded span on an unsound bracket

*Naive:* the loss sits between the two nearest sequence numbers.

*Produces:* **narrowing is the only weakening the contract permits**, so an unsound bracket
exonerates a span nothing observed. Three ways to get it wrong: a missing `seq` on either side, a
bracket spanning an incarnation boundary, or an unnumbered record between the two.

*Guard:* all three conditions checked; refusal returns no endpoints rather than invented ones.
`analysis::narrowing::GapSpan`. `[spec §5.4]`

*Fixtures:* `reader/a_bracket_missing_seq_does_not_narrow`,
`reader/seq_narrows_the_degraded_span_to_its_bracket`,
`reader/seq_across_an_incarnation_boundary_does_not_narrow`

### C6 — Emit a figure without its coverage

*Naive:* the number is the number.

*Produces:* a claim rather than a measurement. This is the floor the whole contract rests on.

*Guard:* held in the type system — no serving read returns a naked number; everything is
`Covered<T>`, and the wire's `Figure` is the same type specialized so one law is not held in two
places. `serving_clickhouse::Covered`, `api::wire::Figure`. `[spec §5.7]`

*Fixtures:* `reader/coverage_a_counted_bracket_is_complete`,
`reader/coverage_loss_inside_the_bracket_reduces_it`,
`reader/coverage_an_unclosed_tail_reduces_it`

### C7 — Call a producer stale from observed cadence alone

*Naive:* it has been quiet for a while; it is stale.

*Produces:* grades a coverage gap as an anomaly. Without declared bounds there is no basis for the
verdict, however long the silence.

*Guard:* four verdicts, and `no basis` is one of them. The threshold rests on the producer's own
declared `heartbeat_secs`/`max_segment_secs`; observed cadence may only *widen* it, never tighten
below the declaration. `analysis::staleness::RunStaleness`. `[spec §5.8]`

*Fixtures:* `reader/staleness_without_declared_bounds_has_no_basis`,
`reader/staleness_is_judged_against_the_larger_bound`,
`reader/staleness_silence_without_a_stop_is_unexplained`,
`reader/staleness_a_stop_makes_the_same_silence_a_departure`

### C8 — Sum across a meaning seam

*Naive:* it is the same metric; add the days.

*Produces:* two generations of *meaning* blended into one number. A table that keys on the meaning
pair beside one that does not leaves the trend honestly segmented and the drill silently
overwriting. **A drill that contradicts its own trend, with no way for the reader to tell which one
is lying.**

*Guard:* `(semantics_version, discriminator)` in the sort key of every table that emits keyed
history, enforced by a static audit that reads the migration file rather than trusting review;
`waste_by_segmented` returns one series per generation rather than one blended one; and a marker
records the seam so a step on a trend has a visible cause.
`crates/serving-clickhouse/migrations/0001_schema.sql`.
`[ours]`

*Guarded by:* `static_audit::the_meaning_pair_is_in_every_keyed_history_sort_key`,
`analysis::discriminator_tests`

### C9 — Compare timestamps across clock domains

*Naive:* milliseconds are milliseconds.

*Produces:* cache facts carry the engine clock, lifecycle facts the tap process's wall clock. The
drift is invisible because both are plausible numbers landing in plausible places.

*Guard:* no query joins wall against engine `at_ms`; the skew allowance widens windows, so excess
skew over-taints toward `indeterminate` and can never manufacture a false `cold`.
`analysis` (crate docs). `[spec §2.6]`

*Fixtures:* `reader/clock_the_domains_never_meet`,
`reader/clock_a_sliver_overlap_is_not_simultaneity`,
`reader/clock_a_robust_overlap_survives_the_allowance`

---

## The shape of the whole thing

Every guard above fails the same way. When the evidence is not there, the number goes **down**,
and the reader says why:

- absent `n_tokens` coalesces to zero — undercounts waste, never inflates it
- a close that fell off the back of the examined history reads as `cold`, not as ETR
- excluding a fact under-reports duplication; admitting one over-reports it
- the skew allowance shrinks residency intervals before overlap tests, so a co-residency claim
  survives only if it is robust to the allowed clock misalignment
- a compacted degradation entry costs precision, never honesty

That is why every figure is served as a floor, with its coverage beside it.

## What this argues

**For a self-hosted shop considering rolling their own:** these are nineteen decisions, each of
which looks like a detail and each of which moves your headline number in the direction you want
to believe.

**Sixteen of the nineteen are normative requirements of the published contract, not our opinions.**
That is the sharper form of the argument: the question is not whether these failure modes are real
— the spec settles that, and ships executable fixtures for each — it is whether a reader
implements 1403 lines of contract and 52 MUST NOTs correctly, and proves it. The remaining three
are this reader's own discipline, found in the course of doing exactly that.

They are individually cheap and collectively the whole product.

**For a partner or integrator:** every one of these is a place where the *egress contract* has to
make the consumer careful, or we have shipped them the bug with our name on it.

---

*Sources: `contract/inference-events/spec/kv-cache-v2.md` (1403 lines, 52 MUST NOTs) and its
conformance corpora — 62 reader cases, 22 telemetry cases, run by
`crates/cli/tests/reader_corpus.rs` and `crates/pipeline/tests/correctness.rs`. Provenance: 16 `[spec §x]`,
3 `[ours]`. Every fixture named above is checked to exist by
`static_audit::error_direction_names_only_real_fixtures`.*
