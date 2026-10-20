# Contract pin

Pinned: **inference-events wire contract 2.2, `main`** (`2e2755a`), as a submodule gitlink — moving the pin is a
visible diff by construction. The contract's home is the public spec repo
([infertap/inference-events](https://github.com/infertap/inference-events),
`spec/kv-cache-v2.md`); the tap is its reference producer and pins the same corpus from its
own side. This pin previously named the tap itself (`infertap v0.2.0-51-gc39a9cf`), when the
tap was the contract's home; the corpus bytes and their hash are identical across that move.

**Contract hash** `6f6b2e94f9d897ad16a0adbe43c0fa41205e9cc2b7f334d5509ae40c33af0395`
(`conformance/CONTRACT_HASH` upstream) — a SHA-256 over `conformance/`, which moves when the
contract moves and not when the tap does. Two checks cover two different failures:
`pin_md_states_the_hash_the_submodule_actually_carries` (in `crates/cli/tests/static_audit.rs`, always-on — it reads two files and needs no engine)
asserts this file names the hash the submodule actually carries, and
`.github/workflows/contract-drift.yml` compares that vendored hash against upstream on a
schedule — a stale pin fails on the ABSENCE of action, and nothing else here would fire.

Content identities use the 128-bit construction. Emitted pseudonyms remain 32 hex
characters. The shared model validates decoded fields without requiring columns omitted
by projected reads.

## What this pin pins

The public contract this repo consumes and conforms to. This section describes the **surface**,
not its history — what changed and why lives in the bump commit.

- **The record schema**: `store` / `evict` / scope-level `clear` / scope-level `holder_reset`,
  plus the lifecycle kinds, plus the declared kind `tag_interval` (spec 2.8): a declarer's
  statement that an identity carried an opaque label over an interval, banked at ingest,
  grouped by and never interpreted. Block
  ids are **u64** and routinely exceed `i64::MAX`; `at_ms` is **milliseconds** (the engine
  publishes seconds, converted at the tap's adapter boundary); every **block** record carries
  `dp_rank` and `group_idx`, which scope block identity; `extra_keys` (renamed from
  `partition` at this pin) carries the engine's extra hash inputs and `lora_name` its adapter
  name, both pseudonymized at egress. Identities are decimal STRINGS in every mode as of this
  pin — the unkeyed path no longer emits JSON numbers — and a clear's absent `tier` is now
  omitted as a scope declaration (global over tiers), while a block record's unknown tier
  stays null.
- **The reserved consumer root** (`reserved_consumer_prefix` in
  `conformance/delivery/layout.json`, new at this pin): under the delivery prefix, the first
  segment `_analyzer/` is the consumer's — its derived tiers, annotations and probes — and a
  shipper never writes beneath it. `the_analyzer_root_is_the_contract_s_reserved_prefix`
  (static audit) holds the analyzer's `ANALYZER_ROOT` to it.
- **`holder_reset`**: the restart nothing announces, said in-band. The
  producer observes its publisher's sequence regress — the engine restarted, the cache is
  gone — and emits one scope-level record BEFORE the regressed message's own events. Two
  clock domains under the `identity_refused` convention: `at_ms` is the producer clock at
  detection, `boundary_ms` the engine clock this reader orders the close at; the
  closes-before-stores tie-break makes a boundary-instant store open the fresh residency.
  This reader expands it over every open interval under its scope (a process boundary:
  always global over `group_idx` and `tier`), closes with `CloseReason::Reset`, classifies
  the post-reset re-store `Cold` (evidence-decided, not absence-decided), and treats the
  boundary as an engine-id domain edge — a `block_id` is never matched across it. Absent
  the record, one merged residency is the honest reading, pinned by the `interval_guards`
  trio. Beside it, `endpoints[]` gains an optional per-endpoint `publisher_restarts`: the
  envelope total's scope, which the sum discards.
- **A `clear`'s scope is declared, never defaulted.** The clear's wire shape has never been
  observed live, so the tap transduces its scope dimension by dimension: a `clear` record
  carrying no `dp_rank`/`group_idx` states the wire named none, and this reader closes residency
  across ALL holders of that instance — never just holder zero (`intervals`' clear join,
  pinned by `clear_scope_is_read_as_declared_never_defaulted_to_zero`). Tap-side the same rule
  releases its identity map globally for the named tier, counted, because over-release lands in
  the visible unresolved counter while a guessed zero leaves silent tombstones.
- **`identity_refused`**: the identity mechanism's `segments_dropped` — a scoped, declared
  capacity loss. The tap's identity map retains by **delete-on-evict** (population = the
  engine's resident set) under a cap derived from declared tier sizes; at the cap it refuses
  NEW promises rather than breaking existing ones, and each refusal window is declared in-band
  naming the holder (`instance_id`, `dp_rank`, `group_idx`), a `refused` count, and
  `window_start_ms`/`window_end_ms` (ENGINE clock, unlike the record's own producer-clock
  `at_ms`). Affected holders' evicts carry no `content_id` over that window; figures over them
  are lower bounds. `content_bridge_evicted` on heartbeats now counts these refusals — the
  alarm meaning ("the bound is biting; some removes will not resolve") is unchanged.
- **Parent has three states.** `parent_id` present names the parent; `parent_id` ABSENT means
  **root** — a positive claim that the block begins a prefix chain, which this reader treats as
  vacuously proven and therefore contributing; `parent_unknown: true` means the tap could not
  determine it and is **neither**. Reading unknown as absent would fabricate roots and inflate
  residual mass, so the three stay three.
- **`locality`**: `LOCAL`/`REMOTE` relative to the publisher, absent where it declared nothing.
  Only own copies enter the residency substrate (classification-semantics §2) — a remote block is not this
  holder's copy, and counting it as one invents duplication.
- **`content_id`**: a block identity derived from what the block CONTAINS, chained from a fixed
  root, comparable across the fleet. `block_id` is the engine's and is comparable only inside the
  process that computed it, because vLLM seeds every prefix chain from `os.urandom(32)` unless
  `PYTHONHASHSEED` is fixed. The two are **domain-separated** — mapped under
  `infertap:block-id:v1` and `infertap:content-id:v1` — so they cannot be joined by accident,
  which is what lets identity be a single `coalesce`. Absent where the tap could not derive one
  honestly, and the heartbeat's `content_unresolved` counts those.
- **Identity-health counters** on heartbeats: `content_unresolved`, `content_bridge_entries`,
  `content_bridge_evicted`, `publisher_restarts`. Always present, never absent-when-zero — they
  are completeness figures, and absence would read as "this build does not report it".
- **`canary`**: on all three lifecycle kinds when egress is pseudonymized, absent when unkeyed.
  32 hex chars, `HMAC(K, "infertap:canary:v1" ‖ 0x00 ‖ epoch:u32-be ‖ 0x00 ‖ hex(nonce))[..16]`,
  where the nonce is the key file's second 32 bytes — MAC'd over a secret input so the
  construction being public does not let canaries be precomputed for guessable keys. Instances
  sharing a key file emit it identically; nothing else can produce it. Exactly ONE canary per
  producer: the key in force. A producer relating two key spaces would be testimony spec §3.4
  forbids, because a mis-provisioned node would testify the same way to silence the alarm. A
  rotation is a declared discontinuity (spec §3.3): mid-rotation the fleet genuinely holds two
  key spaces, and rotation state is inferred from fleet-wide convergence, never from any single
  producer.
  **Obligation: compare canaries before reporting any cross-instance figure.** Instances whose
  canaries disagree are not comparable — a rotation in flight included — and a residual across
  them must be qualified rather than reported.
- **Two encodings, both permanent, dispatched on content** (spec §2.1, as of this pin): JSON
  Lines is the write-ahead form and Apache Parquet is the shipped form, each first-class — a
  reader MUST accept either, and learns which it holds from the BYTES, never the filename: a
  Parquet segment begins with `PAR1`, a JSONL segment with a JSON object. The record model is
  identical in both forms; in Parquet each schema-typed field is a column, a null cell encodes
  ABSENT, and untyped fields (provenance among them) ride a string-to-string `extra` map whose
  entries a reader treats as it would the same field inline. Snappy is the codec floor. The
  covering fixture is the delivery corpus's committed pyarrow twin
  (`delivery/seg-1785153670000-4242-0.parquet`, the same records as the `segment.lines`);
  decoding both files to the same record sequence is the form-equivalence MUST
  (`crates/segments/tests/form_equivalence.rs`).
- **The delivery layout** (`conformance/delivery/layout.json`): `active.jsonl` is being written
  and must never be shipped or ingested, `seg-<incarnation>-<seq>` is sealed and complete,
  and every shipped segment begins with its own `segment_open` (as of v1-draft.24; the
  marker-first `orphan-<ms>` form earlier producers shipped is admitted under §4.1's reader
  tolerance and lands in `orphan_batches`). The layout speaks both forms: `wal_suffix` (`.jsonl`),
  `shipped_suffixes` (`.jsonl`, `.parquet`), `conversion_tmp_suffix` (`.parquet.tmp`, never
  terminal, never shipped), and a mode-keyed `shipper_exclude` object — only a sealed segment
  in its shipped form ships. `ACTIVE_SEGMENT` in `crates/ingest` mirrors it. **New at this
  pin: `dest_key_layout`** (spec §4.4) — object-store keys are dated-first, hour-deep, then
  node: `<prefix>/<utc yyyy-mm-dd>/<utc hh>/<node>/<file>`, the shipper's UTC clock at ship
  time. This is the layout `storage::dated` has always walked, now STATED where its header
  always claimed it was; `crates/ingest/tests/contract_layout.rs` asserts the arithmetic
  against the vendored bytes, closing the divergence this pin exists to prevent (node-first
  keys shipped while dated keys were discovered, both suites green, nothing ingested).
- **`contract_version` and both liveness bounds are now declared**: every `segment_open`
  carries `contract_version` ("1.11" at this pin — the fixtures and the reference producer
  moved together, per the v1.10 addendum), and `agent_start` declares
  `max_segment_secs` beside `heartbeat_secs` — the two bounds a reader may call a producer
  stale against are both in-band as of this pin.
- **The delivery records** (`conformance/delivery/records.json`): `segment_open`,
  `segment_recovered`, `segments_dropped` — records about the FILE rather than the cache, a
  third family beside cache and lifecycle. `segment_open` is every segment's first record and
  carries the `incarnation` (`<run_start_unix_ms>-<pid>`) that the filename merely echoes;
  identity lives there because a crash-recovered orphan must be filed under the incarnation that
  WROTE it. `segments_dropped` is a **declared data loss** — the tap naming which segments its
  disk cap reclaimed. `first`/`last` are FILENAMES, the faithful record of what was unlinked;
  `first_seq`/`last_seq` are the same range as integers, and those are what this reader localizes
  a loss from, because the delivery contract's own rule is that correctness must not bind to a
  filename. It is emitted **once per incarnation**
  reclaimed, and `incarnation` names the run whose data is GONE rather than the process that did
  the dropping; grouping is what keeps the range a description, since across two runs the dropped
  set is never contiguous. The corpus also pins one sealed segment whole: header first, then a
  mixed record stream, which is the containment that lets a reader attribute every row in a file
  to one tap run.
- **Retention order**, newly stated in `layout.json` and load-bearing here: the tap drops oldest
  first, ordered by (incarnation, `segment_seq`) with seq compared as a **number**. So what
  survives a disk-cap drop is the newest data — a contiguous run of the highest seqs per
  incarnation. Coverage math rests on that: it is what makes a declared drop describable as a
  range, and what lets a surviving window be read as the most recent one. The cap is enforced at
  tap startup as well as on every seal, so a restart does not carry an unbounded backlog.
- **The interpretation rules** (`spec/kv-cache-v2.md`): residency intervals, drop-window
  epistemics, the completeness clock, clock domains, epoch seams, dangling parents, truncated
  tail, unknown kinds — what this repository's `docs/classification-semantics.md` builds on.
- **The conformance corpora** (`conformance/telemetry`, `/lifecycle`, `/pseudonym`,
  `/vllm-wire`): the emission fixtures whose expected records are this repo's test
  inputs — the contracts-meet-in-the-middle substrate.
- **The READER corpus** (`conformance/reader/`): streams paired with the verdict a
  conforming reader must reach — the first corpus that can fail THIS repo. Opened with the
  audit family (five-class evict re-derivation grading, classes partition) and the identity
  family (parent conflicts surfaced, never folded); consumed whole-segment through the
  shipped ingest path by `cli/tests/reader_corpus.rs`, discovery by directory so a fixture
  the tap adds fails loudly rather than sitting uncovered.
- **The staleness family** (`reader/staleness_*`, spec §5.8/§2.4), as of this pin: per
  producer run, keyed by its opaque incarnation token, one of live / departed /
  unexplained / no_basis — judged at the stream's knowledge horizon, never wall-clock now.
  The DECLARED bounds are the entire basis: the threshold is the larger of the two, the
  observed heartbeat cadence may only widen it (this reader's allowance), a stop record
  turns the same silence into a departure, and a run with no observed declaration
  supports no staleness verdict at all — as of v1-draft.20 the undeclared fixture is a
  1.1-era stream, the only shape still expressible without bounds now that every header
  since 1.3 declares them (the mixed-fleet law, spec §6.2, still admits it).
  `heartbeat_secs`/`max_segment_secs` are typed
  columns as of this pin (migration 0012) because they gate that verdict.
- **The gap-degraded family** (`reader/gap_*`, spec §5.4), as of this pin: the sorted
  list of detected-gap classes in the stream, in the spec's words — a missing
  `segment_seq` inside an observed sequence, a nonzero `dropped` delta between
  heartbeats, a `segments_dropped` declaration — with the empty list pinning that an
  intact stream (a restart at seq zero included) yields NO fabricated gap. This reader
  answers from the degradation ledger, which cites the completeness clock for drop
  windows, so every §5.4 class is one surface.
- **The uncovered family** (`reader/uncovered_*`, spec §5.3), as of this pin: per
  instance, the EXACT uncovered intervals as producer-clock brackets — stop-to-start,
  crash opening at the last observation, lost start closing at the successor's first
  observation — with overlapping runs yielding nothing and a continuously covered
  bystander expecting the empty list. This reader answers from the ledger's
  incarnation-boundary entries, whose brackets are the verdict values verbatim.
- **The absence family** (`reader/absence_*`, spec §5.6), as of this pin: each fixture
  carries its question span, and the verdict is the register absence may be read at —
  exact only within a counted start-to-stop bracket with zero counter deltas, a lower
  bound before the producer started, during a gap, or outside observation. This reader
  answers via `degradation::absence_register`: an empty ledger over the span plus a
  containing counted bracket, and nothing else, reaches exact.
- **The holder-register family** (`reader/refused_*`, spec §2.4), as of this pin:
  questions of (holder, engine-clock span) answered with the register a figure over
  them carries — lower_bound for the named worker inside the declared refusal window,
  none for the sibling and for the out-of-window spans, and the record's producer-clock
  `at_ms` placed adversarially inside an innocent span, which must not confine the
  loss (§2.6). This reader answers via `Ledger::register_for` verbatim. As of this
  pin the same question form also carries §5.1's blast radius: the corpus's first
  multi-instance incarnation, whose sequence hole degrades BOTH covered instances —
  including the one with no surviving records — while the bystander's intact run
  stands.
- **The coverage family** (`reader/coverage_*`, spec §5.7), as of this pin: a question
  span answered with the coverage a conforming reader attaches — complete (exactly
  1.0) over a counted bracket, strictly reduced under inbound loss or under a tail no
  stop closed. Pins the floor's direction and endpoints, never interior arithmetic;
  this reader answers from `completeness::uncertainty_model(...).coverage()`.
- **The clock-domain fixtures** (`reader/clock_*`, spec §2.6), as of this pin: no
  verdict family of their own — adversarial streams graded by the existing families.
  A month of engine-vs-producer displacement flips no staleness or audit verdict; a
  2 ms cross-instance overlap is a measured absence (no plausible allowance admits
  it) while a ten-minute overlap stays positive — the allowance bracketed from both
  sides without naming a number. Every READER row in the spec's no-covering-fixture
  table is fixtured as of this pin; the remaining rows are producer-side.
- **The table is EMPTY as of this pin.** The §3.4 two-canary split fixture
  (`reader/canary_split_key_material_is_not_a_measured_zero` — a residual_zero verdict:
  a folding reader grades it measured and fails) closed the last reader row; §2.1,
  §5.8 and §6.1 discharged onto named producer tests and existing corpus artifacts,
  cited in the spec's footnotes. The heartbeat deadline now sits EARLY by the loop's
  wake jitter (§5.8 honored by arithmetic). Standing rule, stated in the spec: a new
  requirement enters with its fixture, or it reopens the table — and a non-empty table
  gates publication.
- **Headerless recoveries are discarded and declared** (v1-draft.24, contract 1.11, spec
  §4.3): a producer that finds an unfinished segment whose `segment_open` did not survive
  never ships it. A conforming producer fsyncs the header before any record, so such a file
  was never durably written; the producer unlinks it and declares the discard with
  `segment_recovered` (`attributed: false`, `dropped_bytes` the whole file) second in its
  own first segment. This reader treats that as a declaration and nothing more — the span
  precedes the incarnation's start and is uncovered under §5.3 already — and banks it as a
  lifecycle row (`recovered_truncation_incidents_from_events` mints nothing for it, by
  design). The `orphan-` prefix and pattern left `layout.json`; `delivery/records` pins the
  recovering segment (`recovering_segment`) and `reader/` pins that the declaration changes
  no coverage figure (`uncovered_a_discarded_recovery_declares_no_coverage`). The delivery
  corpus's generator also caught up with its hand-edited fixture upstream, so the pinned
  segment's content is unchanged at this pin.
- **The stream component** (v1-draft.23): the delivery layout names the KV-cache stream, `kv`,
  and states that `<prefix>` — the shipper's destination and this analyzer's `--bucket-prefix`
  — ends in it: `<root>/kv/`. The component was born inside the configured prefix on both
  sides, so the reader is unchanged and nothing shipped before the pin is renamed; the pin
  asserts it (`the_documented_prefix_ends_in_the_contract_s_stream_component`).

## Open at this pin

**The canary is consumed** as of this pin: `canary` is a typed column, key spaces resolve
through `incarnation`, and `ZeroMeaning::SplitKeySpace` reports what used to grade as
`Measured`. Key spaces are DISTINCT CANARIES, counted: a rotation in flight is reported as the
split it genuinely is, and a reader that wants "rotation" rather than "fault" as the label
infers it from fleet-wide convergence (spec §3.4) — recognition that never makes the two
spaces joinable.

What remains is an asymmetry worth stating rather than papering over. **A split is a positive
finding; one space is not a proof of unity.** Incarnations with no canary fall into one
unattributed bucket, and an unkeyed tap is indistinguishable from a keyed tap whose lifecycle
records were never ingested — both emit nothing. So `key_spaces_observed == 1` means "no
disagreement seen". The narrow residual risk: a split-key fleet ingested as a cache-only archive,
with no lifecycle records at all, would report one space. A live tap cannot reach that state,
because the canary rides every heartbeat.

**Declared expected key spaces are not wired.** With nothing declared, any split is an alarm —
the right default, and it catches the `openssl rand` twice case on pilot day one. A customer who
intends several key spaces has no way to say so yet, so their spaces are reported as a fault
rather than as separate domains.

**Declared segment loss reaches storage and nothing reads it.** `segments_dropped` arrives with
`first_seq`/`last_seq` typed, so the tap's disk-cap loss is localizable — but no coverage figure
subtracts it, so a window missing whole segments still reports the coverage it observed.

## Bumping

Fetch, check out the new commit, update the tag+SHA and hash above, add or amend a surface
bullet if the surface moved, re-run the conformance suite — one commit. The re-run against the
NEW corpora is the point; moving the gitlink proves nothing on its own, and that is the half
that must not be automated.
