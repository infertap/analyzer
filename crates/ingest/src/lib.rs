// SPDX-License-Identifier: AGPL-3.0-only

//! Ingest: the reader boundary. Decodes wire segments, applies the truncated-tail and
//! unknown-kind rules, and commits each batch to the lineage's input log
//! (`docs/lake.md`), idempotent on the batch's content hash. Ingest writes the
//! input log and nothing else: the engine is not on this path.
//!
//! **There is no directory mode.** The production reader is [`bucket::ingest_bucket`]:
//! list, fetch, decode and commit against the customer's bucket through the object store.
//! [`ingest_bytes`] is the transport-free entry the conformance corpora and `selftest` feed
//! directly: the same decode and the same commit, minus the transport. That is what makes a
//! corpus golden evidence about the shipped reader rather than about a test harness.
//!
//! Rules this file owns:
//!
//! - **No arrival stamps.** Taint and coverage read the producer clock (`at_ms`);
//!   nothing here writes `arrival_ms`, and every input takes one path.
//! - **Truncated tail.** An unparseable FINAL line is the crash contract's expected
//!   artifact: discarded and counted on the batch record. An unparseable line anywhere else
//!   is corruption and fails closed.
//! - **Unknown kinds are counted, never fatal.** The count lands on the batch record.
//! - **Replay without duplicates.** The batch key is the content hash. A batch the log
//!   already holds on any day it places is a duplicate, and the commit states nothing.
//! - **Replay is recovery.** A cycle is one log commit. A cycle that dies before it commits
//!   leaves nothing committed, and the retry commits under the same position, so there is no
//!   torn state to detect or repair.
//! - **One parse, every surface.** The segment is decoded once, and the batch record carries
//!   what the core needs from it: the cache-fact span and days, the segment arrivals, the
//!   declared drops, the lifecycle records and the stored-mass denominator. Deep history is
//!   the bucket's, through each record's object key.
//!
//! # Where to start
//!
//! - [`ingest_bytes`] is the transport-free entry: one decoded segment, committed to the
//!   input log, idempotent on the content hash. [`bucket`] is the production reader over the
//!   bucket's listing. [`IngestReport`] is what a cycle returns.
//! - [`annotations`] is the declared-annotation contract. The lifecycle records and the
//!   arrival facts (recorded at ingest, read by the derivation without a fetch) are gathered here
//!   because they are written here.

pub use analyzer_segments::{content_batch_id, decode_segment, DecodedSegment};

pub mod annotations;
mod arrivals;
mod error;

pub mod bucket;

pub use error::Error;

/// Fail-honest accounting for one ingest run (the report's `ingest` block reads this).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct IngestReport {
    /// Objects the listing returned this cycle.
    pub files_seen: u64,
    /// Bytes across every object this run fetched (listing `_size`, which is the
    /// object's own byte count — the GET reads the same bytes).
    pub bytes_seen: u64,
    /// Objects ingested this cycle.
    pub files_ingested: u64,
    /// Of the ingested files, how many the projected walk prepared rather than the full
    /// decode. Observable, which is what lets the equivalence pin assert it compared two
    /// DIFFERENT paths rather than the reference against itself, and lets an operator ask
    /// whether the fast path is actually engaged on their fleet.
    pub projected_files: u64,
    /// Objects skipped because the ledger already held their content hash.
    pub duplicates_skipped: u64,
    /// Records inserted this cycle.
    pub records_inserted: u64,
    /// Records of a kind this build does not know, counted and passed over.
    pub unknown_kinds: u64,
    /// Listed but left unfetched when the cycle's slice deadline arrived — the REAL
    /// backlog: untouched, so the same discovery re-lists it next cycle.
    /// Zero in an unbudgeted run.
    pub files_deferred: u64,
    /// Bytes deferred to the next cycle by the slice deadline.
    pub bytes_deferred: u64,
}

/// Per-run state the file loop threads: the fail-honest accounting.
#[derive(Default)]
struct Run {
    report: IngestReport,
}

/// The span rule both write paths share: a row of any cache kind widens the batch's
/// `first_at_ms`/`last_at_ms` — `holder_reset` included, since the spine's chunk holds every
/// cache kind and a read bounded in time prunes chunks by this span — while
/// only the three mass-bearing kinds mark a day and count toward the denominator.
fn span_kind(kind: &str) -> bool {
    matches!(kind, "store" | "evict" | "clear" | "holder_reset")
}

fn mass_kind(kind: &str) -> bool {
    matches!(kind, "store" | "evict" | "clear")
}

fn widen(span: &mut Option<(f64, f64)>, at_ms: f64) {
    *span = Some(span.map_or((at_ms, at_ms), |(lo, hi)| (lo.min(at_ms), hi.max(at_ms))));
}

/// The stored-mass denominator and its basis counts, per `(instance, kind, day)`.
pub(crate) type Denominator = std::collections::BTreeMap<(String, String, i64), Mass>;

/// One `(instance, kind, day)`'s share of a batch's cache facts.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Mass {
    /// Facts of the kind.
    pub records: u64,
    /// Their tokens.
    pub tokens: i64,
    /// Stores of the instance's own cache.
    pub own: u64,
    /// Own stores carrying a `content_id`.
    pub comparable: u64,
}

/// What one batch's cache facts (store/evict/clear) amount to at commit: the stored-mass
/// denominator, the distinct UTC day indices they fall on, and the engine-domain span —
/// three surfaces that cannot drift because one fold computes them, row by row, on both
/// preparation paths.
#[derive(Default)]
struct CacheFacts {
    denominator: Denominator,
    days: std::collections::BTreeSet<i64>,
    span: Option<(f64, f64)>,
}

impl CacheFacts {
    /// One row's share: a cache kind widens the span, and a mass-bearing kind marks its day
    /// and counts toward the denominator under its instance.
    #[allow(clippy::cast_possible_truncation)] // day indices are small integers by construction
    fn feed(&mut self, obj: &serde_json::Map<String, serde_json::Value>) -> Result<(), Error> {
        use serde_json::Value;
        let Some(kind) = obj.get("kind").and_then(Value::as_str) else {
            return Ok(());
        };
        if !span_kind(kind) {
            return Ok(());
        }
        let at_ms = obj.get("at_ms").and_then(Value::as_f64).ok_or_else(|| {
            Error::Malformed(format!(
                "delta emission: a {kind} row carries no numeric at_ms"
            ))
        })?;
        widen(&mut self.span, at_ms);
        if !mass_kind(kind) {
            return Ok(());
        }
        let day = (at_ms / 86_400_000.0).floor() as i64;
        self.days.insert(day);
        let instance = obj
            .get("instance_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let tokens = obj.get("n_tokens").and_then(Value::as_i64).unwrap_or(0);
        let slot = self
            .denominator
            .entry((instance, kind.to_string(), day))
            .or_default();
        slot.records += 1;
        slot.tokens += tokens;
        if kind == "store"
            && analyzer_segments::is_own_copy(obj.get("locality").and_then(Value::as_str))
        {
            slot.own += 1;
            if obj.get("content_id").is_some_and(|c| !c.is_null()) {
                slot.comparable += 1;
            }
        }
        Ok(())
    }
}

/// One pass over the shaped rows, extracting everything the commit records about its cache
/// facts.
fn cache_facts_of(shaped: &str) -> Result<CacheFacts, Error> {
    let mut facts = CacheFacts::default();
    for line in shaped.lines().filter(|l| !l.is_empty()) {
        let row: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            Error::Malformed(format!(
                "delta emission: a shaped row does not re-parse ({e})"
            ))
        })?;
        let Some(obj) = row.as_object() else {
            return Err(Error::Malformed(
                "delta emission: a shaped row is not an object".to_string(),
            ));
        };
        facts.feed(obj)?;
    }
    Ok(facts)
}

/// One batch's ledger identity and counts, shared by both write paths.
struct LedgerEntry {
    batch_id: String,
    source: String,
    object_key: String,
    etag: String,
    records: u64,
    unknown_kinds: u64,
}

/// Ingest one batch of bytes already in hand — the conformance corpora's and
/// `selftest`'s entry. Same shaping, same probe, same intent-row commit as the bucket
/// reader: one code path minus the transport, which is what makes a corpus golden
/// evidence about the shipped reader rather than about a harness.
///
/// # Errors
/// Engine failures or corrupt (non-tail) input.
pub fn ingest_bytes(
    lake: &analyzer_lake::Lake,
    fleet_id: &str,
    source: &str,
    bytes: &[u8],
) -> Result<IngestReport, Error> {
    ingest_bytes_with(lake, fleet_id, source, bytes, false)
}

/// [`ingest_bytes`] with the shaped reference path FORCED, whatever the bytes' form.
///
/// The equivalence pin's reference arm: the ratchet that holds the projected walk to the full
/// decode, record for record (`tests/fast_path_equivalence.rs`).
///
/// # Errors
/// As [`ingest_bytes`].
pub fn ingest_bytes_shaped(
    lake: &analyzer_lake::Lake,
    fleet_id: &str,
    source: &str,
    bytes: &[u8],
) -> Result<IngestReport, Error> {
    ingest_bytes_with(lake, fleet_id, source, bytes, true)
}

fn ingest_bytes_with(
    lake: &analyzer_lake::Lake,
    fleet_id: &str,
    source: &str,
    bytes: &[u8],
    force_shaped: bool,
) -> Result<IngestReport, Error> {
    let mut run = Run::default();
    run.report.files_seen = 1;
    run.report.bytes_seen = bytes.len() as u64;
    let prep = prepare_segment(source, fleet_id, bytes, force_shaped)?;
    let entry = LedgerEntry {
        batch_id: prep.batch_id.clone(),
        source: source.to_string(),
        object_key: String::new(),
        etag: String::new(),
        records: prep.records,
        unknown_kinds: prep.unknown_kinds,
    };
    commit_batch(lake, "bytes", prep.facts, &entry, &mut run, &mut Vec::new())?;
    Ok(run.report)
}

/// Everything one segment's commit needs, however its rows were walked: the delta
/// carrier, the day/span facts, and the arrivals fold — fed, not yet finished, because
/// the arrival stamp belongs to the commit instant.
pub(crate) struct PreparedFacts {
    /// The stored-mass denominator of the segment's cache facts.
    pub stored: Denominator,
    /// Whether the projected walk prepared it (the full decode otherwise).
    pub projected: bool,
    pub days: Vec<i64>,
    pub span: Option<(f64, f64)>,
    pub arrivals: arrivals::ArrivalsFold,
    /// The segment's lifecycle records, the tap's row kept whole as a string; plus one
    /// sentinel per segment so a segment with no lifecycle rows still reads as banked.
    pub lifecycle: Vec<analyzer_lake::ledger::LifecycleRecord>,
    /// The tag intervals a declarer stated in the segment (spec 2.8).
    pub tags: Vec<analyzer_lake::ledger::DeclaredTag>,
}

/// A `tag_interval` row as the statement it makes, or `None` for any other row. A tag
/// interval missing a required field fails closed: a declaration nobody can read is not one.
fn declared_tag(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<analyzer_lake::ledger::DeclaredTag>, Error> {
    if obj.get("kind").and_then(serde_json::Value::as_str) != Some("tag_interval") {
        return Ok(None);
    }
    let text = |key: &str| {
        obj.get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| Error::Malformed(format!("tag_interval without {key}")))
    };
    let number = |key: &str| {
        obj.get(key)
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| Error::Malformed(format!("tag_interval without {key}")))
    };
    Ok(Some(analyzer_lake::ledger::DeclaredTag {
        declaration_id: text("declaration_id")?,
        identity: text("identity")?,
        tag: text("tag")?,
        from_ms: number("from_ms")?,
        until_ms: obj.get("until_ms").and_then(serde_json::Value::as_f64),
        at_ms: number("at_ms")?,
    }))
}

/// One lifecycle record for the bank: the segment, the kind, the instant and the row itself.
fn lifecycle_bank_line(
    out: &mut Vec<analyzer_lake::ledger::LifecycleRecord>,
    segment_key: &str,
    kind: &str,
    at_ms: f64,
    row: &serde_json::Value,
) {
    out.push(analyzer_lake::ledger::LifecycleRecord {
        segment_key: segment_key.to_string(),
        kind: kind.to_string(),
        at_ms,
        row: row.to_string(),
    });
}

/// The sentinel the bank holds for every ingested segment: `kind`
/// `_segment`, no row — a segment the bank has seen, whether or not it carried a
/// lifecycle row, so the read side's walk is exactly the segments the bank never saw.
pub(crate) const LIFECYCLE_BANK_SENTINEL: &str = "_segment";

/// One segment decoded as its commit needs it, plus the ledger counts.
pub(crate) struct PreparedSegment {
    pub batch_id: String,
    pub records: u64,
    pub unknown_kinds: u64,
    pub facts: PreparedFacts,
}

/// The columns the fast path's ONE projected walk decodes: everything the arrivals fold
/// reads, the instance and tokens the stored-mass denominator counts, and every column of
/// the lifecycle records the input log keeps whole. `kind` and `at_ms` ride every
/// projection.
///
/// `block_id` is here for VALIDATION, not consumption: the walk decodes every kind, and
/// the shared shaping fails closed on a `store` whose `block_id` column was not decoded
/// — the projection must carry what the shaping checks for the kinds it asks about.
/// `locality` and `content_id` are here for the holder basis the denominator counts (spec
/// 5.7). The remaining cache work the shaped path pays for — the identity derivation, the
/// per-record text serialization — the fast path never does.
pub const FAST_WALK_PROJECTION: &[&str] = &[
    // A declared tag interval's fields (spec 2.8): absent from every engine-side segment, so
    // the projection skips them there at no cost.
    "declaration_id",
    "identity",
    "tag",
    "from_ms",
    "until_ms",
    "incarnation",
    "segment_seq",
    "heartbeat_secs",
    "max_segment_secs",
    "extra",
    "agent_version",
    "msgs_seen",
    "dropped",
    "oversized",
    "publisher_restarts",
    "endpoints",
    "first_seq",
    "last_seq",
    "instance_id",
    "n_tokens",
    "locality",
    "content_id",
    "key_epoch",
    // The lifecycle records the input log keeps whole: the header's contract version and
    // the heartbeat's own counters, which the arrivals fold does not read.
    "contract_version",
    "events_ingested",
    "content_bridge_entries",
    "content_bridge_evicted",
    "content_unresolved",
    "unknown_types",
    "workload_class",
    "block_id",
    "canary",
    "reuse_reporting",
    "refused",
    "window_start_ms",
    "window_end_ms",
];

/// The reference preparation: full decode to shaped NDJSON, cache facts and the
/// arrivals fold walked from the same lines.
fn shaped_facts(shaped: &str, segment_key: &str) -> Result<PreparedFacts, Error> {
    let cache = cache_facts_of(shaped)?;
    let mut fold = arrivals::ArrivalsFold::new();
    let mut lifecycle = Vec::new();
    let mut tags = Vec::new();
    lifecycle_bank_line(
        &mut lifecycle,
        segment_key,
        LIFECYCLE_BANK_SENTINEL,
        0.0,
        &serde_json::Value::Null,
    );
    for line in shaped.lines().filter(|l| !l.is_empty()) {
        let row: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            Error::Malformed(format!("arrivals: a shaped row does not re-parse ({e})"))
        })?;
        let Some(obj) = row.as_object() else {
            return Err(Error::Malformed(
                "arrivals: a shaped row is not an object".to_string(),
            ));
        };
        if let Some(kind) = obj
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .filter(|k| analyzer_segments::LIFECYCLE_KINDS.contains(k))
        {
            let at_ms = obj
                .get("at_ms")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(0.0);
            lifecycle_bank_line(&mut lifecycle, segment_key, kind, at_ms, &row);
        }
        tags.extend(declared_tag(obj)?);
        fold.feed(obj);
    }
    Ok(PreparedFacts {
        stored: cache.denominator,
        projected: false,
        days: cache.days.into_iter().collect(),
        span: cache.span,
        arrivals: fold,
        lifecycle,
        tags,
    })
}

/// Decode one segment as its commit needs it: the fast path where the bytes allow it,
/// the shaped reference path otherwise.
///
/// The fast path's admission is conservative and every disqualifier falls through to the
/// path that handles everything: the bytes must be sealed Parquet (`PAR1`), the segment
/// must carry NO unknown kinds ("unknown kinds land whole and counted" is a promise a
/// projection cannot keep — it cannot decode columns it was not told about), and it must
/// open attributed (a headerless orphan's empty-incarnation stamping is the shaped path's
/// own edge case, not one worth teaching a second path).
pub(crate) fn prepare_segment(
    source: &str,
    fleet_id: &str,
    bytes: &[u8],
    force_shaped: bool,
) -> Result<PreparedSegment, Error> {
    if !force_shaped && bytes.starts_with(b"PAR1") {
        // The projected walk pays one copy of the bytes (the columnar reader takes the
        // allocation whole, and the full decode needs them again when the walk declines)
        // — ~1 ms per segment against the ~3 s of full shaping it replaces.
        let projected = analyzer_segments::columnar::decode_segment_projected(
            source,
            fleet_id,
            bytes.to_vec(),
            analyzer_segments::columnar::Projection {
                columns: FAST_WALK_PROJECTION,
                ..analyzer_segments::columnar::Projection::default()
            },
        )?;
        let open = projected
            .rows
            .iter()
            .find(|r| r.get("kind").and_then(serde_json::Value::as_str) == Some("segment_open"))
            .and_then(serde_json::Value::as_object);
        let incarnation = open
            .and_then(|o| o.get("incarnation"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        if projected.unknown_kinds == 0 && !incarnation.is_empty() {
            // One walk over the projected rows: the cache facts under the SAME fold as the
            // full decode's (`CacheFacts::feed`), and the arrivals fold.
            let mut fold = arrivals::ArrivalsFold::new();
            let mut cache = CacheFacts::default();
            let mut lifecycle = Vec::new();
            let mut tags = Vec::new();
            lifecycle_bank_line(
                &mut lifecycle,
                source,
                LIFECYCLE_BANK_SENTINEL,
                0.0,
                &serde_json::Value::Null,
            );
            for row in &projected.rows {
                let Some(obj) = row.as_object() else {
                    return Err(Error::Malformed(
                        "fast path: a projected row is not an object".to_string(),
                    ));
                };
                if let Some(kind) = obj
                    .get("kind")
                    .and_then(serde_json::Value::as_str)
                    .filter(|k| analyzer_segments::LIFECYCLE_KINDS.contains(k))
                {
                    let at_ms = obj
                        .get("at_ms")
                        .and_then(serde_json::Value::as_f64)
                        .unwrap_or(0.0);
                    lifecycle_bank_line(&mut lifecycle, source, kind, at_ms, row);
                }
                tags.extend(declared_tag(obj)?);
                cache.feed(obj)?;
                fold.feed(obj);
            }
            return Ok(PreparedSegment {
                batch_id: projected.batch_id.clone(),
                records: projected.rows.len() as u64,
                unknown_kinds: 0,
                facts: PreparedFacts {
                    stored: cache.denominator,
                    projected: true,
                    days: cache.days.into_iter().collect(),
                    span: cache.span,
                    arrivals: fold,
                    lifecycle,
                    tags,
                },
            });
        }
    }
    let prep = decode_segment(source, fleet_id, bytes)?;
    let facts = shaped_facts(&prep.shaped, source)?;
    Ok(PreparedSegment {
        batch_id: prep.batch_id,
        records: prep.records,
        unknown_kinds: prep.unknown_kinds,
        facts,
    })
}

/// The per-cycle bank: every batch a cycle prepared, as the input log records it, under the
/// one position the cycle reserved. The log's head commits them together at the cycle's end,
/// and a cycle that fails before it is not progress: its record objects are orphans, its
/// objects re-listed and fetched again by the next cycle.
#[derive(Default)]
pub(crate) struct MetadataBank {
    /// This cycle's committed batches, as the input log records them.
    records: Vec<analyzer_lake::ledger::BatchRecord>,
    /// Reserved from the input log before the cycle's first record.
    input_position: u64,
}

impl MetadataBank {
    /// Commit the cycle to the input log: its records, then the head. The commit point.
    ///
    /// # Errors
    /// Input log failures.
    pub(crate) fn flush(&mut self, lake: &analyzer_lake::Lake) -> Result<(), Error> {
        let started = std::time::Instant::now();
        let commits = self.records.len();
        if commits > 0 {
            lake.ledger(analyzer_lake::ledger::SHARD)?
                .commit_cycle(self.input_position, std::mem::take(&mut self.records))?;
            log::info!(
                "input log: {commits} batch(es) committed in {:.0}ms",
                started.elapsed().as_secs_f64() * 1000.0
            );
        }
        *self = Self::default();
        Ok(())
    }
}

/// Bank one file's batch for the cycle's commit, unless the input log already holds its
/// content under the days it would be recorded under: `false` for a duplicate, the same
/// content under another key, which is not recorded again.
///
/// # Errors
/// Input log failures.
pub(crate) fn commit_file_facts(
    lake: &analyzer_lake::Lake,
    carrier: &str,
    facts: PreparedFacts,
    entry: &LedgerEntry,
    run: &mut Run,
    timings: &mut Vec<(&'static str, std::time::Duration)>,
    bank: &mut MetadataBank,
) -> Result<bool, Error> {
    let log = lake.ledger(analyzer_lake::ledger::SHARD)?;
    if bank.input_position == 0 {
        // The cycle's position is the input log's next: reserved before its first record,
        // so every record the cycle states carries the position the log commits it under.
        bank.input_position = log.next_position()?;
    }
    // One arrival instant per file, the record's ingest stamp.
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mark = std::time::Instant::now();
    let projected = facts.projected;
    let record = batch_record(carrier, entry, (bank.input_position, stamp), facts);
    let held = log.holds(&record.batch_id, &record.placement_days())?;
    timings.push(("probe", mark.elapsed()));
    if held {
        return Ok(false);
    }
    if projected {
        run.report.projected_files += 1;
    }
    bank.records.push(record);
    run.report.files_ingested += 1;
    run.report.records_inserted += entry.records;
    run.report.unknown_kinds += entry.unknown_kinds;
    Ok(true)
}

/// One committed batch as the input log records it.
fn batch_record(
    carrier: &str,
    entry: &LedgerEntry,
    (input_position, stamp): (u64, u64),
    facts: PreparedFacts,
) -> analyzer_lake::ledger::BatchRecord {
    let arrivals = facts.arrivals.finish();
    analyzer_lake::ledger::BatchRecord {
        batch_id: entry.batch_id.clone(),
        carrier: carrier.to_string(),
        source: entry.source.clone(),
        object_key: entry.object_key.clone(),
        etag: entry.etag.clone(),
        input_position,
        ingested_at_ms: stamp.saturating_mul(1000),
        record_count: entry.records,
        unknown_kinds: entry.unknown_kinds,
        first_at_ms: facts.span.map(|(first, _)| first),
        last_at_ms: facts.span.map(|(_, last)| last),
        days: facts.days,
        segments: arrivals.segments,
        drops: arrivals.drops,
        lifecycle: facts.lifecycle,
        tags: facts.tags,
        stored: facts
            .stored
            .into_iter()
            .map(
                |((instance, kind, day), m)| analyzer_lake::ledger::StoredMass {
                    instance,
                    kind,
                    day,
                    records: m.records,
                    tokens: m.tokens,
                    own: m.own,
                    comparable: m.comparable,
                },
            )
            .collect(),
        annotations: Vec::new(),
    }
}

/// Commit one prepared batch whole — its record and an immediate single-file flush. The
/// transport-free entries ([`ingest_bytes`], the corpora, `selftest`) commit per file; the
/// bucket pool banks across the cycle instead and flushes once.
pub(crate) fn commit_batch(
    lake: &analyzer_lake::Lake,
    carrier: &str,
    facts: PreparedFacts,
    entry: &LedgerEntry,
    run: &mut Run,
    timings: &mut Vec<(&'static str, std::time::Duration)>,
) -> Result<(), Error> {
    let mut bank = MetadataBank::default();
    if !commit_file_facts(lake, carrier, facts, entry, run, timings, &mut bank)? {
        run.report.duplicates_skipped += 1;
        return Ok(());
    }
    let mark = std::time::Instant::now();
    bank.flush(lake)?;
    timings.push(("commit", mark.elapsed()));
    Ok(())
}
