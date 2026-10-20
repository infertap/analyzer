// SPDX-License-Identifier: AGPL-3.0-only

//! The delivery ledger — "of what the taps produced, how much did we get".
//!
//! **Since install, never windowed.** The question is the pipeline's whole-life ledger, not a
//! period figure: spec 4.2 makes `segment_seq` zero-based and gap-free within an incarnation, so
//! every missing sequence number is a loss with a name, and the tap's own `segments_dropped`
//! declarations say which losses the producer already confessed to. What remains is
//! missing without a declaration — in flight, or lost where nobody said so.
//!
//! **The tail is invisible and the caller must say so.** A segment past the newest one seen
//! or declared has left no trace here; the ledger is stated over `[0 ..= max known seq]` per
//! run and claims nothing beyond each tap's last word — the same knowledge-horizon reading
//! Spec 5.8 gives staleness: judged against what arrived, never against the wall clock.
//!
//! **One conservative edge, inherited and accepted:** a liveness-less segment leaves no
//! arrival row (the row would state a clock it does not have, and widening
//! `last_record_at_ms` past the liveness kinds would make arrivals a second staleness
//! reader), so a captured liveness-less segment reads as missing-unexplained here.
//! Delivery understated, never overstated.
//!
//! Ingest-fresh by construction: both statements read the arrival facts the core publishes
//! from the input log as ingest commits (`input_segments`, `input_drops`), and the whole
//! read is one bounded aggregate per producer run.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Dao;
use crate::Error;
use std::collections::BTreeMap;

impl Dao<'_> {
    /// The delivery ledger per producer run, plus the orphan count — the data-quality
    /// headline and the delivery detail.
    ///
    /// **Sequence arithmetic happens here, not in SQL**, because the clamp is a policy: a
    /// declared range can overlap a captured segment (a re-declaration, or a range that
    /// arrived after all), and `declared` is clamped so captured + declared + missing
    /// partitions `expected` exactly. A negative "missing" rendered anywhere would be the
    /// meter accusing itself.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    #[allow(clippy::too_many_lines)] // two aggregates joined into one report: splitting them would be two reads of one ledger
    pub fn delivery(&self) -> Result<DeliveryReport, Error> {
        // Counters aggregate the RUN-LEVEL rows only (`instance = ''` — the fan-out
        // duplicates the envelope per endpoint and would multiply every cumulative mark);
        // the fan-out rows contribute only the instance names. The cumulative marks take
        // `max`, not the latest segment's word: the tap's counters are monotone within a
        // run, so the max IS the current accounting — and a declaring segment that carries
        // no heartbeat cannot zero a run's history by being newest. The binding sits
        // immediately above its statement — the static audit's lookback rule.
        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let arrivals_sql = format!(
            "SELECT incarnation, \
                    uniqExactIf(segment_seq, segment_seq >= 0 AND instance = ''), \
                    maxIf(segment_seq, segment_seq >= 0 AND instance = ''), \
                    countIf(segment_seq < 0 AND instance = ''), \
                    maxIf(msgs_seen, instance = ''), \
                    maxIf(dropped, instance = ''), \
                    maxIf(oversized, instance = ''), \
                    minIf(first_record_at_ms, instance = ''), \
                    maxIf(last_record_at_ms, instance = ''), \
                    arrayStringConcat(arraySort(groupUniqArrayIf(instance, instance != '')), '\\x1f') \
             FROM input_segments FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0 \
             GROUP BY incarnation"
        );
        let mut runs: BTreeMap<String, RunAgg> = BTreeMap::new();
        let mut orphan_batches = 0_u64;
        let arrivals = self.engine.run(&arrivals_sql)?;
        for mut row in Row::each("delivery arrivals", &arrivals) {
            let inc = row.text("incarnation")?;
            let captured: u64 = row.parse("captured")?;
            let max_seq = row.text("max_seq")?;
            // The unattributed bucket: no sequence claim, coverage-indeterminate. Counted,
            // never a row — a "run" named `''` would be an artifact wearing an identity.
            orphan_batches += row.parse::<u64>("orphans")?;
            let records_seen = row.parse("msgs_seen")?;
            let records_dropped = row.parse("dropped")?;
            let records_oversized = row.parse("oversized")?;
            let first = row.text("first_record_at_ms")?;
            let last = row.text("last_record_at_ms")?;
            let instances = row.text("instances")?;
            row.end()?;
            if inc.is_empty() {
                continue;
            }
            // A named run always carries run-level rows with real sequences (ingest writes
            // the run-level row per segment), so captured > 0 here and the `*If` aggregates
            // aggregated over a non-empty set. The guard is belt for a torn write.
            let when_captured = |name: &str, v: &str| -> Result<Option<f64>, Error> {
                (captured > 0)
                    .then(|| Row::new("delivery arrivals", v).parse(name))
                    .transpose()
            };
            runs.insert(
                inc.to_string(),
                RunAgg {
                    captured,
                    max_seq_arrived: (captured > 0)
                        .then(|| Row::new("delivery arrivals", max_seq).parse("max_seq"))
                        .transpose()?,
                    records_seen,
                    records_dropped,
                    records_oversized,
                    first_seen_ms: when_captured("first_record_at_ms", first)?,
                    last_seen_ms: when_captured("last_record_at_ms", last)?,
                    instances: if instances.is_empty() {
                        Vec::new()
                    } else {
                        instances.split('\u{1f}').map(str::to_string).collect()
                    },
                    declared: 0,
                    max_seq_declared: None,
                },
            );
        }

        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let drops_sql = format!(
            "SELECT incarnation, sum(last_seq - first_seq + 1), max(last_seq) \
             FROM input_drops FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0 \
             GROUP BY incarnation"
        );
        let drops = self.engine.run(&drops_sql)?;
        for mut row in Row::each("delivery drops", &drops) {
            let inc = row.text("incarnation")?;
            let declared = row.parse("declared")?;
            let last_seq = row.parse("last_seq")?;
            row.end()?;
            // A declaration can name a run whose segments were ALL reclaimed — such a run
            // still gets a row, because "everything this tap produced was dropped, and it
            // said so" is exactly what this ledger exists to state. Its instants stay
            // absent: the declaration came from a DIFFERENT run's segment, and borrowing
            // that clock would stamp one run with another's time.
            let agg = runs.entry(inc.to_string()).or_default();
            agg.declared = declared;
            agg.max_seq_declared = Some(last_seq);
        }

        let rows: Vec<DeliveryRow> = runs
            .into_iter()
            .map(|(incarnation, agg)| {
                let max_known = match (agg.max_seq_arrived, agg.max_seq_declared) {
                    (Some(a), Some(d)) => Some(a.max(d)),
                    (a, d) => a.or(d),
                };
                // Zero-based and gap-free: the span the tap itself numbered.
                let expected = max_known.map_or(0, |m| u64::try_from(m).unwrap_or(0) + 1);
                let captured = agg.captured.min(expected);
                let declared = agg.declared.min(expected - captured);
                DeliveryRow {
                    incarnation,
                    instances: agg.instances,
                    first_seen_ms: agg.first_seen_ms,
                    last_seen_ms: agg.last_seen_ms,
                    segments_expected: expected,
                    segments_captured: captured,
                    segments_declared_dropped: declared,
                    segments_missing_unexplained: expected - captured - declared,
                    records_seen: agg.records_seen,
                    records_dropped_at_source: agg.records_dropped,
                    records_oversized: agg.records_oversized,
                }
            })
            .collect();
        Ok(DeliveryReport {
            totals: DeliveryTotals::over(&rows),
            rows,
            orphan_batches,
        })
    }
}

/// The fleet's delivery totals: the rows' sums, and `captured / expected` where anything is
/// expected.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryTotals {
    /// Σ `segments_expected` over the rows.
    pub segments_expected: u64,
    /// Σ `segments_captured`.
    pub segments_captured: u64,
    /// Σ `segments_declared_dropped`.
    pub segments_declared_dropped: u64,
    /// Σ `segments_missing_unexplained`.
    pub segments_missing_unexplained: u64,
    /// `captured / expected`; `None` while nothing is expected, never a default to 1.0, which
    /// would render an empty install as perfect delivery.
    pub fraction: Option<f64>,
    /// Σ `records_seen`.
    pub records_seen: u64,
    /// Σ `records_dropped_at_source`.
    pub records_dropped_at_source: u64,
    /// Σ `records_oversized`.
    pub records_oversized: u64,
    /// The newest `last_seen_ms` across the rows (wall domain); `None` before any arrival.
    pub as_of_ms: Option<f64>,
}

impl DeliveryTotals {
    fn over(rows: &[DeliveryRow]) -> Self {
        let sum = |f: fn(&DeliveryRow) -> u64| rows.iter().map(f).sum::<u64>();
        let expected = sum(|r| r.segments_expected);
        let captured = sum(|r| r.segments_captured);
        #[allow(clippy::cast_precision_loss)] // segment counts are far below 2^52
        let fraction = (expected > 0).then(|| captured as f64 / expected as f64);
        Self {
            segments_expected: expected,
            segments_captured: captured,
            segments_declared_dropped: sum(|r| r.segments_declared_dropped),
            segments_missing_unexplained: sum(|r| r.segments_missing_unexplained),
            fraction,
            records_seen: sum(|r| r.records_seen),
            records_dropped_at_source: sum(|r| r.records_dropped_at_source),
            records_oversized: sum(|r| r.records_oversized),
            as_of_ms: rows
                .iter()
                .filter_map(|r| r.last_seen_ms)
                .fold(None, |a, b| Some(a.map_or(b, |a: f64| a.max(b)))),
        }
    }
}

#[derive(Default)]
struct RunAgg {
    captured: u64,
    max_seq_arrived: Option<i64>,
    records_seen: u64,
    records_dropped: u64,
    records_oversized: u64,
    first_seen_ms: Option<f64>,
    last_seen_ms: Option<f64>,
    instances: Vec<String>,
    declared: u64,
    max_seq_declared: Option<i64>,
}

/// The whole fleet's delivery ledger: per-run rows, their totals, and the population no row
/// can carry.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryReport {
    /// The rows' sums.
    pub totals: DeliveryTotals,
    /// Sorted by incarnation — total order, so the read is stable across calls.
    pub rows: Vec<DeliveryRow>,
    /// Arrivals with no sequence claim (incarnation unattributed) — coverage-indeterminate,
    /// counted so a reader knows the rows above are not the whole story when it is nonzero.
    pub orphan_batches: u64,
}

/// One producer run's delivery accounting, since install.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryRow {
    /// The producer run — the tap's run identity, this ledger's key (never the instance).
    pub incarnation: String,
    /// Every instance this run's segments named, sorted. Empty when no segment named one.
    pub instances: Vec<String>,
    /// Wall domain, like every lifecycle figure. Absent for a run known only through a drop
    /// declaration — never another run's clock, never the epoch.
    pub first_seen_ms: Option<f64>,
    /// Wall domain, like `first_seen_ms`: the newest record instant this run's segments carried.
    pub last_seen_ms: Option<f64>,
    /// `max(known seq) + 1` — the span the tap itself numbered, nothing past its last word.
    pub segments_expected: u64,
    /// Segments with an arrival row in the ledger — distinct sequences, clamped to `expected`.
    pub segments_captured: u64,
    /// Losses the tap confessed to, clamped so the three parts partition `expected`.
    pub segments_declared_dropped: u64,
    /// The remainder: in flight, or lost where nobody said so.
    pub segments_missing_unexplained: u64,
    /// The tap's own cumulative envelope marks — the highest-sequence segment's word.
    pub records_seen: u64,
    /// Records the tap itself dropped before shipping, by its own envelope mark (`dropped`).
    pub records_dropped_at_source: u64,
    /// Records the tap refused for size, by its own envelope mark (`oversized`).
    pub records_oversized: u64,
}
