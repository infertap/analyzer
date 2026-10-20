// SPDX-License-Identifier: AGPL-3.0-only

//! The loop's own accounting.
//!
//! Process-lifetime counters, shared between the work loop and the health sampler, which
//! run on separate threads. Atomics rather than a lock: the sampler must never wait on the
//! loop to read them, since the whole reason it holds its own `Engine` is so it can observe
//! while the loop is busy.
//!
//! **Nothing here is persisted.** A counter rebuilt from disk across a restart would be a
//! gauge wearing a counter's name, and every rate computed from it would be wrong in the
//! direction nobody checks. A reset is what a restart means to a scraper.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use analyzer_serving_clickhouse::selfhealth::{LoopStatus, ENVELOPE_WINDOW_CYCLES};

/// Sentinel for "no tick has completed in this process yet". `u64::MAX` rather than 0,
/// because 0 is a legitimate elapsed-seconds value in the first second of a daemon's life.
const NEVER: u64 = u64::MAX;

/// Shared, atomic, cheap to read from the sampler thread.
#[derive(Debug)]
pub struct Counters {
    started: Instant,
    ticks: AtomicU64,
    tick_failures: AtomicU64,
    files_ingested: AtomicU64,
    /// Bytes and records actually taken in, process-lifetime. Together they carry the
    /// fleet's observed events-per-byte, which is what turns the backlog's byte gauge into
    /// an events estimate: a derived figure, absent until both are warm, never a claim
    /// about bytes nobody has decoded yet.
    bytes_ingested: AtomicU64,
    records_ingested: AtomicU64,
    files_projected: AtomicU64,
    reclaimed_bytes: AtomicU64,
    ingest_refused: AtomicU64,
    /// Blocks the sampled audit re-derived.
    audit_blocks: AtomicU64,
    /// The audit's first disagreement, epoch milliseconds as `f64` bits; `NEVER` while none.
    audit_disagreed_at_ms: AtomicU64,
    /// Evicts the evict audit graded, and of those mismatched and underresolved.
    evicts_audited: AtomicU64,
    evicts_mismatched: AtomicU64,
    evicts_underresolved: AtomicU64,
    /// Milliseconds; `NEVER` until a tick completes.
    last_tick_duration_ms: AtomicU64,
    /// Seconds since `started` at which the last SUCCESSFUL tick finished; `NEVER` until
    /// one does. Stored as an offset rather than an `Instant` so it fits in an atomic.
    last_tick_end_secs: AtomicU64,
    /// Records ingested by the MOST RECENT ingest cycle; `NEVER` until one runs.
    ///
    /// **Separate from `pending` because the two answer different questions and `pending` is
    /// drained.** `pending` accumulates until a tick reports it; this is the last cycle's own
    /// figure, kept as a gauge so the envelope check can divide it by the ingest interval and
    /// get the fleet's arrival rate. A drained accumulator cannot answer "how fast right now".
    last_cycle_records: AtomicU64,
    /// Milliseconds spent ingesting, for the last [`ENVELOPE_WINDOW_CYCLES`] STEADY-STATE cycles.
    ///
    /// **Catch-up cycles are excluded, and the test for one needs no constant.** A cycle whose
    /// ingest ran longer than its own cadence cannot have been steady state: by construction it
    /// consumed more than one interval's worth of arrivals. That is what the first cycle after a
    /// restart does — it takes in everything that accumulated while the daemon was down — and
    /// letting it into this window would put a saturated sample in the utilisation figure for a
    /// reason that has nothing to do with capacity.
    ///
    /// **A window rather than the last value, because one slow cycle is not a capacity problem.**
    /// A backfill, a momentary slow bucket or a stalled listing can saturate a single cycle; if
    /// the envelope judged on that alone it would warn, clear, and warn again — and a surface
    /// that flaps is one operators learn to ignore, which costs more than the signal is worth.
    /// Sustained utilisation is the thing worth acting on, so sustained is what is measured.
    recent_ingest_ms: Mutex<VecDeque<u64>>,
    /// Cycles that ran over their own cadence — catch-up rather than steady state. A count, not
    /// a verdict: an operator reading one after a restart is reading a fact about a restart.
    catchup_cycles: AtomicU64,
    /// What the last cycle's listing found deferred past the slice deadline, before a byte
    /// was fetched. Facts, not estimates: `bucket::discover` returns every pending object
    /// with its size before ingest begins. Nothing here divides it by a rate to manufacture
    /// a time.
    last_backlog_files: AtomicU64,
    last_backlog_bytes: AtomicU64,
    /// Ingest accounting not yet reported by a successful tick.
    pending: Mutex<PendingIngest>,
}

/// What one ingest cycle found and cost, as the counters need it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestCycle {
    /// Wall time the cycle's ingest took, listing through its last fetch.
    pub elapsed: Duration,
    /// The cycle's ingest cadence, seconds; `elapsed` past it makes the cycle catch-up.
    pub interval: Duration,
    /// Of those, what the slice deadline left unfetched — the cycle's REAL backlog,
    /// and what the backlog gauges export.
    pub files_deferred: u64,
    /// Their total size — the byte gauge the backlog's events estimate is scaled from.
    pub bytes_deferred: u64,
}

/// The two cadences a snapshot states beside its rates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cadences {
    /// How often the tick runs.
    pub tick: Duration,
    /// How often the ingest cycle runs.
    pub ingest: Duration,
}

/// What has been ingested since the last successful tick. Ingest and the tick run on
/// different cadences, so the only coherent meaning of "this tick's ingest" is everything
/// ingested since the previous one: accumulated here across cycles, drained by a tick that
/// succeeds. A failed tick puts them back, so nothing is silently forgotten.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PendingIngest {
    /// Objects the listings returned, summed since the last report.
    pub files_seen: u64,
    /// Records inserted, summed since the last report.
    pub records: u64,
    /// Records of a kind this build does not know, counted and passed over.
    pub unknown_kinds: u64,
    /// Objects skipped because the ledger already held their content hash.
    pub duplicates_skipped: u64,
}

impl PendingIngest {
    fn add(&mut self, other: Self) {
        self.files_seen += other.files_seen;
        self.records += other.records;
        self.unknown_kinds += other.unknown_kinds;
        self.duplicates_skipped += other.duplicates_skipped;
    }
}

impl Counters {
    /// Fresh counters: every total zero, the gauges at `NEVER`, `started` at now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            ticks: AtomicU64::new(0),
            tick_failures: AtomicU64::new(0),
            files_ingested: AtomicU64::new(0),
            bytes_ingested: AtomicU64::new(0),
            records_ingested: AtomicU64::new(0),
            files_projected: AtomicU64::new(0),
            reclaimed_bytes: AtomicU64::new(0),
            ingest_refused: AtomicU64::new(0),
            audit_blocks: AtomicU64::new(0),
            audit_disagreed_at_ms: AtomicU64::new(NEVER),
            evicts_audited: AtomicU64::new(0),
            evicts_mismatched: AtomicU64::new(0),
            evicts_underresolved: AtomicU64::new(0),
            last_cycle_records: AtomicU64::new(NEVER),
            recent_ingest_ms: Mutex::new(VecDeque::with_capacity(ENVELOPE_WINDOW_CYCLES)),
            catchup_cycles: AtomicU64::new(0),
            last_backlog_files: AtomicU64::new(0),
            last_backlog_bytes: AtomicU64::new(0),
            last_tick_duration_ms: AtomicU64::new(NEVER),
            last_tick_end_secs: AtomicU64::new(NEVER),
            pending: Mutex::new(PendingIngest::default()),
        }
    }

    /// Count a tick START, so a loop that never succeeds still shows ticks climbing.
    pub fn tick_started(&self) {
        self.ticks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a tick's sampled audit: the blocks it re-derived, and where the audit stands.
    pub fn audited(&self, blocks: u64, disagreed_at_ms: Option<f64>) {
        self.audit_blocks.fetch_add(blocks, Ordering::Relaxed);
        self.audit_disagreed_at_ms.store(
            disagreed_at_ms.map_or(NEVER, f64::to_bits),
            Ordering::Relaxed,
        );
    }

    /// Record a tick's evict audit.
    pub fn evicts_audited(&self, outcome: &analyzer_analysis::differential::AuditOutcome) {
        self.evicts_audited
            .fetch_add(outcome.audited(), Ordering::Relaxed);
        self.evicts_mismatched
            .fetch_add(outcome.mismatched(), Ordering::Relaxed);
        self.evicts_underresolved
            .fetch_add(outcome.underresolved(), Ordering::Relaxed);
    }

    /// Count a failed tick. Lag and duration keep their values: a failure clears nothing.
    pub fn tick_failed(&self) {
        self.tick_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful tick and its wall duration.
    pub fn tick_succeeded(&self, duration: Duration) {
        self.last_tick_duration_ms.store(
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.last_tick_end_secs
            .store(self.started.elapsed().as_secs(), Ordering::Relaxed);
    }

    /// Add `n` objects taken in — moved per file as the cycle walks, not at cycle end.
    pub fn files_ingested(&self, n: u64) {
        self.files_ingested.fetch_add(n, Ordering::Relaxed);
    }

    /// Bytes fetched and taken in — accumulated at cycle end from the report's own
    /// `bytes_seen`, the listing's `_size` of every object this run fetched.
    pub fn bytes_ingested(&self, n: u64) {
        self.bytes_ingested.fetch_add(n, Ordering::Relaxed);
    }

    /// Files the projected walk prepared, accumulated at cycle end from the report: the
    /// composition of a cycle is settled when the cycle is.
    pub fn files_projected(&self, n: u64) {
        self.files_projected.fetch_add(n, Ordering::Relaxed);
    }

    /// Add bytes a forced merge returned: the fall in unreclaimed bytes across one `OPTIMIZE`.
    pub fn reclaimed(&self, bytes: u64) {
        self.reclaimed_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Count one cycle whose intake the pressure ladder refused (the `Refuse` rung).
    pub fn ingest_refused(&self) {
        self.ingest_refused.fetch_add(1, Ordering::Relaxed);
    }

    /// Fold one ingest cycle's accounting into what the next report will describe:
    /// what the cycle found waiting and what it cost.
    ///
    /// **Splits steady state from catch-up on a fact rather than a threshold**: a cycle that ran
    /// longer than its own cadence took in more than one interval's arrivals, so it was not
    /// steady state and does not belong in the utilisation window.
    pub fn record_ingest_cycle(&self, observed: IngestCycle) {
        self.last_backlog_files
            .store(observed.files_deferred, Ordering::Relaxed);
        self.last_backlog_bytes
            .store(observed.bytes_deferred, Ordering::Relaxed);

        let ms = u64::try_from(observed.elapsed.as_millis()).unwrap_or(u64::MAX);
        if !observed.interval.is_zero() && observed.elapsed > observed.interval {
            self.catchup_cycles.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Ok(mut window) = self.recent_ingest_ms.lock() {
            if window.len() == ENVELOPE_WINDOW_CYCLES {
                window.pop_front();
            }
            window.push_back(ms);
        }
    }

    /// Fold one cycle's records in: the last-cycle gauge is SET; the totals and `pending` add.
    pub fn record_ingest(&self, cycle: PendingIngest) {
        // The gauge is SET, not accumulated: it describes the cycle that just ran, which is what
        // an arrival rate is computed from. `pending` below accumulates for a different consumer.
        self.last_cycle_records
            .store(cycle.records, Ordering::Relaxed);
        self.records_ingested
            .fetch_add(cycle.records, Ordering::Relaxed);
        if let Ok(mut slot) = self.pending.lock() {
            slot.add(cycle);
        }
    }

    /// Take what the next report should describe, leaving the accumulator empty.
    #[must_use]
    pub fn take_ingest(&self) -> PendingIngest {
        self.pending
            .lock()
            .map(|mut slot| std::mem::take(&mut *slot))
            .unwrap_or_default()
    }

    /// Put taken counts back when the tick that took them failed. Only `pending` moves: the
    /// process totals and the last-cycle gauge already counted these records once.
    pub fn restore_ingest(&self, counts: PendingIngest) {
        if let Ok(mut slot) = self.pending.lock() {
            slot.add(counts);
        }
    }

    /// Cycles refused intake since process start.
    #[must_use]
    pub fn refusals(&self) -> u64 {
        self.ingest_refused.load(Ordering::Relaxed)
    }

    /// Snapshot for the export surface.
    ///
    /// `engine_queries` is passed in rather than held here because it belongs to the
    /// `Engine`s, and the daemon holds several. The exported figure is their sum: a total
    /// that counted only one would understate the engine work this process causes.
    #[must_use]
    pub fn snapshot(&self, engine_queries: u64, cadences: Cadences) -> LoopStatus {
        let ingest_interval_secs = cadences.ingest.as_secs_f64();
        // Records the last cycle took in, over the cadence it runs on: the fleet's arrival
        // rate, which is the quantity the envelope was always about. Absent until a cycle
        // has run, because a rate over no observation is not zero.
        let last_records = self.last_cycle_records.load(Ordering::Relaxed);
        let ingest_rows_per_second = if last_records == NEVER || ingest_interval_secs <= 0.0 {
            None
        } else {
            #[allow(clippy::cast_precision_loss)] // a cycle's record count is far inside f64
            Some(last_records as f64 / ingest_interval_secs)
        };
        // The MEAN fraction of the ingest cadence spent ingesting, over a full window: this
        // machine's own sustained utilisation, needing no ceiling constant to interpret.
        //
        // **Averaged over UP TO the window, not only over a full one.** The figure exists from
        // the first cycle, because an operator watching a fresh deployment should see what it is
        // doing rather than a blank. How many cycles it averages rides beside it, and the check —
        // not this — decides what a partial window may be used to claim.
        let (ingest_duty_cycle, ingest_duty_samples) = self
            .recent_ingest_ms
            .lock()
            .ok()
            .filter(|w| !w.is_empty() && ingest_interval_secs > 0.0)
            .map_or((None, 0), |w| {
                #[allow(clippy::cast_precision_loss)] // milliseconds of a cycle; far inside f64
                let mean_ms = w.iter().sum::<u64>() as f64 / w.len() as f64;
                (Some((mean_ms / 1000.0) / ingest_interval_secs), w.len())
            });
        let duration = self.last_tick_duration_ms.load(Ordering::Relaxed);
        let end = self.last_tick_end_secs.load(Ordering::Relaxed);
        // The backlog's events ESTIMATE: deferred bytes scaled by the observed events-per-byte
        // of everything this process has actually decoded. Absent until both counters are warm
        // — a ratio over nothing is not zero — and always beside the byte gauge it derives
        // from, never in place of it.
        let bytes_ingested_total = self.bytes_ingested.load(Ordering::Relaxed);
        let records_ingested_total = self.records_ingested.load(Ordering::Relaxed);
        let backlog_bytes = self.last_backlog_bytes.load(Ordering::Relaxed);
        #[allow(clippy::cast_precision_loss)] // byte and record totals sit far inside f64
        let ingest_backlog_events_estimate =
            (bytes_ingested_total > 0 && records_ingested_total > 0).then(|| {
                backlog_bytes as f64 * (records_ingested_total as f64 / bytes_ingested_total as f64)
            });
        LoopStatus {
            ticks_total: self.ticks.load(Ordering::Relaxed),
            tick_failures_total: self.tick_failures.load(Ordering::Relaxed),
            files_ingested_total: self.files_ingested.load(Ordering::Relaxed),
            bytes_ingested_total,
            records_ingested_total,
            ingest_backlog_events_estimate,
            files_projected_total: self.files_projected.load(Ordering::Relaxed),
            reclaimed_bytes_total: self.reclaimed_bytes.load(Ordering::Relaxed),
            engine_queries_total: engine_queries,
            #[allow(clippy::cast_precision_loss)] // milliseconds; exact far past any tick
            last_tick_duration_seconds: (duration != NEVER).then(|| duration as f64 / 1000.0),
            #[allow(clippy::cast_precision_loss)] // seconds since start; exact for millennia
            tick_lag_seconds: (end != NEVER)
                .then(|| self.started.elapsed().as_secs().saturating_sub(end) as f64),
            tick_interval_seconds: cadences.tick.as_secs_f64(),
            // overwritten by the daemon, which owns the horizon; the counters do not
            effective_retention_days: 0.0,
            ingest_refused_total: self.ingest_refused.load(Ordering::Relaxed),
            ingest_refusing: false,
            ingest_rows_per_second,
            ingest_duty_cycle,
            ingest_duty_samples,
            ingest_catchup_cycles: self.catchup_cycles.load(Ordering::Relaxed),
            ingest_backlog_files: self.last_backlog_files.load(Ordering::Relaxed),
            ingest_backlog_bytes: self.last_backlog_bytes.load(Ordering::Relaxed),
            audit_blocks_total: self.audit_blocks.load(Ordering::Relaxed),
            audit_disagreed_at_ms: Some(self.audit_disagreed_at_ms.load(Ordering::Relaxed))
                .filter(|bits| *bits != NEVER)
                .map(f64::from_bits),
            evict_audited_total: self.evicts_audited.load(Ordering::Relaxed),
            evict_mismatched_total: self.evicts_mismatched.load(Ordering::Relaxed),
            evict_underresolved_total: self.evicts_underresolved.load(Ordering::Relaxed),
        }
    }
}

impl Default for Counters {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{Cadences, Counters, IngestCycle, PendingIngest};
    use std::time::Duration;

    /// A tick on every minutely ingest, the shipped defaults.
    const CADENCES: Cadences = Cadences {
        tick: Duration::from_mins(1),
        ingest: Duration::from_mins(1),
    };

    fn cycle(secs: f64, interval_secs: u64) -> IngestCycle {
        IngestCycle {
            elapsed: Duration::from_secs_f64(secs),
            interval: Duration::from_secs(interval_secs),
            files_deferred: 3,
            bytes_deferred: 4096,
        }
    }

    #[test]
    fn a_cycle_that_ran_over_its_cadence_is_catch_up_and_never_enters_the_utilisation_figure() {
        // **The split needs no threshold, which is why it is trustworthy.** A cycle whose ingest
        // took longer than the interval it runs on consumed more than one interval's arrivals —
        // by construction, not by a tuned rule. That is exactly what the first cycle after a
        // restart does, and letting it into the window would put a saturated sample into a
        // capacity figure for a reason that has nothing to do with capacity.
        let c = Counters::new();
        c.record_ingest_cycle(cycle(600.0, 60)); // ten minutes of backfill on a one-minute cadence
        let s = c.snapshot(0, CADENCES);

        assert_eq!(
            s.ingest_catchup_cycles, 1,
            "the catch-up is counted as a fact"
        );
        assert_eq!(
            s.ingest_duty_samples, 0,
            "and contributes NO sample to utilisation"
        );
        assert!(s.ingest_duty_cycle.is_none());
    }

    #[test]
    fn steady_cycles_average_over_up_to_the_window() {
        // Averaged over what has been seen, from the first cycle — a fresh deployment shows what
        // it is doing rather than a blank. The VERDICT waiting for a full window is the check's
        // decision, not this one's.
        let c = Counters::new();
        c.record_ingest_cycle(cycle(6.0, 60));
        c.record_ingest_cycle(cycle(12.0, 60));
        let s = c.snapshot(0, CADENCES);

        assert_eq!(s.ingest_duty_samples, 2);
        let duty = s.ingest_duty_cycle.expect("two steady cycles average");
        assert!(
            (duty - 0.15).abs() < 1e-9,
            "mean of 10% and 20% is 15%, got {duty}"
        );
        assert_eq!(s.ingest_catchup_cycles, 0);
    }

    #[test]
    fn the_backlog_is_what_the_slice_deadline_left() {
        // Facts from the walk's own tally, not estimates. Recorded for a catch-up cycle
        // too — that is precisely when an operator wants to know how much is waiting.
        let c = Counters::new();
        c.record_ingest_cycle(cycle(600.0, 60));
        let s = c.snapshot(0, CADENCES);
        assert_eq!(s.ingest_backlog_files, 3);
        assert_eq!(s.ingest_backlog_bytes, 4096);
    }

    #[test]
    fn the_window_keeps_only_its_last_cycles() {
        let c = Counters::new();
        for _ in 0..(super::ENVELOPE_WINDOW_CYCLES + 5) {
            c.record_ingest_cycle(cycle(6.0, 60));
        }
        let s = c.snapshot(0, CADENCES);
        assert_eq!(s.ingest_duty_samples, super::ENVELOPE_WINDOW_CYCLES);
    }

    #[test]
    fn ingest_records_still_accumulate_for_the_report_block() {
        // The two accountings are separate on purpose: `pending` is drained by a successful tick,
        // the window is not.
        let c = Counters::new();
        c.record_ingest(PendingIngest {
            files_seen: 2,
            records: 100,
            unknown_kinds: 0,
            duplicates_skipped: 1,
        });
        assert_eq!(c.take_ingest().records, 100);
    }

    #[test]
    fn a_failed_tick_restores_pending_without_touching_the_totals() {
        // The totals and the arrival gauge counted these records when the cycle landed
        // them; a restore is bookkeeping for the next report, not a second landing.
        let c = Counters::new();
        c.record_ingest(PendingIngest {
            files_seen: 1,
            records: 100,
            unknown_kinds: 0,
            duplicates_skipped: 0,
        });
        let before = c.snapshot(0, CADENCES);
        let taken = c.take_ingest();
        c.restore_ingest(taken);
        let after = c.snapshot(0, CADENCES);
        assert_eq!(after.records_ingested_total, 100);
        assert_eq!(after.records_ingested_total, before.records_ingested_total);
        assert_eq!(after.ingest_rows_per_second, before.ingest_rows_per_second);
        assert_eq!(
            c.take_ingest().records,
            100,
            "the restore put the count back"
        );
    }

    #[test]
    fn a_loop_that_has_not_ticked_yet_reports_absent_not_zero() {
        // Zero seconds of lag would mean "a tick just finished", which is the opposite of
        // the truth on a daemon that has not completed one. The gauge law applies to these
        // two fields even though the counters beside them start at 0.
        let c = Counters::new();
        let s = c.snapshot(0, CADENCES);
        assert_eq!(s.tick_lag_seconds, None);
        assert_eq!(s.last_tick_duration_seconds, None);
        // the COUNTERS, by contrast, are legitimately zero from process start
        assert_eq!(s.ticks_total, 0);
        assert_eq!(s.files_ingested_total, 0);
    }

    #[test]
    fn a_completed_tick_produces_a_duration_and_a_lag() {
        let c = Counters::new();
        c.tick_started();
        c.tick_succeeded(Duration::from_millis(31_500));
        let s = c.snapshot(7, CADENCES);
        assert_eq!(s.ticks_total, 1);
        assert_eq!(s.last_tick_duration_seconds, Some(31.5));
        assert!(s.tick_lag_seconds.is_some(), "lag exists once a tick lands");
        assert_eq!(s.engine_queries_total, 7);
    }

    #[test]
    fn failures_are_counted_separately_from_starts() {
        // ticks_total counts STARTS, so a failing loop shows both climbing and the ratio
        // is readable. A failures-only counter would hide a loop that never succeeds.
        let c = Counters::new();
        c.tick_started();
        c.tick_failed();
        let s = c.snapshot(0, CADENCES);
        assert_eq!((s.ticks_total, s.tick_failures_total), (1, 1));
        assert_eq!(s.tick_lag_seconds, None, "a failed tick does not clear lag");
    }
}
