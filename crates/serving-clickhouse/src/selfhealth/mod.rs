// SPDX-License-Identifier: AGPL-3.0-only

//! Self-health: the analyzer's OWN vitals, not the fleet's (design, amendment).
//!
//! Why this exists in the shape it does: the deployment target is an airgapped customer
//! environment emitting nothing to us. Whatever this module measures is the only signal
//! that will ever exist about whether the system is maintaining itself — so it is built
//! as a MEASUREMENT SURFACE with three consumers, not as a report section:
//!
//! 1. the maintenance loop reads it as control input (when to compact, when to force a
//!    merge, when to apply backpressure, when to refuse rather than fill a disk);
//! 2. `report.md` renders it for a human;
//! 3. [`SelfHealth::to_prometheus`] emits it for the customer's own dashboard and
//!    alarms — a textfile-collector rendering, so it needs NO listener and trips no
//!    static-audit fence until a scrape endpoint is deliberately built.
//!
//! Consequences of that, held deliberately:
//!
//! - **Metric names are API.** Once a customer's alarm is keyed to a name, renaming it
//!   is a silent breakage in an environment we cannot see. `METRIC_NAMES` is a ratchet
//!   test, the SQL-ratchet pattern applied to the export surface.
//! - **Thresholds are labeled constants with stated derivations**, customer-overridable
//!   in their own terms — the `ACTION_THRESHOLD_DEFAULT` discipline. The parts
//!   thresholds are not invented: they are the engine's own verified `parts_to_delay_
//!   insert` / `parts_to_throw_insert` limits at the pin.
//! - **Self-health is not a figure.** The derived figures are byte-deterministic by
//!   construction; these are environment-dependent by nature (free disk, wall clock). They land
//!   as their own artifacts, never mixed with the figures.
//! - **No health history table.** Trends belong to whatever time-series store the
//!   customer scrapes into; adding a persistent table here would answer an
//!   unbounded-growth problem by creating another surface. One sample per run, stateless.
//!
//! Collection is ONE engine invocation (per-thing round trips are how a sampler on a cadence
//! dies slowly) plus filesystem stats over directories that are already bounded
//! or are themselves the thing being measured.

use analyzer_pipeline::Error;
use std::path::Path;

use crate::engine::Engine;
use crate::row::Row;

mod checks;
mod render;
#[cfg(test)]
mod tests;

use checks::{
    check_audit, check_dead_parts, check_disk, check_envelope, check_evict_resolution,
    check_mutations, check_orphans, check_parts, check_retention, check_server, check_tick_lag,
    f64_from,
};
pub use checks::{
    DEAD_PART_RATIO_WARN, DISK_FREE_FRACTION_CRITICAL, DISK_FREE_FRACTION_WARN,
    ENVELOPE_UTILISATION_WARN, ENVELOPE_WINDOW_CYCLES, ORPHAN_TEMP_AGE_WARN_SECONDS,
    PARTS_PER_PARTITION_CRITICAL, PARTS_PER_PARTITION_WARN, RAW_TTL_OVERDUE_FACTOR,
    TICK_LAG_WARN_CADENCES,
};
pub use render::{write_health_artifacts, HealthArtifacts};

/// The exported series a caller reads back by name: the disk gauges the pressure ladder decides
/// on. Each is one of [`METRIC_NAMES`]; naming them here keeps a reader and
/// the exporter on one spelling.
pub mod metric {
    /// Free bytes on the data volume.
    pub const DISK_FREE_BYTES: &str = "infertap_analyzer_disk_free_bytes";
    /// The data volume's size.
    pub const DISK_TOTAL_BYTES: &str = "infertap_analyzer_disk_total_bytes";
}

/// Every metric name this build exports, in emission order. **This list is API**: an
/// alarm in an environment we cannot see may be keyed to any of these, so a name may be
/// ADDED (additive, safe) but never renamed or removed without a deliberate, stated
/// migration. Pinned by `selfhealth::metric_names_are_a_stable_export_surface`.
pub const METRIC_NAMES: &[&str] = &[
    // identity carrier: the value is always 1, the labels are the payload (the standard
    // _info pattern — joined against in a query, never alarmed on directly). Carries
    // analyzer_id ONLY when the operator set one.
    "infertap_analyzer_info",
    // **When this sample was taken, and it is the series that makes every series below it
    // trustworthy.** Everything else here describes the system; this one describes the
    // SAMPLE, and without it a frozen document is indistinguishable from a healthy one
    // whose figures happen not to be moving. `unreachable` already covers a dead ENGINE —
    // the series go absent and a check says why — but nothing covered a dead or wedged
    // EMITTER, which presents as every value holding its last reading forever while the
    // scrape endpoint keeps answering 200. Alarm on `time() - this > 3 × scrape interval`.
    // Same role node_exporter's `node_textfile_mtime_seconds` plays for its textfiles.
    "infertap_analyzer_sample_timestamp_seconds",
    "infertap_analyzer_active_part_bytes",
    "infertap_analyzer_active_parts",
    "infertap_analyzer_data_dir_bytes",
    "infertap_analyzer_unreclaimed_bytes",
    "infertap_analyzer_dead_part_ratio",
    "infertap_analyzer_max_parts_per_partition",
    "infertap_analyzer_unfinished_mutations",
    metric::DISK_FREE_BYTES,
    metric::DISK_TOTAL_BYTES,
    "infertap_analyzer_disk_free_fraction",
    "infertap_analyzer_ledger_rows",
    "infertap_analyzer_raw_oldest_age_days",
    "infertap_analyzer_raw_retention_days",
    "infertap_analyzer_last_ingest_age_seconds",
    "infertap_analyzer_output_dir_files",
    "infertap_analyzer_output_dir_bytes",
    "infertap_analyzer_orphan_temp_files",
    "infertap_analyzer_orphan_temp_bytes",
    // supplied by the process that OWNS the engine, because only it holds the handle:
    // unmeasured wherever no server is owned (the build harness), 0 when one is owned
    // and dead. The distinction is the point — see `check_server`.
    "infertap_analyzer_server_up",
    "infertap_analyzer_server_uptime_seconds",
    // The process's own memory, sampled live — added after a 2.35 GiB/min anon climb
    // ran invisible for hours because store, cache and disk were sampled and the one
    // number that moved was not. Absent (not zero) where /proc and the cgroup files
    // are unreadable — non-Linux dev hosts, containers without the v2 layout.
    "infertap_analyzer_process_rss_bytes",
    "infertap_analyzer_cgroup_anon_bytes",
    // the loop, supplied for the same reason the server figures are: only the process
    // running the loop knows them, and a process that runs no loop says nothing about one.
    // The `_total` names are COUNTERS — process-lifetime, reset on
    // restart, deliberately not persisted.
    "infertap_analyzer_ticks_total",
    "infertap_analyzer_tick_failures_total",
    "infertap_analyzer_files_ingested_total",
    "infertap_analyzer_bytes_ingested_total",
    "infertap_analyzer_records_ingested_total",
    "infertap_analyzer_files_projected_total",
    "infertap_analyzer_reclaimed_bytes_total",
    "infertap_analyzer_engine_queries_total",
    "infertap_analyzer_tick_duration_seconds",
    "infertap_analyzer_tick_lag_seconds",
    "infertap_analyzer_ingest_refused_total",
    "infertap_analyzer_ingest_refusing",
    "infertap_analyzer_audit_blocks_total",
    "infertap_analyzer_audit_disagreed",
    "infertap_analyzer_evict_audit_audited_total",
    "infertap_analyzer_evict_audit_mismatched_total",
    "infertap_analyzer_evict_audit_underresolved_total",
    "infertap_analyzer_effective_retention_days",
    // The can't-keep-up signal: the mean fraction of the ingest cadence
    // spent ingesting, 1.0 = saturated. The backlog gauges stand BESIDE it since,
    // when the slice deadline landed in `bucket::ingest_discovered` and
    // broke duty's dominance (a backlog can now grow while duty sits low) — the
    // dependency the original cut stated, honored in the same change.
    "infertap_analyzer_ingest_duty_cycle",
    "infertap_analyzer_ingest_backlog_files",
    "infertap_analyzer_ingest_backlog_bytes",
    "infertap_analyzer_ingest_backlog_events_estimate",
    // ── The fleet's own stamped figures ──────────────────────────────────────────────
    // The numbers the console's document states, exported so the customer's Alertmanager
    // can watch them: "page me past 5%" binds here. Tick-settled — absent until a tick
    // is applied, refreshed at the tick cadence, computed through the SAME DAO reads the
    // console serves so the glass and the alarms cannot drift. Freshness alarms judge
    // against the frontier timestamp below, never against the scrape clock. The meaning
    // pair rides `infertap_fleet_meaning_info` (the `_info` join pattern established by
    // `infertap_analyzer_info`): figures either side of a discriminator change are
    // different quantities, and the join is where a dashboard sees the seam.
    "infertap_fleet_meaning_info",
    "infertap_fleet_recompute_tokens",
    "infertap_fleet_stored_tokens",
    "infertap_fleet_recompute_fraction",
    "infertap_fleet_coverage_fraction",
    "infertap_fleet_capture_fraction",
    "infertap_fleet_segments_missing_unexplained",
    "infertap_fleet_frontier_timestamp_seconds",
    // Producer RUNS by spec 5.8 staleness verdict, over runs seen in the last seven days —
    // the partial-silence alarm a fleet-global age cannot express: at 100 nodes, one
    // chatty tap keeps `last_ingest_age_seconds` fresh while 99 sit dark. Runs, never
    // taps: staleness is per run, a restarted tap is a new run,
    // and a name saying "taps" would miscount the moment a fleet rolled a deploy. The
    // four names are the WIRE's own state words, so the console's chips and these
    // gauges cannot drift into two vocabularies. `silent_unexplained` (past its own
    // declared bound, no stop record) is the page; `departed` is a clean stop so a
    // rolling deploy does not page. The horizon is KNOWLEDGE, not wall clock: partial
    // silence reads here as living taps advance it, total silence freezes it and reads
    // on `last_ingest_age_seconds` — the pair splits the failure space exactly.
    // Derived through the same DAO roster read the console serves.
    "infertap_fleet_producer_runs_reporting",
    "infertap_fleet_producer_runs_silent_unexplained",
    "infertap_fleet_producer_runs_departed",
    "infertap_fleet_producer_runs_ungradeable",
    // emitted by the renderer rather than collected: how many of the above this sample
    // could not measure
    "infertap_analyzer_metrics_unmeasured",
];

/// Worst-wins severity. `Unknown` is never silently `Ok`: a figure the analyzer could
/// not measure is stated as unmeasured (fail-honest applied to itself).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    /// Within every threshold.
    Ok,
    /// The figure could not be measured; stated as such, never read as `Ok`.
    Unknown,
    /// A threshold crossed with room still left to act inside one retention cycle.
    Warn,
    /// The refuse-rather-than-wedge line: past it, recovery leaves the system's own hands.
    Critical,
}

impl Status {
    /// The status as the lowercase word `health.json` and the check lines carry.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Unknown => "unknown",
            Status::Warn => "warn",
            Status::Critical => "critical",
        }
    }
}

/// One exported gauge: a stable name, its unit-bearing help text, and a value that is
/// either a finite number or explicitly UNMEASURED.
///
/// No labels, deliberately. One analyzer watches one customer's estate, so any label we
/// could add would carry the same value on every series forever — and which GPUs feed
/// which analyzer is the customer's mapping to keep, not a claim for our metrics to
/// make. The only identity that is ours to state rides on `infertap_analyzer_info`.
///
/// `Option` rather than `f64` for a reason worth stating, since it looks like fussiness:
/// JSON cannot represent NaN or either infinity — `serde_json` renders all three as
/// `null` and returns success — and the Prometheus format demands `+Inf`/`-Inf`/`NaN`
/// literals that Rust's own formatting does not produce. Both renderings therefore have
/// a silent-corruption mode reachable from one bad division, in the single channel an
/// airgapped deployment has. Making unmeasured a TYPE rather than a sentinel means
/// neither renderer can be handed a value it cannot express, and the count of
/// unmeasured metrics is itself exported so the gap is alarmable.
#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    /// The series name, one of [`METRIC_NAMES`] — API once a customer's alarm is keyed to it.
    pub name: &'static str,
    /// The `# HELP` text: what the value is and its unit, in the scraper's own words.
    pub help: &'static str,
    /// The reading, always finite; `None` is UNMEASURED and counts toward `metrics_unmeasured`.
    pub value: Option<f64>,
    /// Gauge or counter, stated for the `# TYPE` line rather than inferred from the name.
    pub kind: MetricKind,
}

/// Gauge or counter, carried on the metric rather than inferred from the name.
///
/// The exposition format's `# TYPE` line is part of the contract a scraper reads: a
/// `_total` series declared `gauge` is a promtool lint failure and misleads anything that
/// reasons about resets. Inferring the kind from the `_total` suffix would work today and
/// break the first time a name needs to disagree with its suffix, so the kind is stated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// A reading that can move either way between samples.
    Gauge,
    /// A process-lifetime count that only rises and resets on restart — the `_total` series.
    Counter,
}

impl MetricKind {
    /// The kind as the exposition format's `# TYPE` word.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Gauge => "gauge",
            MetricKind::Counter => "counter",
        }
    }
}

/// One evaluated condition: what was checked, how it came out, and the sentence an
/// operator reads. The message always carries the numbers, never just a verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    /// The condition's stable name (`engine_reachable`, `retention_fits_disk`, …), API like a
    /// metric name once an alarm reads it.
    pub name: &'static str,
    /// How the condition came out; `Unknown` when the figure it needs was not measured.
    pub status: Status,
    /// The sentence an operator reads: always the numbers, never a bare verdict.
    pub message: String,
}

/// One sample of the analyzer's own vitals.
#[derive(Debug, Clone, PartialEq)]
pub struct SelfHealth {
    /// See [`Paths::analyzer_id`] — `None` means the export asserts no identity.
    pub analyzer_id: Option<String>,
    /// Which named body of telemetry the fleet-filtered figures were computed over.
    /// Run provenance for `health.json`, never a metric label: it is a constant for a
    /// deployment, and the fleet-to-analyzer mapping is the customer's to keep.
    pub fleet: String,
    /// The meaning pair the fleet figures were computed under, or `None` before a tick.
    /// Rendered as `infertap_fleet_meaning_info` — the `_info` join pattern — because the
    /// per-figure series carry no labels by this module's own law.
    pub meaning: Option<(String, String)>,
    /// Every exported series, in [`METRIC_NAMES`] order, measured or explicitly unmeasured.
    pub metrics: Vec<Metric>,
    /// Every condition evaluated over this sample, each with its verdict and its sentence.
    pub checks: Vec<Check>,
}

/// Filesystem stats for a directory the caller already walked — the records directory is
/// measured by the reader, which reads every candidate anyway, so self-health never
/// walks it a second time (that walk is O(directory), the exact cost class this build is
/// trying to bound).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DirStats {
    pub files: u64,
    pub bytes: u64,
}

/// What the collector needs from its caller: where the engine's data lives, where
/// artifacts are written, and the reader's already-measured view of the source
/// directory.
#[derive(Debug, Clone, Copy)]
pub struct Paths<'a> {
    /// The engine's database directory, measured directly — see `dir_bytes_recursive` (private).
    pub data_dir: &'a Path,
    /// The artifact output directory, where snapshots and `health.*` land; measured for the
    /// `output_dir_files` and `output_dir_bytes` gauges (snapshots accumulate one per run).
    pub out_dir: &'a Path,
    /// The ingest ledger retention horizon (`ledger_retention_days`), the one event-grain knob.
    pub ledger_retention_days: u32,
    /// Operator-supplied identifier for THIS analyzer. Absent by default and never
    /// derived: hostnames are not unique across an estate, a data-directory path is not
    /// unique across hosts, and a persisted UUID survives right up until
    /// replay-is-recovery rebuilds the data directory and silently mints a new one,
    /// orphaning every dashboard keyed to the old. The tap's law applies — a field the
    /// emitter cannot stand behind gets deleted, not guessed.
    ///
    /// It is needed in exactly one deployment: metric FILES from several analyzers
    /// shipped into one collector, where identical series would otherwise collide. In
    /// that case the operator is already configuring the pipeline and is the only party
    /// who knows what distinguishes the boxes. Everyone scraping directly gets correct
    /// disambiguation from Prometheus's own `job`/`instance` target labels.
    pub analyzer_id: Option<&'a str>,
    /// What the caller knows about the `clickhouse-server` it owns, or `None` when it
    /// owns none.
    ///
    /// Supplied rather than probed, and that is the design: only the process holding the
    /// handle can tell a server it started and lost from one that was never started, and
    /// a sensor that guessed would report the wrong one. The daemon fills this in;
    /// the build harness leaves it absent, which is why the metrics are unmeasured there
    /// instead of falsely zero.
    pub server: Option<ServerStatus>,
    /// What the daemon's loop knows about its own work, or `None` in a process that runs
    /// no loop.
    ///
    /// Same discipline as [`Paths::server`], for the same reason: a one-shot `report`
    /// invocation and the build harness have nothing to say about a cadence they do not
    /// run, so their series are absent rather than zero. A zero tick count would read as
    /// "the loop is wedged" on every harness run.
    pub loop_status: Option<LoopStatus>,
    /// The fleet's stamped figures, or `None` in a process that has not applied a tick.
    /// Supplied like the loop figures: only the daemon computes them, after a tick.
    pub fleet: Option<&'a FleetFigures>,
    /// Run-state counts, or `None` before the daemon's first ingest-cadence count.
    pub runs: Option<RunStates>,
}

/// The owned engine process, as its owner sees it.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct ServerStatus {
    /// Whether the server answered its last liveness probe.
    pub up: bool,
    /// Seconds since this server was started, absent when it is not running.
    pub uptime_seconds: Option<f64>,
}

/// Producer runs by staleness verdict, counted by the daemon at ingest cadence through
/// the same DAO roster read the console serves — one reader. Runs, never taps or
/// holders (three populations, never one): every run carries exactly one verdict, so
/// the four fields sum to the run count by construction. `None` in a process that has
/// not counted yet; a process that runs no loop says nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunStates {
    /// Runs within their own declared bound (`Live`), judged against real now.
    pub reporting: u64,
    /// Runs past their own declared bound with no stop record (`Unexplained`) — the page.
    pub silent_unexplained: u64,
    /// Runs with a clean stop record (`Departed`): silent, and explained, so no page.
    pub departed: u64,
    /// Runs the stream could not grade (`NoBasis`): the roster knows them, nothing grades
    /// them — never defaulted to reporting.
    pub ungradeable: u64,
}

/// The fleet's stamped figures, computed by the daemon AFTER each applied tick through
/// the same DAO reads the console serves — one reader, so the glass and the alarms cannot
/// disagree. `None` until a tick is applied; a process that runs no tick says nothing.
///
/// Every optional field is an ABSENCE with a meaning, never a zero: coverage while nothing
/// is stored, a duplicate figure past comparability, a capture fraction while nothing is
/// expected — each renders as a missing series, the format's own idiom for "no claim".
#[derive(Debug, Clone, PartialEq)]
pub struct FleetFigures {
    /// The meaning pair, rendered as `infertap_fleet_meaning_info` labels.
    pub semantics_version: &'static str,
    /// The pair's second half: the parameter discriminator the figures were computed under.
    pub discriminator: String,
    /// Floor over the stamped window (14 d ending at the frontier), summed over
    /// `Class::RECOMPUTE` only — the same sum the console's hero states.
    pub recompute_tokens: f64,
    /// Floor over the same window, all classes — the hero's denominator.
    pub stored_tokens: f64,
    /// Token-weighted over the recompute classes, the same formula the console derives.
    pub coverage_fraction: Option<f64>,
    /// The delivery ledger's fleet fraction; absent while nothing is expected.
    pub capture_fraction: Option<f64>,
    /// Segments missing without a tap declaration since install, summed over the delivery
    /// ledger's rows: in flight, or lost where nobody said so. Sustained nonzero is an alarm.
    pub segments_missing_unexplained: f64,
    /// End of the newest classified day, epoch seconds — what freshness alarms bind to.
    pub frontier_end_seconds: f64,
}

/// The daemon loop's own accounting, as the loop sees it (daemon).
///
/// The `_total` fields are process-lifetime counters: they reset on restart, which is what
/// a counter reset means to a scraper, and none is persisted — a counter rebuilt from disk
/// would be a gauge wearing a counter's name, and every rate computed from it wrong in the
/// direction nobody checks. `None` in [`Paths`] for a process that runs no loop.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct LoopStatus {
    /// Keyed ticks STARTED since this process began — starts, not completions, so a
    /// failing loop shows this and `tick_failures_total` climbing together.
    pub ticks_total: u64,
    /// Keyed ticks that ended in an error since this process began.
    pub tick_failures_total: u64,
    /// Source files taken in since this process began — not the candidate population, which
    /// is the `records_dir_files` gauge.
    pub files_ingested_total: u64,
    /// Bytes fetched and taken in since this process began — the byte counter beside
    /// `files_ingested_total`.
    pub bytes_ingested_total: u64,
    /// Records taken in since this process began — with the bytes, the observed
    /// events-per-byte the backlog estimate is derived from.
    pub records_ingested_total: u64,
    /// The backlog's byte gauge scaled by the observed events-per-byte. An ESTIMATE and named
    /// one; absent until this process has decoded enough to state a
    /// ratio, because a rate over no observation is not zero.
    pub ingest_backlog_events_estimate: Option<f64>,
    /// Of the files ingested, those the projected walk prepared rather than the full
    /// decode. The dispatch is silent by design, so without this an eligible segment falling
    /// through to the shaped path is invisible from outside.
    pub files_projected_total: u64,
    /// Bytes returned to the volume by part removal since this process began.
    pub reclaimed_bytes_total: u64,
    /// Statements dispatched, from `Engine::queries_executed()`. The scaling-invariance pin
    /// and `one_sample_costs_exactly_one_engine_query` both assert against this number, so a
    /// test's claim about engine work is written against what production exports.
    pub engine_queries_total: u64,
    /// Wall duration of the most recent COMPLETED tick; absent until one completes.
    pub last_tick_duration_seconds: Option<f64>,
    /// Seconds since the most recent SUCCESSFUL tick finished; absent until one does.
    pub tick_lag_seconds: Option<f64>,
    /// The deployment's declared tick cadence (the ingest cadence), which is what lag is judged against
    /// (see [`TICK_LAG_WARN_CADENCES`]).
    pub tick_interval_seconds: f64,
    /// The input horizon discovery holds right now: the declared one, widened while the
    /// driver has input to read past it (daemon). Exported beside the declared horizon, because
    /// a dashboard needs both: what the deployment asked for, and what it is holding.
    pub effective_retention_days: f64,
    /// The fleet's observed ARRIVAL rate: records the last ingest cycle took in, over the
    /// cadence it runs on. Absent until a cycle has run — a rate over no observation is not zero.
    ///
    /// **This is what the envelope was always about.** `--declared-gpus` asked an operator to
    /// state a fleet size so the fence could be checked against ~30% of the measured ingest
    /// ceiling; GPUs were a proxy for load. This is the load, measured, and it needs nobody to
    /// declare anything.
    pub ingest_rows_per_second: Option<f64>,
    /// The MEAN fraction of the ingest cadence spent ingesting, over UP TO the last
    /// [`ENVELOPE_WINDOW_CYCLES`] cycles — **this machine's own sustained utilisation**. Present
    /// from the first cycle; how many it averages is [`Self::ingest_duty_samples`].
    pub ingest_duty_cycle: Option<f64>,
    /// How many cycles [`Self::ingest_duty_cycle`] averages, so a reader can tell a settled
    /// figure from a provisional one.
    pub ingest_duty_samples: usize,
    /// Cycles that ran over their own cadence — catch-up rather than steady state, and excluded
    /// from the utilisation figure for that reason. A count of a fact, never a verdict.
    pub ingest_catchup_cycles: u64,
    /// What the last cycle listed but DEFERRED past its slice deadline — the real backlog.
    /// Facts from the walk's own tally, reported as observed — nothing
    /// divides them by a rate to manufacture a time remaining.
    pub ingest_backlog_files: u64,
    /// Bytes of the files the last cycle listed and deferred — the backlog's byte gauge, the
    /// one [`Self::ingest_backlog_events_estimate`] scales.
    pub ingest_backlog_bytes: u64,
    /// Ingest cycles refused by the disk rule since this process began.
    pub ingest_refused_total: u64,
    /// Whether intake is refused RIGHT NOW.
    ///
    /// The counter above and this gauge answer different questions: how often it has
    /// happened, and whether it is happening. An alarm wants the second; a rate wants the
    /// first. A refusal is surfaced and never silent, and stderr in a journal nobody tails is
    /// not a surface.
    pub ingest_refusing: bool,
    /// Blocks the sampled audit re-derived since this process began.
    pub audit_blocks_total: u64,
    /// Where the sampled audit first disagreed, epoch milliseconds, as the keyed state holds
    /// it: absent while every sample has agreed, and kept across restarts.
    pub audit_disagreed_at_ms: Option<f64>,
    /// Evicts on the sampled engine ids the evict audit graded since this process began.
    pub evict_audited_total: u64,
    /// Of those, evicts whose identity differs from their store's.
    pub evict_mismatched_total: u64,
    /// Of those, evicts carrying nothing where the same run's store carried an identity.
    pub evict_underresolved_total: u64,
}

/// The single engine round-trip: every figure the engine owns, one row, one spawn.
///
/// The part and disk figures are ENGINE-WIDE on purpose: they answer "what is on this volume",
/// and in production this analyzer owns the whole server. What arrived is not the engine's: the
/// input log's head states it ([`collect`]).
fn engine_facts_sql() -> &'static str {
    "SELECT \
     (SELECT ifNull(toUInt64(sum(bytes_on_disk)), 0) FROM system.parts WHERE active), \
     (SELECT count() FROM system.parts WHERE active), \
     (SELECT ifNull(toUInt64(sum(bytes_on_disk)), 0) FROM system.parts WHERE NOT active), \
     (SELECT ifNull(max(c), 0) FROM (SELECT count() AS c FROM system.parts \
     WHERE active GROUP BY table, partition)), \
     (SELECT count() FROM system.mutations WHERE is_done = 0), \
     (SELECT ifNull(toUInt64(min(free_space)), 0) FROM system.disks), \
     (SELECT ifNull(toUInt64(min(total_space)), 0) FROM system.disks), \
     (SELECT ifNull(toFloat64(now()), 0))"
}

/// Bytes held by inactive (merged-away) parts, as a single figure.
///
/// The loop's reclaim path needs a before/after around a forced merge, and a full
/// [`collect`] would be a far larger round trip to read one number. Same definition as the
/// `unreclaimed_bytes` metric — deliberately, so the loop's control input and the exported
/// gauge can never disagree about what "unreclaimed" means.
///
/// # Errors
/// Engine failure, or a figure that does not parse.
pub fn collect_unreclaimed(engine: &Engine) -> Result<f64, Error> {
    engine
        .run("SELECT ifNull(toUInt64(sum(bytes_on_disk)), 0) FROM system.parts WHERE NOT active")?
        .trim()
        .parse::<f64>()
        .map_err(|e| Error::Row(format!("self-health: unparseable inactive-part total: {e}")))
}

/// Total bytes beneath a directory, walked recursively. Ground truth for unreclaimed
/// space: the engine's `system.parts` view of INACTIVE parts is process-local and
/// under-reports badly in batch posture — verified on the phase-1 corpus, where three
/// consecutive invocations reported 124 MiB, then 123 MiB, then 180 KiB while the
/// directory sat unchanged at 288 MB with every part directory present. A sensor that
/// reports zero while a disk fills is worse than no sensor, so this one asks the
/// filesystem.
///
/// Bounded by construction rather than by hope: parts per partition are capped by the
/// engine's own `parts_to_throw_insert`, so the walk is proportional to a bounded part
/// count, not to data volume or to time.
fn dir_bytes_recursive(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += dir_bytes_recursive(&entry.path());
        } else if meta.is_file() {
            total += meta.len();
        }
    }
    total
}

fn dir_stats(dir: &Path) -> DirStats {
    let mut stats = DirStats::default();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return stats;
    };
    for entry in entries.flatten() {
        if let Ok(meta) = entry.metadata() {
            if meta.is_file() {
                stats.files += 1;
                stats.bytes += meta.len();
            }
        }
    }
    stats
}

/// Temp files this product left behind: our own prefixes only, never a blanket sweep of
/// a shared directory, and only those old enough to be orphans rather than work in
/// flight.
fn orphan_temp_stats() -> DirStats {
    let mut stats = DirStats::default();
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return stats;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("analyzer-ingest-") && !name.starts_with("analyzer-probe-") {
            continue;
        }
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.is_file() {
            continue;
        }
        let age_ok = meta.modified().ok().and_then(|m| m.elapsed().ok());
        if age_ok.is_some_and(|age| age.as_secs_f64() >= ORPHAN_TEMP_AGE_WARN_SECONDS) {
            stats.files += 1;
            stats.bytes += meta.len();
        }
    }
    stats
}

/// The one place a raw number becomes an exported metric — so the finite guarantee is
/// enforced once, not remembered at twenty call sites.
fn push(metrics: &mut Vec<Metric>, name: &'static str, help: &'static str, value: f64) {
    push_opt(metrics, name, help, Some(value));
}

fn push_opt(metrics: &mut Vec<Metric>, name: &'static str, help: &'static str, value: Option<f64>) {
    push_kind(metrics, name, help, value, MetricKind::Gauge);
}

/// A monotonic process-lifetime counter.
fn push_counter(
    metrics: &mut Vec<Metric>,
    name: &'static str,
    help: &'static str,
    value: Option<f64>,
) {
    push_kind(metrics, name, help, value, MetricKind::Counter);
}

fn push_kind(
    metrics: &mut Vec<Metric>,
    name: &'static str,
    help: &'static str,
    value: Option<f64>,
    kind: MetricKind,
) {
    metrics.push(Metric {
        name,
        help,
        value: value.filter(|v| v.is_finite()),
        kind,
    });
}

/// Counts reach the export as `f64`, which is exact for every count this system produces
/// (u64 values below 2^53). Above that the conversion would silently round, so it is
/// stated here rather than spread across call sites.
#[allow(clippy::cast_precision_loss)]
fn counter_value(v: u64) -> f64 {
    v as f64
}

/// Every figure the engine owns, from the one round trip.
#[derive(Debug, Clone, Copy)]
struct EngineFacts {
    active_bytes: f64,
    active_parts: f64,
    inactive_bytes: f64,
    max_parts: f64,
    unfinished_mutations: f64,
    disk_free: f64,
    disk_total: f64,
    ledger_rows: f64,
    last_ingest_epoch: f64,
    engine_now: f64,
    oldest_raw_epoch: f64,
}

/// Sample the analyzer's vitals: one engine invocation plus bounded filesystem stats.
///
/// # Errors
/// Engine failure, or a facts row that does not parse. The daemon turns that error into
/// a sample rather than a gap — see `unreachable` (the local fn, not the macro).
pub fn collect(
    engine: &Engine,
    lake: Option<&analyzer_lake::Lake>,
    fleet_id: &str,
    paths: &Paths,
) -> Result<SelfHealth, Error> {
    let out = engine.run(engine_facts_sql())?;
    let mut facts = Row::single("self-health engine facts", &out);
    let mut figure = |name: &str| -> Result<f64, Error> {
        facts.parse(name).map_err(|e| Error::Row(e.to_string()))
    };
    let active_bytes = figure("active_bytes")?;
    let active_parts = figure("active_parts")?;
    let inactive_bytes = figure("inactive_bytes")?;
    let max_parts = figure("max_parts")?;
    let unfinished_mutations = figure("unfinished_mutations")?;
    let disk_free = figure("disk_free")?;
    let disk_total = figure("disk_total")?;
    let engine_now = figure("engine_now")?;
    facts.end().map_err(|e| Error::Row(e.to_string()))?;
    // What arrived, from the input log's head; an engine with no log bound (a bare data
    // directory's health) has received nothing.
    let log = match lake {
        Some(lake) => lake.ledger(analyzer_lake::ledger::SHARD)?.head()?,
        None => analyzer_lake::ledger::LedgerHead::default(),
    };
    #[allow(clippy::cast_precision_loss)] // counts and epoch seconds are exact in f64
    let (ledger_rows, last_ingest_epoch) =
        (log.batches as f64, (log.last_ingested_at_ms / 1000) as f64);
    let span = |pick: fn(&analyzer_lake::ledger::DayIndex) -> Option<f64>, max: bool| {
        log.days
            .values()
            .filter_map(pick)
            .reduce(if max { f64::max } else { f64::min })
            .map_or(0.0, |ms| ms / 1000.0)
    };

    Ok(assemble(
        Some(EngineFacts {
            active_bytes,
            active_parts,
            inactive_bytes,
            max_parts,
            unfinished_mutations,
            disk_free,
            disk_total,
            ledger_rows,
            last_ingest_epoch,
            engine_now,
            oldest_raw_epoch: span(|d| d.first_at_ms, false),
        }),
        fleet_id,
        paths,
        None,
    ))
}

/// The sample a daemon emits when its engine did NOT answer.
///
/// **A failed collection must still produce a sample, and that is the requirement rather
/// than a convenience.** If a dead engine meant no health file was written, the artifact
/// a customer scrapes would simply stop changing — and a stale file reads as a healthy
/// one until somebody notices the timestamp. In an airgapped estate nobody does. So the
/// engine-derived series become explicitly ABSENT (counted by
/// `infertap_analyzer_metrics_unmeasured`, which is itself alarmable), every figure that
/// does not need the engine is still measured, and the failure is stated as a check
/// carrying the engine's own error text.
#[must_use]
pub fn unreachable(fleet_id: &str, paths: &Paths, reason: &str) -> SelfHealth {
    assemble(None, fleet_id, paths, Some(reason))
}

/// The export surface, assembled once for both the reachable and unreachable paths.
///
/// Long by construction: every metric's name, help text and value sit at one call site
/// so the export surface can be read top to bottom in the order it is emitted. Splitting
/// it would hide the contract this module exists to keep stable. Both paths run through
/// here so they cannot emit different name lists — the property
/// `every_declared_metric_name_is_actually_emitted` ratchets, held structurally rather
/// than by remembering to update two functions.
#[allow(clippy::too_many_lines)] // every metric's name, help and value at one site, read top to bottom in emission order
fn assemble(
    facts: Option<EngineFacts>,
    fleet_id: &str,
    paths: &Paths,
    unreachable_reason: Option<&str>,
) -> SelfHealth {
    // **Unreclaimed comes from the ENGINE**: the server holds the whole picture of its
    // inactive parts, so unreclaimed means dead parts, which is what its name says. The data
    // directory's size would carry the engine's own text logs, configs and temp space, and rise
    // across a merge that is reclaiming. `data_dir_bytes` stays as the figure a filling disk
    // follows.
    let data_dir_bytes = f64_from(dir_bytes_recursive(paths.data_dir));
    let unreclaimed_bytes = facts.map(|f| f.inactive_bytes);
    let dead_ratio = facts.map(|f| {
        if f.active_bytes > 0.0 {
            f.inactive_bytes / f.active_bytes
        } else {
            0.0
        }
    });
    let disk_free_fraction = facts.map(|f| {
        if f.disk_total > 0.0 {
            f.disk_free / f.disk_total
        } else {
            0.0
        }
    });
    // absent series rather than a -1 sentinel: a dashboard should show no data, not a
    // negative age it has to be taught to ignore
    let last_ingest_age = facts.and_then(|f| {
        (f.last_ingest_epoch > 0.0).then(|| (f.engine_now - f.last_ingest_epoch).max(0.0))
    });
    let raw_oldest_age_days = facts.and_then(|f| {
        (f.oldest_raw_epoch > 0.0)
            .then(|| ((f.engine_now - f.oldest_raw_epoch) / 86_400.0).max(0.0))
    });
    let output = dir_stats(paths.out_dir);
    let orphans = orphan_temp_stats();
    let retention_days = f64::from(paths.ledger_retention_days);

    let mut metrics = Vec::with_capacity(METRIC_NAMES.len());
    // **The HOST's clock, deliberately, not the engine's.** Every other time-derived figure
    // here comes from `facts.engine_now`, which is right for them — they measure engine
    // state and must be read against the engine's own clock. This one measures whether the
    // SAMPLER is alive, so it must not depend on the thing whose absence it has to survive:
    // on the `unreachable` path there is no `engine_now`, and that is exactly the moment an
    // operator most needs to know the emitter is still running. Absent rather than zero if
    // the host clock is before the epoch, per this module's unmeasured-is-a-type rule.
    push_opt(
        &mut metrics,
        "infertap_analyzer_sample_timestamp_seconds",
        "Unix time at which this sample was assembled, from the host clock. Compare \
         against now: a value that stops advancing means the sampler is dead or wedged, \
         which no other series can express — they would simply hold their last reading.",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|d| d.as_secs_f64()),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_active_part_bytes",
        "Bytes held by active MergeTree parts (live data on disk).",
        facts.map(|f| f.active_bytes),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_active_parts",
        "Active MergeTree parts across all tables.",
        facts.map(|f| f.active_parts),
    );
    push(
        &mut metrics,
        "infertap_analyzer_data_dir_bytes",
        "Bytes on disk beneath the engine's data directory (filesystem ground truth).",
        data_dir_bytes,
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_unreclaimed_bytes",
        "Bytes held by inactive (merged-away) parts, from the engine's own part view.",
        unreclaimed_bytes,
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_dead_part_ratio",
        "Unreclaimed bytes divided by active part bytes.",
        dead_ratio,
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_max_parts_per_partition",
        "Largest active part count in any single partition (engine throttles at 1000, \
         refuses inserts at 3000).",
        facts.map(|f| f.max_parts),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_unfinished_mutations",
        "Mutations not yet applied (mutation debt).",
        facts.map(|f| f.unfinished_mutations),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_disk_free_bytes",
        "Free bytes on the engine's data volume.",
        facts.map(|f| f.disk_free),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_disk_total_bytes",
        "Total bytes on the engine's data volume.",
        facts.map(|f| f.disk_total),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_disk_free_fraction",
        "Free fraction of the engine's data volume.",
        disk_free_fraction,
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_ledger_rows",
        "Rows in the batches ledger for this fleet (grows with files ingested).",
        facts.map(|f| f.ledger_rows),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_raw_oldest_age_days",
        "Age in days of the oldest residency delta (the event-grain fast lane's oldest \
         row); absent when no cache facts exist.",
        raw_oldest_age_days,
    );
    push(
        &mut metrics,
        "infertap_analyzer_raw_retention_days",
        "Configured ingest ledger retention horizon (ledger_retention_days), for comparison \
         against the oldest row.",
        retention_days,
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_last_ingest_age_seconds",
        "Seconds since the newest ledger row; absent when nothing has been ingested.",
        last_ingest_age,
    );
    push(
        &mut metrics,
        "infertap_analyzer_output_dir_files",
        "Files in the artifact output directory (snapshots accumulate one per run).",
        f64_from(output.files),
    );
    push(
        &mut metrics,
        "infertap_analyzer_output_dir_bytes",
        "Bytes in the artifact output directory.",
        f64_from(output.bytes),
    );
    push(
        &mut metrics,
        "infertap_analyzer_orphan_temp_files",
        "Analyzer temp files older than the orphan threshold (crash residue).",
        f64_from(orphans.files),
    );
    push(
        &mut metrics,
        "infertap_analyzer_orphan_temp_bytes",
        "Bytes held by orphaned analyzer temp files.",
        f64_from(orphans.bytes),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_server_up",
        "1 when the owned engine server answered its last probe, 0 when it is down; \
         absent when this analyzer owns no server.",
        paths.server.map(|s| f64::from(u8::from(s.up))),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_server_uptime_seconds",
        "Seconds since the owned engine server started; absent when it is down or when \
         this analyzer owns none.",
        paths.server.and_then(|s| s.uptime_seconds),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_process_rss_bytes",
        "This analyzer process's resident set, sampled live from /proc; absent where \
         unreadable. The number a 2.35 GiB/min anon climb moves first.",
        process_rss_bytes(),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_cgroup_anon_bytes",
        "Anonymous memory charged to this process's cgroup (memory.stat anon), the \
         figure memory.high can NOT reclaim; absent where the v2 layout is unreadable.",
        cgroup_anon_bytes(),
    );
    let lp = paths.loop_status;
    push_counter(
        &mut metrics,
        "infertap_analyzer_ticks_total",
        "Keyed ticks started since this process began; absent in a process that runs no \
         loop.",
        lp.map(|l| counter_value(l.ticks_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_tick_failures_total",
        "Keyed ticks that ended in an error since this process began.",
        lp.map(|l| counter_value(l.tick_failures_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_files_ingested_total",
        "Source files ingested since this process began — not the candidate population, \
         which is the records_dir_files gauge.",
        lp.map(|l| counter_value(l.files_ingested_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_bytes_ingested_total",
        "Bytes fetched and taken in since this process began — the listing's own object \
         sizes, accumulated at cycle end.",
        lp.map(|l| counter_value(l.bytes_ingested_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_records_ingested_total",
        "Records taken in since this process began. With bytes_ingested_total this is the \
         observed events-per-byte, which is what the backlog estimate derives from.",
        lp.map(|l| counter_value(l.records_ingested_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_files_projected_total",
        "Of files_ingested_total, files the projected walk prepared from the segment's \
         own columns (the fast path); the difference is files on the full-decode \
         reference path. A fleet of sealed Parquet where this stops tracking \
         files_ingested_total is falling through the dispatch, and this counter is how \
         that is visible at all.",
        lp.map(|l| counter_value(l.files_projected_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_reclaimed_bytes_total",
        "Bytes returned to the volume by part removal since this process began.",
        lp.map(|l| counter_value(l.reclaimed_bytes_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_engine_queries_total",
        "Statements dispatched to the engine since this process began.",
        lp.map(|l| counter_value(l.engine_queries_total)),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_tick_duration_seconds",
        "Wall duration of the most recent completed tick; absent until one completes.",
        lp.and_then(|l| l.last_tick_duration_seconds),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_tick_lag_seconds",
        "Seconds since the most recent successful tick finished; absent until one does.",
        lp.and_then(|l| l.tick_lag_seconds),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_ingest_refused_total",
        "Ingest cycles refused by the disk rule since this process began.",
        lp.map(|l| counter_value(l.ingest_refused_total)),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_ingest_refusing",
        "1 while intake is refused, 0 otherwise; absent in a process that runs no loop.",
        lp.map(|l| f64::from(u8::from(l.ingest_refusing))),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_audit_blocks_total",
        "Blocks the sampled audit re-derived from the log and compared with what their tick \
         stated, since this process began.",
        lp.map(|l| counter_value(l.audit_blocks_total)),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_audit_disagreed",
        "1 once a sampled block's re-derivation disagreed with what its tick stated, 0 \
         otherwise; kept until the derived state is derived afresh.",
        lp.map(|l| f64::from(u8::from(l.audit_disagreed_at_ms.is_some()))),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_evict_audit_audited_total",
        "Evicts on the sampled engine ids the evict audit graded against their latest prior \
         store, since this process began.",
        lp.map(|l| counter_value(l.evict_audited_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_evict_audit_mismatched_total",
        "Graded evicts whose identity differs from their store's: the producer resolved them \
         wrongly.",
        lp.map(|l| counter_value(l.evict_mismatched_total)),
    );
    push_counter(
        &mut metrics,
        "infertap_analyzer_evict_audit_underresolved_total",
        "Graded evicts carrying no identity where the same producer run's store carried one.",
        lp.map(|l| counter_value(l.evict_underresolved_total)),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_effective_retention_days",
        "Input horizon discovery holds now; above ledger_retention_days while the driver has \
         older input to read.",
        lp.map(|l| l.effective_retention_days),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_ingest_duty_cycle",
        "Mean fraction of the ingest cadence spent ingesting, over the recent window. 1.0 \
         is saturation. Read WITH the backlog gauges: the slice deadline means a backlog \
         can grow while duty sits low. Absent until a cycle has run.",
        lp.and_then(|l| l.ingest_duty_cycle),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_ingest_backlog_files",
        "Objects the last ingest cycle listed but deferred past its slice deadline. \
         Growth here is the fall-behind signal; zero is the steady state. Absent in a \
         process that runs no loop.",
        lp.map(|l| f64_from(l.ingest_backlog_files)),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_ingest_backlog_bytes",
        "Bytes across the deferred objects — the same fact in the unit reclamation \
         plans in.",
        lp.map(|l| f64_from(l.ingest_backlog_bytes)),
    );
    push_opt(
        &mut metrics,
        "infertap_analyzer_ingest_backlog_events_estimate",
        "The backlog's bytes scaled by this process's observed events-per-byte — an \
         ESTIMATE: nobody has decoded the deferred objects. Absent until enough has \
         been ingested to state a ratio; read beside the byte gauge, never instead \
         of it.",
        lp.and_then(|l| l.ingest_backlog_events_estimate),
    );
    let ff = paths.fleet;
    push_opt(
        &mut metrics,
        "infertap_fleet_recompute_tokens",
        "Recomputed prefill over the stamped window, tokens. A floor, never an estimate; \
         absent until a tick is applied.",
        ff.map(|f| f.recompute_tokens),
    );
    push_opt(
        &mut metrics,
        "infertap_fleet_stored_tokens",
        "Tokens stored over the stamped window, the recompute fraction's denominator. A \
         floor; absent until a tick is applied.",
        ff.map(|f| f.stored_tokens),
    );
    push_opt(
        &mut metrics,
        "infertap_fleet_recompute_fraction",
        "Recomputed prefill as a share of tokens stored over the stamped window — the \
         alarmable headline. Absent until a tick is applied or while nothing is stored.",
        ff.and_then(|f| (f.stored_tokens > 0.0).then(|| f.recompute_tokens / f.stored_tokens)),
    );
    push_opt(
        &mut metrics,
        "infertap_fleet_coverage_fraction",
        "Token-weighted coverage of the recompute figure over the stamped window: at what \
         coverage that mass was observed. Absent until a tick is applied.",
        ff.and_then(|f| f.coverage_fraction),
    );
    push_opt(
        &mut metrics,
        "infertap_fleet_capture_fraction",
        "Of the segments the taps produced since install, the fraction that arrived. \
         Absent while nothing is expected — never a default to 1.",
        ff.and_then(|f| f.capture_fraction),
    );
    push_opt(
        &mut metrics,
        "infertap_fleet_segments_missing_unexplained",
        "Segments missing without a tap declaration, since install: in flight, or lost \
         where nobody said so. Any sustained nonzero value is an alarm.",
        ff.map(|f| f.segments_missing_unexplained),
    );
    push_opt(
        &mut metrics,
        "infertap_fleet_frontier_timestamp_seconds",
        "End of the newest classified day. Freshness alarms for the figures above bind \
         here, never to the scrape clock: the figures are tick-settled.",
        ff.map(|f| f.frontier_end_seconds),
    );
    #[allow(clippy::cast_precision_loss)] // run counts sit far below 2^53
    {
        let runs = paths.runs;
        push_opt(
            &mut metrics,
            "infertap_fleet_producer_runs_reporting",
            "Producer runs (the console calls these tap processes) within their own \
             declared heartbeat bound at the knowledge horizon, over runs seen in the \
             last seven days. A restarted tap is a NEW run.",
            runs.map(|r| r.reporting as f64),
        );
        push_opt(
            &mut metrics,
            "infertap_fleet_producer_runs_silent_unexplained",
            "Producer runs past their own declared bound with NO stop record — the \
             partial-silence page. Detection time is set by each tap's OWN declared \
             heartbeat bound plus the cadence allowance, not by this analyzer: a tap \
             declaring a six-hour heartbeat cannot read silent sooner. Total fleet \
             silence freezes the knowledge horizon and reads on last_ingest_age_seconds \
             instead; the pair splits the failure space.",
            runs.map(|r| r.silent_unexplained as f64),
        );
        push_opt(
            &mut metrics,
            "infertap_fleet_producer_runs_departed",
            "Producer runs whose last word was a clean stop record. A rolling deploy \
             raises this, never silent_unexplained — that separation is why paging on \
             silence does not page on deploys.",
            runs.map(|r| r.departed as f64),
        );
        push_opt(
            &mut metrics,
            "infertap_fleet_producer_runs_ungradeable",
            "Producer runs that declared no heartbeat bounds, so no staleness verdict \
             can be reached. A verdict-shaped word without that basis would be an \
             assertion. The four run gauges sum to the runs seen in the window.",
            runs.map(|r| r.ungradeable as f64),
        );
    }

    // Checks split by what the sample could actually observe. The engine-derived ones are
    // OMITTED rather than reported Unknown when the engine did not answer: an Unknown here
    // would be six degraded-looking lines saying the same thing the one honest check
    // below already says, and `check_retention` in particular would claim "no residency
    // deltas" when the truth is that nobody asked.
    let mut checks = Vec::new();
    if let (Some(f), Some(ratio), Some(unreclaimed), Some(free_fraction)) =
        (facts, dead_ratio, unreclaimed_bytes, disk_free_fraction)
    {
        checks.push(check_dead_parts(ratio, unreclaimed));
        checks.push(check_disk(free_fraction, f.disk_free));
        checks.push(check_parts(f.max_parts));
        checks.push(check_mutations(f.unfinished_mutations));
        checks.push(check_retention(raw_oldest_age_days, retention_days));
    }
    checks.push(check_orphans(orphans));
    // No observation, no check: a sample from a process that owns no server has nothing
    // to say about one, and inventing an `Unknown` here would make every harness run
    // report a degraded analyzer. Under the daemon — the only operable mode — the status
    // is always supplied, so this check always exists where it means something.
    if let Some(server) = paths.server {
        checks.push(check_server(server));
    }
    if let Some(status) = lp {
        checks.push(check_tick_lag(status));
        checks.push(check_envelope(status));
        checks.push(check_audit(status));
        checks.push(check_evict_resolution(status));
    }
    if let Some(reason) = unreachable_reason {
        checks.push(Check {
            name: "engine_reachable",
            status: Status::Critical,
            message: format!(
                "the engine did not answer this sample: {reason}. Every engine-derived \
                 figure is ABSENT rather than carried over from the last good sample — a \
                 stale number here would read as a healthy one."
            ),
        });
    }

    SelfHealth {
        analyzer_id: paths.analyzer_id.map(str::to_string),
        fleet: fleet_id.to_string(),
        meaning: paths
            .fleet
            .map(|f| (f.semantics_version.to_string(), f.discriminator.clone())),
        metrics,
        checks,
    }
}

/// Resident set of THIS process, from the `VmRSS` line of `/proc/self/status`, which the
/// kernel states in kB whatever the page size. `None` off Linux or where /proc is absent —
/// unmeasured, never zero.
fn process_rss_bytes() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: f64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kb * 1024.0)
}

/// Anonymous memory charged to this process's cgroup, from the v2 `memory.stat` the
/// cgroup path in `/proc/self/cgroup` names. `None` wherever that layout is absent.
fn cgroup_anon_bytes() -> Option<f64> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = cgroup.lines().find_map(|l| l.strip_prefix("0::"))?;
    let stat =
        std::fs::read_to_string(format!("/sys/fs/cgroup{}/memory.stat", path.trim())).ok()?;
    let anon = stat.lines().find_map(|l| l.strip_prefix("anon "))?;
    anon.trim().parse().ok().map(|b: f64| b)
}
