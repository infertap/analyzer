// SPDX-License-Identifier: AGPL-3.0-only

//! The `ClickHouse` serving store: the one implementation of the core's serving port
//! ([`serving::ClickHouse`]), the engine it runs ([`engine`]), the engine's own health
//! ([`selfhealth`]), and the read layer the API answers from (the rest of this crate). The
//! core depends on none of it: it states its output through the port and never reads it
//! back (`docs/lake.md`).
//!
//! # The read layer
//!
//! Every read the product performs, named for what the caller wants.
//!
//! **Why this crate exists.** The console's panels are a handful of query shapes, and most of
//! them are ONE shape differing only in `(grain, dimensions, window)`. Written as separate
//! methods they drift; written as one they cannot. This crate is that collapse made structural,
//! and `static_audit` pins the count of SQL-building sites per file so a second copy of a shape
//! is a reviewed diff.
//!
//! **One module per shape**, so the file list is the shape table. See the module block at the
//! foot of this file for why that happened and what it costs to add a tenth.
//!
//! **Scope: the READ path, and the boundary is deliberate.** The derivation computes in Rust and
//! writes through the serving port (`serving::ClickHouse`); the dashboard, the API and the
//! daemon's gates read. The derivation issues no query of its own.
//!
//! **The methods.** The surface is fixed here because it is both the DAO API and — the panel
//! grid being the API's design document — the endpoint list.
//!
//! | # | module | method | serves |
//! |---|---|---|---|
//! | 1 | [`shape1`] | `waste_by(window, grain, dims)` | S1 headline, S1 trend, S2 deltas, S2 top movers, S3 class breakdown, S3 worst holders, S3 splits |
//! | 1 | [`shape1`] | `waste_by_segmented(…)` | the same shape across a meaning seam — one read per generation, never one blended series |
//! | 2 | [`roster`] | `roster(window)` | S1 summary, S5 full |
//! | 3 | [`coverage`] | `coverage(window, by)` | S1 trust, S5 |
//! | 4 | [`events`] | `blocks_by_mass(window, limit)` / `holders_of(block)` | S4 tree, top offenders |
//! | 5 | [`events`] | `timeline(holder, window)` | S4 |
//! | 7 | [`markers`] | `markers(window)` | S2 discontinuities |
//! | 9 | [`bands`] | `bands_by(window, grain, dims)` | recompute mass by time-since-eviction — the thrash lens. Fixed bands over `band_rollup_daily` at day grain, `band_rollup_hourly` at hour grain, `band_minute` at minute grain. The t-digest gap distribution it superseded was cut |
//! | — | [`raw`] | `raw_span` / `unpassed_days` / `fleet_health` / `agent_versions` / `degradation_ledger` | reads that cannot be banked, owned here anyway |
//! | — | [`stored`] | `stored_mass(window)` | the anchor's "of N stored" denominator — `facts_rollup_daily`'s first customer-facing reader (console v3) |
//!
//! Shape 1 is one method because it is one shape. `dims` is a set over the closed [`Dim`]
//! enum, so a caller cannot ask for a dimension the rollup does not key on, and adding a
//! dimension is a compile error at every match rather than a silent absence.
//!
//! **Shape 8 is the one row that is not a panel, and the note is here so nobody builds one for
//! it.** S6 was demoted to the report and the scheduled digest because
//! every figure it carries multiplies a measured token mass by a declared rate — the dashboard design's excluded
//! category. The data source and the write path are unchanged; only the rendering surface moved.
//!
//! **Two laws this crate holds structurally rather than by convention:**
//!
//! - **Every accessor states the engine it assumes, and reads it the one right way.** Every
//!   derived table REPLACES now (the fold owns the derivation, pipeline 1): the eight cell
//!   tables and the fact ledger are keyed replace, written whole by the fold, the latest
//!   `version` per key the value and `gone = 1` a tombstone — read under `FINAL` with
//!   `gone = 0`, because a replace read without `FINAL` after a re-pass double-counts, and
//!   passes when a merge happened and fails when it did not. A `sum()` here is the read's own cut over finished cells,
//!   never the derivation of one; nothing in the served path sums a signed or delta stream. The
//!   choice is a property of the accessor, as escaping is a property of the quoting helpers
//!   rather than a step a call site performs.
//! - **No naked number leaves.** Every figure returns coverage-carrying ([`Covered`]), so
//!   enforcement does not depend on where composition happens and the API above can stay thin.
//!
//! # Where to start
//!
//! [`Dao`] is the one type; one module per read shape, and the table above is the module list.
//! Every figure returns coverage-carrying ([`Covered`]); every accessor states the engine
//! semantics it assumes — `FINAL` and `gone = 0` over the cell tables, `FINAL` over the
//! ledgers. The
//! window grammar and the wall-now anchor of the grain ladder are resolved here, once.

use crate::engine::{quote_literal, sql_i64, Engine};
use crate::row::Row;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::parquet_source::SourceConfig;

// Re-exported, not defined here: the write path stamps the discriminator onto every row it
// emits, so the type lives in `analysis`, where `Discriminator::of` is a pure function of
// `Params`, and a crate that depends on `analysis` cannot be its home.
pub use analyzer_analysis::Discriminator;

// Re-exported so a caller reads one surface rather than reaching past this crate into `analysis`
// for the types its own rows are made of. Definitions stay where they are computed: the staleness
// vocabulary is the reader corpus's.
pub use analyzer_analysis::intervals::Holder;
pub use analyzer_analysis::staleness::RunStaleness;

/// How many whole days the stamped window covers, ending at the frontier day: the span the
/// self-health figures and the console's default document are anchored to.
pub const STAMPED_WINDOW_DAYS: i64 = 14;

/// The stamped window: [`STAMPED_WINDOW_DAYS`] whole UTC days through `frontier_day`, the
/// last day a pass classified. Anchored at the frontier rather than at now, so a fleet that
/// stopped writing keeps stating the figures it has rather than a window of zeros.
///
/// # Errors
/// A day range that cannot be represented in signed milliseconds.
pub fn stamped_window(frontier_day: i64) -> Result<analyzer_analysis::Window, Error> {
    let first = frontier_day
        .checked_sub(STAMPED_WINDOW_DAYS - 1)
        .ok_or_else(|| Error::Refused("stamped window day overflow".into()))?;
    Ok(analyzer_analysis::Window::whole_days(first, frontier_day)?)
}

// The replication panel's read vocabulary, re-exported flat like every other shape's.
pub use replication_read::{
    CopiesBucket, CopiesData, CopiesMember, InstanceDupRow, InstanceDuplication,
};

/// A dimension the recompute register can be grouped by.
///
/// **Closed by construction**, which is the point: these are exactly the columns
/// `class_rollup_*` keys on. A caller cannot request a grouping
/// the schema cannot serve, and adding a dimension is a compile error at every `match` rather
/// than a query that silently returns fewer rows than the panel expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Dim {
    /// One of the six classification buckets.
    Class,
    /// The full holder triple — instance, `dp_rank`, cache group. One dimension, never three:
    /// splitting them would let a caller group by `dp_rank` alone, which is meaningless across
    /// instances.
    Holder,
    /// The instance alone — every rank and cache group of one node, rolled up.
    ///
    /// **This does NOT reopen the triple**. The rule against splitting
    /// [`Dim::Holder`] forbids `dp_rank` on its own, which is meaningless across instances:
    /// rank 0 of one node and rank 0 of another share nothing. An INSTANCE is a real entity
    /// — a slot an operator named, restarts, drains and pages about — so grouping by it
    /// states something true. Summing a node's ranks is the right roll-up for mass and
    /// events; it is NOT right for duplication, which is why the copies read has its own
    /// cut and does not take a dimension list.
    Instance,
    /// Declared on the record: `workload_class` on `segment_open` (spec 2.7).
    Workload,
    /// **Observed**, never declared.
    Model,
}

impl Dim {
    /// The rollup column this dimension groups by. `Holder` expands to the triple.
    #[must_use]
    pub fn columns(self) -> &'static [&'static str] {
        match self {
            Dim::Class => &["class"],
            Dim::Holder => &["instance_id", "dp_rank", "group_idx"],
            Dim::Instance => &["instance_id"],
            Dim::Workload => &["workload_class"],
            Dim::Model => &["model"],
        }
    }
}

/// What a read is SCOPED to — the entity whose page is being drawn.
///
/// **Scope is not the same as a dimension, and the difference is the whole point.** `dims`
/// says how to GROUP the answer; this says which slice to answer about. Our reads were
/// group-by-only, so drawing one node meant fetching every node and
/// filtering in the client — fleet-proportional work for a single-entity page, which is
/// exactly the cost law the monoid line exists to hold. A scoped read touches one key
/// slice and stays flat as the fleet grows.
///
/// Every field is `None` by default, and `None` means "not scoped" rather than "matches
/// nothing" — an empty scope is the fleet.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Scope {
    /// A declared workload label. **`undeclared` selects the absent bucket** as a named
    /// slice rather than as a gap — workload is a declaration, and a declaration absence
    /// reads `undeclared`.
    pub workload: Option<String>,
    /// One instance — every rank and cache group on that node.
    pub instance: Option<String>,
    /// One holder: the full triple, the finest scope there is.
    pub holder: Option<Holder>,
}

impl Scope {
    /// The `WHERE` fragment, `AND`-prefixed, or empty where nothing is scoped.
    ///
    /// Every value routes through [`crate::engine::quote_literal`] — the boundary rule,
    /// and these are caller-supplied strings, which is precisely the case it exists for.
    #[must_use]
    pub fn predicate(&self) -> String {
        self.predicate_on(false)
    }

    /// Whether the scope names a holder or an instance — the shape that decides which
    /// minute table a read serves from.
    #[must_use]
    pub fn is_holder_shaped(&self) -> bool {
        self.instance.is_some() || self.holder.is_some()
    }

    /// [`Scope::predicate`], for a table that keeps the instance and the rank but not the
    /// cache group (`rank` true — the rank-grain minute cells): a holder scope reads the
    /// rank whatever the group, because at that grain a holder IS a rank.
    #[must_use]
    pub fn predicate_on(&self, rank: bool) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        if rank {
            if let Some(w) = &self.workload {
                let _ = write!(
                    out,
                    " AND workload_class = {}",
                    crate::engine::quote_literal(w)
                );
            }
            if let Some(h) = &self.holder {
                let _ = write!(
                    out,
                    " AND instance_id = {} AND dp_rank = {}",
                    crate::engine::quote_literal(&h.instance),
                    h.dp_rank
                );
            } else if let Some(i) = &self.instance {
                let _ = write!(
                    out,
                    " AND instance_id = {}",
                    crate::engine::quote_literal(i)
                );
            }
            return out;
        }
        if let Some(w) = &self.workload {
            let _ = write!(
                out,
                " AND workload_class = {}",
                crate::engine::quote_literal(w)
            );
        }
        // The holder is the finer claim, so it wins where both are named: a caller asking
        // for one rank OF an instance is asking about the rank.
        if let Some(h) = &self.holder {
            let _ = write!(
                out,
                " AND instance_id = {} AND dp_rank = {} AND group_idx = {}",
                crate::engine::quote_literal(&h.instance),
                h.dp_rank,
                h.group_idx
            );
        } else if let Some(i) = &self.instance {
            let _ = write!(
                out,
                " AND instance_id = {}",
                crate::engine::quote_literal(i)
            );
        }
        out
    }
}

/// Which minute table, if any, a read's cut and scope can be served from. The fleet cells carry class, workload and
/// model; the rank cells carry those and the instance and the rank. A holder cut
/// has no minute table: the rank cells roll cache groups and tiers up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinuteTier {
    /// No grain serves this window: the tables that would are gone or not yet built.
    Unavailable,
    /// The fleet grain: one row per bucket for the whole fleet.
    Fleet,
    /// The rank grain: one row per bucket per data-parallel rank.
    Rank,
}

impl MinuteTier {
    /// The tier a figures or bands read can use, from its dims and scope.
    #[must_use]
    pub fn of(dims: &[Dim], scope: &Scope) -> Self {
        if dims.iter().any(|d| matches!(d, Dim::Holder)) {
            Self::Unavailable
        } else if scope.is_holder_shaped() || dims.contains(&Dim::Instance) {
            Self::Rank
        } else {
            Self::Fleet
        }
    }
}

/// The time bucket a shape-1 read aggregates into.
///
/// Three tiers: minute for the thrash zoom, hourly for the incident mode (to the hour horizon)
/// and daily for the multi-year story.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grain {
    /// The thrash zoom's spine:
    /// served from `recompute_minute` — class × workload × model (one dimension set on
    /// every minute table), reach `minute_retention_days`. Readers that
    /// cannot serve it refuse it by name rather than downgrading.
    Minute,
    /// Hour buckets.
    Hour,
    /// Day buckets.
    Day,
}

/// The point budget the AUTO grain chooser fits an hourly response inside: a custom range serves hourly only while its hour count
/// stays under this, else daily. A RENDER budget like `PAGE_LIMIT_MAX` — 200 hourly
/// points is ~8.3 days of line, comfortably past the ruled 7-day stop and far under
/// what a chart can show meaningfully — never a data limit: an explicit `grain=hour`
/// request ignores it.
pub const AUTO_GRAIN_HOUR_POINT_BUDGET: f64 = 200.0;

/// The minute tier's own budget, same family: the
/// AUTO chooser serves minute buckets only while the range's minute count fits — 240
/// points is a 4-hour zoom, the incident window's size — and only where the minute
/// table's own reach covers the range (its 48 h TTL, measured off the table, never
/// assumed from config).
pub const AUTO_GRAIN_MINUTE_POINT_BUDGET: f64 = 240.0;

impl Grain {
    /// The AUTO choice: the server picks the grain, because only the
    /// server knows the raw horizon — a console hard-coding "30 days" to choose its own
    /// grain is a duplicated deployment constant that goes stale silently. Hourly iff
    /// the whole window sits inside what the raw ACTUALLY covers (`raw_start_ms`, the
    /// measured span — a fresh install has less than the configured horizon) and the
    /// point count fits the render budget; daily otherwise. Pure, so the rule is
    /// unit-tested without an engine; [`Dao::resolve_grain`] feeds it the measured span.
    #[must_use]
    pub fn auto(window: &Window, raw_start_ms: Option<f64>, minute_start_ms: Option<f64>) -> Grain {
        let minutes = (window.end_ms - window.start_ms) / 60_000.0;
        let hours = minutes / 60.0;
        // Minute first: the tightest tier that can honestly serve wins. Its reach is
        // the minute TABLE's own span (48 h TTL, measured), passed only where the
        // caller's cut the table can serve (fleet × class) — `None` otherwise.
        if minute_start_ms.is_some_and(|start| start <= window.start_ms)
            && minutes <= AUTO_GRAIN_MINUTE_POINT_BUDGET
        {
            return Grain::Minute;
        }
        let raw_covers = raw_start_ms.is_some_and(|start| start <= window.start_ms);
        if raw_covers && hours <= AUTO_GRAIN_HOUR_POINT_BUDGET {
            Grain::Hour
        } else {
            Grain::Day
        }
    }

    /// The wire spelling of the SERVED grain — stated in-band on every response whose
    /// grain the server chose, so a consumer renders what it got instead of what it
    /// assumed.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Grain::Minute => "minute",
            Grain::Hour => "hour",
            Grain::Day => "day",
        }
    }

    /// The served bucket WIDTH in milliseconds — stated in-band beside the grain word on
    /// every bucketed response, so a consumer draws the width it got.
    #[must_use]
    pub fn bucket_ms(self) -> i64 {
        match self {
            Grain::Minute => 60_000,
            Grain::Hour => 3_600_000,
            Grain::Day => 86_400_000,
        }
    }

    /// The tier a relative window of this span naturally serves, before any reach is
    /// consulted: what [`Grain::auto`] would choose with every tier reachable. The window
    /// parser aligns a relative window's edge to THIS bucket, so a stop's buckets are whole.
    #[must_use]
    pub fn natural(span_ms: f64) -> Grain {
        let minutes = span_ms / 60_000.0;
        if minutes <= AUTO_GRAIN_MINUTE_POINT_BUDGET {
            Grain::Minute
        } else if minutes / 60.0 <= AUTO_GRAIN_HOUR_POINT_BUDGET {
            Grain::Hour
        } else {
            Grain::Day
        }
    }

    #[must_use]
    /// The table this grain reads.
    pub fn table(self) -> &'static str {
        match self {
            Grain::Minute => "recompute_minute",
            Grain::Hour => "class_rollup_hourly",
            Grain::Day => "class_rollup_daily",
        }
    }

    /// The class table a read serves from, given whether its cut or scope names a holder:
    /// at minute grain that is the rank-grain cells,
    /// which keep the instance and the rank; at hour and day the rollups carry the triple.
    #[must_use]
    pub fn class_table(self, rank: bool) -> &'static str {
        match (self, rank) {
            (Grain::Minute, true) => "recompute_rank_minute",
            _ => self.table(),
        }
    }

    /// The `WHERE` clause selecting every bucket this window touches.
    ///
    /// The UTC day a bucket at this grain belongs to — the join key for the day's coverage
    /// row: a daily bucket is its own day; an hour or a minute
    /// bucket is the day it falls in.
    #[must_use]
    pub fn day_of_bucket(self) -> &'static str {
        match self {
            Grain::Day => "bucket",
            Grain::Hour | Grain::Minute => "toDate(bucket, 'UTC')",
        }
    }

    /// **A rollup bucket is atomic, and that is a semantic rather than a rounding choice.** A
    /// daily row aggregates a whole UTC day; there is no way to serve half of one, so a window
    /// that overlaps a day gets that day. The first attempt here used an exclusive end bound
    /// on the bucket's own date, which made any window INSIDE a single day select nothing at
    /// all — the panels went blank while the data sat there.
    ///
    /// The end bound is therefore the bucket containing the last instant the window actually
    /// covers (`end_ms - 1`), compared inclusively. A window ending exactly on a boundary
    /// stops at the previous bucket, which is what a half-open window means; a window ending
    /// mid-bucket includes that bucket, which is what atomicity means.
    ///
    /// The two grains store different types — `Date` daily, `DateTime` hourly — so a bound
    /// written for one compares wrong against the other. Rendering here is what lets one query
    /// text serve both.
    #[must_use]
    pub fn window_predicate(self, w: &Window) -> String {
        let at = |ms: f64| {
            // TRUNCATED, not rounded — see `epoch_seconds`. Rounding here made every day-aligned
            // window read one bucket past its own exclusive end.
            let secs = epoch_seconds(ms);
            match self {
                Grain::Minute => format!("toStartOfMinute(toDateTime({secs}, 'UTC'))"),
                Grain::Hour => format!("toStartOfHour(toDateTime({secs}, 'UTC'))"),
                Grain::Day => format!("toDate(toDateTime({secs}, 'UTC'))"),
            }
        };
        // `max` guards a zero-length or inverted window from reaching back a bucket
        format!(
            "bucket >= {} AND bucket <= {}",
            at(w.start_ms),
            at((w.end_ms - 1.0).max(w.start_ms))
        )
    }
}

/// A value that cannot exist without the coverage it was measured at.
///
/// **The one coverage type in the workspace**: the law that a value cannot exist without the
/// coverage it was measured at holds only if there is one place to hold it.
///
/// It is a plain struct with no dependency of its own, so a consumer wanting the coverage
/// discipline without the queries could take it with a one-file move.
#[derive(Debug, Clone, PartialEq)]
pub struct Covered<T> {
    /// The value the query returned.
    pub value: T,
    /// The register the value is stated in — always the lower bound, `>=`. Carried rather than
    /// implied: a figure whose register is assumed reads as exact when it is a floor.
    pub register: &'static str,
    /// Observed fraction of the window this value was measured over, `0.0..=1.0`.
    pub coverage: f64,
    /// How that fraction was computed — window-time, block-comparability, or another basis.
    /// Carried rather than assumed, because two figures at "80% coverage" on different bases
    /// are not comparable and a bare number would imply they are.
    pub coverage_kind: &'static str,
}

/// The read layer, bound to one engine connection and one fleet.
///
/// Fleet is held rather than passed per call: every query in the product is fleet-scoped, and a
/// per-call parameter is one a caller can forget or get wrong. This is the same reasoning that
/// puts quoting behind a single path rather than at each site.
pub struct Dao<'a> {
    engine: &'a Engine,
    /// The horizons the reach probes bound by — the daemon's validated knobs, so the bound
    /// and the TTL agree. Defaults until [`Dao::with_retention`].
    retention: crate::engine::RetentionConfig,
    /// The writer's spine: the consistency boundary reads the same journal the writer commits
    /// to, and the fleet is its namespace's.
    spine: std::sync::Arc<analyzer_pipeline::spine::Spine>,
    /// The serving store this layer reads, as the executor's port: whether it has settled.
    serving: std::sync::Arc<dyn analyzer_pipeline::publish::Serving>,
    /// The derivation source the raw-grain reads open: the same segments, cache and horizon the
    /// pass derives from, so a live figure and a banked one cannot come from two input paths.
    sources: SourceConfig,
    fleet_id: String,
    discriminator: Discriminator,
    /// The parameters these figures are read under, held whole.
    ///
    /// Not everything here is discriminating and that is the point: `delta_skew_ms` shrinks
    /// intervals in classification yet does not belong in the key.
    /// Holding the set and deriving the token from it keeps both facts in one place.
    params: Params,
}

impl<'a> Dao<'a> {
    /// Bind a read layer to an engine, the derivation source, the parameters its figures are
    /// read under, and the writer's spine and serving store.
    #[must_use]
    pub fn new(
        engine: &'a Engine,
        sources: SourceConfig,
        params: &Params,
        spine: std::sync::Arc<analyzer_pipeline::spine::Spine>,
        serving: std::sync::Arc<dyn analyzer_pipeline::publish::Serving>,
    ) -> Self {
        Self {
            engine,
            retention: crate::engine::RetentionConfig::default(),
            fleet_id: spine.namespace().fleet().to_string(),
            spine,
            serving,
            sources,
            discriminator: Discriminator::of(params),
            params: params.clone(),
        }
    }

    /// The horizons this read layer bounds by: the daemon's validated knobs. A read that
    /// probes for reach (`min(bucket)` under a horizon) must agree with the TTL that
    /// expires the same rows, or an island past the horizon reads as reach.
    #[must_use]
    pub fn with_retention(mut self, retention: crate::engine::RetentionConfig) -> Self {
        self.retention = retention;
        self
    }

    /// The executor over the writer's spine: the derivation's own record, which the
    /// standing questions read.
    pub(crate) fn executor(&self) -> Result<analyzer_pipeline::Executor<'_>, Error> {
        Ok(analyzer_pipeline::Executor::open(
            std::sync::Arc::clone(&self.spine),
            &self.params,
            std::sync::Arc::clone(&self.serving),
        )?)
    }

    /// Every segment row the core published for the fleet (`input_segments`), as the input
    /// log states each: what the arrivals-backed folds read.
    pub(crate) fn input_segments(
        &self,
    ) -> Result<Vec<analyzer_pipeline::ledger::SegmentArrival>, Error> {
        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let out = self.engine.run(&format!(
            "SELECT incarnation, segment_seq, instance, first_record_at_ms, last_record_at_ms, \
                    heartbeat_secs, max_segment_secs, hb_count, hb_first_at_ms, hb_last_at_ms, \
                    stop_at_ms, msgs_seen, dropped, oversized, dropped_at_hb_first, \
                    oversized_at_hb_first, publisher_restarts, \
                    agent_version, topic_model, topic_pod, declared_model, declared_pod, \
                    engine_version, attributed, canary \
             FROM input_segments FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' \
               AND gone = 0 \
             FORMAT TSV"
        ))?;
        out.lines()
            .filter(|l| !l.is_empty())
            .map(segment_row)
            .collect()
    }

    /// The derivation source, opened on this DAO's own connection. Raw-grain reads go through
    /// this; banked reads never need it.
    ///
    /// **The content cache is shared with the derivation, and the sharing is the configuration.**
    /// `SourceConfig.cache_dir` names the same directory the daemon's pass source fetches
    /// into (`parquet_source::segment_cache_dir`), and the cache is the directory — so a
    /// read here hits every segment the derivation already landed, a segment a read lands is a hit
    /// for the next pass, and fetch-once holds across the whole process rather than per
    /// instance.
    pub(crate) fn source(
        &self,
    ) -> analyzer_pipeline::parquet_source::ParquetSource<
        Box<dyn analyzer_pipeline::parquet_source::SegmentStore + 'a>,
    > {
        self.sources.source()
    }

    #[must_use]
    /// The fleet every read is keyed by.
    pub fn fleet_id(&self) -> &str {
        &self.fleet_id
    }

    /// The discriminator every read is scoped to.
    ///
    /// **Never sum across discriminators**. Holding it here
    /// rather than passing it per call is what makes that structural: a read cannot span two
    /// semantics generations by omission, only by deliberately constructing a second `Dao`.
    #[must_use]
    pub fn discriminator(&self) -> &Discriminator {
        &self.discriminator
    }

    #[must_use]
    /// The engine the reads go through.
    pub fn engine(&self) -> &Engine {
        self.engine
    }

    /// **The meaning pair as a predicate — `(semantics_version, discriminator)`, both halves,
    /// every keyed-history read** (migration 0001's header law).
    ///
    /// Never sum across either: the discriminator says which parameters produced a row and
    /// `semantics_version` says which join-semantics did, and two generations of either are two
    /// different measurements that happen to share a day. Built here rather than spelled at each
    /// call site for the same reason `FINAL` is a property of the accessor — a rule a caller can
    /// forget is a rule that holds until somebody is in a hurry.
    /// The day's coverage fraction joined at read time. The
    /// rollup cells carry events and tokens; the day's fraction — covered over observed,
    /// summed across the fleet's sources, exactly what [`Dao::coverage`] states for the
    /// day — lives once in `coverage_daily`, and a figure's token-weighted coverage is
    /// `sum(tokens * fraction) / sum(tokens)` over the cells it groups. A day with no
    /// coverage row weights at zero: absent coverage is no coverage, never full. Joining at read
    /// time is what lets a tick write only the cells it touched: the fraction moves while the
    /// day is open, and a cell banking `tokens × fraction` would be re-priced every tick.
    fn coverage_join(&self, grain: Grain) -> String {
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        format!(
            " LEFT JOIN (SELECT day, sum(covered_ms) / sum(window_ms) AS fraction \
                         FROM coverage_daily FINAL \
                         WHERE fleet_id = {fleet} AND {meaning} AND gone = 0 GROUP BY day) AS cov \
              ON {day} = cov.day",
            day = grain.day_of_bucket(),
        )
    }

    /// A standing fact of the derivation (`derivation_status`, `pipeline::publish::Status`) as
    /// the core last stated it under this meaning; `None` before it was ever stated.
    fn status_fact(&self, fact: &str) -> Result<Option<f64>, Error> {
        let fleet = quote_literal(&self.fleet_id);
        let fact = quote_literal(fact);
        let meaning = self.meaning_pair();
        let out = self.engine.run(&format!(
            "SELECT value FROM derivation_status FINAL \
             WHERE fleet_id = {fleet} AND {meaning} AND fact = {fact} AND gone = 0"
        ))?;
        Ok(out.trim().parse::<f64>().ok())
    }

    fn meaning_pair(&self) -> String {
        format!(
            "semantics_version = {} AND discriminator = {}",
            quote_literal(analyzer_analysis::SEMANTICS_VERSION),
            quote_literal(self.discriminator.as_str()),
        )
    }
}

impl Dao<'_> {
    /// Resolve a request's grain: an explicit tier is honored verbatim; absent means
    /// the SERVER chooses ([`Grain::auto`]) from the range and the raw span it
    /// actually holds — the measured span, never the configured horizon, so a fresh
    /// install answers about the data it has.
    ///
    /// # Errors
    /// Engine failure reading the raw span.
    pub fn resolve_grain(
        &self,
        window: &Window,
        requested: Option<Grain>,
        minute: MinuteTier,
    ) -> Result<Grain, Error> {
        if let Some(grain) = requested {
            return Ok(grain);
        }
        let raw_start = self.raw_span()?.map(|s| s.start_ms);
        // The minute tier is a candidate only where the caller's cut one of its tables
        // can serve, and its reach is THAT table's own — measured, never assumed from
        // the TTL setting (two minute tables, each its own reach).
        let minute_start = match minute {
            MinuteTier::Unavailable => None,
            MinuteTier::Fleet => self.minute_span_start("recompute_minute")?,
            MinuteTier::Rank => self.minute_span_start("recompute_rank_minute")?,
        };
        Ok(Grain::auto(window, raw_start, minute_start))
    }

    /// Resolve the copies reads' grain: an explicit tier is honoured where its table's
    /// measured reach covers the window and refused by name where it does not; absent, the
    /// server chooses ([`Grain::auto`]) from the hour and minute tiers' reach.
    ///
    /// # Errors
    /// Engine failure reading a tier's reach, or [`Error::GrainUnavailable`] for an explicit
    /// tier whose reach does not cover the window.
    pub fn copies_grain(&self, window: &Window, requested: Option<Grain>) -> Result<Grain, Error> {
        let minute = self.replication_reach_ms(Grain::Minute)?;
        let hour = self.replication_reach_ms(Grain::Hour)?;
        for (tier, reach, fallback) in [
            (Grain::Minute, minute, "the hour or day tier"),
            (Grain::Hour, hour, "the day tier"),
        ] {
            if requested == Some(tier) && !reach.is_some_and(|r| r <= window.start_ms) {
                let word = tier.as_str();
                return Err(Error::GrainUnavailable(match reach {
                    Some(r) => format!(
                        "the {word} tier's measured reach starts at {r:.0} ms and this \
                         window starts before it; ask a later window, or {fallback}"
                    ),
                    None => format!(
                        "no {word} cells are banked yet, so the {word} tier has no reach; \
                         ask {fallback}"
                    ),
                }));
            }
        }
        Ok(requested.unwrap_or_else(|| Grain::auto(window, hour, minute)))
    }

    /// The oldest minute bucket `recompute_minute` currently holds for this fleet and
    /// meaning INSIDE the minute horizon — the minute tier's measured reach. `None` when
    /// nothing inside the horizon is banked (a fresh install, or history older than it).
    ///
    /// **Bounded by the horizon on purpose**.
    /// The table expires by bucket age at the next merge, so a row past the horizon can
    /// exist for a while, and an unbounded `min(bucket)` read a re-passed old day's cells
    /// as reach: a short window between that island and the live edge then resolved to
    /// minute grain and drew zeros. The bound and the TTL read one constant, so they agree.
    fn minute_span_start(&self, table: &str) -> Result<Option<f64>, Error> {
        let into = crate::engine::quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let horizon = sql_i64(i64::from(self.retention.minute_retention_days));
        let out = self.engine.run(&format!(
            "SELECT toUnixTimestamp(min(bucket)) FROM {into} \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} \
               AND bucket >= now() - INTERVAL {horizon} DAY"
        ))?;
        Ok(out
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|v| *v > 0.0)
            .map(|secs| secs * 1000.0))
    }
}

/// One `input_segments` row as [`Dao::input_segments`] selects it, in TSV.
fn segment_row(line: &str) -> Result<analyzer_pipeline::ledger::SegmentArrival, Error> {
    let mut r = Row::new("input_segments", line);
    let row = analyzer_pipeline::ledger::SegmentArrival {
        incarnation: r.text("incarnation")?.to_string(),
        segment_seq: r.parse("segment_seq")?,
        instance: r.text_or_empty("instance")?.map(str::to_string),
        first_record_at_ms: r.parse("first_record_at_ms")?,
        last_record_at_ms: r.parse("last_record_at_ms")?,
        heartbeat_secs: r.parse("heartbeat_secs")?,
        max_segment_secs: r.parse("max_segment_secs")?,
        hb_count: r.parse("hb_count")?,
        hb_first_at_ms: r.nullable("hb_first_at_ms")?,
        hb_last_at_ms: r.nullable("hb_last_at_ms")?,
        stop_at_ms: r.nullable("stop_at_ms")?,
        msgs_seen: r.parse("msgs_seen")?,
        dropped: r.parse("dropped")?,
        oversized: r.parse("oversized")?,
        dropped_at_hb_first: r.parse("dropped_at_hb_first")?,
        oversized_at_hb_first: r.parse("oversized_at_hb_first")?,
        publisher_restarts: r.parse("publisher_restarts")?,
        agent_version: r.text_or_empty("agent_version")?.map(str::to_string),
        topic_model: r.text_or_empty("topic_model")?.map(str::to_string),
        topic_pod: r.text_or_empty("topic_pod")?.map(str::to_string),
        declared_model: r.text_or_empty("declared_model")?.map(str::to_string),
        declared_pod: r.text_or_empty("declared_pod")?.map(str::to_string),
        engine_version: r.text_or_empty("engine_version")?.map(str::to_string),
        attributed: r.text("attributed")? != "0",
        canary: r.text_or_null("canary")?.map(str::to_string),
    };
    r.end()?;
    Ok(row)
}

/// Epoch SECONDS for an engine-domain instant, TRUNCATED.
///
/// **`{:.0}` rounds, and rounding here is a defect.** Every bucket predicate bounds
/// its upper end with `end_ms - 1` — "the last instant the window actually covers" — which on a
/// day-aligned window is `…399.999`. Formatted with `{:.0}` that becomes `…400`: the exclusive
/// boundary itself, one whole bucket past the window.
///
/// So every window whose end sits on a second — which is every whole-day window, meaning every
/// window this product serves — read ONE EXTRA BUCKET at the top. A fourteen-day window summed
/// fifteen days, across shape 1 (seven panels), shape 3, shape 6 and shape 9.
///
/// Found by probing why a comparison's two sides reported identical event counts over disjoint
/// days. **The half-open convention was correct everywhere it was written down; the rendering to
/// SQL quietly undid it** — which is why no amount of re-reading the window arithmetic would have
/// surfaced it.
// Epoch seconds fit i64 for any instant this product can hold; the truncation the lint warns
// about is the POINT here rather than a hazard.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn epoch_seconds(ms: f64) -> i64 {
    (ms / 1000.0).floor() as i64
}

/// A `Date` literal for an engine-domain instant, so a day-keyed table can be filtered by an
/// engine window without either side guessing at the other's rendering.
pub(crate) fn day_literal(ms: f64) -> String {
    format!("toDate(toDateTime({}, 'UTC'))", epoch_seconds(ms))
}

// ===================================================================================
// The shapes, one module each.
//
// **The module list IS the shape table**, which is the same list as the panel grid and the read
// API's endpoint list. One shape, one module, so a row type sits beside the query that builds it.
//
// Adding a tenth shape is a new file, not a longer scroll. Types live beside the query that
// returns them, because that pairing is the thing that must not drift.
//
// Re-exported flat below: the crate presents ONE surface, and a caller should never need to know
// which shape a row type belongs to in order to name it.
// ===================================================================================

pub mod bands;
pub mod coverage;
pub mod delivery;
pub mod drill;
pub mod engine;
mod error;
pub mod events;
pub mod holders;
pub mod ingest;
pub mod markers;
pub mod persist;
pub mod placement;
pub mod raw;
pub mod replication_read;
pub mod roster;
mod row;
mod rowbinary;
pub mod selfhealth;
pub mod serving;
pub mod shape1;
pub mod stored;
pub mod tags;
pub mod whales;

pub use error::Error;

pub use bands::{BandLens, BandRead, BandRow, HolderBound};
pub use coverage::{CoverageBy, CoverageRow};
pub use delivery::{DeliveryReport, DeliveryRow, DeliveryTotals};
pub use drill::{DayDetail, Evidence, EvidenceCursor, EvidenceOf, EvidenceRecord, RawReach};
pub use events::{
    BlockDay, EventPage, HolderEvent, HolderEventCursor, RankedBlock, ReadFrom, TimelineCursor,
    TimelineEvent,
};
pub use holders::{HolderCursor, HolderFilters, HolderRow, HoldersPage, Page};
pub use ingest::{Batch, IngestStatus};
// The annotation contract itself (object form, prefix, encode/parse) lives in
// `analyzer_ingest::annotations` and is re-exported whole: the API layer builds the durable
// object through this one surface without growing its own edge to ingest.
pub use analyzer_ingest::annotations;
pub use markers::{DeployAnnotation, DeploySource, Marker};
pub use placement::{PairRow, Placement};
pub use raw::Edges;
pub use roster::RunRow;
pub use shape1::{RefusalCause, Segment, SegmentBasis, WasteRow};
pub use stored::StoredMassRow;
pub use tags::TagRead;

#[cfg(test)]
mod boundary_tests {
    use super::{day_literal, epoch_seconds, Grain};
    use analyzer_analysis::Window;

    /// The AUTO grain rule: each tier needs BOTH its render budget and its
    /// own data actually reaching the window's start — the minute tier off the minute
    /// table's measured span, the hour tier off the raw's. A range past a tier's reach
    /// coarsens however short it is, because that tier's data does not exist there.
    #[test]
    fn auto_grain_follows_length_and_reach_never_a_client_constant() {
        const HOUR: f64 = 3_600_000.0;
        let w = |start: f64, hours: f64| Window {
            start_ms: start,
            end_ms: start + hours * HOUR,
        };
        // Short, raw-covered, minute table silent (wrong cut or empty): hourly.
        assert_eq!(
            Grain::auto(&w(1_000.0 * HOUR, 48.0), Some(0.0), None),
            Grain::Hour
        );
        // Exactly the hour budget still fits; one hour past it coarsens.
        assert_eq!(
            Grain::auto(&w(1_000.0 * HOUR, 200.0), Some(0.0), None),
            Grain::Hour
        );
        assert_eq!(
            Grain::auto(&w(1_000.0 * HOUR, 201.0), Some(0.0), None),
            Grain::Day
        );
        // Short but OLDER than the raw's measured reach: daily, however short.
        assert_eq!(
            Grain::auto(&w(10.0 * HOUR, 3.0), Some(500.0 * HOUR), None),
            Grain::Day
        );
        // No raw at all (a fresh install before its first segment): daily.
        assert_eq!(Grain::auto(&w(10.0 * HOUR, 3.0), None, None), Grain::Day);
        // The minute tier: covered and inside its 240-point budget wins the tightest
        // tier; one minute past the budget falls to hourly; a window starting before
        // the minute table's reach falls through even when tiny.
        assert_eq!(
            Grain::auto(&w(1_000.0 * HOUR, 4.0), Some(0.0), Some(0.0)),
            Grain::Minute
        );
        assert_eq!(
            Grain::auto(&w(1_000.0 * HOUR, 4.0 + 1.0 / 60.0), Some(0.0), Some(0.0)),
            Grain::Hour
        );
        assert_eq!(
            Grain::auto(&w(10.0 * HOUR, 2.0), Some(0.0), Some(20.0 * HOUR)),
            Grain::Hour
        );
    }

    #[test]
    fn a_half_open_window_does_not_reach_the_bucket_past_its_own_end() {
        // **The defect this pins, live across four query shapes.** The upper
        // bound is `end_ms - 1` — the last instant covered — which on a day-aligned window is
        // `…399.999`. `{:.0}` ROUNDS, so it rendered as `…400`: the exclusive boundary, one whole
        // bucket past the window. A fourteen-day window summed fifteen days.
        //
        // Caught by a comparison whose two sides reported identical event counts over disjoint
        // days. The window arithmetic was right; its rendering to SQL was not, which is why
        // re-reading the arithmetic could never have found it.
        let day = 86_400_000.0;
        let start = 20_660.0 * day; // a UTC midnight
        let w = Window {
            start_ms: start,
            end_ms: start + day,
        };

        let predicate = Grain::Day.window_predicate(&w);
        assert!(
            predicate.contains(&epoch_seconds(start).to_string()),
            "the lower bound is the window's own first instant: {predicate}"
        );
        assert!(
            !predicate.contains(&epoch_seconds(start + day).to_string()),
            "the upper bound must NOT be the exclusive boundary — that is the next bucket: \
             {predicate}"
        );
    }

    #[test]
    fn epoch_seconds_truncates_rather_than_rounding() {
        let boundary = 1_785_110_400_000.0_f64;
        assert_eq!(epoch_seconds(boundary), 1_785_110_400);
        assert_eq!(
            epoch_seconds(boundary - 1.0),
            1_785_110_399,
            "one millisecond before a boundary is the PREVIOUS second, not the boundary rounded"
        );
    }

    #[test]
    fn a_day_literal_carries_an_instant_inside_the_intended_day() {
        // `day_literal` renders `toDate(toDateTime(secs))`, so two instants in one day produce
        // DIFFERENT literals that resolve to the same date — the engine does the flooring. What
        // is checkable here is the seconds, and the property that matters is that the last
        // instant of a day still falls in that day rather than the next.
        let day = 86_400_000.0;
        let midnight = 20_660.0 * day;
        let last_instant = midnight + day - 1.0;

        assert_eq!(
            epoch_seconds(midnight) / 86_400,
            epoch_seconds(last_instant) / 86_400,
            "the last instant of a day belongs to that day"
        );
        assert_ne!(
            epoch_seconds(midnight) / 86_400,
            epoch_seconds(midnight + day) / 86_400,
            "and the exclusive boundary belongs to the next one"
        );
        assert!(day_literal(last_instant).contains(&epoch_seconds(last_instant).to_string()));
    }
}
