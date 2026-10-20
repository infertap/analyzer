// SPDX-License-Identifier: AGPL-3.0-only

//! The rollup rows: the whole-day fold of classified facts into the eight tables' rows, at the
//! three grains, and the minute collapses.

use super::calendar::{day_index, hour_index, minute_index, month_start};
use super::keys::{
    BandFleetMinuteKey, BandKey, BandRankMinuteKey, ClassKey, FleetMinuteKey, RankMinuteKey,
};
use super::shape::Sums;
use super::{Cells, ClassifiedFact};
use crate::classify::Class;
use crate::intervals::Holder;

/// The time grain a rollup buckets into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grain {
    /// One row per UTC day; the daily tables partition by month, which is why its rows carry
    /// one.
    Daily,
    /// One row per UTC hour, stated from the first tick, since the event grain behind it does
    /// not outlive its retention.
    Hourly,
    /// One row per UTC minute — the thrash zoom's spine. The same fold as the other two, so the
    /// three grains cannot drift; the pipeline collapses its dimensions to fleet × class before
    /// holding it, because holder × minute is a cardinality explosion.
    Minute,
}

impl Grain {
    /// The bucket an instant falls in: the day index, or the hour's or minute's start in unix
    /// seconds.
    #[must_use]
    pub fn bucket(self, at_ms: f64) -> i64 {
        match self {
            Self::Daily => day_index(at_ms),
            Self::Hourly => hour_index(at_ms),
            Self::Minute => minute_index(at_ms),
        }
    }
}

/// One aggregated row of the recompute register.
///
/// **The weighted pair rather than an average**, so a grouped read carries coverage instead of
/// losing it in the `GROUP BY`. A figure without coverage cannot exist, one axis over.
#[derive(Debug, Clone, PartialEq)]
pub struct RollupRow {
    /// Days since the epoch for [`Grain::Daily`]; unix SECONDS for [`Grain::Hourly`]. Each is the
    /// representation its column already holds, so neither can drift from the store through a
    /// formatting difference.
    pub bucket: i64,
    /// The first of `bucket`'s month, days since the epoch. Present only at daily grain, where
    /// the daily tables partition by it.
    pub month: Option<i64>,
    /// The bucket the row's facts share, a key dimension.
    pub class: Class,
    /// The facts' declared workload class, a key dimension; `undeclared` where none was
    /// declared.
    pub workload_class: String,
    /// The facts' relayed model, a key dimension. `None` where the deployment stated none.
    pub model: Option<String>,
    /// The cache the facts landed in, a key dimension: instance, rank and group.
    pub holder: Holder,
    /// Facts in the cell after the identity dedup: one per store replace key, so an
    /// opener and its same-instant re-admit count once.
    pub events: u64,
    /// `n_tokens` summed over the same facts; a lower bound, since an absent count reads 0.
    pub tokens: u64,
}

impl RollupRow {
    /// The row's measures.
    pub(super) fn sums(&self) -> Sums {
        Sums {
            events: self.events,
            tokens: self.tokens,
        }
    }
}

/// Aggregate classified facts into rollup rows — the `GROUP BY` as a fold.
///
/// **The driver already holds these facts, so grouping them is arithmetic rather than a query**,
/// and the rollup rests on no store-side dedup.
///
/// **Never sum across a meaning generation**, which is why the pair is in the key rather than
/// merely on the row: two generations of one day would otherwise add together and report the sum
/// as one number.
#[must_use]
pub fn rollup(facts: &[ClassifiedFact], grain: Grain) -> Vec<RollupRow> {
    // `BTreeMap` so the emission order is a property of the data rather than of hashing: two
    // runs over identical facts produce byte-identical bodies.
    let mut groups: std::collections::BTreeMap<ClassKey, (Class, Sums)> =
        std::collections::BTreeMap::new();
    for f in dedup_identity(facts) {
        let slot = groups
            .entry(ClassKey::of(f, grain))
            .or_insert((f.class, Sums::default()));
        slot.1.add(f.n_tokens);
    }
    groups
        .into_iter()
        .map(|(k, (class, s))| class_row(&k, class, s, grain))
        .collect()
}

/// **Two facts sharing an identity are one fact, and every fold must agree with the drill about
/// that.** A fact's identity is `(holder, block, key_epoch, at_ms, meaning)`, which
/// deliberately excludes `class`. So an opener and a same-instant re-admit on one block are
/// ONE fact in the holder's ring a drill reads, and a fold that
/// counted two would make the trend contradict the drill beneath it. That is the exact failure
/// the meaning pair exists to prevent, one surface over. `key_epoch` is in this tuple for the
/// same agreement: it is identity-bearing, so two same-instant facts differing only in epoch
/// are two facts.
///
/// The store expresses this as replacement and keeps the last row inserted; the folds express
/// it as a dedup in emission order, which is the same law written where the facts are. Pinned
/// by `correctness::same_instant_stores_still_collapse_in_the_rollup`. One function for both
/// [`rollup`] and [`band_rollup`], so the law has one spelling instead of two that can drift.
fn dedup_identity(facts: &[ClassifiedFact]) -> Vec<&ClassifiedFact> {
    let mut identity: std::collections::BTreeMap<StoreKey, &ClassifiedFact> =
        std::collections::BTreeMap::new();
    for f in facts {
        identity.insert(StoreKey::of(f), f);
    }
    identity.into_values().collect()
}

/// A classified store's replace key: the holder, the block, its epoch and the instant. Two
/// facts with one key are one fact, in the store and in every fold.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoreKey {
    /// The cache the store landed in.
    pub holder: Holder,
    /// The block identity.
    pub block: String,
    /// The epoch scoping the identity, where one was declared.
    pub key_epoch: Option<u32>,
    /// The store's instant, as bits so the key is totally ordered.
    pub at_bits: u64,
}

impl StoreKey {
    /// The key of `f`.
    #[must_use]
    pub fn of(f: &ClassifiedFact) -> Self {
        Self {
            holder: f.holder.clone(),
            block: f.block.clone(),
            key_epoch: f.key_epoch,
            at_bits: f.at_ms.to_bits(),
        }
    }
}

/// A fixed time-since-eviction band — the thrash lens's dimension.
///
/// **The bands are fixed, fleet to fleet, so the figure compares.** They partition
/// [`ClassifiedFact::gap_ms`]: each fact lands in exactly one band, so banding a rollup splits
/// its rows without changing any sum. Edges are half-open `[lo, hi)` — the same convention every
/// window in this workspace states.
///
/// **`Unobserved` is a real state, never zero and never a claim about the bound.** A fact with no
/// gap conflates two causes — first stored inside the examined history, or gone longer than the
/// history reaches — and the two are indistinguishable BY CONSTRUCTION. So this band carries that
/// mass under the storage value `none`, and "mass the lookback censored" is a figure no reader
/// may derive from it: the quantity is unknowable, and glass renders this band beside the
/// observed four, never stacked among them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Band {
    /// `[0, 10 s)` — and any negative gap, which clock skew alone can produce.
    Under10Sec,
    /// `[10 s, 1 min)`.
    Sec10ToMin1,
    /// `[1 min, 10 min)`.
    Min1To10,
    /// `[10 min, 60 min)`: up to the examined history, which is at most an hour.
    Min10To60,
    /// No observed prior close — see the type docs before reading anything into this.
    Unobserved,
}

impl Band {
    /// Every band, for callers that must enumerate the closed set rather than match on it.
    /// `every_band_is_in_all` pins the count, as [`Class::ALL`]'s does.
    pub const ALL: [Self; 5] = [
        Self::Under10Sec,
        Self::Sec10ToMin1,
        Self::Min1To10,
        Self::Min10To60,
        Self::Unobserved,
    ];

    /// The observed bands' upper edges in milliseconds, ascending — the half-open `[lo, hi)`
    /// boundaries [`Band::of`] walks. The last observed band, [`Band::Min10To60`], ends at the
    /// examined history, which no gap reaches, and so has no entry.
    ///
    /// **Public because the read layer derives the band in SQL for sub-horizon scans**, and a
    /// boundary spelled twice is the `{:.0}` rounding defect's family: correct in types, broken
    /// at the moment it becomes a SQL string. One constant, two derivations.
    pub const UPPER_EDGES_MS: [(f64, Self); 3] = [
        (10_000.0, Self::Under10Sec),
        (60_000.0, Self::Sec10ToMin1),
        (600_000.0, Self::Min1To10),
    ];

    /// The band a gap lands in. Total: every representable gap has exactly one band.
    #[must_use]
    pub fn of(gap_ms: Option<f64>) -> Self {
        let Some(g) = gap_ms else {
            return Self::Unobserved;
        };
        // A non-number states nothing about duration; it cannot silently read as "over a
        // day". Unreachable from the classifier (a gap is a difference of two finite
        // instants), stated because `<` would otherwise decide it by failing.
        if g.is_nan() {
            return Self::Unobserved;
        }
        for (edge, band) in Self::UPPER_EDGES_MS {
            if g < edge {
                return band;
            }
        }
        Self::Min10To60
    }

    /// The inverse of [`Band::as_str`], for reading a banked band back. `None` on an
    /// unknown token — refused, never guessed.
    #[must_use]
    pub fn parse(token: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|b| b.as_str() == token)
    }

    /// The storage spelling — what `band_rollup_daily.band` holds and the read API serves.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Under10Sec => "<10s",
            Self::Sec10ToMin1 => "10s-1m",
            Self::Min1To10 => "1-10m",
            Self::Min10To60 => "10-60m",
            Self::Unobserved => "none",
        }
    }
}

/// One aggregated row of the recompute register with the band dimension — what
/// `band_rollup_daily` holds.
///
/// The daily table: band history past the event-grain horizon; sub-horizon windows read
/// `band_rollup_hourly` and `band_minute`. So `bucket` is always a day count and `month` is
/// always present, the partition key `class_rollup_daily` also carries.
#[derive(Debug, Clone, PartialEq)]
pub struct BandRollupRow {
    /// Days since the epoch.
    pub bucket: i64,
    /// The first of `bucket`'s month, days since the epoch — the TTL `GROUP BY`'s key prefix.
    pub month: i64,
    /// The bucket the row's facts share, a key dimension.
    pub class: Class,
    /// The facts' band, [`Band::of`] their `gap_ms`: the key dimension this table adds.
    pub band: Band,
    /// The facts' declared workload class, a key dimension; `undeclared` where none was
    /// declared.
    pub workload_class: String,
    /// The facts' relayed model, a key dimension. `None` where the deployment stated none.
    pub model: Option<String>,
    /// The cache the facts landed in, a key dimension: instance, rank and group.
    pub holder: Holder,
    /// Facts in the cell after the identity dedup: one per store replace key, so an
    /// opener and its same-instant re-admit count once.
    pub events: u64,
    /// `n_tokens` summed over the same facts; a lower bound, since an absent count reads 0.
    pub tokens: u64,
}

impl BandRollupRow {
    /// The row's measures.
    pub(super) fn sums(&self) -> Sums {
        Sums {
            events: self.events,
            tokens: self.tokens,
        }
    }
}

/// Aggregate classified facts into banded rollup rows — [`rollup`] with the band in the key.
///
/// A separate fold over a separate table rather than a widening of the class rollup's key:
/// the class rollups' goldens stay untouched, and the two tables state
/// their own grains — this one is daily-only. Everything [`rollup`] documents holds here:
/// identity dedup before grouping, `BTreeMap` for byte-identical emission order, and never
/// summing across a meaning generation.
#[must_use]
pub fn band_rollup(facts: &[ClassifiedFact], grain: Grain) -> Vec<BandRollupRow> {
    let mut groups: std::collections::BTreeMap<BandKey, ((Class, Band), Sums)> =
        std::collections::BTreeMap::new();
    for f in dedup_identity(facts) {
        // The band is stamped per EVENT before any grouping, and that is what makes banded
        // minute cells sound: `Band::of(gap)` is a pure function of one identity's own event
        // pair, so banding partitions every sum without moving it, at ANY bucket width. The
        // grain only chooses the bucket.
        let band = Band::of(f.gap_ms);
        let slot = groups
            .entry(BandKey::of(f, band, grain))
            .or_insert(((f.class, band), Sums::default()));
        slot.1.add(f.n_tokens);
    }
    groups
        .into_iter()
        .map(|(k, (tag, s))| band_row(&k, tag, s, grain))
        .collect()
}

/// One fleet-grain minute cell — what `recompute_minute` holds: the class fold at minute grain
/// with the holder rolled up, workload and model kept (holder × minute is a
/// cardinality explosion).
#[derive(Debug, Clone, PartialEq)]
pub struct FleetMinuteRow {
    /// Unix SECONDS of the UTC minute, as [`minute_index`] renders it.
    pub bucket: i64,
    /// The bucket the row's facts share, a key dimension.
    pub class: Class,
    /// The facts' declared workload class, a key dimension; `undeclared` where none was
    /// declared.
    pub workload_class: String,
    /// The facts' relayed model, a key dimension. `None` where the deployment stated none.
    pub model: Option<String>,
    /// Facts in the cell, summed over the holder from the holder-grain minute rows.
    pub events: u64,
    /// `n_tokens` summed over the same facts; a lower bound, since an absent count reads 0.
    pub tokens: u64,
}

/// One rank-grain minute cell — what `recompute_rank_minute` holds: the instance and the rank
/// kept, the cache group rolled up. Not a [`Holder`], deliberately: a holder with
/// `group_idx: 0` would claim a group this cell has summed away.
#[derive(Debug, Clone, PartialEq)]
pub struct RankMinuteRow {
    /// Unix SECONDS of the UTC minute.
    pub bucket: i64,
    /// The instance kept in the key; with `dp_rank`, the rank this cell sums.
    pub instance: String,
    /// The data-parallel rank kept in the key; the cache group beneath it is summed away.
    pub dp_rank: u32,
    /// The bucket the row's facts share, a key dimension.
    pub class: Class,
    /// The facts' declared workload class, a key dimension; `undeclared` where none was
    /// declared.
    pub workload_class: String,
    /// The facts' relayed model, a key dimension. `None` where the deployment stated none.
    pub model: Option<String>,
    /// Facts in the cell, summed over the cache group from the holder-grain minute rows.
    pub events: u64,
    /// `n_tokens` summed over the same facts; a lower bound, since an absent count reads 0.
    pub tokens: u64,
}

/// [`FleetMinuteRow`] with the band in the key — what `band_minute` holds.
#[derive(Debug, Clone, PartialEq)]
pub struct BandFleetMinuteRow {
    /// Unix SECONDS of the UTC minute.
    pub bucket: i64,
    /// The bucket the row's facts share, a key dimension.
    pub class: Class,
    /// The facts' band, [`Band::of`] their `gap_ms`: the key dimension this table adds.
    pub band: Band,
    /// The facts' declared workload class, a key dimension; `undeclared` where none was
    /// declared.
    pub workload_class: String,
    /// The facts' relayed model, a key dimension. `None` where the deployment stated none.
    pub model: Option<String>,
    /// Facts in the cell, summed over the holder from the holder-grain minute rows.
    pub events: u64,
    /// `n_tokens` summed over the same facts; a lower bound, since an absent count reads 0.
    pub tokens: u64,
}

/// [`RankMinuteRow`] with the band in the key — what `band_rank_minute` holds.
#[derive(Debug, Clone, PartialEq)]
pub struct BandRankMinuteRow {
    /// Unix SECONDS of the UTC minute.
    pub bucket: i64,
    /// The instance kept in the key; with `dp_rank`, the rank this cell sums.
    pub instance: String,
    /// The data-parallel rank kept in the key; the cache group beneath it is summed away.
    pub dp_rank: u32,
    /// The bucket the row's facts share, a key dimension.
    pub class: Class,
    /// The facts' band, [`Band::of`] their `gap_ms`: the key dimension this table adds.
    pub band: Band,
    /// The facts' declared workload class, a key dimension; `undeclared` where none was
    /// declared.
    pub workload_class: String,
    /// The facts' relayed model, a key dimension. `None` where the deployment stated none.
    pub model: Option<String>,
    /// Facts in the cell, summed over the cache group from the holder-grain minute rows.
    pub events: u64,
    /// `n_tokens` summed over the same facts; a lower bound, since an absent count reads 0.
    pub tokens: u64,
}

/// The eight serving tables' rows for one day, folded from its facts with no engine — the
/// finished per-cell totals the fold owns.
///
/// **One fold, eight shapes.** The class and band folds at the three grains, with the minute
/// grain collapsed twice: to the fleet cells (workload and model kept) and to the rank cells
/// (the instance and the rank kept as well). The four collapses are arithmetic over the
/// holder-grain minute rows, so a rank cell summed over its ranks IS the fleet cell and a band
/// cell summed over its bands IS the class cell — pinned engine-free below, and against the
/// engine's own sums by the pipeline's `day_cells` suite. Every vector is in its key's
/// `BTreeMap` order, so two folds over identical facts render byte-identical bodies.
#[derive(Debug, Clone, PartialEq)]
pub struct DayCells {
    /// `class_rollup_daily`.
    pub class_daily: Vec<RollupRow>,
    /// `class_rollup_hourly`.
    pub class_hourly: Vec<RollupRow>,
    /// `recompute_minute`.
    pub fleet_minute: Vec<FleetMinuteRow>,
    /// `recompute_rank_minute`.
    pub rank_minute: Vec<RankMinuteRow>,
    /// `band_rollup_daily`.
    pub band_daily: Vec<BandRollupRow>,
    /// `band_rollup_hourly`.
    pub band_hourly: Vec<BandRollupRow>,
    /// `band_minute`.
    pub band_fleet_minute: Vec<BandFleetMinuteRow>,
    /// `band_rank_minute`.
    pub band_rank_minute: Vec<BandRankMinuteRow>,
}

/// Fold a day's facts into the eight tables' rows — see [`DayCells`]. The whole-day case of
/// [`Cells`]: everything added, nothing subtracted, rendered.
#[must_use]
pub fn day_cells(facts: &[ClassifiedFact]) -> DayCells {
    let mut cells = Cells::default();
    cells.add_facts(facts);
    cells.rows()
}

/// The holder-grain minute rows summed over the holder.
pub(super) fn collapse_to_fleet(rows: &[RollupRow]) -> Vec<FleetMinuteRow> {
    let mut cells: std::collections::BTreeMap<FleetMinuteKey, (Class, Sums)> =
        std::collections::BTreeMap::new();
    for r in rows {
        let slot = cells
            .entry(FleetMinuteKey::of(r))
            .or_insert((r.class, Sums::default()));
        slot.1.merge(r.sums());
    }
    cells
        .into_iter()
        .map(|(k, (class, s))| fleet_minute_row(&k, class, s))
        .collect()
}

/// The holder-grain minute rows summed over the cache group.
pub(super) fn collapse_to_rank(rows: &[RollupRow]) -> Vec<RankMinuteRow> {
    let mut cells: std::collections::BTreeMap<RankMinuteKey, (Class, Sums)> =
        std::collections::BTreeMap::new();
    for r in rows {
        let slot = cells
            .entry(RankMinuteKey::of(r))
            .or_insert((r.class, Sums::default()));
        slot.1.merge(r.sums());
    }
    cells
        .into_iter()
        .map(|(k, (class, s))| rank_minute_row(&k, class, s))
        .collect()
}

/// The banded holder-grain minute rows summed over the holder.
pub(super) fn collapse_bands_to_fleet(rows: &[BandRollupRow]) -> Vec<BandFleetMinuteRow> {
    let mut cells: std::collections::BTreeMap<BandFleetMinuteKey, ((Class, Band), Sums)> =
        std::collections::BTreeMap::new();
    for r in rows {
        let slot = cells
            .entry(BandFleetMinuteKey::of(r))
            .or_insert(((r.class, r.band), Sums::default()));
        slot.1.merge(r.sums());
    }
    cells
        .into_iter()
        .map(|(k, (tag, s))| band_fleet_minute_row(&k, tag, s))
        .collect()
}

/// The banded holder-grain minute rows summed over the cache group.
pub(super) fn collapse_bands_to_rank(rows: &[BandRollupRow]) -> Vec<BandRankMinuteRow> {
    let mut cells: std::collections::BTreeMap<BandRankMinuteKey, ((Class, Band), Sums)> =
        std::collections::BTreeMap::new();
    for r in rows {
        let slot = cells
            .entry(BandRankMinuteKey::of(r))
            .or_insert(((r.class, r.band), Sums::default()));
        slot.1.merge(r.sums());
    }
    cells
        .into_iter()
        .map(|(k, (tag, s))| band_rank_minute_row(&k, tag, s))
        .collect()
}

pub(super) fn class_row(k: &ClassKey, class: Class, s: Sums, grain: Grain) -> RollupRow {
    RollupRow {
        bucket: k.bucket,
        // Neither sub-daily grain carries a month: each sub-daily tier expires by bucket age
        // on its own knob.
        month: match grain {
            Grain::Daily => Some(month_start(k.bucket)),
            Grain::Hourly | Grain::Minute => None,
        },
        class,
        workload_class: k.workload_class.clone(),
        model: k.model.clone(),
        holder: k.holder.clone(),
        events: s.events,
        tokens: s.tokens,
    }
}

pub(super) fn band_row(
    k: &BandKey,
    (class, band): (Class, Band),
    s: Sums,
    grain: Grain,
) -> BandRollupRow {
    BandRollupRow {
        bucket: k.bucket,
        // A month only means something over a day bucket, the daily table's partition; the
        // finer tables have no month column, and zero is what their rows carry.
        month: match grain {
            Grain::Daily => month_start(k.bucket),
            Grain::Hourly | Grain::Minute => 0,
        },
        class,
        band,
        workload_class: k.workload_class.clone(),
        model: k.model.clone(),
        holder: k.holder.clone(),
        events: s.events,
        tokens: s.tokens,
    }
}

pub(super) fn fleet_minute_row(k: &FleetMinuteKey, class: Class, s: Sums) -> FleetMinuteRow {
    FleetMinuteRow {
        bucket: k.bucket,
        class,
        workload_class: k.workload_class.clone(),
        model: k.model.clone(),
        events: s.events,
        tokens: s.tokens,
    }
}

pub(super) fn rank_minute_row(k: &RankMinuteKey, class: Class, s: Sums) -> RankMinuteRow {
    RankMinuteRow {
        bucket: k.bucket,
        instance: k.instance.clone(),
        dp_rank: k.dp_rank,
        class,
        workload_class: k.workload_class.clone(),
        model: k.model.clone(),
        events: s.events,
        tokens: s.tokens,
    }
}

pub(super) fn band_fleet_minute_row(
    k: &BandFleetMinuteKey,
    (class, band): (Class, Band),
    s: Sums,
) -> BandFleetMinuteRow {
    BandFleetMinuteRow {
        bucket: k.bucket,
        class,
        band,
        workload_class: k.workload_class.clone(),
        model: k.model.clone(),
        events: s.events,
        tokens: s.tokens,
    }
}

pub(super) fn band_rank_minute_row(
    k: &BandRankMinuteKey,
    (class, band): (Class, Band),
    s: Sums,
) -> BandRankMinuteRow {
    BandRankMinuteRow {
        bucket: k.bucket,
        instance: k.instance.clone(),
        dp_rank: k.dp_rank,
        class,
        band,
        workload_class: k.workload_class.clone(),
        model: k.model.clone(),
        events: s.events,
        tokens: s.tokens,
    }
}
