// SPDX-License-Identifier: AGPL-3.0-only

//! The eight cell shapes' keys, each a struct whose field order is the emission order.

use super::rows::{Band, BandRollupRow, Grain, RollupRow};
use super::shape::Bucketed;
use super::ClassifiedFact;
use crate::intervals::Holder;

/// The class shapes' key: every dimension of `class_rollup_daily` and `class_rollup_hourly`,
/// bucket first. The field order is the emission order, so a render of the cells is
/// byte-identical to a whole-day fold of the same facts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClassKey {
    /// The day index, or the hour's start in unix seconds, by the grain.
    pub bucket: i64,
    /// The class, as its spelling on the wire.
    pub class: &'static str,
    /// The workload class, `undeclared` where none was.
    pub workload_class: String,
    /// The model, where one was declared.
    pub model: Option<String>,
    /// The cache.
    pub holder: Holder,
}

impl Bucketed for ClassKey {
    fn bucket(&self) -> i64 {
        self.bucket
    }
}

impl ClassKey {
    pub(super) fn of(f: &ClassifiedFact, grain: Grain) -> Self {
        Self {
            bucket: grain.bucket(f.at_ms),
            class: f.class.as_str(),
            workload_class: f.workload_class.clone(),
            model: f.model.clone(),
            holder: f.holder.clone(),
        }
    }
}

/// The band shapes' key: [`ClassKey`] with the gap band after the class.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BandKey {
    /// The day index, or the bucket's start in unix seconds, by the grain.
    pub bucket: i64,
    /// The class, as its spelling on the wire.
    pub class: &'static str,
    /// The gap band, as its spelling on the wire.
    pub band: &'static str,
    /// The workload class, `undeclared` where none was.
    pub workload_class: String,
    /// The model, where one was declared.
    pub model: Option<String>,
    /// The cache.
    pub holder: Holder,
}

impl Bucketed for BandKey {
    fn bucket(&self) -> i64 {
        self.bucket
    }
}

impl BandKey {
    pub(super) fn of(f: &ClassifiedFact, band: Band, grain: Grain) -> Self {
        Self {
            bucket: grain.bucket(f.at_ms),
            class: f.class.as_str(),
            band: band.as_str(),
            workload_class: f.workload_class.clone(),
            model: f.model.clone(),
            holder: f.holder.clone(),
        }
    }
}

/// `recompute_minute`'s key: the class key with the holder and the tier rolled up.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FleetMinuteKey {
    /// The minute's start in unix seconds.
    pub bucket: i64,
    /// The class, as its spelling on the wire.
    pub class: &'static str,
    /// The workload class, `undeclared` where none was.
    pub workload_class: String,
    /// The model, where one was declared.
    pub model: Option<String>,
}

impl Bucketed for FleetMinuteKey {
    fn bucket(&self) -> i64 {
        self.bucket
    }
}

impl FleetMinuteKey {
    pub(super) fn of(r: &RollupRow) -> Self {
        Self {
            bucket: r.bucket,
            class: r.class.as_str(),
            workload_class: r.workload_class.clone(),
            model: r.model.clone(),
        }
    }
}

/// `recompute_rank_minute`'s key: the class key with the cache group and the tier rolled up.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RankMinuteKey {
    /// The minute's start in unix seconds.
    pub bucket: i64,
    /// The instance.
    pub instance: String,
    /// The data-parallel rank on the instance.
    pub dp_rank: u32,
    /// The class, as its spelling on the wire.
    pub class: &'static str,
    /// The workload class, `undeclared` where none was.
    pub workload_class: String,
    /// The model, where one was declared.
    pub model: Option<String>,
}

impl Bucketed for RankMinuteKey {
    fn bucket(&self) -> i64 {
        self.bucket
    }
}

impl RankMinuteKey {
    pub(super) fn of(r: &RollupRow) -> Self {
        Self {
            bucket: r.bucket,
            instance: r.holder.instance.clone(),
            dp_rank: r.holder.dp_rank,
            class: r.class.as_str(),
            workload_class: r.workload_class.clone(),
            model: r.model.clone(),
        }
    }
}

/// `band_minute`'s key: [`FleetMinuteKey`] with the gap band after the class.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BandFleetMinuteKey {
    /// The minute's start in unix seconds.
    pub bucket: i64,
    /// The class, as its spelling on the wire.
    pub class: &'static str,
    /// The gap band, as its spelling on the wire.
    pub band: &'static str,
    /// The workload class, `undeclared` where none was.
    pub workload_class: String,
    /// The model, where one was declared.
    pub model: Option<String>,
}

impl Bucketed for BandFleetMinuteKey {
    fn bucket(&self) -> i64 {
        self.bucket
    }
}

impl BandFleetMinuteKey {
    pub(super) fn of(r: &BandRollupRow) -> Self {
        Self {
            bucket: r.bucket,
            class: r.class.as_str(),
            band: r.band.as_str(),
            workload_class: r.workload_class.clone(),
            model: r.model.clone(),
        }
    }
}

/// `band_rank_minute`'s key: [`RankMinuteKey`] with the gap band after the class.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BandRankMinuteKey {
    /// The minute's start in unix seconds.
    pub bucket: i64,
    /// The instance.
    pub instance: String,
    /// The data-parallel rank on the instance.
    pub dp_rank: u32,
    /// The class, as its spelling on the wire.
    pub class: &'static str,
    /// The gap band, as its spelling on the wire.
    pub band: &'static str,
    /// The workload class, `undeclared` where none was.
    pub workload_class: String,
    /// The model, where one was declared.
    pub model: Option<String>,
}

impl Bucketed for BandRankMinuteKey {
    fn bucket(&self) -> i64 {
        self.bucket
    }
}

impl BandRankMinuteKey {
    pub(super) fn of(r: &BandRollupRow) -> Self {
        Self {
            bucket: r.bucket,
            instance: r.holder.instance.clone(),
            dp_rank: r.holder.dp_rank,
            class: r.class.as_str(),
            band: r.band.as_str(),
            workload_class: r.workload_class.clone(),
            model: r.model.clone(),
        }
    }
}
