// SPDX-License-Identifier: AGPL-3.0-only

//! **The emitted-fact contract: what a pass produces, named once.**
//!
//! Every figure the product prints is derived from one of the rows below. The types here are
//! the declaration, and the serving crate's generic writer encodes them from each type's
//! `Persisted` spec, so a field renamed here cannot go silent at the store.
//!
//! **The record model carries no field names, deliberately.** That is the same split the inbound
//! spec already draws between a record model and its encoding: the semantics are what a
//! `ClassifiedFact` IS, and the wire spelling is the serializer's business. It is also what keeps
//! this crate free of a serialization dependency, so the derivation stays a pure function.
//!
//! **The meaning pair rides every row and there is no constructor that omits it.** A row without
//! it collides with the previous generation's row on the same key and overwrites it — history
//! rewritten with no seam, on a table somebody is drawing a trend from. `Meaning::current` is the
//! only way to make one, so "forgot the discriminator" is not a reachable state.
//!
//! **Versioned separately from `SEMANTICS_VERSION`, because they answer different questions.**
//! The semantics version says what the numbers MEAN. This says what SHAPE they arrive in. A
//! consumer reading these rows needs both, and conflating them would make a field addition look
//! like a meaning change.

use crate::classify::{Class, Classification};
use crate::intervals::Holder;
use crate::module::{Column, DayOf, Kind, Row, Table, Value};
use crate::{Discriminator, SEMANTICS_VERSION};

mod calendar;
mod cells;
mod keys;
mod rows;
mod shape;
#[cfg(test)]
mod tests;

pub use calendar::{
    civil_from_days, day_index, days_from_civil, hour_index, minute_index, month_start,
};
pub use cells::{CellChanges, Cells, Change};
pub use keys::{
    BandFleetMinuteKey, BandKey, BandRankMinuteKey, ClassKey, FleetMinuteKey, RankMinuteKey,
};
pub use rows::{
    band_rollup, day_cells, rollup, Band, BandFleetMinuteRow, BandRankMinuteRow, BandRollupRow,
    DayCells, FleetMinuteRow, Grain, RankMinuteRow, RollupRow, StoreKey,
};
pub use shape::{Bucketed, Shape, Sums};

/// The declared-absent bucket for an attribution dimension nothing has claimed.
///
/// **A real value rather than a null**, because these dimensions are DECLARED: the customer either
/// stated a mapping or did not, and "did not" is a bucket a reader can group by. Observed
/// dimensions take the opposite treatment — see [`ClassifiedFact::model`].
/// The workload class of a producer that declared none.
///
/// The declaration absence: the producer or deployment STATED NOTHING on the record.
/// `workload_class` (a producer MAY declare it on `segment_open` — spec 2.7, since 1.2),
/// `model` (declared at the deployment boundary) take this word.
///
/// **A bucket, never a null.** Absence of a declaration is not a declaration of absence, and a
/// reader must be able to group by "nobody said" rather than have those rows vanish from a split.
///
/// The class rides the record itself, declared by the producer, so its absence is a
/// DECLARATION absence, not a mapping miss: spec 2.7's own words decide the word.
pub const UNDECLARED: &str = "undeclared";

/// What every keyed-history row must carry to sit beside, rather than on top of, its predecessor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meaning {
    /// Our join-semantics — the human-legible half.
    pub semantics_version: &'static str,
    /// A token over the classification-affecting parameters — the half a hash covers.
    pub discriminator: String,
}

impl Meaning {
    /// The meaning this binary computes under. With [`Meaning::input`] and
    /// [`Meaning::generations`], **the only constructors**, so a row cannot be built without
    /// one.
    #[must_use]
    pub fn current(discriminator: &Discriminator) -> Self {
        Self {
            semantics_version: SEMANTICS_VERSION,
            discriminator: discriminator.as_str().to_string(),
        }
    }

    /// The meaning generation seams are stated under: facts about every meaning at once,
    /// which none of them owns ([`GENERATIONS`], no discriminator).
    #[must_use]
    pub fn generations() -> Self {
        Self {
            semantics_version: GENERATIONS,
            discriminator: String::new(),
        }
    }

    /// The meaning the served input tables are stated under: what arrived, in the input
    /// records' format ([`INPUT_FORMAT`]), with no discriminator, since what arrived does not
    /// depend on the question asked of it.
    #[must_use]
    pub fn input() -> Self {
        Self {
            semantics_version: INPUT_FORMAT,
            discriminator: String::new(),
        }
    }
}

/// The input records' format, as the served input tables state it in `semantics_version`.
pub const INPUT_FORMAT: &str = "input-1";

/// The generation seams' statement, as `markers` states it in `semantics_version`: the
/// seams between meanings, derived across all of them.
pub const GENERATIONS: &str = "generations-1";

/// The discontinuities a reader's trend steps at: declared deploys (stated under the input
/// meaning, from the input log) and generation seams (stated under the meaning that derived
/// them, from the journal). Keyed changes; a seam that no longer holds is tombstoned.
pub const MARKERS: Table = Table {
    name: "markers",
    key: &[
        Column::new("at_ms", Kind::F64),
        Column::new("kind", Kind::Str),
        Column::new("label", Kind::Str),
    ],
    measures: &[
        Column::new("provenance", Kind::Str),
        Column::new("from_value", Kind::NullableStr),
        Column::new("to_value", Kind::NullableStr),
        Column::new("source", Kind::Str),
        Column::new("scope", Kind::NullableStr),
        Column::new("declared_at_ms", Kind::NullableF64),
    ],
    day: DayOf::Undated,
};

/// One marker: a discontinuity a reader should know about when it draws a trend across it.
#[derive(Debug, Clone, PartialEq)]
pub struct Marker<'a> {
    /// When the discontinuity is, engine-domain milliseconds.
    pub at_ms: f64,
    /// Its kind: `deploy`, `param_change`, `semantics_change`.
    pub kind: &'a str,
    /// Its label, as a reader sees it.
    pub label: &'a str,
    /// How it was learned: `recorded`.
    pub provenance: &'a str,
    /// The value before the change, where the change is a step.
    pub from_value: Option<&'a str>,
    /// The value after the change, where one is named.
    pub to_value: Option<&'a str>,
    /// Who stated it: `analyzer`, `cli`, or the annotation's own source.
    pub source: &'a str,
    /// The scope it applies to, where it is not fleet-wide.
    pub scope: Option<&'a str>,
    /// When it was declared, where the declaration carried an instant of its own.
    pub declared_at_ms: Option<f64>,
}

impl Marker<'_> {
    /// The marker as a row of [`MARKERS`]; `gone` tombstones it.
    #[must_use]
    pub fn row(&self, gone: bool) -> Row {
        Row {
            table: MARKERS.name,
            key: vec![
                Value::F64(self.at_ms),
                Value::Str(self.kind.to_string()),
                Value::Str(self.label.to_string()),
            ],
            measures: vec![
                Value::Str(self.provenance.to_string()),
                Value::nullable(self.from_value),
                Value::nullable(self.to_value),
                Value::Str(self.source.to_string()),
                Value::nullable(self.scope),
                Value::nullable_f64(self.declared_at_ms),
            ],
            gone,
        }
    }
}

/// One classified store event — the grain every waste figure is aggregated from.
#[derive(Debug, Clone, PartialEq)]
pub struct ClassifiedFact {
    /// The cache the store landed in and was classified against: instance, rank and group.
    pub holder: Holder,
    /// The identity the block was classified under: `content_id` where the tap derived one.
    pub block: String,
    /// The epoch scoping that identity — the interval key's third element, so it is
    /// IDENTITY-BEARING and rides in the store's replace key (and in [`rollup`]'s dedup, which
    /// must agree with the store about what one fact is). `None` where no epoch was declared.
    pub key_epoch: Option<u32>,
    /// The store's instant, engine-domain milliseconds. Part of the fact's identity.
    pub at_ms: f64,
    /// Tokens in the stored block, as the record carried them; absent coalesced to 0 upstream,
    /// so every sum over this undercounts and never inflates.
    pub n_tokens: i64,
    /// The bucket the taxonomy assigned. Payload, never identity: the replace key excludes it,
    /// so a same-instant re-admit over its opener is ONE fact.
    pub class: Class,
    /// Milliseconds since this identity's most recent OBSERVED prior close on this holder —
    /// [`crate::classify::ClassifiedStore::gap_ms`], carried through to the drill row instead
    /// of surviving only as the banked distribution. `None` = no observed prior close within
    /// the examined history. Payload, never identity.
    pub gap_ms: Option<f64>,
    /// The coverage this figure was measured at, and the basis it is stated on. A figure without
    /// its coverage is a claim rather than a measurement.
    pub coverage: f64,
    /// DECLARED (spec 2.7, on `segment_open`). Producers that declared nothing read
    /// `undeclared` rather than falling to a default class.
    pub workload_class: String,
    /// **Declared at the deployment boundary, relayed verbatim**,
    /// joined per RUN from `fleet_inventory`. Absence is `None` and never a value: the
    /// deployment stated nothing, and a guessed value would be worse than an absent one.
    pub model: Option<String>,
    /// The meaning pair the fact was derived under. Part of the replace key, so a new
    /// generation sits beside its predecessor's row rather than over it.
    pub meaning: Meaning,
}

/// One day's observed coverage for one source.
///
/// **Whole days only.** These rows replace on `(fleet, day, instance, meaning)`, so a partial day
/// lands on the whole day's key and silently shrinks both the covered figure and the denominator
/// it is stated over.
#[derive(Debug, Clone, PartialEq)]
pub struct CoverageFact {
    /// Days since the epoch — `Date`'s own representation, and exactly what the store's day
    /// expression yields for that instant. One form, so the two cannot drift through formatting.
    pub day: i64,
    /// The heartbeat source the day's coverage is stated for: the instance, which is the
    /// `instance` in the row's replace key.
    pub source: String,
    /// Milliseconds of `window_ms` the source was observed for: the day's covered time, the
    /// numerator of its coverage fraction.
    pub covered_ms: f64,
    /// The day's intersection with the window, milliseconds: the denominator `covered_ms` is
    /// stated over. A whole day, or [`CoverageFact::is_whole_day`] refuses the row.
    pub window_ms: f64,
    /// The meaning pair, part of the replace key as on every keyed-history row.
    pub meaning: Meaning,
}

/// The served coverage table: one row per `(day, instance)`, stated whole-day under the meaning
/// pair (`crates/serving-clickhouse/migrations/0001_schema.sql`, pinned equal by the static audit).
pub const COVERAGE_DAILY: Table = Table {
    name: "coverage_daily",
    key: &[
        Column::new("day", Kind::Date),
        Column::new("instance_id", Kind::Str),
    ],
    measures: &[
        Column::new("covered_ms", Kind::F64),
        Column::new("window_ms", Kind::F64),
    ],
    day: DayOf::Date("day"),
};

impl CoverageFact {
    /// This fact as a row of [`COVERAGE_DAILY`].
    #[must_use]
    pub fn row(&self) -> Row {
        Row {
            table: COVERAGE_DAILY.name,
            key: vec![Value::Date(self.day), Value::Str(self.source.clone())],
            measures: vec![Value::F64(self.covered_ms), Value::F64(self.window_ms)],
            gone: false,
        }
    }

    /// Whether this row states a WHOLE day.
    ///
    /// **The rule is here rather than at the insertion site, because it is a property of the
    /// record and not of the write.** These rows replace on `(fleet, day, instance, meaning)`, so
    /// a partial-day row lands on the whole day's key and silently shrinks both the covered figure
    /// and the denominator it is stated over — the same defect the whole-days widening exists to
    /// prevent, one table down. A caller that skips the widening gets a refusal instead of a
    /// quietly wrong day.
    #[must_use]
    pub fn is_whole_day(&self) -> bool {
        (self.window_ms - crate::DAY_MS).abs() <= 0.5
    }
}

/// Everything one emitted day produces, assembled from a classification.
///
/// **Assembled here rather than at the insertion site**, so the mapping from a `Classification` to
/// the rows that leave the process is one function a reader can check against the contract above.
#[derive(Debug, Clone, PartialEq)]
pub struct EmittedDay {
    /// One [`ClassifiedFact`] per classified store, in the classification's order.
    pub facts: Vec<ClassifiedFact>,
    /// One [`CoverageFact`] per (day, source) the classification's coverage was sliced into.
    pub coverage: Vec<CoverageFact>,
}

impl EmittedDay {
    /// Build a day's emission from its classification.
    ///
    /// **`label` takes the instant as well as the instance, and the second argument is the whole
    /// apply half.** A mapping is effective-dated: the same instance can carry different classes on
    /// different days, so a labeller receiving only a name could not express it — and a first
    /// version of this signature did exactly that, which made every row read `undeclared` while the
    /// comment beside it claimed otherwise. Caught by
    /// `correctness::a_config_change_does_not_relabel_the_history_it_predates`.
    ///
    /// # Panics
    /// Never; the signature takes only owned data.
    #[must_use]
    pub fn from_classification(
        c: &Classification,
        meaning: &Meaning,
        models: &std::collections::BTreeMap<(String, String), String>,
    ) -> Self {
        let coverage_fraction = c.coverage.fraction;

        let facts = c
            .stores
            .iter()
            .map(|s| ClassifiedFact {
                holder: Holder {
                    instance: s.instance.clone(),
                    dp_rank: s.dp_rank,
                    group_idx: s.group_idx,
                },
                block: s.block.clone(),
                key_epoch: s.key_epoch,
                at_ms: s.at_ms,
                n_tokens: s.n_tokens,
                class: s.class,
                gap_ms: s.gap_ms,
                coverage: coverage_fraction,
                workload_class: s
                    .workload_class
                    .clone()
                    .unwrap_or_else(|| UNDECLARED.to_string()),
                // The run-scoped model relay, carried from fleet_inventory exactly as
                // workload_class is carried from the segment header: keyed by
                // (instance, incarnation) because the inventory is keyed per RUN — an
                // instance-only join would relabel history across a restart, C1's
                // slot-reuse hazard. Absent stays absent.
                model: models
                    .get(&(s.instance.clone(), s.incarnation.clone()))
                    .cloned(),
                meaning: meaning.clone(),
            })
            .collect();

        let coverage = c
            .coverage
            .per_day
            .iter()
            .map(|d| CoverageFact {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                // exact: a per-day coverage row starts on a day boundary
                day: (d.day_ms / crate::DAY_MS) as i64,
                source: d.source.clone(),
                covered_ms: d.covered_ms,
                window_ms: d.window_ms,
                meaning: meaning.clone(),
            })
            .collect();

        Self { facts, coverage }
    }
}
