// SPDX-License-Identifier: AGPL-3.0-only

//! The per-cell totals the fold owns as running state across ticks, and their persistence.

use super::calendar::month_start;
use super::keys::{
    BandFleetMinuteKey, BandKey, BandRankMinuteKey, ClassKey, FleetMinuteKey, RankMinuteKey,
};
use super::rows::{
    band_fleet_minute_row, band_rank_minute_row, band_rollup, band_row, class_row,
    collapse_bands_to_fleet, collapse_bands_to_rank, collapse_to_fleet, collapse_to_rank,
    fleet_minute_row, rank_minute_row, rollup, Band, BandFleetMinuteRow, BandRankMinuteRow,
    BandRollupRow, DayCells, FleetMinuteRow, Grain, RankMinuteRow, RollupRow,
};
use super::shape::{Bucketed, Shape, Sums};
use super::ClassifiedFact;
use crate::classify::Class;
use crate::intervals::Holder;
use crate::module::{Column, DayOf, Kind, Persisted, Row, Table, Value};
use crate::Error;

/// The per-cell totals the fold owns, as running state across ticks, for the open days.
///
/// Eight keyed shapes, one per serving table. Facts fold in and out through the SAME folds
/// [`day_cells`](super::day_cells) uses — [`rollup`], [`band_rollup`] and the four minute collapses — so a
/// cell's value is exactly what a whole-day fold of the same facts would state, and
/// subtracting a fact set is the exact inverse of adding it. A cell whose events reach zero
/// leaves its shape. Days are the unit of retirement (a settled day leaves the state); every
/// key carries its bucket, so a day is recoverable from any key.
///
/// Every change is tracked ([`Cells::drain_changes`]): a tick writes the cells that moved,
/// whole, and a tombstone for each key that left; [`Cells::rows_of_day`] states a day whole.
///
/// Engine-free: nothing here reads or writes a store; the pipeline writes the rows.
#[derive(Debug, Clone, PartialEq)]
pub struct Cells {
    class_daily: Shape<ClassKey, Class>,
    class_hourly: Shape<ClassKey, Class>,
    fleet_minute: Shape<FleetMinuteKey, Class>,
    rank_minute: Shape<RankMinuteKey, Class>,
    band_daily: Shape<BandKey, (Class, Band)>,
    band_hourly: Shape<BandKey, (Class, Band)>,
    band_fleet_minute: Shape<BandFleetMinuteKey, (Class, Band)>,
    band_rank_minute: Shape<BandRankMinuteKey, (Class, Band)>,
}

impl Default for Cells {
    fn default() -> Self {
        Self {
            class_daily: Shape::new(Grain::Daily),
            class_hourly: Shape::new(Grain::Hourly),
            fleet_minute: Shape::new(Grain::Minute),
            rank_minute: Shape::new(Grain::Minute),
            band_daily: Shape::new(Grain::Daily),
            band_hourly: Shape::new(Grain::Hourly),
            band_fleet_minute: Shape::new(Grain::Minute),
            band_rank_minute: Shape::new(Grain::Minute),
        }
    }
}

/// The module boundary: the class/band cells are one
/// fact's [`crate::module::Mergeable`], by the methods they already have.
impl crate::module::Mergeable for Cells {
    fn merge(&mut self, other: Self) {
        Cells::merge(self, other);
    }
}

impl crate::module::Tracked for Cells {
    type Changes = CellChanges;
    fn len(&self) -> usize {
        Cells::len(self)
    }
    fn drain_changes(&mut self) -> CellChanges {
        Cells::drain_changes(self)
    }
    fn discard_changes(&mut self) {
        Cells::discard_changes(self);
    }
}

impl crate::module::DayPartitioned for Cells {
    fn clear_day(&mut self, day: i64) {
        Cells::clear_day(self, day);
    }
    fn retire_before(&mut self, before: i64) {
        Cells::retire_before(self, before);
    }
    fn days(&self) -> std::collections::BTreeSet<i64> {
        Cells::days(self)
    }
}

const fn col(name: &'static str, kind: Kind) -> Column {
    Column::new(name, kind)
}

/// The three measures every cell table ends in, saturated into the store's `Int64`.
fn sums((events, tokens): (u64, u64)) -> Vec<Value> {
    [events, tokens]
        .into_iter()
        .map(|v| Value::I64(i64::try_from(v).unwrap_or(i64::MAX)))
        .collect()
}

fn class_daily_rendered(r: &RollupRow, gone: bool) -> Row {
    Row {
        table: "class_rollup_daily",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::Str(r.holder.instance.clone()),
            Value::U32(r.holder.dp_rank),
            Value::U32(r.holder.group_idx),
            Value::Date(r.month.unwrap_or_else(|| month_start(r.bucket))),
            Value::Date(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

fn class_hourly_rendered(r: &RollupRow, gone: bool) -> Row {
    Row {
        table: "class_rollup_hourly",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::Str(r.holder.instance.clone()),
            Value::U32(r.holder.dp_rank),
            Value::U32(r.holder.group_idx),
            Value::DateTime(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

fn fleet_minute_rendered(r: &FleetMinuteRow, gone: bool) -> Row {
    Row {
        table: "recompute_minute",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::DateTime(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

fn rank_minute_rendered(r: &RankMinuteRow, gone: bool) -> Row {
    Row {
        table: "recompute_rank_minute",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.instance.clone()),
            Value::U32(r.dp_rank),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::DateTime(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

fn band_daily_rendered(r: &BandRollupRow, gone: bool) -> Row {
    Row {
        table: "band_rollup_daily",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.band.as_str().to_string()),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::Str(r.holder.instance.clone()),
            Value::U32(r.holder.dp_rank),
            Value::U32(r.holder.group_idx),
            Value::Date(r.month),
            Value::Date(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

fn band_hourly_rendered(r: &BandRollupRow, gone: bool) -> Row {
    Row {
        table: "band_rollup_hourly",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.band.as_str().to_string()),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::Str(r.holder.instance.clone()),
            Value::U32(r.holder.dp_rank),
            Value::U32(r.holder.group_idx),
            Value::DateTime(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

fn band_fleet_minute_rendered(r: &BandFleetMinuteRow, gone: bool) -> Row {
    Row {
        table: "band_minute",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.band.as_str().to_string()),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::DateTime(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

fn band_rank_minute_rendered(r: &BandRankMinuteRow, gone: bool) -> Row {
    Row {
        table: "band_rank_minute",
        key: vec![
            Value::Str(r.class.as_str().to_string()),
            Value::Str(r.band.as_str().to_string()),
            Value::Str(r.instance.clone()),
            Value::U32(r.dp_rank),
            Value::Str(r.workload_class.clone()),
            Value::nullable(r.model.as_deref()),
            Value::DateTime(r.bucket),
        ],
        measures: sums((r.events, r.tokens)),
        gone,
    }
}

impl Cells {
    /// Every row of `cells` rendered present — what a whole-day write states, and what the
    /// heal writes from its rebuild.
    #[must_use]
    pub fn render_rows(cells: &DayCells) -> Vec<Row> {
        let mut out = Vec::new();
        out.extend(
            cells
                .class_daily
                .iter()
                .map(|r| class_daily_rendered(r, false)),
        );
        out.extend(
            cells
                .class_hourly
                .iter()
                .map(|r| class_hourly_rendered(r, false)),
        );
        out.extend(
            cells
                .fleet_minute
                .iter()
                .map(|r| fleet_minute_rendered(r, false)),
        );
        out.extend(
            cells
                .rank_minute
                .iter()
                .map(|r| rank_minute_rendered(r, false)),
        );
        out.extend(
            cells
                .band_daily
                .iter()
                .map(|r| band_daily_rendered(r, false)),
        );
        out.extend(
            cells
                .band_hourly
                .iter()
                .map(|r| band_hourly_rendered(r, false)),
        );
        out.extend(
            cells
                .band_fleet_minute
                .iter()
                .map(|r| band_fleet_minute_rendered(r, false)),
        );
        out.extend(
            cells
                .band_rank_minute
                .iter()
                .map(|r| band_rank_minute_rendered(r, false)),
        );
        out
    }
}

/// The module boundary's store half: the eight cell tables in the store's own column order
/// (`crates/serving-clickhouse/migrations/0001_schema.sql`, pinned equal by the static audit), the
/// changed and the whole-day rows rendered by the row types above, and a served row back in
/// by `Cells::place`.
impl Persisted for Cells {
    const TABLES: &'static [Table] = &[
        Table {
            name: "class_rollup_daily",
            key: &[
                col("class", Kind::Str),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("instance_id", Kind::Str),
                col("dp_rank", Kind::U32),
                col("group_idx", Kind::U32),
                col("month", Kind::Date),
                col("bucket", Kind::Date),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::Date("bucket"),
        },
        Table {
            name: "class_rollup_hourly",
            key: &[
                col("class", Kind::Str),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("instance_id", Kind::Str),
                col("dp_rank", Kind::U32),
                col("group_idx", Kind::U32),
                col("bucket", Kind::DateTime),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::DateTime("bucket"),
        },
        Table {
            name: "recompute_minute",
            key: &[
                col("class", Kind::Str),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("bucket", Kind::DateTime),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::DateTime("bucket"),
        },
        Table {
            name: "recompute_rank_minute",
            key: &[
                col("class", Kind::Str),
                col("instance_id", Kind::Str),
                col("dp_rank", Kind::U32),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("bucket", Kind::DateTime),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::DateTime("bucket"),
        },
        Table {
            name: "band_rollup_daily",
            key: &[
                col("class", Kind::Str),
                col("band", Kind::Str),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("instance_id", Kind::Str),
                col("dp_rank", Kind::U32),
                col("group_idx", Kind::U32),
                col("month", Kind::Date),
                col("bucket", Kind::Date),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::Date("bucket"),
        },
        Table {
            name: "band_rollup_hourly",
            key: &[
                col("class", Kind::Str),
                col("band", Kind::Str),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("instance_id", Kind::Str),
                col("dp_rank", Kind::U32),
                col("group_idx", Kind::U32),
                col("bucket", Kind::DateTime),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::DateTime("bucket"),
        },
        Table {
            name: "band_minute",
            key: &[
                col("class", Kind::Str),
                col("band", Kind::Str),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("bucket", Kind::DateTime),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::DateTime("bucket"),
        },
        Table {
            name: "band_rank_minute",
            key: &[
                col("class", Kind::Str),
                col("band", Kind::Str),
                col("instance_id", Kind::Str),
                col("dp_rank", Kind::U32),
                col("workload_class", Kind::Str),
                col("model", Kind::NullableStr),
                col("bucket", Kind::DateTime),
            ],
            measures: &[col("events", Kind::I64), col("tokens", Kind::I64)],
            day: DayOf::DateTime("bucket"),
        },
    ];

    fn render(changes: &CellChanges) -> Vec<Row> {
        let mut out = Vec::with_capacity(changes.len());
        out.extend(
            changes
                .class_daily
                .iter()
                .map(|c| class_daily_rendered(&c.row, c.gone)),
        );
        out.extend(
            changes
                .class_hourly
                .iter()
                .map(|c| class_hourly_rendered(&c.row, c.gone)),
        );
        out.extend(
            changes
                .fleet_minute
                .iter()
                .map(|c| fleet_minute_rendered(&c.row, c.gone)),
        );
        out.extend(
            changes
                .rank_minute
                .iter()
                .map(|c| rank_minute_rendered(&c.row, c.gone)),
        );
        out.extend(
            changes
                .band_daily
                .iter()
                .map(|c| band_daily_rendered(&c.row, c.gone)),
        );
        out.extend(
            changes
                .band_hourly
                .iter()
                .map(|c| band_hourly_rendered(&c.row, c.gone)),
        );
        out.extend(
            changes
                .band_fleet_minute
                .iter()
                .map(|c| band_fleet_minute_rendered(&c.row, c.gone)),
        );
        out.extend(
            changes
                .band_rank_minute
                .iter()
                .map(|c| band_rank_minute_rendered(&c.row, c.gone)),
        );
        out
    }

    fn render_day(&self, day: i64) -> Vec<Row> {
        Self::render_rows(&self.rows_of_day(day))
    }

    fn restore(&mut self, table: &str, key: &[&str], measures: &[&str]) -> Result<(), Error> {
        let refuse = |message: String| Error::Parse {
            name: table.to_string(),
            message,
        };
        let measure = |i: usize| -> Result<u64, Error> {
            let v = measures
                .get(i)
                .ok_or_else(|| refuse(format!("measure {i} missing in {measures:?}")))?;
            v.parse::<u64>()
                .map_err(|e| refuse(format!("measure {i} {v:?}: {e}")))
        };
        let s = Sums {
            events: measure(0)?,
            tokens: measure(1)?,
        };
        self.place(table, key, s)
    }
}

impl Cells {
    /// Fold `facts` in. Identity-deduped exactly as the store's replace key has it, through
    /// the folds themselves.
    pub fn add_facts(&mut self, facts: &[ClassifiedFact]) {
        self.apply(facts, true);
    }

    /// Fold `facts` out — the exact inverse of [`Cells::add_facts`] over the same facts.
    pub fn subtract_facts(&mut self, facts: &[ClassifiedFact]) {
        self.apply(facts, false);
    }

    /// One served row back into the state, placed rather than touched — the state resuming
    /// from its own finished output; [`Persisted::restore`]'s body, with the measures already
    /// summed. `key` is the row's key fields in the table's column order after the framework's
    /// head — the spec's order ([`Persisted::TABLES`]) — as the read-back renders them: `\N`
    /// the engine's null, a `Date` its day index, a `DateTime` its unix seconds.
    ///
    /// # Errors
    /// A table this state does not hold, a field short or unparseable, or a class or band
    /// spelling the fold never writes.
    #[allow(clippy::too_many_lines)] // eight tables, one inverse each: the inventory IS the function
    fn place(&mut self, table: &str, key: &[&str], s: Sums) -> Result<(), Error> {
        // Every refusal is the row's own parse failure, named by its table: the message a
        // caller prints is unchanged from the string it replaced.
        let refuse = |message: String| Error::Parse {
            name: table.to_string(),
            message,
        };
        let field = |i: usize| -> Result<&str, Error> {
            key.get(i)
                .copied()
                .ok_or_else(|| refuse(format!("key field {i} missing in {key:?}")))
        };
        let text = |i: usize| -> Result<String, Error> { field(i).map(str::to_string) };
        let opt = |i: usize| -> Result<Option<String>, Error> {
            field(i).map(|v| (v != crate::module::NULL_TEXT).then(|| v.to_string()))
        };
        let num = |i: usize| -> Result<u32, Error> {
            field(i)?
                .parse::<u32>()
                .map_err(|e| refuse(format!("key field {i} {:?}: {e}", key[i])))
        };
        let day = |i: usize| -> Result<i64, Error> {
            field(i)?
                .parse::<i64>()
                .map_err(|e| refuse(format!("day index {:?}: {e}", key[i])))
        };
        let secs = |i: usize| -> Result<i64, Error> {
            field(i)?
                .parse::<i64>()
                .map_err(|e| refuse(format!("unix seconds {:?}: {e}", key[i])))
        };
        let class = |i: usize| -> Result<Class, Error> {
            Class::parse(field(i)?).ok_or_else(|| refuse(format!("class {:?}", key[i])))
        };
        let band = |i: usize| -> Result<Band, Error> {
            Band::parse(field(i)?).ok_or_else(|| refuse(format!("band {:?}", key[i])))
        };
        match table {
            "class_rollup_daily" => {
                let c = class(0)?;
                // class, workload_class, model, instance, dp_rank, group_idx, month, bucket
                let k = ClassKey {
                    bucket: day(7)?,
                    class: c.as_str(),
                    workload_class: text(1)?,
                    model: opt(2)?,
                    holder: Holder {
                        instance: text(3)?,
                        dp_rank: num(4)?,
                        group_idx: num(5)?,
                    },
                };
                self.class_daily.set(k, c, s);
            }
            "class_rollup_hourly" => {
                let c = class(0)?;
                let k = ClassKey {
                    bucket: secs(6)?,
                    class: c.as_str(),
                    workload_class: text(1)?,
                    model: opt(2)?,
                    holder: Holder {
                        instance: text(3)?,
                        dp_rank: num(4)?,
                        group_idx: num(5)?,
                    },
                };
                self.class_hourly.set(k, c, s);
            }
            "recompute_minute" => {
                let c = class(0)?;
                // class, workload_class, model, bucket
                let k = FleetMinuteKey {
                    bucket: secs(3)?,
                    class: c.as_str(),
                    workload_class: text(1)?,
                    model: opt(2)?,
                };
                self.fleet_minute.set(k, c, s);
            }
            "recompute_rank_minute" => {
                let c = class(0)?;
                // class, instance, dp_rank, workload_class, model, bucket
                let k = RankMinuteKey {
                    bucket: secs(5)?,
                    instance: text(1)?,
                    dp_rank: num(2)?,
                    class: c.as_str(),
                    workload_class: text(3)?,
                    model: opt(4)?,
                };
                self.rank_minute.set(k, c, s);
            }
            "band_rollup_daily" => {
                let (c, b) = (class(0)?, band(1)?);
                // class, band, workload_class, model, instance, dp_rank, group_idx, month, bucket
                let k = BandKey {
                    bucket: day(8)?,
                    class: c.as_str(),
                    band: b.as_str(),
                    workload_class: text(2)?,
                    model: opt(3)?,
                    holder: Holder {
                        instance: text(4)?,
                        dp_rank: num(5)?,
                        group_idx: num(6)?,
                    },
                };
                self.band_daily.set(k, (c, b), s);
            }
            "band_rollup_hourly" => {
                let (c, b) = (class(0)?, band(1)?);
                let k = BandKey {
                    bucket: secs(7)?,
                    class: c.as_str(),
                    band: b.as_str(),
                    workload_class: text(2)?,
                    model: opt(3)?,
                    holder: Holder {
                        instance: text(4)?,
                        dp_rank: num(5)?,
                        group_idx: num(6)?,
                    },
                };
                self.band_hourly.set(k, (c, b), s);
            }
            "band_minute" => {
                let (c, b) = (class(0)?, band(1)?);
                // class, band, workload_class, model, bucket
                let k = BandFleetMinuteKey {
                    bucket: secs(4)?,
                    class: c.as_str(),
                    band: b.as_str(),
                    workload_class: text(2)?,
                    model: opt(3)?,
                };
                self.band_fleet_minute.set(k, (c, b), s);
            }
            "band_rank_minute" => {
                let (c, b) = (class(0)?, band(1)?);
                // class, band, instance, dp_rank, workload_class, model, bucket
                let k = BandRankMinuteKey {
                    bucket: secs(6)?,
                    instance: text(2)?,
                    dp_rank: num(3)?,
                    class: c.as_str(),
                    band: b.as_str(),
                    workload_class: text(4)?,
                    model: opt(5)?,
                };
                self.band_rank_minute.set(k, (c, b), s);
            }
            other => {
                return Err(Error::Parse {
                    name: other.to_string(),
                    message: "not a cell table".to_string(),
                })
            }
        }
        Ok(())
    }

    fn apply(&mut self, facts: &[ClassifiedFact], adding: bool) {
        fn fold<K: Ord + Clone + Bucketed, T: Copy>(
            shape: &mut Shape<K, T>,
            adding: bool,
            (k, t, s): (K, T, Sums),
        ) {
            if adding {
                shape.add(k, t, s);
            } else {
                shape.subtract(&k, s);
            }
        }
        let minute = rollup(facts, Grain::Minute);
        let band_minute = band_rollup(facts, Grain::Minute);
        for r in rollup(facts, Grain::Daily) {
            fold(&mut self.class_daily, adding, class_cell(r));
        }
        for r in rollup(facts, Grain::Hourly) {
            fold(&mut self.class_hourly, adding, class_cell(r));
        }
        for r in collapse_to_fleet(&minute) {
            fold(&mut self.fleet_minute, adding, fleet_minute_cell(r));
        }
        for r in collapse_to_rank(&minute) {
            fold(&mut self.rank_minute, adding, rank_minute_cell(r));
        }
        for r in band_rollup(facts, Grain::Daily) {
            fold(&mut self.band_daily, adding, band_cell(r));
        }
        for r in band_rollup(facts, Grain::Hourly) {
            fold(&mut self.band_hourly, adding, band_cell(r));
        }
        for r in collapse_bands_to_fleet(&band_minute) {
            fold(
                &mut self.band_fleet_minute,
                adding,
                band_fleet_minute_cell(r),
            );
        }
        for r in collapse_bands_to_rank(&band_minute) {
            fold(&mut self.band_rank_minute, adding, band_rank_minute_cell(r));
        }
    }

    /// Another state's cells into this one — a lane's partial into the day's, a day's into
    /// the loop's. Order-independent: sums add.
    pub fn merge(&mut self, other: Self) {
        self.class_daily.merge(other.class_daily);
        self.class_hourly.merge(other.class_hourly);
        self.fleet_minute.merge(other.fleet_minute);
        self.rank_minute.merge(other.rank_minute);
        self.band_daily.merge(other.band_daily);
        self.band_hourly.merge(other.band_hourly);
        self.band_fleet_minute.merge(other.band_fleet_minute);
        self.band_rank_minute.merge(other.band_rank_minute);
    }

    /// Drop every cell of `day` — what a whole-day pass does before it re-states the day.
    pub fn clear_day(&mut self, day: i64) {
        self.class_daily.clear_day(day);
        self.class_hourly.clear_day(day);
        self.fleet_minute.clear_day(day);
        self.rank_minute.clear_day(day);
        self.band_daily.clear_day(day);
        self.band_hourly.clear_day(day);
        self.band_fleet_minute.clear_day(day);
        self.band_rank_minute.clear_day(day);
    }

    /// Drop every cell on a day before `before` — the settled days leaving the state.
    pub fn retire_before(&mut self, before: i64) {
        self.class_daily.retire_before(before);
        self.class_hourly.retire_before(before);
        self.fleet_minute.retire_before(before);
        self.rank_minute.retire_before(before);
        self.band_daily.retire_before(before);
        self.band_hourly.retire_before(before);
        self.band_fleet_minute.retire_before(before);
        self.band_rank_minute.retire_before(before);
    }

    /// The days this state holds cells for — from the daily class shape, which every fact
    /// lands in.
    #[must_use]
    pub fn days(&self) -> std::collections::BTreeSet<i64> {
        self.class_daily.cells.keys().map(|k| k.bucket).collect()
    }

    /// Cells across the eight shapes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.class_daily.len()
            + self.class_hourly.len()
            + self.fleet_minute.len()
            + self.rank_minute.len()
            + self.band_daily.len()
            + self.band_hourly.len()
            + self.band_fleet_minute.len()
            + self.band_rank_minute.len()
    }

    /// Whether the eight shapes hold no cell at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Subtractions that found less than they took, across the eight shapes. See
    /// [`Shape`]; zero is the only value a state derived from the facts can have.
    #[must_use]
    pub fn underflows(&self) -> u64 {
        self.class_daily.underflows
            + self.class_hourly.underflows
            + self.fleet_minute.underflows
            + self.rank_minute.underflows
            + self.band_daily.underflows
            + self.band_hourly.underflows
            + self.band_fleet_minute.underflows
            + self.band_rank_minute.underflows
    }

    /// The state as the eight tables' rows, in each key's order — what [`day_cells`](super::day_cells)
    /// returns for a whole day.
    #[must_use]
    pub fn rows(&self) -> DayCells {
        self.rows_where(|_| true)
    }

    /// The state's rows on one day alone — what a whole-day pass writes, whole.
    #[must_use]
    pub fn rows_of_day(&self, day: i64) -> DayCells {
        self.rows_where(|d| d == day)
    }

    fn rows_where(&self, keep: impl Fn(i64) -> bool) -> DayCells {
        DayCells {
            class_daily: self
                .class_daily
                .cells_on(&keep)
                .map(|(k, (c, s))| class_row(k, *c, *s, Grain::Daily))
                .collect(),
            class_hourly: self
                .class_hourly
                .cells_on(&keep)
                .map(|(k, (c, s))| class_row(k, *c, *s, Grain::Hourly))
                .collect(),
            fleet_minute: self
                .fleet_minute
                .cells_on(&keep)
                .map(|(k, (c, s))| fleet_minute_row(k, *c, *s))
                .collect(),
            rank_minute: self
                .rank_minute
                .cells_on(&keep)
                .map(|(k, (c, s))| rank_minute_row(k, *c, *s))
                .collect(),
            band_daily: self
                .band_daily
                .cells_on(&keep)
                .map(|(k, (t, s))| band_row(k, *t, *s, Grain::Daily))
                .collect(),
            band_hourly: self
                .band_hourly
                .cells_on(&keep)
                .map(|(k, (t, s))| band_row(k, *t, *s, Grain::Hourly))
                .collect(),
            band_fleet_minute: self
                .band_fleet_minute
                .cells_on(&keep)
                .map(|(k, (t, s))| band_fleet_minute_row(k, *t, *s))
                .collect(),
            band_rank_minute: self
                .band_rank_minute
                .cells_on(&keep)
                .map(|(k, (t, s))| band_rank_minute_row(k, *t, *s))
                .collect(),
        }
    }

    /// Every cell that moved since the last drain, as the row to write: its current value,
    /// or its tombstone (zero measures, `gone`) when the key left. The touches are
    /// forgotten — what a tick writes, and only that.
    #[must_use]
    pub fn drain_changes(&mut self) -> CellChanges {
        let zero = Sums::default();
        CellChanges {
            class_daily: self
                .class_daily
                .drain()
                .into_iter()
                .map(|(k, c, now)| Change {
                    row: class_row(&k, c, now.unwrap_or(zero), Grain::Daily),
                    gone: now.is_none(),
                })
                .collect(),
            class_hourly: self
                .class_hourly
                .drain()
                .into_iter()
                .map(|(k, c, now)| Change {
                    row: class_row(&k, c, now.unwrap_or(zero), Grain::Hourly),
                    gone: now.is_none(),
                })
                .collect(),
            fleet_minute: self
                .fleet_minute
                .drain()
                .into_iter()
                .map(|(k, c, now)| Change {
                    row: fleet_minute_row(&k, c, now.unwrap_or(zero)),
                    gone: now.is_none(),
                })
                .collect(),
            rank_minute: self
                .rank_minute
                .drain()
                .into_iter()
                .map(|(k, c, now)| Change {
                    row: rank_minute_row(&k, c, now.unwrap_or(zero)),
                    gone: now.is_none(),
                })
                .collect(),
            band_daily: self
                .band_daily
                .drain()
                .into_iter()
                .map(|(k, t, now)| Change {
                    row: band_row(&k, t, now.unwrap_or(zero), Grain::Daily),
                    gone: now.is_none(),
                })
                .collect(),
            band_hourly: self
                .band_hourly
                .drain()
                .into_iter()
                .map(|(k, t, now)| Change {
                    row: band_row(&k, t, now.unwrap_or(zero), Grain::Hourly),
                    gone: now.is_none(),
                })
                .collect(),
            band_fleet_minute: self
                .band_fleet_minute
                .drain()
                .into_iter()
                .map(|(k, t, now)| Change {
                    row: band_fleet_minute_row(&k, t, now.unwrap_or(zero)),
                    gone: now.is_none(),
                })
                .collect(),
            band_rank_minute: self
                .band_rank_minute
                .drain()
                .into_iter()
                .map(|(k, t, now)| Change {
                    row: band_rank_minute_row(&k, t, now.unwrap_or(zero)),
                    gone: now.is_none(),
                })
                .collect(),
        }
    }

    /// Forget the touches without rendering them — a whole-day pass writes the day whole
    /// and owes no per-cell changes.
    pub fn discard_changes(&mut self) {
        let _dropped = self.drain_changes();
    }
}

/// One cell's row to write: its current value, or (`gone`) the tombstone that hides the
/// key — zero measures under the same key, the latest version.
#[derive(Debug, Clone, PartialEq)]
pub struct Change<R> {
    /// The row to write: the cell's current value, or, when `gone`, zero measures under the
    /// same key.
    pub row: R,
    /// The cell left the state: `row` is its tombstone, the latest version, hiding the key.
    pub gone: bool,
}

/// What a tick writes: the changed cells of the eight tables, each as its row or its
/// tombstone. See [`Cells::drain_changes`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CellChanges {
    /// The changed cells of `class_rollup_daily`.
    pub class_daily: Vec<Change<RollupRow>>,
    /// The changed cells of `class_rollup_hourly`.
    pub class_hourly: Vec<Change<RollupRow>>,
    /// The changed cells of `recompute_minute`.
    pub fleet_minute: Vec<Change<FleetMinuteRow>>,
    /// The changed cells of `recompute_rank_minute`.
    pub rank_minute: Vec<Change<RankMinuteRow>>,
    /// The changed cells of `band_rollup_daily`.
    pub band_daily: Vec<Change<BandRollupRow>>,
    /// The changed cells of `band_rollup_hourly`.
    pub band_hourly: Vec<Change<BandRollupRow>>,
    /// The changed cells of `band_minute`.
    pub band_fleet_minute: Vec<Change<BandFleetMinuteRow>>,
    /// The changed cells of `band_rank_minute`.
    pub band_rank_minute: Vec<Change<BandRankMinuteRow>>,
}

impl CellChanges {
    /// Changes across the eight shapes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.class_daily.len()
            + self.class_hourly.len()
            + self.fleet_minute.len()
            + self.rank_minute.len()
            + self.band_daily.len()
            + self.band_hourly.len()
            + self.band_fleet_minute.len()
            + self.band_rank_minute.len()
    }

    /// Whether no cell moved: nothing for the tick to write.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The tombstones among the changes.
    #[must_use]
    pub fn gone(&self) -> usize {
        self.class_daily.iter().filter(|c| c.gone).count()
            + self.class_hourly.iter().filter(|c| c.gone).count()
            + self.fleet_minute.iter().filter(|c| c.gone).count()
            + self.rank_minute.iter().filter(|c| c.gone).count()
            + self.band_daily.iter().filter(|c| c.gone).count()
            + self.band_hourly.iter().filter(|c| c.gone).count()
            + self.band_fleet_minute.iter().filter(|c| c.gone).count()
            + self.band_rank_minute.iter().filter(|c| c.gone).count()
    }
}

/// A class rollup row as its cell: its key, its class and its sums.
fn class_cell(r: RollupRow) -> (ClassKey, Class, Sums) {
    let sums = r.sums();
    (
        ClassKey {
            bucket: r.bucket,
            class: r.class.as_str(),
            workload_class: r.workload_class,
            model: r.model,
            holder: r.holder,
        },
        r.class,
        sums,
    )
}

/// A band rollup row as its cell: its key, its class and band, and its sums.
fn band_cell(r: BandRollupRow) -> (BandKey, (Class, Band), Sums) {
    let sums = r.sums();
    (
        BandKey {
            bucket: r.bucket,
            class: r.class.as_str(),
            band: r.band.as_str(),
            workload_class: r.workload_class,
            model: r.model,
            holder: r.holder,
        },
        (r.class, r.band),
        sums,
    )
}

fn fleet_minute_cell(r: FleetMinuteRow) -> (FleetMinuteKey, Class, Sums) {
    let sums = Sums {
        events: r.events,
        tokens: r.tokens,
    };
    (
        FleetMinuteKey {
            bucket: r.bucket,
            class: r.class.as_str(),
            workload_class: r.workload_class,
            model: r.model,
        },
        r.class,
        sums,
    )
}

fn rank_minute_cell(r: RankMinuteRow) -> (RankMinuteKey, Class, Sums) {
    let sums = Sums {
        events: r.events,
        tokens: r.tokens,
    };
    (
        RankMinuteKey {
            bucket: r.bucket,
            instance: r.instance,
            dp_rank: r.dp_rank,
            class: r.class.as_str(),
            workload_class: r.workload_class,
            model: r.model,
        },
        r.class,
        sums,
    )
}

fn band_fleet_minute_cell(r: BandFleetMinuteRow) -> (BandFleetMinuteKey, (Class, Band), Sums) {
    let sums = Sums {
        events: r.events,
        tokens: r.tokens,
    };
    (
        BandFleetMinuteKey {
            bucket: r.bucket,
            class: r.class.as_str(),
            band: r.band.as_str(),
            workload_class: r.workload_class,
            model: r.model,
        },
        (r.class, r.band),
        sums,
    )
}

fn band_rank_minute_cell(r: BandRankMinuteRow) -> (BandRankMinuteKey, (Class, Band), Sums) {
    let sums = Sums {
        events: r.events,
        tokens: r.tokens,
    };
    (
        BandRankMinuteKey {
            bucket: r.bucket,
            instance: r.instance,
            dp_rank: r.dp_rank,
            class: r.class.as_str(),
            band: r.band.as_str(),
            workload_class: r.workload_class,
            model: r.model,
        },
        (r.class, r.band),
        sums,
    )
}

#[cfg(test)]
mod persisted_tests {
    use super::{month_start, Cells};
    use crate::module::{round_trip_day, Persisted, Row, Value};

    const DAY: i64 = 20_703;

    fn secs(hour: i64) -> i64 {
        DAY * 86_400 + hour * 3_600
    }

    fn s(v: &str) -> Value {
        Value::Str(v.to_string())
    }

    fn sums() -> Vec<Value> {
        vec![Value::I64(2), Value::I64(16)]
    }

    fn row(table: &'static str, key: Vec<Value>) -> Row {
        Row {
            table,
            key,
            measures: sums(),
            gone: false,
        }
    }

    /// One row of every table, in the spec's column order, as the fold renders them.
    fn one_of_each() -> Vec<Row> {
        let m = month_start(DAY);
        let (u0, nul) = (Value::U32(0), Value::Null);
        vec![
            row(
                "class_rollup_daily",
                vec![
                    s("ETR"),
                    s("agentic"),
                    nul.clone(),
                    s("i1"),
                    u0.clone(),
                    u0.clone(),
                    Value::Date(m),
                    Value::Date(DAY),
                ],
            ),
            row(
                "class_rollup_hourly",
                vec![
                    s("ETR"),
                    s("agentic"),
                    nul.clone(),
                    s("i1"),
                    u0.clone(),
                    u0.clone(),
                    Value::DateTime(secs(1)),
                ],
            ),
            row(
                "recompute_minute",
                vec![
                    s("ETR"),
                    s("agentic"),
                    nul.clone(),
                    Value::DateTime(secs(1)),
                ],
            ),
            row(
                "recompute_rank_minute",
                vec![
                    s("ETR"),
                    s("i1"),
                    u0.clone(),
                    s("agentic"),
                    nul.clone(),
                    Value::DateTime(secs(1)),
                ],
            ),
            row(
                "band_rollup_daily",
                vec![
                    s("ETR"),
                    s("1-10m"),
                    s("agentic"),
                    nul.clone(),
                    s("i1"),
                    u0.clone(),
                    u0.clone(),
                    Value::Date(m),
                    Value::Date(DAY),
                ],
            ),
            row(
                "band_rollup_hourly",
                vec![
                    s("ETR"),
                    s("1-10m"),
                    s("agentic"),
                    nul.clone(),
                    s("i1"),
                    u0.clone(),
                    u0.clone(),
                    Value::DateTime(secs(1)),
                ],
            ),
            row(
                "band_minute",
                vec![
                    s("ETR"),
                    s("1-10m"),
                    s("agentic"),
                    nul.clone(),
                    Value::DateTime(secs(1)),
                ],
            ),
            row(
                "band_rank_minute",
                vec![
                    s("ETR"),
                    s("1-10m"),
                    s("i1"),
                    u0,
                    s("agentic"),
                    nul,
                    Value::DateTime(secs(1)),
                ],
            ),
        ]
    }

    fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
        rows.sort_by_key(|r| format!("{r:?}"));
        rows
    }

    #[test]
    fn every_table_s_row_restores_from_its_text_and_renders_back_the_same() {
        let rows = one_of_each();
        let tables = <Cells as Persisted>::TABLES;
        assert_eq!(rows.len(), tables.len(), "one row per table");
        let mut state = Cells::default();
        for r in &rows {
            let table = tables
                .iter()
                .find(|t| t.name == r.table)
                .expect("a listed table");
            table.accepts(r).unwrap_or_else(|e| panic!("{e}"));
            let key: Vec<String> = r.key.iter().map(Value::text).collect();
            let measures: Vec<String> = r.measures.iter().map(Value::text).collect();
            let key: Vec<&str> = key.iter().map(String::as_str).collect();
            let measures: Vec<&str> = measures.iter().map(String::as_str).collect();
            Persisted::restore(&mut state, r.table, &key, &measures)
                .unwrap_or_else(|e| panic!("{}: {e}", r.table));
        }
        state.discard_changes();
        assert_eq!(
            sorted(state.render_day(DAY)),
            sorted(rows.clone()),
            "render_day is the inverse of restore over the read-back's text"
        );
        assert!(
            state.render_day(DAY + 1).is_empty(),
            "another day renders nothing"
        );
        let back = round_trip_day(&state, DAY).expect("the round trip");
        assert_eq!(back, state, "the state survives its own text");
        assert_eq!(
            sorted(Cells::render_rows(&state.rows())),
            sorted(rows),
            "the whole-day rows are the same rendering"
        );
    }

    #[test]
    fn a_row_of_the_wrong_shape_is_refused_by_its_spec() {
        let t = <Cells as Persisted>::TABLES[0];
        let refused = |row: &Row| match t.accepts(row) {
            Err(crate::Error::Row { table, message }) => {
                assert_eq!(table, t.name);
                message
            }
            other => panic!("the spec refuses with a row error, not {other:?}"),
        };
        let mut short = one_of_each().remove(0);
        short.key.pop();
        assert!(refused(&short).contains("value(s) for"));
        let mut wrong = one_of_each().remove(0);
        wrong.key[0] = Value::U32(1);
        assert!(refused(&wrong).contains("is not a"));
        let other = one_of_each().remove(1);
        assert!(refused(&other).contains("offered to"));
    }
}
