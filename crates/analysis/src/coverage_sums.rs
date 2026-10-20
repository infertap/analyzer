// SPDX-License-Identifier: AGPL-3.0-only

//! Observed coverage as cells that sum (`docs/pipeline.md`): per source,
//! the milliseconds observed complete and the milliseconds stated over, by minute and by UTC day.
//!
//! The arithmetic is [`crate::observed_coverage::ObservedCoverage`]'s: a source is uncertain
//! before its first heartbeat unless an agent start attests it, inside each drop window widened
//! by the skew, and after its last heartbeat unless a stop closes it. What changes is the
//! window. The keyed pipeline states coverage tick by tick, each tick over the stretch its
//! predecessor stopped at, so the cells are sums: a window's coverage is the sum of its minutes,
//! a day's the sum of its ticks, and the last hour reads like any other window.
//!
//! A stretch is stated only once it is settled ([`crate::completeness::UncertaintyModel::
//! coverage_settled_to`]): every source's tail after its last heartbeat is uncertain until a
//! later heartbeat attests it, so a tick states coverage up to the earliest tail among the
//! sources still inside the staleness grace. A source silent past the grace no longer holds
//! the edge back; its tail stays uncertain, as the roster calls it inactive.

use crate::completeness::{Coverage, DailyCoverage, UncertaintySpan};
use crate::emitted::COVERAGE_DAILY;
use crate::error::Error;
use crate::module::{Column, DayOf, Kind, Persisted, Row, Table, Value};
use crate::{Window, DAY_MS};
use std::collections::{BTreeMap, BTreeSet};

/// A minute in milliseconds: the finest bucket.
pub const MINUTE_MS: f64 = 60_000.0;

/// One source's minute of coverage, per minute: `coverage_minute`.
pub const COVERAGE_MINUTE: Table = Table {
    name: "coverage_minute",
    key: &[
        Column::new("bucket", Kind::DateTime),
        Column::new("instance_id", Kind::Str),
    ],
    measures: &[
        Column::new("covered_ms", Kind::F64),
        Column::new("window_ms", Kind::F64),
    ],
    day: DayOf::DateTime("bucket"),
};

/// Covered and stated milliseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Observed {
    /// Milliseconds observed complete.
    pub covered_ms: f64,
    /// Milliseconds stated over: the numerator's denominator.
    pub window_ms: f64,
}

impl Observed {
    fn add(&mut self, other: Self) {
        self.covered_ms += other.covered_ms;
        self.window_ms += other.window_ms;
    }
}

/// A minute cell's key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MinuteKey {
    /// The minute's start in unix seconds.
    pub minute: i64,
    /// The source.
    pub source: String,
}

impl MinuteKey {
    /// The day the minute falls in.
    fn day(&self) -> i64 {
        self.minute.div_euclid(86_400)
    }
}

/// A day cell's key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DayKey {
    /// The day index.
    pub day: i64,
    /// The source.
    pub source: String,
}

/// Coverage cells: per (minute, source) and per (day, source), each a sum.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CoverageSums {
    minute: BTreeMap<MinuteKey, Observed>,
    daily: BTreeMap<DayKey, Observed>,
    touched_minutes: BTreeSet<MinuteKey>,
    touched_days: BTreeSet<DayKey>,
}

/// The cells a drain names: each touched key's value now, `None` for one that left.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CoverageChanges {
    /// `coverage_minute`'s changed cells.
    pub minute: Vec<(MinuteKey, Option<Observed>)>,
    /// `coverage_daily`'s changed cells.
    pub daily: Vec<(DayKey, Option<Observed>)>,
}

#[allow(clippy::cast_possible_truncation)] // bucket indices of finite instants fit i64
fn floor_div(at_ms: f64, bucket_ms: f64) -> i64 {
    (at_ms / bucket_ms).floor() as i64
}

impl CoverageSums {
    /// One source's coverage of `window`, given its uncertain stretches clipped to it: the
    /// window cut at minutes and at days, each piece charged its own length and the part of it
    /// no uncertain stretch covers.
    #[must_use]
    pub fn of_source(source: &str, window: Window, uncertain: &[UncertaintySpan]) -> Self {
        let mut sums = Self::default();
        let piece = |start: f64, end: f64| -> Observed {
            let clipped: Vec<UncertaintySpan> = uncertain
                .iter()
                .filter_map(|s| {
                    let (a, b) = (s.start_ms.max(start), s.end_ms.min(end));
                    (a < b).then_some(UncertaintySpan {
                        start_ms: a,
                        end_ms: b,
                    })
                })
                .collect();
            let window_ms = end - start;
            Observed {
                covered_ms: (window_ms - crate::completeness::union_length(clipped)).max(0.0),
                window_ms,
            }
        };
        let mut at = window.start_ms;
        while at < window.end_ms {
            let minute = floor_div(at, MINUTE_MS);
            #[allow(clippy::cast_precision_loss)] // minute indices are far below 2^52
            let end = ((minute + 1) as f64 * MINUTE_MS).min(window.end_ms);
            let observed = piece(at, end);
            sums.add_minute(
                MinuteKey {
                    minute: minute * 60,
                    source: source.to_string(),
                },
                observed,
            );
            sums.add_day(
                DayKey {
                    day: floor_div(at, DAY_MS),
                    source: source.to_string(),
                },
                observed,
            );
            at = end;
        }
        sums
    }

    fn add_minute(&mut self, key: MinuteKey, observed: Observed) {
        self.minute.entry(key.clone()).or_default().add(observed);
        self.touched_minutes.insert(key);
    }

    fn add_day(&mut self, key: DayKey, observed: Observed) {
        self.daily.entry(key.clone()).or_default().add(observed);
        self.touched_days.insert(key);
    }

    /// The minute cells.
    #[must_use]
    pub fn minutes(&self) -> &BTreeMap<MinuteKey, Observed> {
        &self.minute
    }

    /// The day cells.
    #[must_use]
    pub fn days_held(&self) -> &BTreeMap<DayKey, Observed> {
        &self.daily
    }

    /// Restate `source` over `window` with `restated`, its cells over that window stated again:
    /// each of its minute cells in the window takes the restated value, its day cells move by
    /// the difference, and only the cells whose value changed are touched. Returns the days
    /// that changed.
    ///
    /// The window starts on a minute, and the source's minute cells inside it hold the window's
    /// stretch alone: a restatement runs before the tick states past the window's end.
    pub fn restate_source(
        &mut self,
        source: &str,
        window: Window,
        restated: &Self,
    ) -> BTreeSet<i64> {
        let at = |minute: i64| MinuteKey {
            minute,
            source: source.to_string(),
        };
        let span = at(floor_div(window.start_ms, MINUTE_MS) * 60)
            ..=at(floor_div(window.end_ms - 1.0, MINUTE_MS) * 60);
        let keys: BTreeSet<i64> = self
            .minute
            .range(span.clone())
            .chain(restated.minute.range(span))
            .filter(|(k, _)| k.source == source)
            .map(|(k, _)| k.minute)
            .collect();
        let mut changed = BTreeSet::new();
        for minute in keys {
            let key = at(minute);
            let old = self.minute.get(&key).copied().unwrap_or_default();
            let new = restated.minute.get(&key).copied().unwrap_or_default();
            if old == new {
                continue;
            }
            if new == Observed::default() {
                self.minute.remove(&key);
            } else {
                self.minute.insert(key.clone(), new);
            }
            let day = DayKey {
                day: key.day(),
                source: source.to_string(),
            };
            self.touched_minutes.insert(key);
            let held = self.daily.entry(day.clone()).or_default();
            held.covered_ms += new.covered_ms - old.covered_ms;
            held.window_ms += new.window_ms - old.window_ms;
            if held.window_ms <= 0.0 {
                self.daily.remove(&day);
            }
            changed.insert(day.day);
            self.touched_days.insert(day);
        }
        changed
    }

    /// Day `day`'s cells, one per source, in source order.
    #[must_use]
    pub fn day(&self, day: i64) -> Vec<(String, Observed)> {
        let from = |day| DayKey {
            day,
            source: String::new(),
        };
        self.daily
            .range(from(day)..from(day + 1))
            .map(|(k, observed)| (k.source.clone(), *observed))
            .collect()
    }
}

/// One day's coverage from its day cells, as `coverage_daily` publishes them: what a day-grain
/// module depending on coverage reads. Each source's stated milliseconds are its own, so a
/// source first seen partway through the day is stated over the part it was seen for, and the
/// fraction is the covered sum over the stated sum. The cells carry no uncertain stretches, so
/// [`Coverage::uncertain`] is empty.
#[must_use]
pub fn day_coverage(day: i64, cells: &[(String, Observed)]) -> Coverage {
    let (covered, stated) = cells.iter().fold((0.0, 0.0), |(c, w), (_, o)| {
        (c + o.covered_ms, w + o.window_ms)
    });
    #[allow(clippy::cast_precision_loss)] // day indexes are small integers
    let day_ms = day as f64 * DAY_MS;
    Coverage {
        fraction: if stated > 0.0 { covered / stated } else { 0.0 },
        per_source_covered_ms: cells
            .iter()
            .map(|(source, o)| (source.clone(), o.covered_ms))
            .collect(),
        uncertain: Vec::new(),
        per_day: cells
            .iter()
            .map(|(source, o)| DailyCoverage {
                day_ms,
                source: source.clone(),
                covered_ms: o.covered_ms,
                window_ms: o.window_ms,
            })
            .collect(),
    }
}

impl crate::module::Mergeable for CoverageSums {
    /// Another window's cells into these: sums add.
    fn merge(&mut self, other: Self) {
        for (key, observed) in other.minute {
            self.add_minute(key, observed);
        }
        for (key, observed) in other.daily {
            self.add_day(key, observed);
        }
    }
}

impl crate::module::Tracked for CoverageSums {
    type Changes = CoverageChanges;

    fn len(&self) -> usize {
        self.minute.len() + self.daily.len()
    }

    fn drain_changes(&mut self) -> CoverageChanges {
        CoverageChanges {
            minute: std::mem::take(&mut self.touched_minutes)
                .into_iter()
                .map(|k| {
                    let now = self.minute.get(&k).copied();
                    (k, now)
                })
                .collect(),
            daily: std::mem::take(&mut self.touched_days)
                .into_iter()
                .map(|k| {
                    let now = self.daily.get(&k).copied();
                    (k, now)
                })
                .collect(),
        }
    }

    fn discard_changes(&mut self) {
        self.touched_minutes.clear();
        self.touched_days.clear();
    }
}

impl crate::module::DayPartitioned for CoverageSums {
    fn clear_day(&mut self, day: i64) {
        let minutes: Vec<MinuteKey> = self
            .minute
            .keys()
            .filter(|k| k.day() == day)
            .cloned()
            .collect();
        for key in minutes {
            self.minute.remove(&key);
            self.touched_minutes.insert(key);
        }
        let days: Vec<DayKey> = self
            .daily
            .keys()
            .filter(|k| k.day == day)
            .cloned()
            .collect();
        for key in days {
            self.daily.remove(&key);
            self.touched_days.insert(key);
        }
    }

    fn retire_before(&mut self, before: i64) {
        self.minute.retain(|k, _| k.day() >= before);
        self.touched_minutes.retain(|k| k.day() >= before);
        self.daily.retain(|k, _| k.day >= before);
        self.touched_days.retain(|k| k.day >= before);
    }

    fn days(&self) -> BTreeSet<i64> {
        self.daily.keys().map(|k| k.day).collect()
    }
}

fn row(table: &'static str, key: Vec<Value>, observed: Option<Observed>) -> Row {
    let o = observed.unwrap_or_default();
    Row {
        table,
        key,
        measures: vec![Value::F64(o.covered_ms), Value::F64(o.window_ms)],
        gone: observed.is_none(),
    }
}

fn minute_row(k: &MinuteKey, observed: Option<Observed>) -> Row {
    row(
        COVERAGE_MINUTE.name,
        vec![Value::DateTime(k.minute), Value::Str(k.source.clone())],
        observed,
    )
}

fn day_row(k: &DayKey, observed: Option<Observed>) -> Row {
    row(
        COVERAGE_DAILY.name,
        vec![Value::Date(k.day), Value::Str(k.source.clone())],
        observed,
    )
}

impl Persisted for CoverageSums {
    const TABLES: &'static [Table] = &[COVERAGE_MINUTE, COVERAGE_DAILY];

    fn render(changes: &CoverageChanges) -> Vec<Row> {
        changes
            .minute
            .iter()
            .map(|(k, v)| minute_row(k, *v))
            .chain(changes.daily.iter().map(|(k, v)| day_row(k, *v)))
            .collect()
    }

    fn render_day(&self, day: i64) -> Vec<Row> {
        self.minute
            .iter()
            .filter(|(k, _)| k.day() == day)
            .map(|(k, v)| minute_row(k, Some(*v)))
            .chain(
                self.daily
                    .iter()
                    .filter(|(k, _)| k.day == day)
                    .map(|(k, v)| day_row(k, Some(*v))),
            )
            .collect()
    }

    fn restore(&mut self, table: &str, key: &[&str], measures: &[&str]) -> Result<(), Error> {
        let bad = |what: &str| Error::Parse {
            name: table.to_string(),
            message: what.to_string(),
        };
        let number = |v: &str| v.parse::<f64>().map_err(|e| bad(&format!("{v:?}: {e}")));
        let ([bucket, source], [covered, window]) = (key, measures) else {
            return Err(bad("a key of two columns and two measures"));
        };
        let observed = Observed {
            covered_ms: number(covered)?,
            window_ms: number(window)?,
        };
        let bucket: i64 = bucket
            .parse()
            .map_err(|e| bad(&format!("{bucket:?}: {e}")))?;
        let source = (*source).to_string();
        match table {
            t if t == COVERAGE_MINUTE.name => {
                let key = MinuteKey {
                    minute: bucket,
                    source,
                };
                self.minute.insert(key, observed);
            }
            t if t == COVERAGE_DAILY.name => {
                let key = DayKey {
                    day: bucket,
                    source,
                };
                self.daily.insert(key, observed);
            }
            _ => return Err(bad("not a coverage table")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CoverageSums, DayKey, Observed};
    use crate::completeness::{
        uncertainty_model_from_parts, AgentBoundaries, HeartbeatSpan, UncertaintySpan,
    };
    use crate::module::{round_trip_day, Mergeable};
    use crate::{Params, Window, DAY_MS};
    use std::collections::BTreeSet;

    fn window(start_ms: f64, end_ms: f64) -> Window {
        Window { start_ms, end_ms }
    }

    #[test]
    #[allow(clippy::float_cmp)] // whole milliseconds: every figure here is exact
    fn two_windows_sum_to_the_window_they_split() {
        let uncertain = [UncertaintySpan {
            start_ms: DAY_MS - 90_000.0,
            end_ms: DAY_MS + 30_000.0,
        }];
        let whole = CoverageSums::of_source(
            "i1",
            window(DAY_MS - 150_000.0, DAY_MS + 70_000.0),
            &uncertain,
        );
        let mut split = CoverageSums::of_source(
            "i1",
            window(DAY_MS - 150_000.0, DAY_MS - 45_500.0),
            &uncertain,
        );
        split.merge(CoverageSums::of_source(
            "i1",
            window(DAY_MS - 45_500.0, DAY_MS + 70_000.0),
            &uncertain,
        ));
        assert_eq!(split.minutes(), whole.minutes());
        assert_eq!(split.days_held(), whole.days_held());
        // Midnight cuts the uncertain stretch: 90 s before it, 30 s after.
        assert_eq!(
            whole.days_held()[&DayKey {
                day: 0,
                source: "i1".to_string(),
            }],
            Observed {
                covered_ms: 60_000.0,
                window_ms: 150_000.0
            }
        );
        assert_eq!(
            whole.days_held()[&DayKey {
                day: 1,
                source: "i1".to_string(),
            }],
            Observed {
                covered_ms: 40_000.0,
                window_ms: 70_000.0
            }
        );
    }

    #[test]
    #[allow(clippy::float_cmp)] // whole milliseconds: every figure here is exact
    fn a_restated_source_holds_what_stating_it_whole_would_and_touches_only_what_changed() {
        let late_gap = [UncertaintySpan {
            start_ms: 150_000.0,
            end_ms: 270_000.0,
        }];
        let whole = window(0.0, 330_000.0);
        // Stated once with a gap, then restated over its first five minutes with the gap
        // shortened, as a late segment would: the cells are those of stating it so at once.
        let mut held = CoverageSums::of_source("i1", whole, &late_gap);
        held.merge(CoverageSums::of_source("i2", whole, &[]));
        let _ = crate::module::Tracked::drain_changes(&mut held);
        let shorter = [UncertaintySpan {
            start_ms: 150_000.0,
            end_ms: 180_000.0,
        }];
        let restated = window(0.0, 300_000.0);
        let changed = held.restate_source(
            "i1",
            restated,
            &CoverageSums::of_source("i1", restated, &shorter),
        );
        let mut want = CoverageSums::of_source("i1", restated, &shorter);
        want.merge(CoverageSums::of_source(
            "i1",
            window(300_000.0, 330_000.0),
            &late_gap,
        ));
        want.merge(CoverageSums::of_source("i2", whole, &[]));
        assert_eq!(held.minutes(), want.minutes());
        assert_eq!(held.days_held(), want.days_held());
        assert_eq!(changed, BTreeSet::from([0]));
        let moved = crate::module::Tracked::drain_changes(&mut held);
        assert_eq!(
            moved
                .minute
                .iter()
                .map(|(k, _)| k.minute)
                .collect::<Vec<_>>(),
            vec![180, 240],
            "only the minutes the gap left"
        );
        // Restating what is already held changes nothing.
        let again = held.restate_source(
            "i1",
            restated,
            &CoverageSums::of_source("i1", restated, &shorter),
        );
        assert!(again.is_empty());
    }

    #[test]
    #[allow(clippy::float_cmp)] // whole milliseconds: every figure here is exact
    fn a_days_coverage_is_its_day_cells_and_no_other_days() {
        let mut sums = CoverageSums::of_source(
            "i2",
            window(DAY_MS - 60_000.0, DAY_MS + 120_000.0),
            &[UncertaintySpan {
                start_ms: DAY_MS,
                end_ms: DAY_MS + 30_000.0,
            }],
        );
        sums.merge(CoverageSums::of_source(
            "i1",
            window(DAY_MS + 60_000.0, DAY_MS + 120_000.0),
            &[],
        ));
        let day = sums.day(1);
        let coverage = super::day_coverage(1, &day);
        assert_eq!(
            coverage.per_source_covered_ms,
            vec![("i1".to_string(), 60_000.0), ("i2".to_string(), 90_000.0)],
            "source order, day 1's cells only"
        );
        assert_eq!(coverage.fraction, 150_000.0 / 180_000.0);
        assert!(coverage.per_day.iter().all(|d| d.day_ms == DAY_MS));
        assert!(coverage.uncertain.is_empty());
        assert_eq!(super::day_coverage(2, &sums.day(2)).fraction, 0.0);
    }

    /// Heartbeats every minute from `from` to `to`, `source`'s spans between them, one of them
    /// a drop window.
    fn beats(source: &str, from: f64, to: f64, dropped_at: f64) -> Vec<HeartbeatSpan> {
        let mut spans = Vec::new();
        let mut at = from;
        while at + 60_000.0 <= to {
            spans.push(HeartbeatSpan {
                source: source.into(),
                start_at_ms: at,
                end_at_ms: at + 60_000.0,
                dropped_delta: i64::from((at - dropped_at).abs() < 1.0),
                oversized_delta: 0,
                silence: None,
            });
            at += 60_000.0;
        }
        spans
    }

    #[test]
    #[allow(clippy::float_cmp)] // whole milliseconds: the edge is exact
    fn ticks_cut_at_the_settled_edge_sum_to_one_window() {
        let params = Params::default();
        let start = DAY_MS - 3_600_000.0;
        let now = DAY_MS + 3_600_000.0;
        // "a" beats throughout; "b" goes silent 40 minutes before now, past the grace.
        let mut spans = beats("a", start, now, DAY_MS - 600_000.0);
        spans.extend(beats("b", start, now - 2_400_000.0, f64::NAN));
        let whole_window = window(start, now);
        let model = uncertainty_model_from_parts(
            spans,
            AgentBoundaries::default(),
            BTreeSet::new(),
            &whole_window,
            &params,
        );
        let settled = model.coverage_settled_to(1_800_000.0);
        assert_eq!(
            settled,
            now - params.delta_skew_ms,
            "a's tail holds the edge, b's does not"
        );
        let whole = model.coverage_sums(window(start + 1.0, settled));
        let mut ticks = CoverageSums::default();
        let mut at = start + 1.0;
        for cut in [
            DAY_MS - 1_234_567.0,
            DAY_MS - 5.0,
            DAY_MS + 777_000.0,
            settled,
        ] {
            ticks.merge(model.coverage_sums(window(at, cut)));
            at = cut;
        }
        assert_eq!(ticks.minutes(), whole.minutes());
        assert_eq!(ticks.days_held(), whole.days_held());
        let b_today = whole.days_held()[&DayKey {
            day: 1,
            source: "b".to_string(),
        }];
        assert!(
            b_today.covered_ms < b_today.window_ms - 2_000_000.0,
            "b's silence is uncertain: {b_today:?}"
        );
    }

    #[test]
    fn a_day_reads_back_from_its_rows() {
        let sums = CoverageSums::of_source("i1", window(DAY_MS + 10.0, DAY_MS + 200_000.0), &[]);
        let back = round_trip_day(&sums, 1).expect("round trip");
        assert_eq!(back.minutes(), sums.minutes());
        assert_eq!(back.days_held(), sums.days_held());
    }
}
