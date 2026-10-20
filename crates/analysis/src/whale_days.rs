// SPDX-License-Identifier: AGPL-3.0-only

//! The whale register per day: the identities that held the most duplicated token-time.
//!
//! Each identity accrues its own day totals as placement folds it
//! ([`crate::dup_cells::WhaleAccrual`]), so a settled day's totals are exact for every identity
//! without a row per identity per tick. Once a day settles, and again when a correction restates
//! it, one pass over the identities states the day: the top [`WHALE_CUT`] by duplicated
//! token-time, whole, and the day's own row with the exact totals over every identity.
//!
//! **What a reader is told.** Within a day the cut is exact. The day row's `floor_dup_token_ms`
//! is the duplicated token-time of the cut's last identity when more identities duplicated than
//! the cut holds, and zero when the cut holds them all: an identity outside a day's cut held at
//! most the floor that day. So a ranking over several days sums each identity's rows, exact for
//! an identity in every day's cut and otherwise a lower bound short by at most the floors of the
//! days it is missing from.

use crate::dup_cells::WhaleSum;
use crate::module::{Column, DayOf, Kind, Row, Table, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The identities a day states.
pub const WHALE_CUT: usize = 1_000;

/// Each stated identity's day row: `replication_whale_day`.
pub const WHALE_DAY: Table = Table {
    name: "replication_whale_day",
    key: &[
        Column::new("day", Kind::Date),
        Column::new("identity", Kind::Str),
    ],
    measures: &[
        Column::new("n_tokens", Kind::I64),
        Column::new("dup_token_ms", Kind::F64),
        Column::new("held_token_ms", Kind::F64),
    ],
    day: DayOf::Date("day"),
};

/// Each day's totals over every identity, and the cut its rows were taken under:
/// `replication_whale_day_total`.
pub const WHALE_DAY_TOTAL: Table = Table {
    name: "replication_whale_day_total",
    key: &[Column::new("day", Kind::Date)],
    measures: &[
        Column::new("held_identities", Kind::I64),
        Column::new("duplicated_identities", Kind::I64),
        Column::new("dup_token_ms", Kind::F64),
        Column::new("held_token_ms", Kind::F64),
        Column::new("cut", Kind::U32),
        Column::new("floor_dup_token_ms", Kind::F64),
    ],
    day: DayOf::Date("day"),
};

/// The whale family's tables.
pub const TABLES: &[Table] = &[WHALE_DAY, WHALE_DAY_TOTAL];

/// One day stated: its cut and its totals.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Day {
    /// The top identities by duplicated token-time, heaviest first, ties by identity.
    pub top: Vec<(String, WhaleSum)>,
    /// Identities that held anything that day.
    pub held_identities: u64,
    /// Identities that held at two or more copies at once that day.
    pub duplicated_identities: u64,
    /// Duplicated token-time over every identity.
    pub dup_token_ms: f64,
    /// Held token-time over every identity.
    pub held_token_ms: f64,
    /// The cut it was taken under.
    pub cut: usize,
    /// The most an identity outside the cut duplicated: the cut's last when the cut is full and
    /// identities were left out, else zero.
    pub floor_dup_token_ms: f64,
}

impl Day {
    /// The day over every identity's totals, cut at `cut`.
    #[must_use]
    pub fn of(totals: impl IntoIterator<Item = (String, WhaleSum)>, cut: usize) -> Self {
        let mut day = Self {
            cut,
            ..Self::default()
        };
        let mut duplicated = Vec::new();
        for (identity, sum) in totals {
            if sum.held_token_ms > 0.0 {
                day.held_identities += 1;
                day.held_token_ms += sum.held_token_ms;
            }
            if sum.dup_token_ms > 0.0 {
                day.duplicated_identities += 1;
                day.dup_token_ms += sum.dup_token_ms;
                duplicated.push((identity, sum));
            }
        }
        duplicated.sort_by(|a, b| {
            b.1.dup_token_ms
                .total_cmp(&a.1.dup_token_ms)
                .then_with(|| a.0.cmp(&b.0))
        });
        if duplicated.len() > cut {
            day.floor_dup_token_ms = cut
                .checked_sub(1)
                .map_or(duplicated[0].1.dup_token_ms, |last| {
                    duplicated[last].1.dup_token_ms
                });
            duplicated.truncate(cut);
        }
        day.top = duplicated;
        day
    }
}

/// The whale register, one stated day each, and what each day last published.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WhaleDays {
    days: BTreeMap<i64, Day>,
    published: BTreeMap<i64, BTreeSet<String>>,
    touched: BTreeSet<i64>,
}

impl WhaleDays {
    /// State `day` as `stated`: the next drain publishes it.
    pub fn set_day(&mut self, day: i64, stated: Day) {
        self.days.insert(day, stated);
        self.touched.insert(day);
    }

    /// A day and the identities it last published, as a store kept them.
    pub fn restore_day(&mut self, day: i64, stated: Day, published: BTreeSet<String>) {
        self.days.insert(day, stated);
        self.published.insert(day, published);
    }

    /// Each stated day.
    #[must_use]
    pub fn days(&self) -> &BTreeMap<i64, Day> {
        &self.days
    }

    /// The identities `day` last published.
    #[must_use]
    pub fn published(&self, day: i64) -> Option<&BTreeSet<String>> {
        self.published.get(&day)
    }

    /// The rows of every day stated since the last call: its cut whole, a tombstone for an
    /// identity that left it, and its totals. Returns the days drained.
    pub fn drain(&mut self) -> (Vec<Row>, Vec<i64>) {
        let mut rows = Vec::new();
        let days: Vec<i64> = std::mem::take(&mut self.touched).into_iter().collect();
        for day in &days {
            let now: BTreeSet<String> = self
                .days
                .get(day)
                .map(|d| d.top.iter().map(|(i, _)| i.clone()).collect())
                .unwrap_or_default();
            for gone in self
                .published
                .get(day)
                .into_iter()
                .flatten()
                .filter(|i| !now.contains(*i))
            {
                rows.push(identity_row(*day, gone, None));
            }
            rows.extend(self.day_rows(*day));
            self.published.insert(*day, now);
        }
        (rows, days)
    }

    /// `day`'s rows whole: its cut and its totals, nothing for a day not stated. What a
    /// restatement of the day replaces the published day with.
    #[must_use]
    pub fn day_rows(&self, day: i64) -> Vec<Row> {
        let Some(stated) = self.days.get(&day) else {
            return Vec::new();
        };
        let mut rows: Vec<Row> = stated
            .top
            .iter()
            .map(|(identity, sum)| identity_row(day, identity, Some(sum)))
            .collect();
        rows.push(Row {
            table: WHALE_DAY_TOTAL.name,
            key: vec![Value::Date(day)],
            measures: vec![
                Value::I64(i64::try_from(stated.held_identities).unwrap_or(i64::MAX)),
                Value::I64(i64::try_from(stated.duplicated_identities).unwrap_or(i64::MAX)),
                Value::F64(stated.dup_token_ms),
                Value::F64(stated.held_token_ms),
                Value::U32(u32::try_from(stated.cut).unwrap_or(u32::MAX)),
                Value::F64(stated.floor_dup_token_ms),
            ],
            gone: false,
        });
        rows
    }

    /// Forget the days before `day`: their rows stand as published.
    pub fn retire_before(&mut self, day: i64) {
        self.days = self.days.split_off(&day);
        self.published = self.published.split_off(&day);
        self.touched.retain(|d| *d >= day);
    }
}

fn identity_row(day: i64, identity: &str, sum: Option<&WhaleSum>) -> Row {
    Row {
        table: WHALE_DAY.name,
        key: vec![Value::Date(day), Value::Str(identity.to_string())],
        measures: vec![
            Value::I64(sum.map_or(0, |s| s.n_tokens)),
            Value::F64(sum.map_or(0.0, |s| s.dup_token_ms)),
            Value::F64(sum.map_or(0.0, |s| s.held_token_ms)),
        ],
        gone: sum.is_none(),
    }
}

#[cfg(test)]
mod tests {
    use super::{Day, WhaleDays, WHALE_DAY};
    use crate::dup_cells::WhaleSum;

    fn sum(dup: f64, held: f64) -> WhaleSum {
        WhaleSum {
            n_tokens: 16,
            dup_token_ms: dup,
            held_token_ms: held,
        }
    }

    #[test]
    fn a_day_cuts_its_heaviest_and_states_every_identity_s_totals() {
        let day = Day::of(
            [
                ("a".to_string(), sum(5.0, 9.0)),
                ("b".to_string(), sum(7.0, 7.0)),
                ("c".to_string(), sum(5.0, 6.0)),
                ("d".to_string(), sum(0.0, 4.0)),
            ],
            2,
        );
        let names: Vec<&str> = day.top.iter().map(|(i, _)| i.as_str()).collect();
        assert_eq!(names, vec!["b", "a"], "heaviest first, ties by identity");
        assert_eq!((day.held_identities, day.duplicated_identities), (4, 3));
        assert!((day.dup_token_ms - 17.0).abs() < 1e-9 && (day.held_token_ms - 26.0).abs() < 1e-9);
        assert!(
            (day.floor_dup_token_ms - 5.0).abs() < 1e-9,
            "c is left out at 5"
        );
        let all = Day::of([("a".to_string(), sum(5.0, 9.0))], 2);
        assert!(
            all.floor_dup_token_ms.abs() < f64::EPSILON,
            "a cut holding all has no floor"
        );
    }

    #[test]
    fn a_restated_day_tombstones_what_left_its_cut() {
        let mut whales = WhaleDays::default();
        whales.set_day(3, Day::of([("a".to_string(), sum(5.0, 5.0))], 1));
        let (first, days) = whales.drain();
        assert_eq!(days, vec![3]);
        assert_eq!(
            first.iter().filter(|r| r.table == WHALE_DAY.name).count(),
            1
        );
        whales.set_day(
            3,
            Day::of(
                [
                    ("a".to_string(), sum(5.0, 5.0)),
                    ("b".to_string(), sum(8.0, 8.0)),
                ],
                1,
            ),
        );
        let (second, _) = whales.drain();
        let gone: Vec<_> = second.iter().filter(|r| r.gone).collect();
        assert_eq!(gone.len(), 1, "a left the cut: {second:?}");
        assert!(
            whales.drain().0.is_empty(),
            "nothing restated, nothing drained"
        );
    }
}
