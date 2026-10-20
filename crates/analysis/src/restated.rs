// SPDX-License-Identifier: AGPL-3.0-only

//! What the correction paths restated, per hour: a ledger figure beside the cells, so a reader
//! (the receipt, the console) can say when a window was stated again after it was first
//! published, and what arrived too late to be.
//!
//! Each late arrival counts in the hour of its own instant, under the cell family it restated:
//! the class and band cells, placement's cells, or coverage. A record corrected counts as
//! corrected; one of a day no longer open to change as too late; a late scope closer, which no
//! one identity's re-derivation corrects, as a closer. Coverage counts segment arrivals whose
//! heartbeats reached behind the stated edge.

use crate::error::Error;
use crate::module::{Column, DayOf, Kind, Row, Table, Value};
use std::collections::{BTreeMap, BTreeSet};

/// The restatements per hour and family: `restated_hourly`.
pub const RESTATED_HOURLY: Table = Table {
    name: "restated_hourly",
    key: &[
        Column::new("bucket", Kind::DateTime),
        Column::new("family", Kind::Str),
    ],
    measures: &[
        Column::new("corrected", Kind::I64),
        Column::new("too_late", Kind::I64),
        Column::new("closers", Kind::I64),
    ],
    day: DayOf::DateTime("bucket"),
};

/// The cell family a restatement reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// The class and band cells: repeat compute's correction path.
    Classes,
    /// Placement's cells: its correction path.
    Placement,
    /// The coverage cells: a source stated again from its heartbeats.
    Coverage,
}

impl Family {
    /// The family's spelling in `restated_hourly`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Classes => "classes",
            Self::Placement => "placement",
            Self::Coverage => "coverage",
        }
    }
}

/// What became of one late arrival.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Corrected: the cells it reaches were stated again.
    Corrected,
    /// Of a day no longer open to change: counted, left uncorrected.
    TooLate,
    /// A scope closer no identity's re-derivation corrects: counted, left uncorrected.
    Closer,
}

/// One hour and family's counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Arrivals corrected.
    pub corrected: i64,
    /// Arrivals of days no longer open.
    pub too_late: i64,
    /// Late scope closers.
    pub closers: i64,
}

impl Counts {
    fn add(&mut self, o: Self) {
        self.corrected += o.corrected;
        self.too_late += o.too_late;
        self.closers += o.closers;
    }
}

/// A cell's key: the hour's start in unix seconds, and the family.
pub type RestatedKey = (i64, String);

/// The restatement cells.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Restated {
    cells: BTreeMap<RestatedKey, Counts>,
    touched: BTreeSet<RestatedKey>,
}

impl Restated {
    /// Count one late arrival at `at_ms` under `family`.
    pub fn count(&mut self, family: Family, at_ms: f64, outcome: Outcome) {
        #[allow(clippy::cast_possible_truncation)] // hours of finite instants fit i64
        let hour = (at_ms / 3_600_000.0).floor() as i64 * 3_600;
        let key = (hour, family.as_str().to_string());
        let cell = self.cells.entry(key.clone()).or_default();
        match outcome {
            Outcome::Corrected => cell.corrected += 1,
            Outcome::TooLate => cell.too_late += 1,
            Outcome::Closer => cell.closers += 1,
        }
        self.touched.insert(key);
    }

    /// The cells.
    #[must_use]
    pub fn cells(&self) -> &BTreeMap<RestatedKey, Counts> {
        &self.cells
    }
}

impl crate::module::Mergeable for Restated {
    fn merge(&mut self, other: Self) {
        for (key, counts) in other.cells {
            self.cells.entry(key.clone()).or_default().add(counts);
            self.touched.insert(key);
        }
    }
}

impl crate::module::Tracked for Restated {
    type Changes = Vec<(RestatedKey, Option<Counts>)>;

    fn len(&self) -> usize {
        self.cells.len()
    }

    fn drain_changes(&mut self) -> Self::Changes {
        std::mem::take(&mut self.touched)
            .into_iter()
            .map(|k| {
                let now = self.cells.get(&k).copied();
                (k, now)
            })
            .collect()
    }

    fn discard_changes(&mut self) {
        self.touched.clear();
    }
}

impl crate::module::DayPartitioned for Restated {
    fn clear_day(&mut self, day: i64) {
        let gone: Vec<RestatedKey> = self
            .cells
            .keys()
            .filter(|k| k.0.div_euclid(86_400) == day)
            .cloned()
            .collect();
        for k in gone {
            self.cells.remove(&k);
            self.touched.insert(k);
        }
    }

    fn retire_before(&mut self, before: i64) {
        self.cells.retain(|k, _| k.0.div_euclid(86_400) >= before);
        self.touched.retain(|k| k.0.div_euclid(86_400) >= before);
    }

    fn days(&self) -> BTreeSet<i64> {
        self.cells.keys().map(|k| k.0.div_euclid(86_400)).collect()
    }
}

fn row((bucket, family): &RestatedKey, counts: Option<Counts>) -> Row {
    let c = counts.unwrap_or_default();
    Row {
        table: RESTATED_HOURLY.name,
        key: vec![Value::DateTime(*bucket), Value::Str(family.clone())],
        measures: vec![
            Value::I64(c.corrected),
            Value::I64(c.too_late),
            Value::I64(c.closers),
        ],
        gone: counts.is_none(),
    }
}

impl crate::module::Persisted for Restated {
    const TABLES: &'static [Table] = &[RESTATED_HOURLY];

    fn render(changes: &Self::Changes) -> Vec<Row> {
        changes.iter().map(|(k, v)| row(k, *v)).collect()
    }

    fn render_day(&self, day: i64) -> Vec<Row> {
        self.cells
            .iter()
            .filter(|(k, _)| k.0.div_euclid(86_400) == day)
            .map(|(k, v)| row(k, Some(*v)))
            .collect()
    }

    fn restore(&mut self, table: &str, key: &[&str], measures: &[&str]) -> Result<(), Error> {
        let bad = |what: &str| Error::Parse {
            name: table.to_string(),
            message: what.to_string(),
        };
        if table != RESTATED_HOURLY.name {
            return Err(bad("not the restatement table"));
        }
        let ([bucket, family], [corrected, too_late, closers]) = (key, measures) else {
            return Err(bad("a key of two columns and three measures"));
        };
        if ![Family::Classes, Family::Placement, Family::Coverage]
            .iter()
            .any(|f| f.as_str() == *family)
        {
            return Err(bad(&format!("{family:?} is no cell family")));
        }
        let int = |v: &str| v.parse::<i64>().map_err(|e| bad(&format!("{v:?}: {e}")));
        self.cells.insert(
            (int(bucket)?, (*family).to_string()),
            Counts {
                corrected: int(corrected)?,
                too_late: int(too_late)?,
                closers: int(closers)?,
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Family, Outcome, Restated};
    use crate::module::round_trip_day;

    #[test]
    fn arrivals_count_in_their_own_hour_and_the_cells_read_back_as_written() {
        let mut restated = Restated::default();
        let day = 86_400_000.0 * 100.0;
        restated.count(Family::Classes, day + 60_000.0, Outcome::Corrected);
        restated.count(Family::Classes, day + 3_000_000.0, Outcome::Corrected);
        restated.count(Family::Classes, day + 3_700_000.0, Outcome::TooLate);
        restated.count(Family::Placement, day + 60_000.0, Outcome::Closer);
        let cells: Vec<_> = restated
            .cells()
            .iter()
            .map(|((hour, family), c)| (*hour, family.as_str(), c.corrected, c.too_late, c.closers))
            .collect();
        let h = 8_640_000;
        assert_eq!(
            cells,
            vec![
                (h, "classes", 2, 0, 0),
                (h, "placement", 0, 0, 1),
                (h + 3_600, "classes", 0, 1, 0),
            ]
        );
        assert_eq!(round_trip_day(&restated, 100).expect("round trip"), {
            let mut placed = restated.clone();
            crate::module::Tracked::discard_changes(&mut placed);
            placed
        });
    }
}
