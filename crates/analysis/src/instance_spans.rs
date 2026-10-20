// SPDX-License-Identifier: AGPL-3.0-only

//! The pair basis (`replication_instance_spans_day`, [`crate::dup_cells::INSTANCE_SPANS_DAY`]):
//! each instance's observed heartbeat spans, per UTC day, merged, as cells. A placement pair's
//! coverage is the time both its instances were observed ([`pair_observed_ms`]), so the spans
//! are kept beside the placement cells for as long as they are.
//!
//! They come from the same arrivals model coverage is stated from, over the same stretches: a
//! tick states the stretch it states coverage over, and a source whose coverage a late backlog
//! restates has its spans over those days restated with it.

use crate::completeness::UncertaintyModel;
use crate::dup_cells::{instance_span_row, INSTANCE_SPANS_DAY};
use crate::error::Error;
use crate::module::{Row, Table};
use crate::{Window, DAY_MS};
use std::collections::{BTreeMap, BTreeSet};

/// A span's key: its UTC day, its instance, and its start's bits (positive instants order as
/// their bits do).
pub type SpanKey = (i64, String, u64);

/// The spans, each its end.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct InstanceSpans {
    cells: BTreeMap<SpanKey, f64>,
    touched: BTreeSet<SpanKey>,
}

#[allow(clippy::cast_possible_truncation)] // day indexes of finite instants fit i64
fn day_of(at_ms: f64) -> i64 {
    (at_ms / DAY_MS).floor() as i64
}

impl InstanceSpans {
    /// `model`'s observed spans over `window`, of `only` that source where given, cut at UTC
    /// days and merged per instance within each day.
    #[must_use]
    pub fn of_model(model: &UncertaintyModel, window: Window, only: Option<&str>) -> Self {
        let mut per: BTreeMap<(i64, String), Vec<(f64, f64)>> = BTreeMap::new();
        for span in model.observed_spans() {
            if only.is_some_and(|s| s != span.source) {
                continue;
            }
            // Observed except where the heartbeats fell silent.
            let observed = match span.silence {
                Some((from, to)) => vec![(span.start_at_ms, from), (to, span.end_at_ms)],
                None => vec![(span.start_at_ms, span.end_at_ms)],
            };
            for (start, end) in observed {
                let (mut lo, hi) = (start.max(window.start_ms), end.min(window.end_ms));
                while lo < hi {
                    let day = day_of(lo);
                    #[allow(clippy::cast_precision_loss)] // day indexes are small integers
                    let cut = ((day + 1) as f64 * DAY_MS).min(hi);
                    per.entry((day, span.source.clone()))
                        .or_default()
                        .push((lo, cut));
                    lo = cut;
                }
            }
        }
        let mut cells = BTreeMap::new();
        for ((day, instance), mut spans) in per {
            spans.sort_by(|a, b| a.0.total_cmp(&b.0));
            let mut merged: Vec<(f64, f64)> = Vec::new();
            for (lo, hi) in spans {
                match merged.last_mut() {
                    Some((_, end)) if lo <= *end => *end = end.max(hi),
                    _ => merged.push((lo, hi)),
                }
            }
            for (lo, hi) in merged {
                cells.insert((day, instance.clone(), lo.to_bits()), hi);
            }
        }
        let touched = cells.keys().cloned().collect();
        Self { cells, touched }
    }

    /// Restate `source` over `window` with `restated`: its spans starting inside the window
    /// leave, `restated`'s take their place, and only the spans that changed are touched.
    /// Returns the days that changed. The window starts on a day, so no span crosses its start.
    pub fn restate_source(
        &mut self,
        source: &str,
        window: Window,
        restated: &Self,
    ) -> BTreeSet<i64> {
        let inside = |(_, s, start): &SpanKey| {
            s == source && {
                let at = f64::from_bits(*start);
                at >= window.start_ms && at < window.end_ms
            }
        };
        let old: BTreeMap<SpanKey, f64> = self
            .cells
            .iter()
            .filter(|(k, _)| inside(k))
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        let new: BTreeMap<SpanKey, f64> = restated
            .cells
            .iter()
            .filter(|(k, _)| inside(k))
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        let mut changed = BTreeSet::new();
        for key in old.keys().chain(new.keys()) {
            let (was, now) = (old.get(key), new.get(key));
            if was.map(|v| v.to_bits()) == now.map(|v| v.to_bits()) {
                continue;
            }
            match now {
                Some(end) => self.cells.insert(key.clone(), *end),
                None => self.cells.remove(key),
            };
            self.touched.insert(key.clone());
            changed.insert(key.0);
        }
        let groups: BTreeSet<(i64, String)> = changed
            .iter()
            .map(|day| (*day, source.to_string()))
            .collect();
        self.coalesce(&groups);
        changed
    }

    /// Store each of `groups`' spans in one form whatever order they arrived in: spans of one
    /// instance on one day that meet or overlap become one. A stretch stated tick by tick and
    /// the same stretch restated whole are then the same cells.
    fn coalesce(&mut self, groups: &BTreeSet<(i64, String)>) {
        for (day, instance) in groups {
            let lo = (*day, instance.clone(), 0);
            let hi = (*day, instance.clone(), u64::MAX);
            let spans: Vec<(SpanKey, f64)> = self
                .cells
                .range(lo..=hi)
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            let mut merged: Vec<(SpanKey, f64)> = Vec::new();
            for (key, end) in spans {
                match merged.last_mut() {
                    Some((_, last)) if f64::from_bits(key.2) <= *last => {
                        *last = last.max(end);
                        self.cells.remove(&key);
                        self.touched.insert(key);
                    }
                    _ => merged.push((key, end)),
                }
            }
            for (key, end) in merged {
                if self.cells.get(&key).map(|v| v.to_bits()) != Some(end.to_bits()) {
                    self.cells.insert(key.clone(), end);
                    self.touched.insert(key);
                }
            }
        }
    }

    /// The spans.
    #[must_use]
    pub fn cells(&self) -> &BTreeMap<SpanKey, f64> {
        &self.cells
    }
}

/// `|observed(a) ∩ observed(b)|` over two merged, sorted span lists: a placement pair's
/// coverage basis. A rank pair within one engine passes the same instance twice and gets its
/// own observed time, which is the same formula.
#[must_use]
pub fn pair_observed_ms(a: &[(f64, f64)], b: &[(f64, f64)]) -> f64 {
    let (mut i, mut j, mut total) = (0usize, 0usize, 0.0f64);
    while i < a.len() && j < b.len() {
        let lo = a[i].0.max(b[j].0);
        let hi = a[i].1.min(b[j].1);
        if lo < hi {
            total += hi - lo;
        }
        if a[i].1 <= b[j].1 {
            i += 1;
        } else {
            j += 1;
        }
    }
    total
}

impl crate::module::Mergeable for InstanceSpans {
    /// Another stretch's spans beside these. Two ticks' spans meet at the tick's edge and are
    /// stored as one; a span stated twice from one start keeps the later end.
    fn merge(&mut self, other: Self) {
        let mut groups = BTreeSet::new();
        for (key, end) in other.cells {
            groups.insert((key.0, key.1.clone()));
            let slot = self.cells.entry(key.clone()).or_insert(end);
            *slot = slot.max(end);
            self.touched.insert(key);
        }
        self.coalesce(&groups);
    }
}

impl crate::module::Tracked for InstanceSpans {
    type Changes = Vec<(SpanKey, Option<f64>)>;

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

impl crate::module::DayPartitioned for InstanceSpans {
    fn clear_day(&mut self, day: i64) {
        let gone: Vec<SpanKey> = self.cells.keys().filter(|k| k.0 == day).cloned().collect();
        for k in gone {
            self.cells.remove(&k);
            self.touched.insert(k);
        }
    }

    fn retire_before(&mut self, before: i64) {
        self.cells.retain(|k, _| k.0 >= before);
        self.touched.retain(|k| k.0 >= before);
    }

    fn days(&self) -> BTreeSet<i64> {
        self.cells.keys().map(|k| k.0).collect()
    }
}

fn row((day, instance, start): &SpanKey, end: Option<f64>) -> Row {
    let mut row = instance_span_row(*day, instance, f64::from_bits(*start), end.unwrap_or(0.0));
    row.gone = end.is_none();
    row
}

impl crate::module::Persisted for InstanceSpans {
    const TABLES: &'static [Table] = &[INSTANCE_SPANS_DAY];

    fn render(changes: &Self::Changes) -> Vec<Row> {
        changes.iter().map(|(k, v)| row(k, *v)).collect()
    }

    fn render_day(&self, day: i64) -> Vec<Row> {
        self.cells
            .iter()
            .filter(|(k, _)| k.0 == day)
            .map(|(k, v)| row(k, Some(*v)))
            .collect()
    }

    fn restore(&mut self, table: &str, key: &[&str], measures: &[&str]) -> Result<(), Error> {
        let bad = |what: &str| Error::Parse {
            name: table.to_string(),
            message: what.to_string(),
        };
        if table != INSTANCE_SPANS_DAY.name {
            return Err(bad("not the pair basis table"));
        }
        let ([day, instance, start], [end]) = (key, measures) else {
            return Err(bad("a key of three columns and one measure"));
        };
        let int = |v: &str| v.parse::<i64>().map_err(|e| bad(&format!("{v:?}: {e}")));
        let float = |v: &str| v.parse::<f64>().map_err(|e| bad(&format!("{v:?}: {e}")));
        self.cells.insert(
            (int(day)?, (*instance).to_string(), float(start)?.to_bits()),
            float(end)?,
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{pair_observed_ms, InstanceSpans};
    use crate::module::round_trip_day;

    #[test]
    fn a_pair_is_observed_while_both_are() {
        assert!(
            (pair_observed_ms(&[(0.0, 10.0), (20.0, 30.0)], &[(5.0, 25.0)]) - 10.0).abs()
                < f64::EPSILON
        );
        assert!(pair_observed_ms(&[(0.0, 1.0)], &[]).abs() < f64::EPSILON);
    }

    #[test]
    #[allow(clippy::float_cmp)] // whole milliseconds: every figure here is exact
    fn a_stretch_stated_tick_by_tick_is_the_stretch_stated_whole() {
        let d = crate::DAY_MS;
        let span = |at: f64, end: f64| {
            let mut one = InstanceSpans::default();
            one.cells
                .insert((1, "i1".to_string(), (d + at).to_bits()), d + end);
            one
        };
        let mut ticked = span(0.0, 100.0);
        crate::module::Mergeable::merge(&mut ticked, span(100.0, 250.0));
        crate::module::Mergeable::merge(&mut ticked, span(250.0, 300.0));
        let whole = span(0.0, 300.0);
        assert_eq!(ticked.cells, whole.cells);
        let mut restated = span(0.0, 100.0);
        let day = crate::Window {
            start_ms: d,
            end_ms: 2.0 * d,
        };
        restated.restate_source("i1", day, &whole);
        assert_eq!(restated.cells, whole.cells);
    }

    #[test]
    #[allow(clippy::float_cmp)] // whole milliseconds: every figure here is exact
    fn a_restatement_moves_only_the_spans_that_changed_and_the_cells_read_back() {
        let d = crate::DAY_MS;
        let key = |instance: &str, at: f64| (1, instance.to_string(), (d + at).to_bits());
        let mut held = InstanceSpans::default();
        held.cells.insert(key("i1", 100.0), d + 200.0);
        held.cells.insert(key("i2", 100.0), d + 150.0);
        let mut restated = InstanceSpans::default();
        restated.cells.insert(key("i1", 100.0), d + 300.0);
        let day = crate::Window {
            start_ms: d,
            end_ms: 2.0 * d,
        };
        assert_eq!(held.restate_source("i1", day, &restated), [1].into());
        assert_eq!(held.touched.len(), 1, "i2 is untouched");
        assert_eq!(held.cells[&key("i1", 100.0)], d + 300.0);
        assert!(
            held.restate_source("i1", day, &restated).is_empty(),
            "stated again, nothing changes"
        );
        let mut placed = held.clone();
        crate::module::Tracked::discard_changes(&mut placed);
        assert_eq!(round_trip_day(&held, 1).expect("round trip"), placed);
    }
}
