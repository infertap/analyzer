// SPDX-License-Identifier: AGPL-3.0-only

//! Observed coverage as a derivation module: source-keyed, stateless, mergeable.
//!
//! The per-source half of [`crate::completeness::UncertaintyModel::coverage`] — one source's
//! uncertainty spans over the window (the stretch before its first heartbeat unless an agent
//! start attests it, each drop window widened by the skew, the tail after its last heartbeat
//! unless a stop closes it), its covered time over the window and over each UTC day — is
//! [`ObservedCoverage::fold`]. The window's figure is assembled from the merged cells
//! ([`Coverage::of_window`]), which is how the fleet-level fraction stays what it was: the
//! sources' covered time summed in source order, over the window's length times the source
//! count.
//!
//! The fact is source-keyed and mergeable: every window derives it whole from the window's
//! spans, so it carries no state (the module's `State` is `()`) and nothing holds its cells
//! across windows. It is the module the class/band module NEEDS: that module's facts carry the
//! day's coverage figure, so the framework folds coverage first
//! ([`crate::module::DerivationModule::NEEDS`]).

use crate::completeness::{
    clip, per_day_coverage, union_length, AgentBoundaries, Coverage, DailyCoverage, HeartbeatSpan,
    UncertaintySpan,
};
use crate::module::{Contribution, DerivationModule};
use crate::{Window, DAY_MS};
use std::collections::{BTreeMap, BTreeSet};

/// The window's context one source's fold reads beside its own spans.
#[derive(Debug, Clone, Copy)]
pub struct CoverageCtx<'a> {
    /// The skew the producer clocks compare under; it widens every uncertain stretch.
    pub delta_skew_ms: f64,
    /// The fleet's agent start and stop, which attest the stretches before the first and
    /// after the last heartbeat.
    pub boundaries: &'a AgentBoundaries,
}

/// One window's coverage cells: per source over the window, per (day, source) over each
/// UTC day the window touches. The day cells are the keyed rows (`coverage_daily`); the
/// window-level figures are the window's own and never outlive it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CoverageCells {
    /// Each source's covered milliseconds over the whole window.
    pub per_source_covered_ms: BTreeMap<String, f64>,
    /// Each source's uncertainty spans, in the order the fold states them.
    pub uncertain: BTreeMap<String, Vec<UncertaintySpan>>,
    /// Each (day, source)'s covered milliseconds and the day's intersection with the window.
    pub per_day: BTreeMap<(i64, String), DailyCoverage>,
    touched: BTreeSet<(i64, String)>,
}
impl crate::module::Mergeable for CoverageCells {
    /// Two windows' cells side by side. A source folds once per window, so the keys are
    /// disjoint; on a repeat the later fold's figure stands (a re-derivation replaces).
    fn merge(&mut self, other: Self) {
        self.per_source_covered_ms
            .extend(other.per_source_covered_ms);
        self.uncertain.extend(other.uncertain);
        for (key, row) in other.per_day {
            self.touched.insert(key.clone());
            self.per_day.insert(key, row);
        }
    }
}

impl crate::module::Tracked for CoverageCells {
    /// Each touched (day, source), with its row or a tombstone.
    type Changes = Vec<((i64, String), Option<DailyCoverage>)>;

    fn len(&self) -> usize {
        self.per_day.len()
    }

    fn drain_changes(&mut self) -> Self::Changes {
        std::mem::take(&mut self.touched)
            .into_iter()
            .map(|key| {
                let now = self.per_day.get(&key).cloned();
                (key, now)
            })
            .collect()
    }

    fn discard_changes(&mut self) {
        self.touched.clear();
    }
}

impl crate::module::DayPartitioned for CoverageCells {
    fn clear_day(&mut self, day: i64) {
        let gone: Vec<_> = self
            .per_day
            .keys()
            .filter(|k| k.0 == day)
            .cloned()
            .collect();
        for key in gone {
            self.per_day.remove(&key);
            self.touched.insert(key);
        }
    }

    fn retire_before(&mut self, before: i64) {
        self.per_day.retain(|k, _| k.0 >= before);
        self.touched.retain(|k| k.0 >= before);
    }

    fn days(&self) -> BTreeSet<i64> {
        self.per_day.keys().map(|k| k.0).collect()
    }
}

/// The observed-coverage fact: one source's attested time over the window and its days.
pub struct ObservedCoverage;

impl DerivationModule for ObservedCoverage {
    type Key = String;
    type State = ();
    type Event = HeartbeatSpan;
    type Cells = CoverageCells;
    type Ctx<'a> = CoverageCtx<'a>;
    const NAME: &'static str = "coverage";
    const INITIALIZATION: crate::module::Initialization =
        crate::module::Initialization::ExaminedHistory;

    /// One source, one window: its uncertain stretches, then what is left of the window and
    /// of each day. The arithmetic is the window model's, unchanged: a source with no span is
    /// uncertain for the whole window; before its first heartbeat it is attested only by an
    /// agent start that precedes it; every drop window taints its own bracket widened by the
    /// skew; after its last heartbeat it is attested only by a stop that follows it, unless
    /// counters moved between them.
    fn fold(
        &self,
        source: &String,
        spans: &[HeartbeatSpan],
        (): (),
        window: Window,
        ctx: &CoverageCtx<'_>,
    ) -> Contribution<CoverageCells, ()> {
        let w = &window;
        let mut uncertain: Vec<UncertaintySpan> = Vec::new();
        if let Some((first, last)) = spans.first().zip(spans.last()) {
            if ctx
                .boundaries
                .start_at_ms
                .is_none_or(|s| s > first.start_at_ms)
            {
                let end = first.start_at_ms + ctx.delta_skew_ms;
                if let Some(span) = clip(w.start_ms, end, w) {
                    uncertain.push(span);
                }
            }
            // A gap between heartbeats past the liveness bound attests nothing over its rest.
            for (from, to) in spans
                .iter()
                .filter(|s| !s.is_drop_window())
                .filter_map(|s| s.silence)
            {
                if let Some(span) = clip(from, to, w) {
                    uncertain.push(span);
                }
            }
            for span in spans.iter().filter(|s| s.is_drop_window()) {
                let extent = (
                    span.start_at_ms - ctx.delta_skew_ms,
                    span.end_at_ms + ctx.delta_skew_ms,
                );
                if let Some(span) = clip(extent.0, extent.1, w) {
                    uncertain.push(span);
                }
            }
            let stop_closes = ctx
                .boundaries
                .stop_at_ms
                .is_some_and(|s| s >= last.end_at_ms);
            if !stop_closes || ctx.boundaries.stop_span_dropped {
                let start = last.end_at_ms - ctx.delta_skew_ms;
                if let Some(span) = clip(start, w.end_ms, w) {
                    uncertain.push(span);
                }
            }
        } else if let Some(span) = clip(w.start_ms, w.end_ms, w) {
            uncertain.push(span);
        }

        let mut cells = CoverageCells::default();
        let covered = (w.length_ms() - union_length(uncertain.clone())).max(0.0);
        cells.per_source_covered_ms.insert(source.clone(), covered);

        // The same arithmetic over each UTC day's intersection with the window, by the one
        // helper that always did it — exact, not apportioned: a span straddling midnight is
        // cut at the boundary.
        let pairs: Vec<(String, UncertaintySpan)> =
            uncertain.iter().map(|s| (source.clone(), *s)).collect();
        for row in per_day_coverage(w, std::slice::from_ref(source), &pairs) {
            #[allow(clippy::cast_possible_truncation)] // exact: day_ms is a whole day count
            let day_index = (row.day_ms / DAY_MS) as i64;
            cells.touched.insert((day_index, source.clone()));
            cells.per_day.insert((day_index, source.clone()), row);
        }
        cells.uncertain.insert(source.clone(), uncertain);
        Contribution {
            cells,
            state: (),
            repair_days: Vec::new(),
        }
    }
}

impl Coverage {
    /// The window's figure from its merged cells: the sources' covered time summed in
    /// source order over the window's length times the source count, the spans and the
    /// day rows flattened in that same order (source, then day then source) — the shape and
    /// the arithmetic the model's coverage always had.
    #[must_use]
    pub fn of_window(cells: CoverageCells, window: &Window) -> Self {
        let mut covered_sum = 0.0;
        let mut per_source_covered_ms = Vec::with_capacity(cells.per_source_covered_ms.len());
        for (source, covered) in cells.per_source_covered_ms {
            covered_sum += covered;
            per_source_covered_ms.push((source, covered));
        }
        #[allow(clippy::cast_precision_loss)] // source counts are tiny
        let denom = window.length_ms() * per_source_covered_ms.len() as f64;
        let uncertain = cells
            .uncertain
            .into_iter()
            .flat_map(|(source, spans)| spans.into_iter().map(move |s| (source.clone(), s)))
            .collect();
        Self {
            fraction: if denom > 0.0 {
                covered_sum / denom
            } else {
                0.0
            },
            per_source_covered_ms,
            uncertain,
            per_day: cells.per_day.into_values().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CoverageCells, CoverageCtx, ObservedCoverage};
    use crate::completeness::{AgentBoundaries, Coverage, HeartbeatSpan};
    use crate::module::{run_window, DayPartitioned, DerivationModule, Mergeable, Tracked};
    use crate::{Window, DAY_MS};
    use std::collections::BTreeMap;

    /// A span between two heartbeats the producer's liveness bound does not cut short.
    fn span(source: &str, start_at_ms: f64, end_at_ms: f64, dropped: i64) -> HeartbeatSpan {
        HeartbeatSpan {
            source: source.into(),
            start_at_ms,
            end_at_ms,
            dropped_delta: dropped,
            oversized_delta: 0,
            silence: None,
        }
    }

    #[test]
    #[allow(clippy::zero_sized_map_values)] // a stateless fact's carried map is exactly that
    #[allow(clippy::float_cmp)] // whole milliseconds: every figure here is exact
    fn a_gap_between_heartbeats_past_the_liveness_bound_is_silence() {
        let h = 3_600_000.0;
        let w = Window {
            start_ms: 0.0,
            end_ms: 4.0 * h,
        };
        let boundaries = AgentBoundaries {
            start_at_ms: Some(0.0),
            stop_at_ms: Some(4.0 * h),
            stop_span_dropped: false,
        };
        let ctx = CoverageCtx {
            delta_skew_ms: 1_000.0,
            boundaries: &boundaries,
        };
        let bound = crate::staleness::STALENESS_GRACE_MS;
        let silent = |start: f64, end: f64| HeartbeatSpan {
            silence: crate::completeness::silence_between(start, end, bound),
            ..span("a", start, end, 0)
        };
        let mut grouped: BTreeMap<String, Vec<HeartbeatSpan>> = BTreeMap::new();
        // A beat each quarter hour, then two hours with none, then beats again.
        grouped.insert(
            "a".into(),
            vec![
                silent(0.0, h / 4.0),
                silent(h / 4.0, 2.25 * h),
                silent(2.25 * h, 4.0 * h),
            ],
        );
        let out = run_window(&ObservedCoverage, grouped, &BTreeMap::new(), w, &ctx);
        let a = &out.cells.uncertain["a"];
        // The two-hour gap is attested for the bound after its first beat, then silent until
        // the beat that ends it; the last span is cut short the same way.
        assert_eq!(
            a.iter().map(|s| (s.start_ms, s.end_ms)).collect::<Vec<_>>(),
            vec![(h / 4.0 + bound, 2.25 * h), (2.25 * h + bound, 4.0 * h)]
        );
    }

    #[test]
    #[allow(clippy::zero_sized_map_values)] // a stateless fact's carried map is exactly that
    #[allow(clippy::float_cmp)] // whole milliseconds: every figure here is exact
    fn a_source_folds_its_own_uncertainty_and_the_window_assembles_from_the_cells() {
        // Two days; "a" heartbeats from an hour in with one drop window across midnight and
        // no stop, "b" has evidence but no span at all.
        let w = Window {
            start_ms: 0.0,
            end_ms: 2.0 * DAY_MS,
        };
        let h = 3_600_000.0;
        let skew = 1_000.0;
        let boundaries = AgentBoundaries {
            start_at_ms: Some(h / 2.0),
            stop_at_ms: None,
            stop_span_dropped: false,
        };
        let ctx = CoverageCtx {
            delta_skew_ms: skew,
            boundaries: &boundaries,
        };
        let mut grouped: BTreeMap<String, Vec<HeartbeatSpan>> = BTreeMap::new();
        grouped.insert(
            "a".into(),
            vec![
                span("a", h, DAY_MS - h, 0),
                span("a", DAY_MS - h, DAY_MS + h, 3),
                span("a", DAY_MS + h, 2.0 * DAY_MS - h, 0),
            ],
        );
        grouped.insert("b".into(), Vec::new());
        let out = run_window(&ObservedCoverage, grouped, &BTreeMap::new(), w, &ctx);
        assert_eq!(out.keys, 2);
        assert!(out.states.is_empty(), "coverage carries nothing");
        assert_eq!(out.cells.len(), 4, "two days times two sources");

        let a = &out.cells.uncertain["a"];
        // The start attests the stretch before the first heartbeat; the drop window taints
        // its bracket widened by the skew; the tail is uncertain with no stop.
        assert_eq!(a.len(), 2, "{a:?}");
        assert_eq!(
            (a[0].start_ms, a[0].end_ms),
            (DAY_MS - h - skew, DAY_MS + h + skew)
        );
        assert_eq!(
            (a[1].start_ms, a[1].end_ms),
            (2.0 * DAY_MS - h - skew, 2.0 * DAY_MS)
        );
        let b = &out.cells.uncertain["b"];
        assert_eq!(
            (b[0].start_ms, b[0].end_ms),
            (0.0, 2.0 * DAY_MS),
            "no span: the window"
        );

        let day0_a = &out.cells.per_day[&(0, "a".to_string())];
        assert_eq!(day0_a.covered_ms, DAY_MS - (h + skew));
        assert_eq!(day0_a.window_ms, DAY_MS);
        let day1_a = &out.cells.per_day[&(1, "a".to_string())];
        assert_eq!(day1_a.covered_ms, DAY_MS - (h + skew) - (h + skew));
        assert_eq!(out.cells.per_day[&(1, "b".to_string())].covered_ms, 0.0);

        let cov = Coverage::of_window(out.cells, &w);
        let a_covered = 2.0 * DAY_MS - (2.0 * h + 2.0 * skew) - (h + skew);
        assert_eq!(
            cov.per_source_covered_ms,
            vec![("a".into(), a_covered), ("b".into(), 0.0)]
        );
        assert_eq!(cov.fraction, a_covered / (2.0 * DAY_MS * 2.0));
        assert_eq!(cov.uncertain.len(), 3, "a's two, then b's one");
        assert_eq!(cov.uncertain[2].0, "b");
        assert_eq!(
            cov.per_day
                .iter()
                .map(|d| (d.day_ms, d.source.as_str()))
                .collect::<Vec<_>>(),
            vec![(0.0, "a"), (0.0, "b"), (DAY_MS, "a"), (DAY_MS, "b")],
            "day then source"
        );
        let days: f64 = cov
            .per_day
            .iter()
            .filter(|d| d.source == "a")
            .map(|d| d.covered_ms)
            .sum();
        assert_eq!(days, a_covered, "the days sum back to the window's figure");
    }

    #[test]
    fn the_cells_drain_by_touch_and_clear_by_day() {
        let w = Window {
            start_ms: 0.0,
            end_ms: DAY_MS,
        };
        let boundaries = AgentBoundaries::default();
        let ctx = CoverageCtx {
            delta_skew_ms: 0.0,
            boundaries: &boundaries,
        };
        let mut held = CoverageCells::default();
        held.merge(
            ObservedCoverage
                .fold(&"s".to_string(), &[], (), w, &ctx)
                .cells,
        );
        assert_eq!(held.days(), [0].into_iter().collect());
        let changes = held.drain_changes();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, (0, "s".to_string()));
        assert!(changes[0].1.is_some());
        held.clear_day(0);
        let changes = held.drain_changes();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].1.is_none(), "a tombstone");
        assert!(held.is_empty());
    }
}
