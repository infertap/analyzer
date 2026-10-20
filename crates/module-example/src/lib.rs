// SPDX-License-Identifier: AGPL-3.0-only

//! A statically registered coverage-gap derivation using only public dependencies.
//! Unobserved source time is reconstructed from heartbeat coverage and published as
//! per-source exposure and a daily total. The module declares its two tables and renders
//! rows; the framework creates, writes, versions and retires them, and the executor owns
//! all attempt bookkeeping. There is no SQL here and no engine.

use analyzer_analysis::module::{
    Column, Contribution, DayOf, DerivationModule, Initialization, Kind, Mergeable, Row, Table,
    Value,
};
use analyzer_analysis::Window;
use analyzer_pipeline::{
    adapter::{Adapter, Cadence, Context, Dependencies},
    Error,
};

/// A source's unobserved duration in the requested day.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceGap {
    /// Source identity from completed coverage.
    pub source: String,
    /// Milliseconds without observation, not a claim of source downtime.
    pub unobserved_ms: f64,
}

/// Independent source contributions concatenate; a complete scope replaces prior output.
#[derive(Debug, Default, PartialEq)]
pub struct GapExposure(pub Vec<SourceGap>);
impl Mergeable for GapExposure {
    fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

/// Derive unobserved duration from per-source heartbeat coverage.
pub struct CoverageGaps;
impl DerivationModule for CoverageGaps {
    type Key = String;
    type State = ();
    type Event = f64;
    type Cells = GapExposure;
    type Ctx<'a> = ();
    const NAME: &'static str = "coverage_gaps";
    const INITIALIZATION: Initialization = Initialization::OutputWindow;
    const NEEDS: &'static [&'static str] = &["coverage"];
    fn fold(
        &self,
        source: &String,
        gaps: &[f64],
        (): (),
        _: Window,
        (): &(),
    ) -> Contribution<GapExposure, ()> {
        Contribution {
            cells: GapExposure(vec![SourceGap {
                source: source.clone(),
                unobserved_ms: gaps.iter().sum(),
            }]),
            state: (),
            repair_days: Vec::new(),
        }
    }
}

/// Per-source unobserved duration, one row per source and day.
pub const GAP_SOURCES: Table = Table {
    name: "example_gap_sources",
    key: &[
        Column::new("day", Kind::I64),
        Column::new("source", Kind::Str),
    ],
    measures: &[Column::new("unobserved_ms", Kind::F64)],
    day: DayOf::Day("day"),
};

/// The day's total unobserved duration, one row per day.
pub const GAP_TOTAL: Table = Table {
    name: "example_gap_total",
    key: &[Column::new("day", Kind::I64)],
    measures: &[Column::new("unobserved_ms", Kind::F64)],
    day: DayOf::Day("day"),
};

/// Day adapter; two output tables share one durable attempt.
pub struct GapAdapter;
impl Adapter for GapAdapter {
    type Definition = CoverageGaps;
    type Output = GapExposure;
    const CADENCE: Cadence = Cadence::Day;
    const TABLES: &'static [Table] = &[GAP_SOURCES, GAP_TOTAL];
    fn reconstruct(ctx: &Context<'_>, deps: &Dependencies<'_>) -> Result<GapExposure, Error> {
        let coverage = deps.coverage()?.coverage();
        let mut output = GapExposure::default();
        for day in &coverage.per_day {
            output.merge(
                CoverageGaps
                    .fold(
                        &day.source,
                        &[day.window_ms - day.covered_ms],
                        (),
                        ctx.output,
                        &(),
                    )
                    .cells,
            );
        }
        Ok(output)
    }
    fn rows(ctx: &Context<'_>, output: &GapExposure) -> Vec<Row> {
        let day = analyzer_analysis::emitted::day_index(ctx.output.start_ms);
        let mut rows: Vec<Row> = output
            .0
            .iter()
            .map(|gap| Row {
                table: GAP_SOURCES.name,
                key: vec![Value::I64(day), Value::Str(gap.source.clone())],
                measures: vec![Value::F64(gap.unobserved_ms)],
                gone: false,
            })
            .collect();
        rows.push(Row {
            table: GAP_TOTAL.name,
            key: vec![Value::I64(day)],
            measures: vec![Value::F64(output.0.iter().map(|g| g.unobserved_ms).sum())],
            gone: false,
        });
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn independent_sources_compose_and_unobserved_time_is_not_downtime() {
        let w = Window::whole_days(1, 1).unwrap();
        let mut cells = CoverageGaps
            .fold(&"a".into(), &[100.0, 300.0], (), w, &())
            .cells;
        cells.merge(CoverageGaps.fold(&"b".into(), &[200.0], (), w, &()).cells);
        assert_eq!(
            cells.0,
            vec![
                SourceGap {
                    source: "a".into(),
                    unobserved_ms: 400.0
                },
                SourceGap {
                    source: "b".into(),
                    unobserved_ms: 200.0
                }
            ]
        );
    }
}
