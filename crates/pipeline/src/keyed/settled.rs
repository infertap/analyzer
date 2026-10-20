// SPDX-License-Identifier: AGPL-3.0-only

//! The settled-day hook (`docs/pipeline.md`): the day-grain outputs,
//! stated once a day is settled and again when a correction restates it.
//!
//! A tick captures each day it settled or restated with the day's coverage cells, before the
//! held cells retire it, and the emission batch carries them. After the batch is published,
//! [`Hook::state`] states the generation seams and runs every registered public adapter over
//! each day, its coverage dependency built from the summed coverage cells, so it agrees with the
//! published `coverage_daily`. A replay of an unapplied batch states the same days again, whole.

use crate::Error;
use analyzer_analysis::coverage_sums::Observed;
use analyzer_analysis::emitted::{Meaning, MARKERS};
use analyzer_analysis::module::Row;
use analyzer_analysis::{Params, DAY_MS};

/// A day the settled-day hook states: its index and its coverage cells.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SettledDay {
    /// The UTC day index.
    pub day: i64,
    /// Each source's coverage of the day, in source order.
    pub coverage: Vec<SourceDay>,
}

/// One source's coverage of a day.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SourceDay {
    /// The source.
    pub source: String,
    /// Milliseconds observed complete.
    pub covered_ms: f64,
    /// Milliseconds stated over.
    pub window_ms: f64,
}

impl SettledDay {
    /// Day `day` with the cells `cells` hold for it.
    #[must_use]
    pub fn of(day: i64, cells: &analyzer_analysis::coverage_sums::CoverageSums) -> Self {
        Self {
            day,
            coverage: cells
                .day(day)
                .into_iter()
                .map(|(source, o)| SourceDay {
                    source,
                    covered_ms: o.covered_ms,
                    window_ms: o.window_ms,
                })
                .collect(),
        }
    }

    /// The day's coverage, as a module's dependency reads it.
    #[must_use]
    pub fn coverage(&self) -> analyzer_analysis::completeness::Coverage {
        let cells: Vec<(String, Observed)> = self
            .coverage
            .iter()
            .map(|s| {
                (
                    s.source.clone(),
                    Observed {
                        covered_ms: s.covered_ms,
                        window_ms: s.window_ms,
                    },
                )
            })
            .collect();
        analyzer_analysis::coverage_sums::day_coverage(self.day, &cells)
    }
}

/// What the hook states against.
pub struct Hook<'a> {
    /// The lineage's lake: the input log, and the state log the seams stated before are read
    /// from.
    pub lake: &'a analyzer_lake::Lake,
    /// The committed input a module reconstructs from.
    pub source: &'a dyn analyzer_analysis::events::EventSource,
    /// The fleet.
    pub fleet: &'a str,
    /// The parameters the driver classified under.
    pub params: &'a Params,
    /// The modules registered, in their order; the public ones run.
    pub modules: &'a crate::modules::ModuleSet,
    /// The writer: the lake's state log, then serving.
    pub publish: &'a dyn crate::publish::Publish,
}

impl Hook<'_> {
    /// State the generation seams of `generations`, then every public module over each of
    /// `days`, under `publication`. Nothing is stated when `days` is empty: the seams move only
    /// when a day settles.
    ///
    /// # Errors
    /// The state log or the writer failed, a module's history is gone from the input log, or a
    /// module failed.
    pub fn state(
        &self,
        days: &[SettledDay],
        generations: &[super::store::Generation],
        publication: u64,
    ) -> Result<(), Error> {
        if days.is_empty() {
            return Ok(());
        }
        self.seams(generations, publication)?;
        let at = crate::adapters::SettledDay {
            lake: self.lake,
            source: self.source,
            fleet: self.fleet,
            params: self.params,
            publish: self.publish,
            publication,
        };
        for day in days {
            crate::adapters::settle_day(self.modules.enabled(), &at, day.day, day.coverage())?;
        }
        Ok(())
    }

    /// The seams of `generations` under the generations meaning, each seam stated before and no
    /// longer derived tombstoned.
    fn seams(
        &self,
        generations: &[super::store::Generation],
        publication: u64,
    ) -> Result<(), Error> {
        #[allow(clippy::cast_precision_loss)] // day indexes are small integers
        let ranges: Vec<analyzer_analysis::generations::Generation> = generations
            .iter()
            .map(|g| analyzer_analysis::generations::Generation {
                semantics_version: g.semantics_version.clone(),
                discriminator: g.discriminator.clone(),
                first_ms: g.first_day as f64 * DAY_MS,
                last_ms: g.last_day as f64 * DAY_MS,
            })
            .collect();
        let mut rows = analyzer_analysis::generations::seam_rows(&ranges);
        let meaning = Meaning::generations();
        let before = crate::state::LakePublish::of(self.lake)?.read(&MARKERS, &meaning, None)?;
        let gone: Vec<Row> = before
            .into_iter()
            .filter(|old| !old.gone && !rows.iter().any(|r| r.key == old.key))
            .map(|old| Row { gone: true, ..old })
            .collect();
        rows.extend(gone);
        self.publish.apply(
            &crate::publish::Head {
                fleet_id: self.fleet,
                meaning: &meaning,
            },
            &[MARKERS],
            publication,
            &rows,
        )?;
        Ok(())
    }
}
