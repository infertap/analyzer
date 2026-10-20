// SPDX-License-Identifier: AGPL-3.0-only

//! The sampler's side of the loop: what the loop publishes and what a sample reads.
//!
//! The inputs the loop writes and the emission thread reads ([`SamplerInputs`]), the sample
//! taken through the SAMPLER's engine and never the loop's, the run states and fleet figures
//! derived for the export surface, and the health artifacts written from a sample.

use super::run::Cadence;
use super::{lock, wall_now_ms, Daemon, DaemonConfig, LoopSnapshot};
use crate::counters::{Cadences, Counters};
use crate::pressure::Pressure;
use crate::Error;
use analyzer_analysis::{Window, DAY_MS};
use analyzer_serving_clickhouse::engine::Engine;
use analyzer_serving_clickhouse::selfhealth::{self, ServerStatus};
use std::sync::RwLock;

/// How far back the run-state gauges look: a silent run matters as an incident for a week;
/// past that it is inventory, and the roster page is where inventory lives.
const TAP_STATE_WINDOW_DAYS: f64 = 7.0;

/// The loop's testimony to the sampler: one write per cycle, one read per sample, no
/// partial states in between.
///
/// What rides here is only what legitimately changes at cycle boundaries. The loop's own
/// counters do not: they are atomics, and the sampler reads them live at sample time. A
/// copy of a live counter here would let the sampler republish a fresh timestamp over
/// figures taken before the current cycle's work, so a tick that ran for minutes would
/// show `ticks_total` unmoved under an ever-fresh stamp. No field of this struct may be a
/// copy of a live counter.
#[derive(Debug, Clone, Default)]
pub(super) struct SamplerInputs {
    /// Whether the engine child is alive, and for how long.
    pub(super) server: ServerStatus,
    /// The horizon currently held: loop state the loop owns and the counters do
    /// not. It moves only in [`Daemon::set_effective_horizon_days`], at cycle boundaries,
    /// so a copy here is exact by construction.
    pub(super) effective_retention_days: f64,
    /// Whether the disk ladder is refusing intake: loop state too, at the same boundary.
    pub(super) ingest_refusing: bool,
    /// The fleet's stamped figures, recomputed after each applied tick through the same
    /// DAO reads the console serves. `None` until a tick is applied; a failed refresh keeps
    /// the last figures, because they are tick-settled facts about classified days: they cannot
    /// go stale-wrong, only stale-old, and the frontier timestamp says how old.
    pub(super) figures: Option<selfhealth::FleetFigures>,
    /// Run-state counts, recomputed at each ingest cycle through the roster read; a failed
    /// refresh keeps the last counts for the same reason as the figures.
    pub(super) runs: Option<selfhealth::RunStates>,
}

/// One sample: the loop's pressure sample and the sampler's scrape sample are the same
/// computation. The counters are read live here, never from a published copy;
/// `other_queries` is the statement count of every engine but the one sampling through,
/// so the exported total covers them all. Only the cycle-boundary state comes from
/// `shared`, because that is all it carries.
pub(super) fn sample_with(
    engine: &Engine,
    lake: &analyzer_pipeline::Lake,
    config: &DaemonConfig,
    shared: &RwLock<SamplerInputs>,
    counters: &Counters,
    other_queries: u64,
) -> selfhealth::SelfHealth {
    let inputs = shared
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let mut status = counters.snapshot(
        engine.queries_executed() + other_queries,
        Cadences {
            tick: config.ingest_interval,
            ingest: config.ingest_interval,
        },
    );
    status.effective_retention_days = inputs.effective_retention_days;
    status.ingest_refusing = inputs.ingest_refusing;
    let paths = selfhealth::Paths {
        data_dir: &config.data_dir,
        out_dir: &config.out_dir,
        ledger_retention_days: config.retention.ledger_retention_days,
        analyzer_id: config.analyzer_id.as_deref(),
        server: Some(inputs.server),
        loop_status: Some(status),
        fleet: inputs.figures.as_ref(),
        runs: inputs.runs,
    };
    match selfhealth::collect(engine, Some(lake), &config.fleet_id, &paths) {
        Ok(vitals) => vitals,
        Err(e) => selfhealth::unreachable(&config.fleet_id, &paths, &e.to_string()),
    }
}

impl Daemon {
    /// Producer runs by staleness verdict, for the export surface: the same roster read the
    /// console's data-quality section serves, so the chips and the alarms cannot drift.
    /// Silence is judged against real now, since the alarm exists for the fleet that
    /// stopped writing.
    ///
    /// # Errors
    /// Engine failure.
    pub(super) fn derive_run_states(&self) -> Result<selfhealth::RunStates, Error> {
        let now = wall_now_ms();
        let window = Window {
            start_ms: now - TAP_STATE_WINDOW_DAYS * DAY_MS,
            end_ms: now,
        };
        let mut runs = selfhealth::RunStates::default();
        // One count per run, not per fan-out row: the roster fans a run out across the
        // instances it covered, and counting rows would count one silent tap once per
        // engine it watched.
        let mut seen = std::collections::BTreeSet::new();
        for row in self.dao().roster(&window, Some(now))? {
            if !seen.insert(row.incarnation.clone()) {
                continue;
            }
            match row.staleness {
                analyzer_analysis::staleness::RunStaleness::Live => runs.reporting += 1,
                analyzer_analysis::staleness::RunStaleness::Departed => runs.departed += 1,
                analyzer_analysis::staleness::RunStaleness::Unexplained => {
                    runs.silent_unexplained += 1;
                }
                analyzer_analysis::staleness::RunStaleness::NoBasis => runs.ungradeable += 1,
            }
        }
        Ok(runs)
    }

    /// The fleet's stamped figures for the export surface, derived through the DAO: the
    /// same reads `/v1/figures`, `/v1/residual` and `/v1/delivery` serve. The stamped window
    /// is the DAO's own, anchored at the frontier day, so the API and this sampler share
    /// one rule.
    ///
    /// # Errors
    /// Engine failure, or no classified day to anchor the stamped window to.
    pub(super) fn derive_fleet_figures(&self) -> Result<selfhealth::FleetFigures, Error> {
        let dao = self.dao();
        let frontier = dao
            .edges()?
            .classified_to_ms
            .map(|ms| analyzer_analysis::emitted::day_index(ms - 1.0))
            .ok_or(Error::NoClassifiedDay)?;
        let window = analyzer_serving_clickhouse::stamped_window(frontier)?;

        // The hero's sums, derived the way the console derives them: stored over every
        // class, recompute over `Class::RECOMPUTE` only, coverage token-weighted over
        // exactly the classes summed. The fleet's figures by definition, never scoped.
        let rows = dao.waste_by(
            &window,
            analyzer_serving_clickhouse::Grain::Day,
            &[analyzer_serving_clickhouse::Dim::Class],
            &analyzer_serving_clickhouse::Scope::default(),
        )?;
        let (mut recompute, mut stored, mut weighted) = (0.0_f64, 0.0_f64, 0.0_f64);
        for row in &rows {
            stored += row.tokens.value;
            let is_recompute = row
                .dims
                .iter()
                .find(|(d, _)| *d == analyzer_serving_clickhouse::Dim::Class)
                .is_some_and(|(_, v)| {
                    analyzer_analysis::classify::Class::RECOMPUTE
                        .iter()
                        .any(|c| c.as_str() == v)
                });
            if is_recompute {
                recompute += row.tokens.value;
                weighted += row.tokens.coverage * row.tokens.value;
            }
        }

        let delivery = dao.delivery()?.totals;

        #[allow(clippy::cast_precision_loss)] // token masses and segment counts sit far below 2^53
        Ok(selfhealth::FleetFigures {
            semantics_version: analyzer_analysis::SEMANTICS_VERSION,
            discriminator: self.discriminator.as_str().to_string(),
            recompute_tokens: recompute,
            stored_tokens: stored,
            coverage_fraction: (recompute > 0.0).then(|| weighted / recompute),
            capture_fraction: delivery.fraction,
            segments_missing_unexplained: delivery.segments_missing_unexplained as f64,
            frontier_end_seconds: window.end_ms / 1000.0,
        })
    }

    /// Hand the serving layer, if any, what the loop currently knows. Called on the loop's
    /// own thread after the cycle, so a snapshot always describes a settled state rather
    /// than one caught mid-tick. Nothing here can block the loop: the observer is a plain
    /// closure the composition root supplied.
    pub(super) fn publish_snapshot(&self, tick: &Cadence) {
        if let Some(observe) = self.observer.as_ref() {
            observe(LoopSnapshot {
                next_tick_at_ms: tick
                    .remaining()
                    .map(|left| wall_now_ms() + left.as_secs_f64() * 1000.0),
                refusing: self.last_pressure == Pressure::Refuse,
                ledger: self.latest_ledger.clone(),
            });
        }
    }

    /// Sample self-health through the SAMPLER's engine, never the loop's.
    ///
    /// A failed sample still produces a document: `selfhealth::unreachable` keeps the
    /// export surface identical with the engine-derived series absent, because a health
    /// file that simply stops changing reads as a healthy one.
    #[must_use]
    pub fn sample(&self) -> selfhealth::SelfHealth {
        self.publish_inputs();
        let engine = lock(&self.sampler_engine);
        sample_with(
            &engine,
            &self.spine.lake(),
            &self.config,
            &self.shared,
            &self.counters,
            self.engine.queries_executed() + self.serving_queries.total(),
        )
    }

    /// Publish the loop's current testimony for the sampler, as one consistent write: the
    /// ladder state it owns, the server's status, and the latest tick-settled products.
    /// The loop's counters are deliberately not here (see [`SamplerInputs`]).
    pub(super) fn publish_inputs(&self) {
        let inputs = SamplerInputs {
            server: self.server_status(),
            effective_retention_days: f64::from(self.effective_horizon_days),
            ingest_refusing: self.last_pressure == Pressure::Refuse,
            figures: self.latest_figures.clone(),
            runs: self.latest_runs,
        };
        if let Ok(mut shared) = self.shared.write() {
            *shared = inputs;
        }
    }

    /// The server's liveness, at the moment asked: a loop held in a long stage must not
    /// serve its startup uptime for hours.
    fn server_status(&self) -> ServerStatus {
        ServerStatus {
            up: true,
            uptime_seconds: Some(self.handle.uptime().as_secs_f64()),
        }
    }

    /// Write the health artifacts from a sample.
    ///
    /// # Errors
    /// If either artifact cannot be written.
    pub fn emit_health(&self, vitals: &selfhealth::SelfHealth) -> Result<(), Error> {
        selfhealth::write_health_artifacts(&self.config.out_dir, vitals)?;
        Ok(())
    }
}
