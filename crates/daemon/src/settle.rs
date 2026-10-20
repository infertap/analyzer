// SPDX-License-Identifier: AGPL-3.0-only

//! What follows a tick, and what keeps the log: the degradation ledger over the days the tick
//! settled, the fleet figures re-derived for the sampler, the served input tables, the input
//! log kept to its retention, and the working state closed at a stop.

use super::{Daemon, TickSource};

impl Daemon {
    /// Close the working state cleanly at a stop, so a restart on this box resumes where it
    /// stood instead of replaying the days past its newest checkpoint
    /// (`docs/lake.md`). A failed unit is not closed, and
    /// the restart replays.
    pub(super) fn close_working_state(&self) {
        let lake = self.spine.lake();
        let Ok(working) = lake.working() else {
            return;
        };
        match working.close() {
            Ok(true) => {}
            Ok(false) => log::warn!(
                "working state not closed cleanly: the restart replays the days past its checkpoint"
            ),
            Err(e) => log::warn!(
                "working state close failed, the restart replays the days past its checkpoint: {e}"
            ),
        }
    }

    /// What arrived, as readers see it: the served input tables through everything the input
    /// log committed, whoever committed it. A failure leaves them behind the log until the
    /// next ingest cadence.
    pub(super) fn publish_input_tables(&self) {
        match self
            .executor()
            .and_then(|executor| executor.publish_inputs())
        {
            Ok(0) => {}
            Ok(n) => log::info!("input tables: {n} batch(es) published"),
            Err(e) => log::warn!("input publication deferred: {e}"),
        }
    }

    /// Keep the input log to the ledger's retention and compact the days past the raw horizon,
    /// at the maintenance cadence.
    pub(super) fn maintain_input_log(&self) {
        match self.executor().and_then(|executor| {
            executor.maintain_input_log(
                self.config.retention.ledger_retention_days,
                self.effective_horizon_days,
            )
        }) {
            Ok(done) if done.retired.is_empty() && done.compacted.is_empty() => {}
            Ok(done) => log::info!(
                "input log: retired {:?}, compacted {:?}",
                done.retired,
                done.compacted
            ),
            Err(error) => log::info!("input log maintenance deferred: {error}"),
        }
        // The published state outlives the longest serving horizon, the rollups', by whole
        // years of days: a leap day more each year keeps it at or past the horizon.
        let years = self.config.retention.rollup_retention_years;
        match self
            .executor()
            .and_then(|executor| executor.retire_published(years * 366))
        {
            Ok(days) if days.is_empty() => {}
            Ok(days) => log::info!("published state: retired {days:?}"),
            Err(error) => log::info!("published state retirement deferred: {error}"),
        }
    }

    /// The ledger's uncertainty model, from banked arrivals or nothing: there is no flat
    /// fallback, because the walk it would take is an O(window events) materialisation in
    /// the daemon's own heap. A model that cannot be built from banked facts holds
    /// yesterday's ledger, which is a claim the product can read.
    fn ledger_model(
        &self,
        window: &analyzer_analysis::Window,
    ) -> Option<analyzer_analysis::UncertaintyModel> {
        match analyzer_pipeline::arrivals::window_model_from_arrivals(
            &self.spine.lake(),
            window,
            &self.config.params,
        ) {
            Ok(Some(model)) => Some(model),
            Ok(None) => {
                log::warn!(
                    "degradation ledger refused: the window holds no banked arrivals, and the \
                     model is not derived from a flat walk — yesterday's ledger stands"
                );
                None
            }
            Err(e) => {
                log::warn!("ledger model derivation failed, will retry: {e}");
                None
            }
        }
    }

    /// The segment opens the ledger reads, from the store's banked arrivals; the source
    /// walk remains the loud no-arrivals fallback.
    fn ledger_opens(
        &self,
        source: &TickSource,
    ) -> Option<Vec<analyzer_analysis::events::LifecycleEvent>> {
        match analyzer_pipeline::arrivals::opens_from_arrivals(&self.spine.lake()) {
            Ok(Some(opens)) => Some(opens),
            Ok(None) => {
                log::warn!("no arrivals hold segment opens — walking the source for them");
                match analyzer_analysis::events::EventSource::segment_opens_at_retention_horizon(
                    source,
                    &self.config.fleet_id,
                ) {
                    Ok(opens) => Some(opens),
                    Err(e) => {
                        log::warn!("ledger opens derivation failed, will retry: {e}");
                        None
                    }
                }
            }
            Err(e) => {
                log::warn!("ledger opens derivation failed, will retry: {e}");
                None
            }
        }
    }

    /// The degradation ledger over the days a tick settled. A tick that settled none holds the
    /// previous ledger: yesterday's incidents do not stop having happened.
    pub(super) fn bank_ledger(&mut self, source: &TickSource, days: &[i64]) {
        let (Some(&first), Some(&last)) = (days.iter().min(), days.iter().max()) else {
            return;
        };
        let window = match analyzer_analysis::Window::whole_days(first, last) {
            Ok(window) => window,
            Err(error) => {
                self.latest_ledger = None;
                log::warn!("ledger output range is invalid: {error}");
                return;
            }
        };
        let Some(model) = self.ledger_model(&window) else {
            return;
        };
        let Some(opens) = self.ledger_opens(source) else {
            return;
        };
        match analyzer_pipeline::degradation_io::ledger(
            source,
            &self.config.fleet_id,
            &window,
            &model,
            &opens,
        ) {
            Ok(ledger) => self.latest_ledger = Some(ledger),
            Err(e) => log::warn!("ledger derivation failed, will retry: {e}"),
        }
    }

    /// What follows an applied tick: the ledger over the days it settled, and the fleet figures
    /// through the same reads the console serves, so the glass and the alarms cannot drift on
    /// what the numbers are.
    pub(super) fn settle_tick(
        &mut self,
        source: &TickSource,
        tick: &analyzer_pipeline::executor::Advance,
    ) {
        self.bank_ledger(source, &tick.settled_days);
        match self.derive_fleet_figures() {
            Ok(figures) => self.latest_figures = Some(figures),
            Err(e) => log::warn!("fleet figures not refreshed, will retry next tick: {e}"),
        }
    }
}
