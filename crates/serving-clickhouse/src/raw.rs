// SPDX-License-Identifier: AGPL-3.0-only

//! Raw-grain reads — computed live rather than banked, and owned here anyway.
//!
//! The read path owns every read the product performs, including the ones that cannot be
//! banked: these read raw or standing state, so they answer only over the retained horizon, and
//! they state that bound.

use crate::row::Row;
use crate::Dao;
use crate::Error;
use analyzer_analysis::Window;

impl Dao<'_> {
    // Self-health (`selfhealth::collect`) is not here: this seam governs FLEET data, and the meter
    // observing itself is a different subject with a different trust story.

    /// Every registered public module's standing, from the executor's own record: whether it
    /// is enabled, and through which settled day its output is stated.
    ///
    /// # Errors
    /// The working state failed, or a namespace this database is not bound to.
    pub fn module_availability(
        &self,
    ) -> Result<Vec<analyzer_pipeline::executor::ModuleAvailability>, Error> {
        Ok(self.executor()?.availability()?)
    }

    /// Fleet health over `window` — departures, idle-or-stalled producers, agent and instance
    /// counts. Live over raw (Q6), graded against the OBSERVATION HORIZON rather than real now.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn fleet_health(
        &self,
        window: &Window,
    ) -> Result<analyzer_analysis::staleness::FleetHealth, Error> {
        analyzer_pipeline::read::fleet_health(&self.source(), &self.fleet_id, window)
            .map_err(Error::from)
    }

    /// The span of cache facts this fleet's event-grain store actually holds — the span of
    /// the ingest ledger — or `None` when it holds none.
    ///
    /// **Measured, never declared.** The bound is the oldest committed batch's first record, not
    /// `ledger_retention_days` — the configured horizon is what retention is SUPPOSED to be, and
    /// a consumer reasoning from it would state a scope the data does not have. It is what the
    /// read API's freshness stamp carries as `raw_horizon.from_ms`.
    ///
    /// **Padded one millisecond each side**, because the window predicate is half-open and an
    /// unpadded `max(at_ms)` would exclude the last fact from its own default window.
    ///
    /// **Not window-scoped, and not scoped to the meaning pair either.** It asks what the RECORDS
    /// hold, which is a question about retention rather than about a semantics generation — the
    /// same reason [`Dao::agent_versions`] is not window-scoped.
    ///
    /// # Errors
    /// Engine failure, or a row that does not parse.
    pub fn raw_span(&self) -> Result<Option<Window>, Error> {
        let (fleet, input) = (
            crate::quote_literal(&self.fleet_id),
            crate::quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let out = self.engine.run(&format!(
            "SELECT minOrNull(first_at_ms), maxOrNull(last_at_ms) FROM input_batches FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0"
        ))?;
        let mut row = Row::single("raw span", &out);
        let min: Option<f64> = row.nullable("first_at_ms")?;
        let max: Option<f64> = row.nullable("last_at_ms")?;
        row.end()?;
        Ok(min.zip(max).map(|(min, max)| Window {
            start_ms: min - 1.0,
            end_ms: max + 1.0,
        }))
    }

    /// The derivation's edges as the last applied tick stated them (`derivation_status`), so
    /// every process reads the same instants: what the stamp names. Each is `None` before a
    /// tick stated it.
    ///
    /// # Errors
    /// Engine failure.
    pub fn edges(&self) -> Result<Edges, Error> {
        #[allow(clippy::cast_possible_truncation)] // a day index stated as a float
        Ok(Edges {
            classified_to_ms: self.status_fact("classified_to_ms")?,
            emitted_to_ms: self.status_fact("emitted_to_ms")?,
            covered_to_ms: self.status_fact("covered_to_ms")?,
            audited_to_ms: self.status_fact("audited_to_ms")?,
            settled_through_day: self
                .status_fact("settled_through_day")?
                .map(|day| day.round() as i64),
            last_tick_at_ms: self.status_fact("last_tick_at_ms")?,
        })
    }

    /// The durable revision, absent while a tick's intent is unapplied or serving is unsettled.
    /// Check it on both sides of a read that claims consistent output.
    ///
    /// # Errors
    /// Engine failure or malformed durable standing.
    pub fn revision(&self) -> Result<Option<u64>, Error> {
        Ok(analyzer_pipeline::execution::revision(
            &*self.serving,
            &self.spine,
        )?)
    }

    /// Producer agent versions OBSERVED on this fleet's lifecycle stream, deduplicated —
    /// folded from the seam's declaration slice at the retention horizon.
    ///
    /// Deliberately not window-scoped, like every lifecycle-identity fetch (Q11): which agent
    /// versions a fleet has run is a fact about the fleet, not about a window of it.
    ///
    /// # Errors
    /// Source failure, or malformed input.
    pub fn agent_versions(&self) -> Result<Vec<String>, Error> {
        // Arrivals-backed: this read walked the raw bucket's declaration
        // slice per request — an unbounded lookup serving a console tile. Every published
        // segment row carries the version; distinct-and-sorted here is the same
        // fact at ingest freshness. The raw path (`read::agent_versions`) remains the
        // corpus-pinned reference reader.
        let (fleet, input) = (
            crate::quote_literal(&self.fleet_id),
            crate::quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let sql = format!(
            "SELECT DISTINCT agent_version FROM input_segments FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0 AND agent_version != '' ORDER BY agent_version"
        );
        Ok(self
            .engine
            .run(&sql)?
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect())
    }

    /// The degradation ledger over `window` — every entry whose scope and extent touch it.
    ///
    /// **Computed from raw, which is why an empty return is not a clean bill of health.** Past
    /// the raw horizon there is nothing to compute from, and `report::Ledgered` is what keeps
    /// those two readings apart; this method reports what the records say and makes no claim
    /// about what it could not see.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn degradation_ledger(
        &self,
        window: &Window,
    ) -> Result<analyzer_analysis::degradation::Ledger, Error> {
        let source = self.source();
        // Arrivals-first, like the daemon's cycle; the source-walk model remains the
        // loud fallback for a span with no arrivals (the ninth family member's fix).
        let segments = self.input_segments()?;
        let banked = analyzer_pipeline::arrivals::window_model_from_segments(
            &self.spine.lake(),
            segments.clone(),
            window,
            &self.params,
        )?;
        let model = if let Some(model) = banked {
            model
        } else {
            log::warn!(
                "ledger window has no arrivals — lifecycle \
                 folding from the source"
            );
            analyzer_pipeline::read::uncertainty_model(
                &source,
                &self.fleet_id,
                window,
                &self.params,
            )?
        };
        let banked_opens = analyzer_pipeline::arrivals::opens_of(&segments);
        let opens = if let Some(opens) = banked_opens {
            opens
        } else {
            log::warn!(
                "no arrivals hold segment opens — walking the \
                 source for them"
            );
            analyzer_analysis::events::EventSource::segment_opens_at_retention_horizon(
                &source,
                &self.fleet_id,
            )?
        };
        analyzer_pipeline::degradation_io::ledger(&source, &self.fleet_id, window, &model, &opens)
            .map_err(Error::from)
    }
}

/// The derivation's edges, as the last applied tick stated them.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Edges {
    /// Repeat compute's classified edge: every store before it is classified.
    pub classified_to_ms: Option<f64>,
    /// Placement's emitted edge: the placement cells are stated before it.
    pub emitted_to_ms: Option<f64>,
    /// Coverage's stated edge: coverage is settled before it.
    pub covered_to_ms: Option<f64>,
    /// The sampled audit's edge: every block it re-derived before it agreed with what was
    /// stated.
    pub audited_to_ms: Option<f64>,
    /// The last UTC day every edge has passed by the settle delay.
    pub settled_through_day: Option<i64>,
    /// When the last tick was applied, wall-clock epoch milliseconds.
    pub last_tick_at_ms: Option<f64>,
}
