// SPDX-License-Identifier: AGPL-3.0-only

//! Shape 2 — the fleet roster. "The fleet is who spoke."
//!
//! Observed, never declared, and keyed on incarnation rather than instance. The window is a
//! wall-side CLIP rather than a filter, which is visible in the output and documented at the
//! method so it does not read as a bug.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Dao;
use crate::Error;
use analyzer_analysis::Window;

impl Dao<'_> {
    /// Instance × incarnation with each run's staleness verdict — shape 2, the summary and S5's
    /// full roster.
    ///
    /// **Keyed on incarnation, never instance**. One tap run holds one
    /// declaration set for its whole life, so a holder restarted three times is three rows, and
    /// `Departed` on the first while the second is `Live` is a healthy rolling deploy. A roster
    /// keyed by instance renders every deploy as a fleet-wide outage.
    ///
    /// **CLOCK DOMAIN: wall only, and the window is a CLIP rather than a filter**.
    /// `first_seen_ms` and `last_seen_ms`
    /// come from lifecycle records, so they are wall domain, while [`Window`] is engine domain.
    /// Comparing them in a `WHERE` clause is the cross-domain join design forbids, and
    /// `docs/conformance.md` registers Q2 and Q6 as avoiding it the same way: the
    /// labeled wall-side clip, a scan bound with days of slack, never a comparison.
    ///
    /// **Visible consequence, so a caller does not mistake it for a bug:** the roster is not
    /// exactly scoped to `window`. A run whose whole life sits outside it but inside the clip
    /// margin still appears. "Which producers could this window's figures be about" is a
    /// wall-domain question and the two clocks do not convert — a roster that claimed an exact
    /// engine-domain boundary would be asserting a mapping nobody measured.
    ///
    /// **`now_ms` is the posture switch** (`staleness::run_verdicts_from_facts`): the serve
    /// posture passes real now so silence is judged against the wall clock; `None` is the
    /// batch posture, judged against the fleet's own knowledge horizon.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn roster(&self, window: &Window, now_ms: Option<f64>) -> Result<Vec<RunRow>, Error> {
        self.roster_page(window, now_ms, u32::MAX, None)
    }

    /// The roster, one page at a time.
    ///
    /// **Ordered by `(instance, incarnation)` — total, so paging is stable.** The same reasoning
    /// as the holders table: a partial order leaves rows of equal key in whatever order the
    /// engine returned them, and a page boundary inside such a group repeats or drops rows.
    ///
    /// **Paged at all because the roster grows with PRODUCER RUNS, not holders**, and a restarted
    /// instance is a new run — so a fleet that rolls deploys accumulates rows faster than it
    /// accumulates hardware.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn roster_page(
        &self,
        window: &Window,
        now_ms: Option<f64>,
        limit: u32,
        after: Option<(&str, &str)>,
    ) -> Result<Vec<RunRow>, Error> {
        let all = self.roster_all(window, now_ms)?;
        let start = match after {
            None => 0,
            Some((instance, incarnation)) => all
                .iter()
                .position(|r| r.instance == instance && r.incarnation == incarnation)
                .map_or(all.len(), |i| i + 1),
        };
        let end = all.len().min(start.saturating_add(limit as usize));
        Ok(all[all.len().min(start)..end].to_vec())
    }

    #[allow(clippy::too_many_lines)] // the facts, the fan-out and the run-level fallback are one reading of the roster
    fn roster_all(&self, window: &Window, now_ms: Option<f64>) -> Result<Vec<RunRow>, Error> {
        // **Arrivals-backed, ingest-fresh**: both halves come from `input_segments`, which the
        // core publishes from the input log as ingest commits, never a walk of the bucket per
        // request: facts at ingest, verdicts at read. The verdict math itself is `analysis`' shared spec 5.8 core, so this path and
        // the corpus-pinned raw path cannot drift into two readers.
        let (lo, hi) = (
            window.start_ms - analyzer_analysis::completeness::LIFECYCLE_CLIP_MARGIN_MS,
            window.end_ms + analyzer_analysis::completeness::LIFECYCLE_CLIP_MARGIN_MS,
        );

        // Verdict facts aggregate the RUN-LEVEL rows only (`instance = ''`, one per segment):
        // the per-instance fan-out duplicates each segment's heartbeat summary, and summing
        // across the fan-out would inflate the cadence allowance's sample count.
        //
        // The binding sits IMMEDIATELY above its statement, and again above the second
        // statement below — the static audit's lookback rule.
        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let facts_sql = format!(
            "SELECT incarnation, min(first_record_at_ms), max(last_record_at_ms), \
                    max(stop_at_ms), \
                    max(greatest(heartbeat_secs, max_segment_secs)) \
             FROM input_segments FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0 AND instance = '' \
             GROUP BY incarnation \
             HAVING max(last_record_at_ms) >= {lo} AND min(first_record_at_ms) < {hi}"
        );
        let mut facts = Vec::new();
        let mut spans: std::collections::BTreeMap<String, (f64, f64, bool)> =
            std::collections::BTreeMap::new();
        let facts_out = self.engine.run(&facts_sql)?;
        for mut row in Row::each("roster facts", &facts_out) {
            let inc = row.text("incarnation")?;
            let first_seen_ms: f64 = row.parse("first_record_at_ms")?;
            let last_record_ms: f64 = row.parse("last_record_at_ms")?;
            let stop_at_ms = row.nullable("stop_at_ms")?;
            let bound: f64 = row.parse("bound_secs")?;
            row.end()?;
            facts.push(analyzer_analysis::staleness::RunFacts {
                incarnation: inc.to_string(),
                last_record_ms,
                stop_at_ms,
                larger_bound_ms: (bound > 0.0).then_some(bound * 1000.0),
            });
            spans.insert(
                inc.to_string(),
                (first_seen_ms, last_record_ms, stop_at_ms.is_some()),
            );
        }
        let verdicts = analyzer_analysis::staleness::run_verdicts_from_facts(&facts, now_ms);
        // The number each verdict was judged by, stated beside it (`None` = no basis): the
        // console's hover says "silent 42m (threshold 30m)" from facts, never from prose.
        let thresholds: std::collections::BTreeMap<String, Option<f64>> = facts
            .iter()
            .map(|f| {
                (
                    f.incarnation.clone(),
                    analyzer_analysis::staleness::run_threshold_ms(f.larger_bound_ms),
                )
            })
            .collect();

        // Display rows fan out per instance; the model-dimension precedence (topic outranks declaration)
        // is decided HERE from the two carried facts, `argMax` picking each source's latest
        // non-empty value. A run whose segments never named an instance keeps its run-level
        // identity (`''`) rather than vanishing.
        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let rows_sql = format!(
            "SELECT instance, incarnation, min(first_record_at_ms), max(last_record_at_ms), \
                    max(stop_at_ms IS NOT NULL), \
                    argMax(topic_model, (topic_model != '', last_record_at_ms)), \
                    argMax(declared_model, (declared_model != '', last_record_at_ms)), \
                    argMax(engine_version, (engine_version != '', last_record_at_ms)), \
                    argMax(topic_pod, (topic_pod != '', last_record_at_ms)), \
                    argMax(declared_pod, (declared_pod != '', last_record_at_ms)) \
             FROM input_segments FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0 AND instance != '' \
             GROUP BY instance, incarnation \
             HAVING max(last_record_at_ms) >= {lo} AND min(first_record_at_ms) < {hi} \
             ORDER BY instance, incarnation"
        );
        let mut out = Vec::new();
        let mut named: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        let rows_out = self.engine.run(&rows_sql)?;
        for mut row in Row::each("roster", &rows_out) {
            let instance = row.text("instance")?.to_string();
            let inc = row.text("incarnation")?;
            let first_seen_ms = row.parse("first_seen_ms")?;
            let last_seen_ms = row.parse("last_seen_ms")?;
            let stop_observed = row.text("stop_observed")? != "0";
            let topic_model = row.text_or_empty("topic_model")?;
            let declared_model = row.text_or_empty("declared_model")?;
            let engine_version = row.text_or_empty("engine_version")?;
            let topic_pod = row.text_or_empty("topic_pod")?;
            let declared_pod = row.text_or_empty("declared_pod")?;
            row.end()?;
            named.insert(inc.to_string());
            out.push(RunRow {
                instance,
                incarnation: inc.to_string(),
                first_seen_ms,
                last_seen_ms,
                stop_observed,
                // The model pair's precedence: what the engine's own topic said outranks what
                // the deployment declared. The same for the pod.
                model: topic_model.or(declared_model).map(str::to_string),
                engine_version: engine_version.map(str::to_string),
                pod: topic_pod.or(declared_pod).map(str::to_string),
                // A run the roster knows about but the stream could not grade has NO BASIS —
                // never a default to Live, which would render an ungradeable run as healthy.
                staleness: verdicts
                    .get(inc)
                    .copied()
                    .unwrap_or(analyzer_analysis::staleness::RunStaleness::NoBasis),
                threshold_ms: thresholds.get(inc).copied().flatten(),
            });
        }
        for (inc, (first, last, stopped)) in spans {
            if !named.contains(&inc) {
                out.push(RunRow {
                    instance: String::new(),
                    incarnation: inc.clone(),
                    first_seen_ms: first,
                    last_seen_ms: last,
                    stop_observed: stopped,
                    model: None,
                    engine_version: None,
                    pod: None,
                    staleness: verdicts
                        .get(&inc)
                        .copied()
                        .unwrap_or(analyzer_analysis::staleness::RunStaleness::NoBasis),
                    threshold_ms: thresholds.get(&inc).copied().flatten(),
                });
            }
        }
        out.sort_by(|a, b| {
            a.instance
                .cmp(&b.instance)
                .then_with(|| a.incarnation.cmp(&b.incarnation))
        });
        Ok(out)
    }
}

/// One producer RUN and its staleness verdict.
#[derive(Debug, Clone, PartialEq)]
pub struct RunRow {
    /// The slot this run filled — the operator's `instance_id`; `""` where no segment named one.
    pub instance: String,
    /// The tap run. Identity is per run, never per instance — see [`Dao::roster`].
    pub incarnation: String,
    /// Wall domain, like every lifecycle figure. Never compared against an engine window.
    pub first_seen_ms: f64,
    /// Wall domain, like `first_seen_ms`: the newest record instant this run's segments carried.
    pub last_seen_ms: f64,
    /// An announced `agent_stop` was observed for this run.
    pub stop_observed: bool,
    /// **Observed, never declared**. Absent reads as *unobserved* and is
    /// never defaulted.
    pub model: Option<String>,
    /// The serving engine's version, observed on the run's segments. Absent reads as unobserved.
    pub engine_version: Option<String>,
    /// **The slot's current occupant** — the chart-relayed pod name, observed or declared.
    /// `instance_id` names a SLOT the operator chose; this names the
    /// process filling it right now, and the two have different lifetimes. Absent reads as
    /// unobserved: a deployment that relays no pod identity has said nothing, which is a
    /// different claim from a pod with no name.
    pub pod: Option<String>,
    /// The verdict at read time, judged against `threshold_ms`; `NoBasis` with no declaration.
    pub staleness: analyzer_analysis::staleness::RunStaleness,
    /// The silence threshold this run was judged by — the larger declared bound widened by
    /// the platform grace — or `None` where no declaration was observed ([`analyzer_analysis::staleness::RunStaleness::NoBasis`]).
    pub threshold_ms: Option<f64>,
}
