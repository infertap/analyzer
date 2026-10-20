// SPDX-License-Identifier: AGPL-3.0-only

//! Observation operations and their resource-specific mapping.

use crate::envelope::envelope;
use crate::error::ApiError;
use crate::extract::Params;
use crate::pagination::refused_page;
use crate::pagination::{Outcome, Unavailable};
use crate::requests::stamp_and_window;
use crate::requests::CoverageQuery;
use crate::requests::NoQuery;
use crate::requests::RosterQuery;
use crate::resources::positions::roster_cursor;
use crate::resources::positions::roster_position;
use crate::wire;
use crate::ApiState;
use axum::extract::State;
use axum::Json;

const ROSTER_CANNOT: &str = "a holder that never reported leaves no trace, so nothing here flags \
                             a tap that was never installed";

pub(crate) async fn coverage(
    State(state): State<ApiState>,
    Params(q): Params<CoverageQuery>,
) -> Result<Json<wire::Envelope<wire::CoverageData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let by = q.by();
    let rows = state.read(move |dao| dao.coverage(&window, by)).await?;
    Ok(envelope(
        &stamp,
        wire::CoverageData {
            rows: rows.iter().map(coverage_row).collect(),
        },
    ))
}

pub(crate) fn coverage_row(r: &analyzer_serving_clickhouse::CoverageRow) -> wire::CoverageRow {
    wire::CoverageRow {
        day: r.day.clone(),
        instance: r.instance.clone(),
        covered_ms: r.covered_ms,
        window_ms: r.window_ms,
        fraction: r.fraction,
        instances: r.instances,
        coverage_kind: "instance-time",
        token_wsum: r.token_wsum,
        token_mass: r.token_mass,
        token_weighted_fraction: r.token_weighted_fraction,
    }
}

pub(crate) async fn delivery(
    State(state): State<ApiState>,
    Params(_q): Params<NoQuery>,
) -> Result<Json<wire::Envelope<wire::DeliveryData>>, ApiError> {
    let stamp = crate::envelope::Stamp::read(&state, None, state.declared_ledger_days()).await?;
    // The closure looks redundant and is not: `Dao<'_>` carries the engine's lifetime, so the
    // bare method path fails `read`'s for<'a> bound where the closure generalizes fine.
    #[allow(clippy::redundant_closure_for_method_calls)]
    let report = state.read(|dao| dao.delivery()).await?;

    let t = &report.totals;
    Ok(envelope(
        &stamp,
        wire::DeliveryData {
            fleet: wire::DeliveryTotals {
                segments_expected: t.segments_expected,
                segments_captured: t.segments_captured,
                segments_declared_dropped: t.segments_declared_dropped,
                segments_missing_unexplained: t.segments_missing_unexplained,
                fraction: t.fraction,
                records_seen: t.records_seen,
                records_dropped_at_source: t.records_dropped_at_source,
                records_oversized: t.records_oversized,
                as_of_ms: t.as_of_ms,
            },
            rows: report
                .rows
                .iter()
                .map(|r| wire::DeliveryRow {
                    incarnation: r.incarnation.clone(),
                    instances: r.instances.clone(),
                    first_seen_ms: r.first_seen_ms,
                    last_seen_ms: r.last_seen_ms,
                    segments_expected: r.segments_expected,
                    segments_captured: r.segments_captured,
                    segments_declared_dropped: r.segments_declared_dropped,
                    segments_missing_unexplained: r.segments_missing_unexplained,
                    records_seen: r.records_seen,
                    records_dropped_at_source: r.records_dropped_at_source,
                    records_oversized: r.records_oversized,
                })
                .collect(),
            orphan_batches: report.orphan_batches,
            cannot_tell_you: "segments past each tap's last word (arrived or declared) have \
                              left no trace yet; this ledger claims nothing about them",
        },
    ))
}

pub(crate) async fn roster(
    State(state): State<ApiState>,
    Params(q): Params<RosterQuery>,
) -> Result<Json<wire::Envelope<wire::RosterData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let limit = q.paging().limit();
    // Serve posture: silence is judged against real now, so a fleet that went dark reads
    // as dark rather than as fresh-at-its-own-horizon. Tests pin this clock.
    let now = q.context().now();
    let question = q.paging().question("roster", &window, &[]);
    let refused = |e: Unavailable| {
        envelope(
            &stamp,
            wire::RosterData {
                scope: clip_scope(),
                cannot_tell_you: ROSTER_CANNOT,
                rows: None,
                state_counts: None,
                producer_runs: None,
                page: refused_page(limit),
                refused: Some(e.into_wire()),
            },
        )
    };
    let read = q
        .paging()
        .execute(
            &state,
            &stamp,
            question,
            roster_cursor,
            move |dao, after| {
                dao.roster_page(
                    &window,
                    Some(now),
                    limit,
                    after.as_ref().map(|(i, n)| (i.as_str(), n.as_str())),
                )
            },
        )
        .await?;
    let (rows, paging) = match read {
        Outcome::Available { data, paging } => (data, paging),
        Outcome::Unavailable(e) => return Ok(refused(e)),
    };
    let mut states: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for r in &rows {
        *states
            .entry(state_count_key(r.staleness).to_string())
            .or_default() += 1;
    }
    Ok(envelope(
        &stamp,
        wire::RosterData {
            // The clip is ON the response, because a run whose whole life sits outside the window
            // but inside the margin still appears — and a console author will otherwise file that
            // as a bug.
            scope: clip_scope(),
            cannot_tell_you: ROSTER_CANNOT,
            rows: Some(
                rows.iter()
                    .map(|r| {
                        let (run_state, reason) = state_words(r.staleness);
                        wire::RunRow {
                            instance: r.instance.clone(),
                            incarnation: r.incarnation.clone(),
                            first_seen_ms: r.first_seen_ms,
                            last_seen_ms: r.last_seen_ms,
                            stop_observed: r.stop_observed,
                            model: r.model.clone(),
                            engine_version: r.engine_version.clone(),
                            pod: r.pod.clone(),
                            state: run_state,
                            reason,
                            silent_for_ms: (reason == Some("silent"))
                                .then(|| (now - r.last_seen_ms).max(0.0)),
                            threshold_ms: r.threshold_ms,
                        }
                    })
                    .collect(),
            ),
            // **The PAGE's run count, and the field says so.** It totals to the run count and
            // never the holder count; across a paged roster it is this
            // page's, which is why the paging is exposed rather than hidden.
            producer_runs: Some(rows.len()),
            state_counts: Some(states),
            refused: None,
            page: paging.page(
                rows.len(),
                None,
                (rows.len() == limit as usize)
                    .then(|| rows.last())
                    .flatten()
                    .map(|r| roster_position(&r.instance, &r.incarnation)),
            ),
        },
    ))
}

pub(crate) fn clip_scope() -> wire::ClipScope {
    wire::ClipScope {
        kind: "wall_clip",
        margin_ms: analyzer_analysis::completeness::LIFECYCLE_CLIP_MARGIN_MS,
        note: "the roster is clipped on the WALL clock, not filtered on the engine window; the \
               two domains do not convert. A run whose life sits outside the window but inside \
               the margin still appears.",
    }
}

pub(crate) fn state_words(
    s: analyzer_serving_clickhouse::RunStaleness,
) -> (&'static str, Option<&'static str>) {
    use analyzer_serving_clickhouse::RunStaleness as R;
    match s {
        R::Live => ("active", None),
        // An announced `agent_stop`: expected, and a rolling deploy looks like this.
        R::Departed => ("inactive", Some("stopped")),
        // Past the threshold with no stop — the alarming one.
        R::Unexplained => ("inactive", Some("silent")),
        // No declaration observed, so no verdict was possible: a coverage gap, never a
        // health claim.
        R::NoBasis => ("inactive", Some("ungraded")),
    }
}

pub(crate) fn state_count_key(s: analyzer_serving_clickhouse::RunStaleness) -> &'static str {
    let (state, reason) = state_words(s);
    reason.unwrap_or(state)
}
