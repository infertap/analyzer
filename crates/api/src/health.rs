// SPDX-License-Identifier: AGPL-3.0-only

//! `/v1/health` — the METER's health, and the strip's hydration source.
//!
//! **Naming, stated because it is the classic confusion:** this is the meter's health, not the
//! process's. Liveness stays on `/metrics` — a different listener, a different bind, no auth, and
//! `STALE_SAMPLE_INTERVALS`' 503 semantics unchanged. Nothing about that surface moves.
//!
//! **The mixed composition is lawful** because it is not fleet-data composition: DAO fleet reads
//! plus daemon-native self-observation and ingest accounting. The read seam governs fleet data,
//! and this is the meter observing itself.

use crate::error::ApiError;
use crate::wire;
use crate::ApiState;
use analyzer_analysis::degradation::{Cause, Degradation};

use std::collections::BTreeMap;

/// What the daemon knows and the read layer cannot: its own cadence and its own backlog.
///
/// **Published by the daemon into [`ApiState`], and ABSENT rather than faked when it has not
/// been.** An API embedded in a test, or serving before the loop's first tick, genuinely does not
/// know when the next tick is; rendering a guess would put a number on the strip that nothing
/// measured — on the one surface whose entire job is saying how fresh things are.
#[derive(Debug, Clone, Default)]
pub struct LoopHints {
    /// Wall-clock ms at which the next PASS becomes due — the cadence that moves the
    /// figures. Absent while a pass is overdue or has never run.
    pub next_tick_at_ms: Option<f64>,
    /// Files listed and not yet ingested. Absent when nothing measured it — which the shell
    /// makes always, today: it publishes `None` rather than a derived count.
    pub backlog_files: Option<u64>,
    /// Their bytes. Absent with `backlog_files`.
    pub backlog_bytes: Option<u64>,
    /// Whether the disk-pressure ladder is refusing intake right now. Absent before the
    /// loop's first publish.
    pub refusing: Option<bool>,
}

/// The three clauses every degradation notice owes.
///
/// **A closed match over [`Cause`], so a new cause is a COMPILE ERROR until somebody writes its
/// three clauses.** That is read-api section 1.3 made structural rather than remembered: a notice that says only
/// what is degraded is an alarm without a scope, and an operator reading it either discounts the
/// whole verdict or ignores it — both wrong, and the product knows the correct reading.
///
/// The vocabulary lives HERE, in the renderer, rather than on `analysis::Cause`. What is degraded
/// is a fact about the data; how to say it and what to do about it are rendering, and the same
/// cause is phrased differently in `report.md` than on glass.
fn clauses(cause: &Cause) -> (String, &'static str, &'static str) {
    match cause {
        Cause::SequenceGap {
            first_missing,
            last_missing,
        } => (
            format!(
                "segments {first_missing}–{last_missing} never arrived and were never declared, \
                 so figures over the span they covered are indeterminate"
            ),
            "spans outside the hole, and every other producer run",
            "check the tap's delivery path and its disk; a hole with no `segments_dropped` \
             declaration means the segments were lost in transit rather than reclaimed",
        ),
        Cause::DeclaredDrop {
            first_seq,
            last_seq,
        } => (
            format!(
                "the producer declared segments {first_seq}–{last_seq} reclaimed, so their \
                 records are gone and the span they covered is indeterminate"
            ),
            "spans outside the declared drop, and every other producer run",
            "raise the tap's segment retention, or shorten the delivery lag, so a segment is \
             shipped before its own rotation reclaims it",
        ),
        Cause::IncarnationBoundary { from, to } => (
            format!(
                "nothing was observed between producer run {from} ending and {to} starting, so \
                 that interval is uncovered rather than quiet"
            ),
            "both runs' own observed spans, and every other instance",
            "expected across a deliberate restart; if it was not one, check why the tap exited \
             — an unannounced gap is the signal the roster's `silent, unexplained` state names",
        ),
        Cause::IdentityRefusal { refused } => (
            format!(
                "{refused} stores were denied an identity, so evicts over the declared window \
                 cannot resolve and figures over them read as a lower bound"
            ),
            "the register only — the figures stand, as `≥` and only as `≥`",
            "this is the engine declining to name a block, not a collection fault; no operator \
             action closes it, and the `≥` register is the honest rendering",
        ),
        Cause::RecoveredTruncation { dropped_bytes } => (
            format!(
                "a crashed segment was recovered with {dropped_bytes} trailing bytes discarded \
                 as incomplete, so records at that segment's tail are gone without a sequence \
                 hole and its span is indeterminate"
            ),
            "spans outside the recovered segment's bracket, and every other producer run",
            "expected after a producer crash; the recovery itself is the tap working. If it \
             recurs without crashes, check the node's disk and the tap's shutdown path",
        ),
        Cause::DropWindow { dropped, oversized } => (
            format!(
                "{dropped} inbound messages were dropped and {oversized} were oversized between \
                 two heartbeats, and the loss cannot be localized inside that span"
            ),
            "spans outside the drop window, and every other producer run",
            "the tap's inbound queue overflowed; raise its capacity or reduce publisher volume \
             — `dropped` is the tap's own counter, so this is measured rather than inferred",
        ),
    }
}

fn degradation_json(d: &Degradation) -> wire::DegradationJson {
    // All three, always. An entry missing `unaffected` or `remedy` is a schema violation rather
    // than a sparse response — the only way a rule about SENTENCES survives a repository
    // boundary, and the struct is what makes omitting one impossible.
    let (degraded, unaffected, remedy) = clauses(&d.cause);
    wire::DegradationJson {
        scope: d.scope.describe(),
        class: d.cause.class(),
        register: d.register().as_str(),
        incidents: d.incidents,
        degraded,
        unaffected,
        remedy,
    }
}

/// `GET /v1/health`.
///
/// # Errors
/// Engine failure.
pub(crate) async fn health(
    axum::extract::State(state): axum::extract::State<ApiState>,
    crate::extract::Params(q): crate::extract::Params<crate::requests::WindowQuery>,
) -> Result<axum::Json<wire::Envelope<wire::HealthData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = crate::requests::stamp_and_window(&state, &q).await?;
    let ledger_window = window;

    // Store reads only under the engine lock — every one bounded and indexed. The ledger
    // comes from the loop-published snapshot below, and `agent_versions`/`roster` read the
    // ingest-grain arrivals table: nothing here walks the raw source,
    // so the worst case of this route is a slow tile, never a wedged console.
    let now_ms = q.now();
    let (ingest, coverage, roster, versions, modules) = state
        .read(move |dao| {
            Ok((
                dao.ingest_status()?,
                dao.coverage(&window, analyzer_serving_clickhouse::CoverageBy::Instance)?,
                dao.roster(&window, Some(now_ms))?,
                dao.agent_versions()?,
                dao.module_availability()?,
            ))
        })
        .await?;
    // Window-filtered in memory: the ledger is incident-shaped, and `in_window` is the
    // module's own overlap rule (an entry with no engine extent degrades every window).
    let ledger_entries: Vec<analyzer_analysis::degradation::Degradation> = state
        .pass_ledger()
        .map(|l| l.in_window(&ledger_window).into_iter().cloned().collect())
        .unwrap_or_default();

    let hints = state.loop_hints();

    // **Declared at the deployment boundary, relayed verbatim**. Absent reads as `undeclared`, which
    // is a different claim from `unattributed` and the two are never interchangeable.
    let (observed, undeclared) = roster.iter().fold((0_u64, 0_u64), |(o, u), r| {
        if r.model.is_some() {
            (o + 1, u)
        } else {
            (o, u + 1)
        }
    });
    let mut states: BTreeMap<String, usize> = BTreeMap::new();
    for r in &roster {
        *states
            .entry(crate::resources::observation::state_count_key(r.staleness).to_string())
            .or_default() += 1;
    }

    Ok(axum::Json(wire::Envelope {
        stamp: stamp.to_json(),
        data: wire::HealthData {
            ingest: ingest_json(&ingest, &hints, now_ms),
            coverage_by_instance: coverage
                .iter()
                .map(crate::resources::observation::coverage_row)
                .collect(),
            // Three populations, never one. Staleness is per RUN, which
            // is why these total to the run count and not the holder count.
            producer_runs: roster.len(),
            run_states: states,
            models: wire::ModelCounts {
                observed,
                undeclared,
            },
            agent_versions: versions,
            degradations: ledger_entries.iter().map(degradation_json).collect(),
            modules: modules.iter().map(module_json).collect(),
        },
    }))
}

fn module_json(m: &analyzer_pipeline::executor::ModuleAvailability) -> wire::ModuleJson {
    wire::ModuleJson {
        name: m.definition.name.to_string(),
        enabled: m.enabled,
        revision: m.revision,
        stated_from_day: m.stated_from_day.map(crate::envelope::day_string),
        stated_through_day: m.stated_through_day.map(crate::envelope::day_string),
    }
}

fn ingest_json(
    status: &analyzer_serving_clickhouse::IngestStatus,
    hints: &LoopHints,
    now_ms: f64,
) -> wire::IngestJson {
    wire::IngestJson {
        last_segment: status.last.as_ref().map(|b| b.source.clone()),
        last_segment_at_ms: status.last.as_ref().map(|b| b.ingested_at_ms),
        last_segment_records: status.last.as_ref().map(|b| b.records),
        // Cache facts only — a span across lifecycle and cache `at_ms` would have endpoints in
        // two clock domains. `None` where the batch carried no cache facts, which is ordinary.
        last_segment_span: status
            .last
            .as_ref()
            .and_then(|b| b.cache_span_ms)
            .map(|(a, z)| wire::SpanJson {
                start_ms: a,
                end_ms: z,
                domain: "engine",
            }),
        // **Wall domain on both sides**: our insert stamp against our own clock. Never the
        // producer's `at_ms`, which would be a cross-domain comparison wearing a duration's
        // clothes.
        lag_ms: status
            .last
            .as_ref()
            .map(|b| (now_ms - b.ingested_at_ms).max(0.0)),
        batches_total: status.batches_total,
        events_since_install: status.records_total,
        unknown_kinds_total: status.unknown_kinds_total,
        // Daemon-native. Absent rather than faked when the loop has published nothing.
        next_tick_at_ms: hints.next_tick_at_ms,
        backlog_files: hints.backlog_files,
        backlog_bytes: hints.backlog_bytes,
        refusing: hints.refusing,
    }
}
