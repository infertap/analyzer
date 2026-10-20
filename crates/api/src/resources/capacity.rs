// SPDX-License-Identifier: AGPL-3.0-only

//! Capacity operations and their resource-specific mapping.

use crate::envelope::envelope;
use crate::error::ApiError;
use crate::extract::Params;
use crate::extract::Subject;
use crate::requests::stamp_and_window;
use crate::requests::InstanceDuplicationQuery;
use crate::requests::PlacementQuery;
use crate::requests::ReplicationQuery;
use crate::requests::WhalesQuery;
use crate::wire;
use crate::wire::Figure;
use crate::ApiState;
use analyzer_serving_clickhouse::Grain;
use axum::extract::State;
use axum::Json;

pub(crate) async fn placement(
    State(state): State<ApiState>,
    Params(q): Params<PlacementQuery>,
) -> Result<Json<wire::Envelope<wire::PlacementData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    // **Bounded by PAIRS, and the bound is the point.** Pairs grow with the square of holders:
    // 300 instances is 44,850 of them before the day and workload factors, which is a payload
    // that hurts the analyzer long before it hurts a browser. The default is generous enough
    // that a small fleet never meets it and every excluded pair is counted and priced.
    //
    // It has its OWN bound rather than `PAGE_LIMIT_MAX`: that ceiling is 200, sized for rows a
    // reader pages through, and pairs are not that — a 25-engine fleet is already 300 of them,
    // so borrowing the row ceiling would truncate the smallest fleet this product targets.
    let limit = q.limit() as usize;
    let out = state.read(move |dao| dao.placement(&window, limit)).await?;

    let mut rows = Vec::new();
    let mut within_engine = Vec::new();
    for r in &out.rows {
        if r.a.instance == r.b.instance {
            within_engine.push(wire::WithinEngineRow {
                workload: r.workload.clone(),
                day: r.day.clone(),
                engine: r.a.instance.clone(),
                rank_a: r.a.dp_rank,
                group_a: r.a.group_idx,
                rank_b: r.b.dp_rank,
                group_b: r.b.group_idx,
                token_ms: Figure::from(&r.token_ms),
            });
        } else {
            rows.push(wire::PlacementRow {
                workload: r.workload.clone(),
                day: r.day.clone(),
                engine_a: r.a.instance.clone(),
                rank_a: r.a.dp_rank,
                group_a: r.a.group_idx,
                engine_b: r.b.instance.clone(),
                rank_b: r.b.dp_rank,
                group_b: r.b.group_idx,
                token_ms: Figure::from(&r.token_ms),
            });
        }
    }

    Ok(envelope(
        &stamp,
        wire::PlacementData {
            register: "placement",
            never_summed_with: vec!["capacity", "recompute"],
            unit: "token_ms",
            day_rows_sum_to_window: true,
            bound: wire::PlacementBound {
                source: "replication_pairs_day",
            },
            window_total: Figure::from(&out.window_total),
            truncation: out.truncation.as_ref().map(|t| wire::TruncationJson {
                pairs_returned: t.pairs_returned,
                pairs_total: t.pairs_total,
                token_ms_excluded: Figure::from(&t.token_ms_excluded),
            }),
            rows,
            within_engine,
        },
    ))
}

pub(crate) async fn replication(
    State(state): State<ApiState>,
    Params(q): Params<ReplicationQuery>,
) -> Result<Json<wire::Envelope<wire::ReplicationData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let requested = q.grain();
    let custom_window = q.custom_window();
    let workload = q.workload();
    let (grain, data, basis) = state
        .read(move |dao| {
            let grain = dao.copies_grain(&window, requested)?;
            Ok((
                grain,
                dao.copies(&window, grain, workload.as_deref())?,
                dao.zero_basis(&window)?,
            ))
        })
        .await?;
    let mut members: Vec<wire::CopiesMemberJson> = data
        .members
        .iter()
        .map(|m| wire::CopiesMemberJson {
            key: m.workload.clone(),
            label: m.workload.clone(),
            share: m.share,
            profile: m.profile,
        })
        .collect();
    members.insert(
        0,
        wire::CopiesMemberJson {
            key: "fleet".to_string(),
            label: "fleet".to_string(),
            share: 1.0,
            profile: data.fleet_profile,
        },
    );
    Ok(envelope(
        &stamp,
        wire::ReplicationData {
            register: "capacity",
            never_summed_with: vec!["recompute"],
            unit: "token_ms",
            grain: grain.as_str(),
            bucket_ms: grain.bucket_ms(),
            start_ms: window.start_ms,
            end_ms: window.end_ms,
            derived: (custom_window && requested.is_none()).then(|| {
                format!(
                    "range × retention: {} over {:.0} min",
                    grain.as_str(),
                    (window.end_ms - window.start_ms) / 60_000.0
                )
            }),
            workload: q.workload(),
            window_dup_token_ms: data.window_dup_token_ms,
            rows: data
                .rows
                .iter()
                .map(|b| wire::CopiesBucketJson {
                    bucket_ms: b.bucket_ms,
                    k2: b.bands_token_ms[0],
                    k34: b.bands_token_ms[1],
                    k58: b.bands_token_ms[2],
                    k9: b.bands_token_ms[3],
                })
                .collect(),
            members,
            cross_engine_token_ms: data.cross_engine_token_ms,
            within_engine_token_ms: data.within_engine_token_ms,
            cache_group_token_ms: data.cache_group_token_ms,
            copies_token_ms: data.copies_token_ms,
            window_held_token_ms: data.window_held_token_ms,
            emitted_to_ms: data.emitted_to_ms,
            zero_basis: wire::ZeroBasisJson {
                meaning: basis.meaning(data.cross_engine_token_ms).as_str(),
                instances: basis.instances,
                stores: basis.stores,
                comparable_stores: basis.comparable_stores,
                key_spaces: basis.key_spaces,
            },
        },
    ))
}

pub(crate) async fn instance_duplication(
    State(state): State<ApiState>,
    Subject(instance): Subject<String>,
    Params(q): Params<InstanceDuplicationQuery>,
) -> Result<Json<wire::Envelope<wire::InstanceDuplicationData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    // The grain the copies read resolves, on the same reach: the instance cut is the same
    // sweep's cells and rides the same tick, so the fleet tiers' reach is its reach. A day
    // ask is served from the hourly cut and says so.
    let requested = q.grain();
    let subject = instance.clone();
    let (grain, data) = state
        .read(move |dao| {
            let grain = match dao.copies_grain(&window, requested)? {
                Grain::Minute => Grain::Minute,
                Grain::Hour | Grain::Day => Grain::Hour,
            };
            Ok((grain, dao.instance_duplication(&window, &subject, grain)?))
        })
        .await?;
    Ok(envelope(
        &stamp,
        wire::InstanceDuplicationData {
            register: "capacity",
            never_summed_with: "recompute",
            instance,
            grain: grain.as_str(),
            bucket_ms: grain.bucket_ms(),
            start_ms: window.start_ms,
            end_ms: window.end_ms,
            within_ranks_token_ms: data.within_token_ms,
            cache_groups_token_ms: data.group_token_ms,
            copies_token_ms: data.copies_token_ms,
            held_token_ms: data.held_token_ms,
            rows: data
                .rows
                .iter()
                .map(|r| wire::InstanceDupRowJson {
                    #[allow(clippy::cast_precision_loss)] // ms instants sit inside f64
                    bucket_ms: r.bucket_ms as f64,
                    copies_band: r.copies_band.clone(),
                    locality: r.locality.clone(),
                    token_ms: r.token_ms,
                })
                .collect(),
            cannot_tell_you: "duplication SPANNING engines, which belongs to no single node \
                              and would double on any sum across instances — the fleet \
                              copies histogram carries that half",
        },
    ))
}

pub(crate) async fn whales(
    State(state): State<ApiState>,
    Params(q): Params<WhalesQuery>,
) -> Result<Json<wire::Envelope<wire::WhalesData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let limit = q.limit() as usize;
    let out = state.read(move |dao| dao.whales(&window, limit)).await?;
    Ok(envelope(
        &stamp,
        wire::WhalesData {
            register: "whales",
            never_summed_with: vec!["capacity", "recompute"],
            unit: "token_ms",
            source: "replication_whale_day",
            days_stated: out.days_stated,
            cut: out.cut,
            duplicated_identity_days: out.duplicated_identity_days,
            window_dup_token_ms: out.window_dup_token_ms,
            window_held_token_ms: out.window_held_token_ms,
            rows: out
                .rows
                .into_iter()
                .map(|r| wire::WhaleJson {
                    identity: r.identity,
                    n_tokens: r.n_tokens,
                    dup_token_ms: r.dup_token_ms,
                    dup_token_ms_bound: r.dup_token_ms_bound,
                    held_token_ms: r.held_token_ms,
                    days_in_cut: r.days_in_cut,
                })
                .collect(),
        },
    ))
}
