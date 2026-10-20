// SPDX-License-Identifier: AGPL-3.0-only

//! Drills operations and their resource-specific mapping.

use crate::envelope::envelope;
use crate::envelope::window_json;
use crate::envelope::Stamp;
use crate::error::ApiError;
use crate::error::Cause;
use crate::extract::Params;
use crate::extract::Subject;
use crate::pagination::refused_page;
use crate::pagination::{Outcome, Unavailable};
use crate::requests::stamp_and_window;
use crate::requests::BlocksQuery;
use crate::requests::DayQuery;
use crate::requests::DrillQuery;
use crate::requests::EvidenceQuery;
use crate::resources::common::holder_id;
use crate::resources::common::reach_json;
use crate::resources::positions::evidence_cursor;
use crate::resources::positions::evidence_position;
use crate::resources::positions::holder_cursor;
use crate::resources::positions::holder_event_cursor;
use crate::resources::positions::holder_event_position;
use crate::resources::positions::holder_position;
use crate::resources::positions::timeline_cursor;
use crate::resources::positions::timeline_position;
use crate::wire;
use crate::wire::Figure;
use crate::ApiState;
use axum::extract::State;
use axum::Json;
use serde_json::json;

pub(crate) async fn day_detail(
    State(state): State<ApiState>,
    Subject(day): Subject<String>,
    Params(q): Params<DayQuery>,
) -> Result<Json<wire::Envelope<wire::DayData>>, ApiError> {
    let q = q.validate(day, state.now_ms())?;
    let day = q.day();
    let limit = q.paging().limit();
    // Refused before the read, through the DAO's own parser — not by sniffing the engine error's
    // message afterwards, which is the grep-over-output shape: a filter that must be kept right
    // where an up-front verdict needs no keeping.
    let day_window = q.context().window();
    let now = q.context().now();
    let probe = Stamp::read(&state, None, state.declared_ledger_days()).await?;
    // The question is the day (its own window) and the limit: this handler takes no window
    // of its own.
    let question = q.paging().question("day", &day_window, &[&day]);
    // **Scoped to the holders section.** The day's class breakdown and coverage are unpaged and
    // read fine, so a stale holders cursor must not blank them — it refuses one section and the
    // rest of the response stands.
    let subject = day.clone();
    let read = q
        .paging()
        .execute(
            &state,
            &probe,
            question,
            holder_cursor,
            move |dao, after| dao.day_detail(&subject, Some(now), limit, after.as_ref()),
        )
        .await?;
    let (probe, detail, holders_refused, paging) = match read {
        Outcome::Available { data, paging } => (probe, data, None, Some(paging)),
        Outcome::Unavailable(moved) => {
            // The unpaged sections remain available; never expose the fallback's holder page.
            let probe = Stamp::read(&state, None, state.declared_ledger_days()).await?;
            let subject = day.clone();
            let detail = state
                .read(move |dao| dao.day_detail(&subject, Some(now), limit, None))
                .await?;
            (probe, detail, Some(moved), None)
        }
    };
    let holders_ok = holders_refused.is_none();
    let mut stamp = probe;
    stamp.window = Some(detail.window);

    Ok(envelope(
        &stamp,
        wire::DayData {
            day: detail.day.clone(),
            by_class: detail
                .by_class
                .iter()
                .map(|(c, f)| wire::ClassMass {
                    class: c.clone(),
                    tokens: Figure::from(f),
                })
                .collect(),
            holders: holders_ok.then(|| {
                detail
                    .holders
                    .iter()
                    .map(|r| wire::DayHolder {
                        holder: holder_id(&r.holder),
                        tokens: Figure::from(&r.tokens),
                        share_of_fleet_total: Figure::from(&r.share_of_fleet_total),
                    })
                    .collect()
            }),
            holders_refused: holders_refused.map(Unavailable::into_wire),
            // On a refused cursor this is `refused_page`, like every other refusal: the DAO read
            // the section anyway (one call returns every section), but a `returned` above zero
            // beside absent rows — or a cursor minted for rows nobody saw — would contradict the
            // refusal one field up.
            holders_page: if holders_ok {
                paging.as_ref().expect("available page").page(
                    detail.holders.len(),
                    Some(detail.holders_total),
                    detail
                        .holders_next
                        .as_ref()
                        .map(|c| holder_position(&c.holder)),
                )
            } else {
                refused_page(limit)
            },
            coverage: detail.coverage.as_ref().map(Figure::from),
            // Carried rather than inferred from an empty drill: "this day is older than the raw
            // horizon" is a retention fact, not an absence of activity.
            evidence_available: detail.evidence.covers(),
            evidence_bound: reach_json(detail.evidence, state.declared_ledger_days()),
        },
    ))
}

pub(crate) async fn evidence(
    State(state): State<ApiState>,
    Params(q): Params<EvidenceQuery>,
) -> Result<Json<wire::Envelope<wire::EvidenceData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let limit = q.paging().limit();
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let subject = q.subject();
    // **In-band, not an HTTP error**, same as every other paged surface: a 200 whose body is an
    // error object rather than the envelope is the worst shape on the wire. The parse refuses a
    // corrupt position rather than silently restarting at page one.
    let question = q.paging().question("evidence", &window, &[]);
    let page_subject = subject.clone();
    let read = q
        .paging()
        .execute(
            &state,
            &stamp,
            question,
            evidence_cursor,
            move |dao, after| dao.evidence_page(&page_subject, &window, limit, after.as_ref()),
        )
        .await?;
    let (out, paging) = match read {
        Outcome::Available { data, paging } => (data, paging),
        Outcome::Unavailable(e) => {
            let read_from = match subject {
                analyzer_serving_clickhouse::EvidenceOf::Holder(_) => "ring",
                analyzer_serving_clickhouse::EvidenceOf::Block(_) => "log",
            };
            let reach = state
                .read(move |dao| dao.reach_of(&subject, &window))
                .await?;
            return Ok(envelope(
                &stamp,
                wire::EvidenceData {
                    reach: reach_json(reach, state.declared_ledger_days()),
                    read_from,
                    records: None,
                    truncated: false,
                    page: refused_page(limit),
                    refused: Some(e.into_wire()),
                },
            ));
        }
    };

    // Built before the records so the two stay one fact: the payload is absent exactly when the
    // refusal is present, and `Some(vec![])` — "looked and found nothing" — is unreachable on a
    // refused read.
    let refused = (!out.reach.covers()).then(|| wire::Refusal {
        cause: Cause::OutsideRawHorizon,
        message: "event grain lives inside the raw window; this subject is older than \
                  the oldest record this fleet holds"
            .to_string(),
        bound: serde_json::to_value(reach_json(out.reach, state.declared_ledger_days())).ok(),
        requested: None,
    });
    Ok(envelope(
        &stamp,
        wire::EvidenceData {
            reach: reach_json(out.reach, state.declared_ledger_days()),
            read_from: out.read_from.as_str(),
            records: refused.is_none().then(|| {
                out.records
                    .iter()
                    .map(|r| wire::EvidenceRecord {
                        at_ms: r.at_ms,
                        block_id: r.block_id.clone(),
                        class: r.class.clone(),
                        n_tokens: r.n_tokens,
                        gap_ms: r.gap_ms,
                        key_epoch: r.key_epoch,
                        coverage: r.coverage,
                    })
                    .collect()
            }),
            truncated: out.truncated,
            page: paging.page(
                out.records.len(),
                None,
                out.truncated
                    .then(|| out.records.last())
                    .flatten()
                    .map(|r| evidence_position(&r.cursor())),
            ),
            refused,
        },
    ))
}

pub(crate) async fn blocks(
    State(state): State<ApiState>,
    Params(q): Params<BlocksQuery>,
) -> Result<Json<wire::Envelope<wire::BlocksData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let limit = q.limit();
    let (days, coverage) = state
        .read(move |dao| {
            Ok((
                dao.block_days(&window, limit)?,
                dao.coverage(&window, analyzer_serving_clickhouse::CoverageBy::Fleet)?,
            ))
        })
        .await?;
    // Each day's stored mass at the coverage its stores were observed at; absent coverage is
    // none observed, never full.
    let observed: std::collections::BTreeMap<String, f64> = coverage
        .into_iter()
        .map(|row| (row.day, row.token_weighted_fraction.unwrap_or(0.0)))
        .collect();
    Ok(envelope(
        &stamp,
        wire::BlocksData {
            register: "stored",
            days: days
                .into_iter()
                .map(|d| wire::BlockDayJson {
                    #[allow(clippy::cast_precision_loss)] // token counts stay far below 2^52
                    tokens: wire::Figure(analyzer_serving_clickhouse::Covered {
                        value: d.tokens as f64,
                        register: ">=",
                        coverage: observed.get(&d.day).copied().unwrap_or(0.0),
                        coverage_kind: "token-weighted",
                    }),
                    day: d.day,
                    capacity: d.capacity,
                    cut: d.cut,
                    error_bound: d.error_bound,
                    blocks: d
                        .blocks
                        .into_iter()
                        .map(|b| wire::BlockRow {
                            block_id: b.block_id,
                            tokens_at_least: b.tokens_at_least,
                            tokens_at_most: b.tokens_at_most,
                            certain: b.certain,
                            holders_at_least: b.holders_at_least,
                        })
                        .collect(),
                })
                .collect(),
        },
    ))
}

pub(crate) async fn tags(
    State(state): State<ApiState>,
    Params(q): Params<crate::requests::WindowQuery>,
) -> Result<Json<wire::Envelope<wire::TagsData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q).await?;
    let read = state.read(move |dao| dao.tags(&window)).await?;
    let ratio = |r: analyzer_analysis::tags::Ratio| wire::RatioJson {
        value: r.value,
        low_90: r.interval_90.map(|(lo, _)| lo),
        high_90: r.interval_90.map(|(_, hi)| hi),
    };
    Ok(envelope(
        &stamp,
        wire::TagsData {
            through_ms: read.through_ms,
            coverage: wire::TagCoverageJson {
                covered_ms: read.covered_ms,
                window_ms: read.window_ms,
                fraction: (read.window_ms > 0.0).then(|| read.covered_ms / read.window_ms),
            },
            tags: read
                .tags
                .into_iter()
                .map(|(tag, f)| wire::TagRowJson {
                    tag,
                    identities: f.identities,
                    identity_minutes: f.identity_minutes,
                    stores: f.sums.stores,
                    prefill_tokens: f.sums.prefill_tokens,
                    refetch_tokens: f.sums.refetch_tokens,
                    indeterminate_tokens: f.sums.indeterminate_tokens,
                    ambiguous_stores: f.sums.ambiguous_stores,
                    refetch_per_identity_minute: ratio(f.refetch_per_identity_minute),
                    refetch_per_stored_token: ratio(f.refetch_per_stored_token),
                })
                .collect(),
        },
    ))
}

pub(crate) async fn block_holders(
    State(state): State<ApiState>,
    Subject(block_id): Subject<String>,
    Params(q): Params<DrillQuery>,
) -> Result<Json<wire::Envelope<wire::BlockHoldersData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let limit = q.paging().limit();
    // **In-band, not a 400**: the token was valid when minted and a tick moved the edges. The
    // reach is still read — it is a property of the window and retention, not of the cursor, and
    // a bound this layer could state but did not is the vague refusal read-api section 7.1 forbids.
    let question = q.paging().question("block_holders", &window, &[&block_id]);
    let block = block_id.clone();
    let read = q
        .paging()
        .execute(
            &state,
            &stamp,
            question,
            holder_event_cursor,
            move |dao, after| {
                Ok((
                    dao.raw_reach_of(&window)?,
                    dao.holders_of(&block, &window, limit, after.as_ref())?,
                ))
            },
        )
        .await?;
    let ((reach, page), paging) = match read {
        Outcome::Available { data, paging } => (data, paging),
        Outcome::Unavailable(e) => {
            let reach = state.read(move |dao| dao.raw_reach_of(&window)).await?;
            return Ok(envelope(
                &stamp,
                wire::BlockHoldersData {
                    block_id,
                    bound: reach_json(reach, state.declared_ledger_days()),
                    read_from: "log",
                    rows: None,
                    page: refused_page(limit),
                    refused: Some(e.into_wire()),
                },
            ));
        }
    };
    let read_from = page.read_from.as_str();
    let rows = page.rows;
    // Only the FIRST page can say a block was never stored. A later page is empty because the
    // walk ended, and calling that "unknown block" would 404 a drill that had been answering.
    if rows.is_empty() && reach.covers() && page.next.is_none() && q.paging().is_first() {
        return Err(ApiError::new(
            Cause::UnknownBlock,
            format!("no store of block {block_id:?} in this window"),
        )
        .with_bound(json!({ "window": window_json(&window) })));
    }
    Ok(envelope(
        &stamp,
        wire::BlockHoldersData {
            block_id,
            bound: reach_json(reach, state.declared_ledger_days()),
            read_from,
            rows: Some(
                rows.iter()
                    .map(|h| wire::BlockHolderRow {
                        holder: wire::HolderId {
                            instance: h.instance.clone(),
                            dp_rank: h.dp_rank,
                            group_idx: h.group_idx,
                        },
                        at_ms: h.at_ms,
                        class: h.class.clone(),
                        n_tokens: h.tokens,
                    })
                    .collect(),
            ),
            refused: None,
            page: paging.page(
                rows.len(),
                None,
                page.next.as_ref().map(holder_event_position),
            ),
        },
    ))
}

pub(crate) async fn holder_timeline(
    State(state): State<ApiState>,
    Subject((instance, dp_rank, group_idx)): Subject<(String, u32, u32)>,
    Params(q): Params<DrillQuery>,
) -> Result<Json<wire::Envelope<wire::TimelineData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let limit = q.paging().limit();
    let holder = analyzer_serving_clickhouse::Holder {
        instance,
        dp_rank,
        group_idx,
    };
    let question = q
        .paging()
        .question("holder_timeline", &window, &[&holder_position(&holder)]);
    let subject = holder.clone();
    let read = q
        .paging()
        .execute(
            &state,
            &stamp,
            question,
            timeline_cursor,
            move |dao, after| {
                Ok((
                    dao.reach_of(
                        &analyzer_serving_clickhouse::EvidenceOf::Holder(subject.clone()),
                        &window,
                    )?,
                    dao.timeline(&subject, &window, limit, after.as_ref())?,
                ))
            },
        )
        .await?;
    let ((reach, page), paging) = match read {
        Outcome::Available { data, paging } => (data, paging),
        Outcome::Unavailable(e) => {
            let subject = holder.clone();
            let reach = state
                .read(move |dao| {
                    dao.reach_of(
                        &analyzer_serving_clickhouse::EvidenceOf::Holder(subject),
                        &window,
                    )
                })
                .await?;
            return Ok(envelope(
                &stamp,
                wire::TimelineData {
                    holder: holder_id(&holder),
                    bound: reach_json(reach, state.declared_ledger_days()),
                    read_from: "ring",
                    rows: None,
                    page: refused_page(limit),
                    refused: Some(e.into_wire()),
                },
            ));
        }
    };
    let read_from = page.read_from.as_str();
    let rows = page.rows;
    Ok(envelope(
        &stamp,
        wire::TimelineData {
            holder: holder_id(&holder),
            bound: reach_json(reach, state.declared_ledger_days()),
            read_from,
            rows: Some(
                rows.iter()
                    .map(|t| wire::TimelineRow {
                        at_ms: t.at_ms,
                        block_id: t.block_id.clone(),
                        class: t.class.clone(),
                        n_tokens: t.tokens,
                        gap_ms: t.gap_ms,
                        key_epoch: t.key_epoch,
                    })
                    .collect(),
            ),
            refused: None,
            page: paging.page(rows.len(), None, page.next.as_ref().map(timeline_position)),
        },
    ))
}
