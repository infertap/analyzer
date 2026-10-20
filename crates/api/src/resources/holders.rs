// SPDX-License-Identifier: AGPL-3.0-only

//! Holders operations and their resource-specific mapping.

use super::observation::state_words;
use crate::envelope::envelope;
use crate::error::ApiError;
use crate::extract::Params;
use crate::pagination::refused_page;
use crate::pagination::{Outcome, Unavailable};
use crate::requests::stamp_and_window;
use crate::requests::HoldersQuery;
use crate::resources::common::holder_id;
use crate::resources::positions::holder_cursor;
use crate::resources::positions::holder_position;
use crate::wire;
use crate::wire::Figure;
use crate::ApiState;
use axum::extract::State;
use axum::Json;

pub(crate) async fn holders(
    State(state): State<ApiState>,
    Params(q): Params<HoldersQuery>,
) -> Result<Json<wire::Envelope<wire::HoldersData>>, ApiError> {
    let q = q.validate(state.now_ms())?;
    let (stamp, window) = stamp_and_window(&state, &q.context()).await?;
    let (requested, limit) = (q.grain(), q.paging().limit());
    let filters = q.filters();
    let question = q.paging().question("holders", &window, &[]);
    // **A stale cursor is a RESULT here, not a 400**: the token was correct when it was
    // minted and a tick moved underneath it. Refusing in-band keeps the stamp, the heading and
    // the columns, so a console replaces the rows and says what happened instead of blanking.
    // The same refusal answers a pass that lands during the read: the page is cut whole
    // under one tick or not served.
    let refused = |e: Unavailable| {
        envelope(
            &stamp,
            wire::HoldersData {
                register: "recompute",
                // Refused, so no grain is stated.
                grain: None,
                rows: None,
                fleet_total: None,
                page: refused_page(limit),
                refused: Some(e.into_wire()),
            },
        )
    };
    let now = q.context().now();
    let read = q
        .paging()
        .execute(
            &state,
            &stamp,
            question,
            holder_cursor,
            move |dao, after| {
                let grain = dao.resolve_grain(
                    &window,
                    requested,
                    analyzer_serving_clickhouse::MinuteTier::Unavailable,
                )?;
                Ok((
                    grain,
                    dao.holders_page(
                        &window,
                        Some(now),
                        grain,
                        &filters,
                        &analyzer_serving_clickhouse::Page { limit, after },
                    )?,
                ))
            },
        )
        .await?;
    let ((grain, page), paging) = match read {
        Outcome::Available { data, paging } => (data, paging),
        Outcome::Unavailable(e) => return Ok(refused(e)),
    };

    Ok(envelope(
        &stamp,
        wire::HoldersData {
            register: "recompute",
            grain: Some(grain.as_str()),
            rows: Some(
                page.rows
                    .iter()
                    .map(|r| {
                        let (run_state, reason) = state_words(r.state);
                        wire::HolderRow {
                            holder: holder_id(&r.holder),
                            state: run_state,
                            reason,
                            workload: r.workload.clone(),
                            model: r.model.clone(),
                            events: r.events,
                            tokens: Figure::from(&r.tokens),
                            share_of_fleet_total: Figure::from(&r.share_of_fleet_total),
                        }
                    })
                    .collect(),
            ),
            fleet_total: Some(Figure::from(&page.fleet_total)),
            refused: None,
            page: paging.page(
                page.rows.len(),
                Some(page.total_rows),
                page.next.as_ref().map(|c| holder_position(&c.holder)),
            ),
        },
    ))
}
