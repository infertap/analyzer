// SPDX-License-Identifier: AGPL-3.0-only

//! System operations and their resource-specific mapping.

use crate::envelope::envelope;
use crate::envelope::Stamp;
use crate::error::ApiError;
use crate::error::Cause;
use crate::extract::Params;
use crate::requests::NoQuery;
use crate::wire;
use crate::ApiState;
use axum::extract::State;
use axum::Json;

pub(crate) async fn stamp(
    State(state): State<ApiState>,
    Params(_): Params<NoQuery>,
) -> Result<Json<wire::Envelope<wire::StampData>>, ApiError> {
    // **The one endpoint that answers before any tick has been applied, and it reports NO
    // window.** It takes no window parameter, so it has none to report; saying so is the same
    // answer `tick` and `raw_horizon` give beside it.
    let stamp = Stamp::read(&state, None, state.declared_ledger_days()).await?;
    // `ready` is a fact, never a constant: on the one endpoint whose job is answering "is there
    // anything yet", a tick having classified a store is what the caller asks.
    let ready = stamp.classified_to_ms.is_some();
    Ok(envelope(&stamp, wire::StampData { ready }))
}

pub(crate) async fn analyze(
    State(state): State<ApiState>,
    Params(_): Params<NoQuery>,
) -> Result<Json<wire::Envelope<wire::AnalyzeData>>, ApiError> {
    let stamp = crate::envelope::Stamp::read(&state, None, state.declared_ledger_days()).await?;
    let data = match state.tick_kick() {
        Some(kick) => {
            kick.store(true, std::sync::atomic::Ordering::Relaxed);
            wire::AnalyzeData {
                accepted: true,
                note: "the loop will ingest and tick within a second; figures advance \
                       when the tick is applied",
            }
        }
        // In band, not a cause: serve-mode is a designed state, and a reader deciding
        // whether to show the control needs the fact, not an error.
        None => wire::AnalyzeData {
            accepted: false,
            note: "this process serves reads only; no analysis loop is attached",
        },
    };
    Ok(envelope(&stamp, data))
}

pub(crate) async fn event_stream(
    State(state): State<ApiState>,
    Params(_): Params<NoQuery>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let last = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    crate::sse::stream(&state.events, last.as_deref()).into_response()
}

pub(crate) async fn embedded_console(
    uri: axum::http::Uri,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let path = uri.path();
    if let Some(response) = crate::console::serve(path, &headers) {
        return response;
    }
    if path.starts_with("/assets/") {
        return (
            axum::http::StatusCode::NOT_FOUND,
            "no such console asset in this build",
        )
            .into_response();
    }
    crate::console::serve("/index.html", &headers).unwrap_or_else(|| {
        ApiError::new(
            Cause::ConsoleNotConfigured,
            "this analyzer was built without a console bundle",
        )
        .into_response()
    })
}

pub(crate) async fn unknown_v1_route(method: axum::http::Method, uri: axum::http::Uri) -> ApiError {
    ApiError::new(
        Cause::UnknownRoute,
        format!(
            "this build serves no such endpoint: {method} {}. The console bundle is not \
             involved — if a consumer expects this route, its contract is newer than this binary",
            uri.path()
        ),
    )
}

pub(crate) async fn console_absent() -> ApiError {
    ApiError::new(
        Cause::ConsoleNotConfigured,
        "this analyzer was built without a console bundle. The console is compiled into the \
         binary, so there is no path to point at and nothing to configure — a release build \
         carries one and this build does not",
    )
}
