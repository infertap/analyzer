// SPDX-License-Identifier: AGPL-3.0-only

//! Session operations and their resource-specific mapping.

use crate::error::ApiError;
use crate::error::Cause;
use crate::extract::Body;
use crate::extract::Params;
use crate::requests::NoQuery;
use crate::wire::{SessionData, SessionState};
use crate::ApiState;
use axum::extract::State;
use axum::Json;
use serde::Deserialize;

#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct TokenBody {
    /// The plaintext viewer token issued by the token command.
    token: String,
}

pub(crate) async fn session_create(
    State(state): State<ApiState>,
    Params(_): Params<NoQuery>,
    Body(body): Body<TokenBody>,
) -> Result<axum::response::Response, ApiError> {
    use axum::response::IntoResponse as _;
    // `verify` re-reads the store from disk, so a token minted by `token issue` in another process
    // works without restarting the analyzer, and a rotated one stops working immediately.
    let Some(token_id) = state.tokens.verify(&body.token) else {
        // The same refusal for a bad token and for no token at all: distinguishing them tells an
        // attacker which half to work on.
        return Err(ApiError::new(
            Cause::Unauthenticated,
            "the presented token is not an active viewer token",
        ));
    };
    let id = state
        .sessions
        .create(&token_id)
        .map_err(|e| ApiError::engine(format!("minting a session: {e}")))?;
    let cookie = crate::auth::session_cookie_header(
        &id,
        crate::auth::SESSION_ABSOLUTE_TTL.as_secs(),
        state.config.cookie_secure,
    );
    Ok((
        [(axum::http::header::SET_COOKIE, cookie)],
        Json(SessionData {
            session: SessionState::Created,
        }),
    )
        .into_response())
}

pub(crate) async fn session_destroy(
    State(state): State<ApiState>,
    Params(_): Params<NoQuery>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    if let Some(id) = crate::auth::session_cookie(&headers) {
        state.sessions.destroy(&id);
    }
    (
        // Same builder as the mint, because a browser replaces a cookie by matching name, domain
        // and path — a delete that spells any of them differently leaves the original in place
        // and the sign-out silently does nothing.
        [(
            axum::http::header::SET_COOKIE,
            crate::auth::session_cookie_header("", 0, state.config.cookie_secure),
        )],
        Json(SessionData {
            session: SessionState::Destroyed,
        }),
    )
        .into_response()
}
