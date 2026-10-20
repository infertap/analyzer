// SPDX-License-Identifier: AGPL-3.0-only

//! Method/path registration, authentication, CORS and fallback composition.
use crate::auth::require_session;
use crate::resources::{capacity, drills, figures, holders, markers, observation, session, system};
use crate::ApiState;
use axum::routing::{delete, get, post};
use axum::Router;

pub fn router(state: ApiState) -> Router {
    // Session lifecycle is separate from the authenticated resource operations.
    let realm = Router::new()
        .route("/v1/session", post(session::session_create))
        .route("/v1/session", delete(session::session_destroy));

    // Everything else. `require_session` wraps the whole group rather than each handler, so a new
    // route is authenticated by default — an opt-in would eventually be forgotten once.
    let read = Router::new()
        .route("/v1/stamp", get(system::stamp))
        .route("/v1/figures", get(figures::figures))
        .route("/v1/holders", get(holders::holders))
        .route("/v1/coverage", get(observation::coverage))
        .route("/v1/delivery", get(observation::delivery))
        .route("/v1/analyze", post(system::analyze))
        .route("/v1/annotations", post(markers::annotations_create))
        .route("/v1/health", get(crate::health::health))
        .route("/v1/roster", get(observation::roster))
        .route("/v1/residual/placement", get(capacity::placement))
        .route("/v1/residual/whales", get(capacity::whales))
        .route(
            "/v1/instances/{instance}/duplication",
            get(capacity::instance_duplication),
        )
        .route("/v1/residual/replication", get(capacity::replication))
        .route("/v1/figures/bands", get(figures::bands))
        .route("/v1/markers", get(markers::markers))
        .route("/v1/markers/{id}", get(markers::marker_detail))
        .route("/v1/blocks", get(drills::blocks))
        .route("/v1/tags", get(drills::tags))
        .route("/v1/blocks/{block_id}/holders", get(drills::block_holders))
        .route(
            "/v1/holders/{instance}/{dp_rank}/{group_idx}/timeline",
            get(drills::holder_timeline),
        )
        .route("/v1/days/{day}", get(drills::day_detail))
        .route("/v1/evidence", get(drills::evidence))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ));

    let events = Router::new()
        .route("/v1/events", get(system::event_stream))
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ));

    let mut app = realm.merge(read).merge(events);

    // **The console mount, read-api section 8.3.** Unset answers a decided 503 rather than a 404: a missing
    // bundle is a deployment fact and should read as one. An SPA fallback to `index.html` is what
    // makes level-1 URLs deep-linkable, which the whole three-level addressing depends on.
    // **The API namespace claims its own misses BEFORE the console fallback sees them.**
    // Otherwise an unmatched `/v1` path falls through to the console mount: with no bundle it
    // answers `503 console_not_configured`, and with one — every release build — it serves
    // `index.html` at **200**, so a JSON client asking for an endpoint this binary lacks gets a
    // successful HTML page. No status-code check can see that, which is why the golden for this
    // asserts the CONTENT TYPE. The console fallback keeps everything that is not `/v1`.
    app = app.route("/v1", axum::routing::any(system::unknown_v1_route));
    app = app.route("/v1/", axum::routing::any(system::unknown_v1_route));
    app = app.route("/v1/{*rest}", axum::routing::any(system::unknown_v1_route));
    // A known path with a verb it does not serve is the same statement about the binary as an
    // unknown path: the route (verb and path together) does not exist here. Answered in the
    // envelope rather than by the framework's bare 405.
    app = app.method_not_allowed_fallback(system::unknown_v1_route);

    app = match state.config.console_dir.clone() {
        // The library escape hatch, reachable only from a test fixture — `crates/cli` never sets
        // this. See `ApiConfig::console_dir`.
        Some(dir) => {
            let index = dir.join("index.html");
            app.fallback_service(
                tower_http::services::ServeDir::new(dir)
                    .fallback(tower_http::services::ServeFile::new(index)),
            )
        }
        None if crate::console::present() => app.fallback(system::embedded_console),
        None => app.fallback(system::console_absent),
    };

    // **The ONE dev-mode allowance, read-api section 6.4.** Off unless configured; exactly one origin, echoed
    // literally, never a wildcard and never a pattern. In production the daemon serves the
    // console itself, so the two share an origin and NO cors header is emitted at all —
    // emitting a permissive one anyway is how a same-origin deployment quietly becomes
    // cross-origin.
    if let Some(origin) = state.config.dev_origin.clone() {
        if let Ok(value) = origin.parse::<axum::http::HeaderValue>() {
            app = app.layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(value)
                    .allow_credentials(true)
                    .allow_headers([axum::http::header::CONTENT_TYPE])
                    .allow_methods([
                        axum::http::Method::GET,
                        axum::http::Method::POST,
                        axum::http::Method::DELETE,
                        axum::http::Method::OPTIONS,
                    ]),
            );
        }
    }

    app.with_state(state)
}
