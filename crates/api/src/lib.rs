// SPDX-License-Identifier: AGPL-3.0-only

//! The read API: the versioned seam between the analyzer and the console.
//!
//! **The contract is `docs/read-api.md`.** This crate implements it and adds
//! nothing to it. Where the two disagree the document is right and this is a bug.
//!
//! **What this is.** The read surface `run` serves beside the daemon loop. The console lives in a
//! separate repository and consumes only this contract — no SQL, no file layout, no engine.
//!
//! **Served by default**: `--api-addr` defaults to loopback and `off` is the opt-out. The
//! console it serves is **the only reading surface this product has.** The CLI verbs beside it
//! (`preflight`, `token`, `health`, `diag`, `annotate`) are box operations performed by somebody
//! already on the host, never a second way to read a figure. So a figure this crate cannot serve
//! is a figure the product cannot show, which is the weight every route here carries.
//!
//! **What it is not.** No endpoint alters a measurement. Endpoints MAY record declarations: the
//! analyze-now kick asks for a tick, and `POST /v1/annotations` records an operator's deploy
//! declaration as a bucket object and nothing else. The auth realm owns `POST` and
//! `DELETE /v1/session`. The engine this crate holds is read-only.
//!
//! # The three laws this crate holds
//!
//! - **Thin.** A handler maps a path and a query string to ONE read-layer call (`analyzer_serving_clickhouse`) and serializes
//!   the result. Anything needing composition became a DAO method instead,
//!   which is why `static_audit`'s SQL ratchet has no entry for this crate — that absence is
//!   an assertion, not an omission.
//! - **No naked number.** Figures serialize through `wire::covered_json`, the one figure
//!   serializer in the workspace: four fields always, and no path to JSON that omits them.
//!   `static_audit::api_wire_figures_are_typed` counts the spellings
//!   of those four fields and requires exactly one.
//! - **Never sum across a meaning generation.** Held structurally below this layer — a `Dao` is
//!   bound to one `(semantics_version, discriminator)` pair — and this crate's obligation is not
//!   to undo it: no response carries a total spanning segments, and no parameter requests one.
//!
//! # Blocking, and why it is not a compromise
//!
//! `analyzer_serving_clickhouse::engine::Engine` drives its own runtime and every read-layer method is
//! synchronous. Calling one from a task on this crate's runtime would nest `block_on` inside an
//! async worker, which panics. So **every handler hands its read to
//! [`tokio::task::spawn_blocking`]**, and the engine sits behind a mutex because it is a
//! single-threaded resource by construction — the same shape `daemon`'s sampler already uses for
//! the same reason.
//!
//! The consequence is stated rather than hidden: **reads serialize.** At the ≤500-GPU envelope,
//! against a console with one operator, that is the correct trade — a second engine connection
//! per request would multiply the connection count the engine design bounds. The revisit trigger is more
//! than one concurrent viewer mattering, at which point the answer is a small pool of
//! connections, not an async DAO.
//!
//! # Where to start
//!
//! - [`serve`] binds and serves; [`ApiConfig`] and [`ApiState`] are what the daemon's shell
//!   hands it. `routes` is the handler table, thin by law: one DAO call each.
//! - The contract is `docs/read-api.md` and `contract/openapi.json` is derived from the wire
//!   types in [`wire`] — CI diffs it. [`auth`] is the session realm; `sse` the event stream;
//!   [`console`] the embedded console bundle, pinned by content hash.

pub mod auth;
pub mod console;
pub mod cursor;
pub mod envelope;
pub mod error;
pub mod extract;
pub mod health;
pub mod openapi;
mod pagination;
mod requests;
mod resources;
pub(crate) mod routes;
pub mod serve;
pub(crate) mod sse;
pub mod wire;

pub use error::Error;

use analyzer_analysis::Params;
use analyzer_pipeline::parquet_source::SourceConfig;
use analyzer_serving_clickhouse::engine::Engine;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// How the listener is configured. Every field is a decision the operator makes, and every
/// default is the conservative one.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// **Loopback by default**, the same posture and the same reasoning as `--metrics-addr`:
    /// reaching this from elsewhere is an explicit decision, stated in config rather than
    /// defaulted.
    ///
    /// **Not 8443.** The listener speaks plaintext and sits behind the operator's proxy, and a
    /// port that reads as TLS to every operator and scanner alive is a lie in the one place
    /// nobody re-reads.
    pub addr: SocketAddr,
    /// Where a console bundle lives on disk.
    ///
    /// **Not reachable from the CLI.** The shipped console is compiled into the binary, and we
    /// vend no console directory for an operator to point a flag at. This is a library field so
    /// the fixture test can exercise the file service.
    /// `None` makes the mount answer a decided `console_not_configured` rather than a 404 — a
    /// missing bundle is a deployment fact.
    pub console_dir: Option<PathBuf>,
    /// The ONE dev-mode cross-origin allowance, off unless set. Exactly one origin, echoed
    /// literally, never a wildcard and never a pattern.
    pub dev_origin: Option<String>,
    /// Emit `Secure` on the session cookie. **Off by default**, and the reasoning is on
    /// `auth::session_cookie_header`: this listener speaks plaintext, and Safari
    /// discards a `Secure` cookie over `http://localhost` entirely, which silently breaks sign-in
    /// on both the demo kit and the SSH-tunnel topology. A deployment that terminates TLS in front
    /// turns it on with `--api-cookie-secure on`.
    pub cookie_secure: bool,
    /// Where the viewer token digests live. `0600`, and no plaintext ever reaches it.
    pub token_store: PathBuf,
}

/// Where the viewer-token digests live, derived from the data directory.
///
/// **One definition, because two callers need it and they must not disagree**: `token issue`
/// writes it from the CLI without starting an engine, and the daemon reads it in serve mode. A
/// path assembled twice is a path that eventually differs by a directory.
#[must_use]
pub fn token_store_for(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("api-tokens")
}

/// Where the first-run viewer token's PLAINTEXT is left, derived from the data directory.
///
/// Same rule as [`token_store_for`] and the same reason: a path assembled twice is a path that
/// eventually differs by a directory, and this one an operator is told to `cat`.
///
/// It sits beside the digest store rather than somewhere tidier because the credential family
/// belongs in one place — an operator auditing what secrets a data directory holds should find
/// them adjacent, not one in `<data-dir>` and one under `<out>`.
#[must_use]
pub fn initial_token_for(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("initial-token")
}

impl ApiConfig {
    /// The default bind: loopback, port 8088.
    #[must_use]
    pub fn new(token_store: PathBuf) -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 8088)),
            console_dir: None,
            dev_origin: None,
            cookie_secure: false,
            token_store,
        }
    }
}

/// The annotation-object writer the shell injects: key and bytes in, durable object out.
/// A closure rather than a store handle, so the api crate keeps zero edges to the object
/// store and the sink's prefix scoping lives with the store's owner.
pub(crate) type AnnotationSink = std::sync::Arc<
    dyn Fn(&str, &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
>;

/// Everything a handler can reach.
///
/// **One fleet, held rather than routed.** Every query in this product is fleet-scoped and the
/// daemon serves exactly one; a fleet path parameter would be a second place to get it wrong and
/// would invite a consumer to believe it could ask about another.
#[derive(Clone)]
pub struct ApiState {
    engine: Arc<Mutex<Engine>>,
    /// The derivation source the DAO's raw-grain reads open — the same segments, cache and
    /// horizon the derivation reads from.
    sources: SourceConfig,
    fleet_id: String,
    params: Params,
    /// The daemon's validated retention, read through to every DAO the API builds.
    retention: analyzer_serving_clickhouse::engine::RetentionConfig,
    /// The writer's lake spine, where the execution journal lives: every read's consistency
    /// boundary reads that journal.
    spine: Arc<analyzer_pipeline::spine::Spine>,
    /// The serving store the reads answer from, as the executor's port.
    serving: Arc<dyn analyzer_pipeline::publish::Serving>,
    pub(crate) sessions: Arc<auth::Sessions>,
    pub(crate) tokens: Arc<auth::TokenStore>,
    pub(crate) events: sse::Events,
    pub(crate) config: ApiConfig,
    /// What the daemon publishes about its own loop. Behind a lock because the loop writes it on
    /// its cadence while handlers read it on theirs.
    hints: Arc<Mutex<health::LoopHints>>,
    /// The degradation ledger as of the last completed pass, published by the loop through
    /// the same composition-root seam as the hints: health answers from
    /// THIS, window-filtered in memory, and never walks the raw source under a request.
    ledger: Arc<Mutex<Option<analyzer_analysis::degradation::Ledger>>>,
    /// The daemon's analyze-now handle, or `None` in a process that runs no loop.
    kick: Arc<Mutex<Option<std::sync::Arc<std::sync::atomic::AtomicBool>>>>,
    /// The annotation-object writer, or `None` where no bucket is attached — the daemon
    /// injects a closure over its own store (`Daemon::annotation_sink`), so this crate has
    /// no edge to the object store; a process without one refuses a declaration.
    annotation_sink: Arc<Mutex<Option<AnnotationSink>>>,
    /// The clock, injected. **Not `SystemTime::now()` at the call site**, for the same reason
    /// no wall-clock function enters query text: a test that cannot fix the clock is a test
    /// that races one.
    clock: Option<f64>,
    /// The admission bound on reads: at most [`READ_CONCURRENCY`] blocking reads in flight
    /// before a request is refused rather than queued behind the one serialised engine.
    reads: Arc<tokio::sync::Semaphore>,
}

/// How many reads may be in flight at once. Every read serialises on one engine connection,
/// so the bound is not parallelism but the queue's length: past it a request is refused at
/// once with `engine_unavailable` instead of waiting behind every slower read for a turn that
/// may come minutes later. Eight is a console's whole document plus headroom for a second
/// reader; a deployment with more concurrent readers wants a second connection, not a longer
/// queue.
pub const READ_CONCURRENCY: usize = 8;

/// How long a read may take before its request is answered `engine_unavailable`. The engine's
/// own call ceiling bounds a pass at thirty minutes; a serving read that has not answered in
/// two minutes is not one a console is still waiting for. The blocking work runs to its end
/// either way (it holds the engine, and nothing can interrupt a statement), so the deadline
/// bounds the caller's wait, not the engine's.
pub const READ_DEADLINE: std::time::Duration = std::time::Duration::from_mins(2);

impl ApiState {
    /// Bind the API to an engine, the writer's spine and serving store, and the parameters its
    /// figures are read under. The fleet is the spine's namespace's.
    ///
    /// # Errors
    /// The token store cannot be read or created.
    #[allow(clippy::too_many_arguments)] // each a distinct binding the daemon's shell hands in
    pub fn new(
        engine: Arc<Mutex<Engine>>,
        (spine, serving): (
            Arc<analyzer_pipeline::spine::Spine>,
            Arc<dyn analyzer_pipeline::publish::Serving>,
        ),
        sources: SourceConfig,
        params: &Params,
        retention: analyzer_serving_clickhouse::engine::RetentionConfig,
        config: ApiConfig,
    ) -> Result<Self, Error> {
        Ok(Self {
            engine,
            sources,
            fleet_id: spine.namespace().fleet().to_string(),
            params: params.clone(),
            retention,
            spine,
            serving,
            sessions: Arc::new(auth::Sessions::default()),
            tokens: Arc::new(auth::TokenStore::open(&config.token_store)?),
            events: sse::Events::new(),
            config,
            hints: Arc::new(Mutex::new(health::LoopHints::default())),
            ledger: Arc::new(Mutex::new(None)),
            kick: Arc::new(Mutex::new(None)),
            annotation_sink: Arc::new(Mutex::new(None)),
            clock: None,
            reads: Arc::new(tokio::sync::Semaphore::new(READ_CONCURRENCY)),
        })
    }

    /// The stream handlers publish to and `GET /v1/events` subscribes to.
    #[must_use]
    pub fn events(&self) -> sse::Events {
        self.events.clone()
    }

    /// Run `f` against a `Dao` on a blocking thread.
    ///
    /// **The one path from a handler to the read layer**, for the reason in this crate's header:
    /// the DAO is synchronous and the engine drives its own runtime, so a read taken on an async
    /// worker would nest one runtime inside another. Routing every read through here means a
    /// future handler cannot make that mistake by accident — the same shape as `quote_literal`
    /// being the only path to a statement.
    ///
    /// Admission is bounded ([`READ_CONCURRENCY`]) and the wait has a deadline
    /// ([`READ_DEADLINE`]); both refuse with `engine_unavailable`, the 503 a consumer already
    /// retries. The permit travels with the blocking work, so a caller that stopped waiting
    /// still counts against the bound until its statement returns.
    ///
    /// # Errors
    /// Whatever `f` returns, a refusal at the bound or the deadline, or a join failure if the
    /// blocking thread panicked.
    pub(crate) async fn read<T, F>(&self, f: F) -> Result<T, error::ApiError>
    where
        T: Send + 'static,
        F: FnOnce(
                &analyzer_serving_clickhouse::Dao,
            ) -> Result<T, analyzer_serving_clickhouse::Error>
            + Send
            + 'static,
    {
        let Ok(permit) = Arc::clone(&self.reads).try_acquire_owned() else {
            return Err(error::ApiError::engine(format!(
                "{READ_CONCURRENCY} reads are already in flight on this analyzer's one \
                 connection; retry shortly"
            )));
        };
        let engine = Arc::clone(&self.engine);
        let sources = self.sources.clone();
        let params = self.params.clone();
        let retention = self.retention;
        let spine = Arc::clone(&self.spine);
        let serving = Arc::clone(&self.serving);
        let work = tokio::task::spawn_blocking(move || {
            let _held = permit;
            let guard = engine.lock().map_err(|_| {
                error::ApiError::engine("the engine lock was poisoned by a previous panic")
            })?;
            let dao =
                analyzer_serving_clickhouse::Dao::new(&guard, sources, &params, spine, serving)
                    .with_retention(retention);
            f(&dao).map_err(|e| match e {
                // The one refusal the read layer makes by name: a tier this cut, scope or
                // window cannot be served at. Everything else is the engine not answering.
                analyzer_serving_clickhouse::Error::GrainUnavailable(message) => {
                    error::ApiError::new(error::Cause::GrainUnavailable, message)
                }
                other => error::ApiError::engine(other.to_string()),
            })
        });
        match tokio::time::timeout(READ_DEADLINE, work).await {
            Ok(joined) => {
                joined.map_err(|e| error::ApiError::engine(format!("read task failed: {e}")))?
            }
            Err(_) => Err(error::ApiError::engine(format!(
                "the read did not answer within {}s; the engine is still working it and this \
                 request is not waiting for it",
                READ_DEADLINE.as_secs()
            ))),
        }
    }

    /// Attach the daemon's analyze-now handle. The shell calls this once at startup in
    /// the deployment form; a serve-mode process never does, and the endpoint says so
    /// in band rather than pretending a loop exists.
    pub fn set_tick_kick(&self, kick: std::sync::Arc<std::sync::atomic::AtomicBool>) {
        if let Ok(mut slot) = self.kick.lock() {
            *slot = Some(kick);
        }
    }

    pub(crate) fn tick_kick(&self) -> Option<std::sync::Arc<std::sync::atomic::AtomicBool>> {
        self.kick.lock().ok().and_then(|slot| slot.clone())
    }

    /// Attach the annotation-object writer. The shell calls this once at startup in the
    /// deployment form, through the `tick_kick` seam and for the same reason: the loop's
    /// vocabulary meets the API's in the shell, and neither crate depends on the other.
    pub fn set_annotation_sink(&self, sink: AnnotationSink) {
        if let Ok(mut slot) = self.annotation_sink.lock() {
            *slot = Some(sink);
        }
    }

    pub(crate) fn annotation_sink(&self) -> Option<AnnotationSink> {
        self.annotation_sink
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// Publish the degradation ledger the loop derived, for the read API to qualify figures with.
    pub fn publish_ledger(&self, ledger: Option<analyzer_analysis::degradation::Ledger>) {
        if let Ok(mut slot) = self.ledger.lock() {
            *slot = ledger;
        }
    }

    pub(crate) fn pass_ledger(&self) -> Option<analyzer_analysis::degradation::Ledger> {
        self.ledger.lock().ok().and_then(|slot| slot.clone())
    }

    /// Publish the loop's hints for the health endpoint.
    pub fn publish_loop_hints(&self, hints: health::LoopHints) {
        if let Ok(mut slot) = self.hints.lock() {
            *slot = hints;
        }
    }

    pub(crate) fn loop_hints(&self) -> health::LoopHints {
        self.hints.lock().map(|h| h.clone()).unwrap_or_default()
    }

    /// The ledger horizon this box declares: the configured `ledger_retention_days`, never
    /// the compile-time default. It is stated beside the measured reach as a second labelled
    /// number and never substitutes for it.
    pub(crate) fn declared_ledger_days(&self) -> u32 {
        self.retention.ledger_retention_days
    }

    /// Wall now, or the fixed clock a test injected.
    pub(crate) fn now_ms(&self) -> f64 {
        self.clock.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
        })
    }

    /// Fix the clock, so a test asserting a lag figure does not race a real one.
    #[must_use]
    pub fn with_clock(mut self, now_ms: f64) -> Self {
        self.clock = Some(now_ms);
        self
    }

    /// The token store, for tests that need to mint a credential through the shipped path
    /// rather than reaching around it.
    #[must_use]
    pub fn tokens_for_test(&self) -> Arc<auth::TokenStore> {
        Arc::clone(&self.tokens)
    }

    /// The session table, same reason.
    #[must_use]
    pub fn sessions_for_test(&self) -> Arc<auth::Sessions> {
        Arc::clone(&self.sessions)
    }

    #[must_use]
    /// The fleet this API serves.
    pub fn fleet_id(&self) -> &str {
        &self.fleet_id
    }

    #[must_use]
    /// The derivation parameters the served figures were derived under.
    pub fn params(&self) -> &Params {
        &self.params
    }
}

/// Assemble the router.
///
/// **The route list is the endpoint list is the DAO method list is the panel grid.** A route
/// that is not on that list has no DAO method to call, which is the shortest way to notice it.
pub fn router(state: ApiState) -> axum::Router {
    routes::router(state)
}
