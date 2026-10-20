// SPDX-License-Identifier: AGPL-3.0-only

//! Decided errors with named causes — `docs/read-api.md` section 7.
//!
//! **Every cause has a producer and a golden.** A cause with no code path is
//! contract fiction — it teaches a console to handle a state that cannot occur, and it makes the
//! enum look considered where it is merely inherited. [`Cause`] is closed, it is in the committed
//! `OpenAPI` document, and adding one is a contract diff in review.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};

/// The closed set of named causes.
///
/// **Every variant here has a producer**, and `every_named_cause_has_a_producer` (in the static
/// audit) enforces it: a cause no code path raises would teach a console to handle a state that
/// cannot occur. A cause arrives with its producer and leaves with it. There is no fleet cause:
/// this API serves one fleet held in state, with no fleet path parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// No session: no cookie, or a presented token that is not an active viewer token.
    /// `401`.
    Unauthenticated,
    /// A cookie naming a session this process no longer holds — its idle or absolute
    /// lifetime passed, an analyzer restart ended it, or the token behind it was rotated.
    /// `401`.
    SessionExpired,
    /// A holder selector that is not an `instance/dp_rank/group_idx` triple, or names one
    /// this fleet never spoke from. `404`: a holder that never spoke is not an empty page.
    UnknownHolder,
    /// A `block_id` no store in the window carries. `404`, with the window as the bound.
    UnknownBlock,
    /// A marker `id` the window does not hold. `404`, with the window as the bound.
    UnknownMarker,
    /// A `/v1` path this build does not serve — VERSION SKEW, not a missing subject.
    ///
    /// **Its own cause because the alternative is not a 404 at all.** Without it an unmatched
    /// path falls through to the console mount: on a build with no bundle it answers `503
    /// console_not_configured`, and on any release build — which embeds one — it answers
    /// `index.html` with a **200**. A JSON client asking for an endpoint this binary lacks gets a
    /// successful HTML page, which no status-code check anywhere can see, and both readings send
    /// the operator to debug a console that has nothing to do with the request.
    ///
    /// The distinction from its neighbours is the point: `unknown_holder` and friends mean the
    /// SUBJECT was not found by a route that exists. This means the ROUTE does not exist here,
    /// which is a statement about the binary's age rather than about the fleet's contents.
    UnknownRoute,
    /// A request the contract's shapes cannot read: a query string that does not
    /// deserialise (an unknown parameter, a `limit` that is not a number), a body that is
    /// not the declared JSON shape, or a path segment of the wrong type. `400`, raised by
    /// the extractors so a framework rejection never answers outside the envelope.
    RequestInvalid,
    /// A `window` that is not `<N>d`, `<N>h` or `start_ms,end_ms`, or that ends at or
    /// before it starts; or a `day` the DAO's own parser refuses. `400`.
    WindowInvalid,
    /// A `limit` outside `1..=PAGE_LIMIT_MAX`.
    ///
    /// **Its own cause rather than `window_invalid`**, which would be a lie a consumer acts on: a
    /// console narrowing `window_invalid` to re-render its date picker would do it for a paging
    /// bug, and the one thing a closed cause set buys is that the token names what went wrong.
    LimitInvalid,
    /// A `dims` entry outside the closed set (`class`, `holder`, `instance`, `workload`,
    /// `model`), or a coverage `by` that is neither `fleet` nor `instance`. `400`.
    DimUnknown,
    /// A `grain` outside `minute`, `hour`, `day`, `auto` — or `minute` asked of a cut the
    /// minute cells do not keep (a holder dim). `400`.
    GrainUnavailable,
    /// A drill that is always behind a subject got none.
    ///
    /// **Its own cause rather than `window_invalid`** — the `limit_invalid` argument verbatim: a
    /// console narrowing `window_invalid` re-renders its date picker, which is the wrong remedy
    /// for a missing `day`/`block`/`holder` selector.
    SubjectMissing,
    /// A bounded refusal: the subject is older than the oldest record this fleet holds, so
    /// event grain cannot reach it. Rides inside a `200`, with the horizon as its bound.
    OutsideRawHorizon,
    /// A bounded refusal: the span was computed under a prior join-semantics generation
    /// this binary has no semantics to explain. Inside a `200` — unexamined, never zero.
    SupersededSemantics,
    /// A bounded refusal: the marker chain does not link, so which parameters produced the
    /// span is unknown. Inside a `200` — stated, never guessed.
    UnlinkedGeneration,
    /// A bounded refusal: a cursor minted against an older stamp, or one this API cannot
    /// read back. Inside a `200` with the current pass as its bound; restart at the first
    /// page.
    CursorStale,
    /// A declaration the recorded-declaration class refuses: a non-finite instant, an empty
    /// or over-long label — the request is malformed about its own statement, which is the
    /// `400` family, one cause over from `window_invalid` and named for the same reason: the
    /// closed set's one job is that the token says what went wrong.
    AnnotationInvalid,
    /// The bucket cannot take the declaration's durable object right now. Its own cause
    /// rather than `engine_unavailable`, because a consumer narrowing that one renders "the
    /// engine is down" — the wrong sentence and the wrong remedy for an object-store outage.
    /// Nothing was recorded: the row is not written without its durable form where a bucket
    /// is attached, so a retry after the outage records exactly once.
    BucketUnavailable,
    /// The engine could not answer. `503`, never a zero: a figure this layer could not read
    /// is not a figure that measured nothing.
    EngineUnavailable,
    /// A console path on a build with no bundle compiled in. `503`: a deployment fact, the
    /// same class as an engine that cannot answer.
    ConsoleNotConfigured,
}

impl serde::Serialize for Cause {
    /// As its token. **So `Refusal.cause` and `Error.cause` are the SAME closed set on the wire**,
    /// which they were not: the in-band refusal — the path read-api section 7.2 makes PRIMARY —
    /// carried a free string while the 4xx path got the enum. A consumer narrowing a refusal had
    /// to do it against a hand-transcribed copy of a set the contract already declares.
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl utoipa::PartialSchema for Cause {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, Type};
        ObjectBuilder::new()
            .schema_type(Type::String)
            .description(Some(
                "The closed set of named causes. A consumer switches on this; it never parses \
                 the message.",
            ))
            // Read from `all()`, the same slice the error path switches on, so the document
            // cannot list a cause the code lacks or omit one it has.
            .enum_values(Some(
                Cause::all().iter().map(|c| c.as_str()).collect::<Vec<_>>(),
            ))
            .into()
    }
}

impl utoipa::ToSchema for Cause {}

/// The read layer's refusals, mapped onto the wire's closed set.
///
/// **Exhaustive, so a new `RefusalCause` in the DAO is a compile error here until somebody maps
/// it.** That is the whole reason the DAO stopped raising bare strings: a token crossing the seam
/// unchecked would have reached a console whose generated union does not contain it.
impl From<analyzer_serving_clickhouse::RefusalCause> for Cause {
    fn from(c: analyzer_serving_clickhouse::RefusalCause) -> Self {
        use analyzer_serving_clickhouse::RefusalCause as R;
        match c {
            R::SupersededSemantics => Self::SupersededSemantics,
            R::UnlinkedGeneration => Self::UnlinkedGeneration,
        }
    }
}

impl Cause {
    /// The stable token a consumer switches on.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::SessionExpired => "session_expired",
            Self::UnknownHolder => "unknown_holder",
            Self::UnknownBlock => "unknown_block",
            Self::UnknownMarker => "unknown_marker",
            Self::UnknownRoute => "unknown_route",
            Self::RequestInvalid => "request_invalid",
            Self::WindowInvalid => "window_invalid",
            Self::LimitInvalid => "limit_invalid",
            Self::DimUnknown => "dim_unknown",
            Self::GrainUnavailable => "grain_unavailable",
            Self::SubjectMissing => "subject_missing",
            Self::OutsideRawHorizon => "outside_raw_horizon",
            Self::SupersededSemantics => "superseded_semantics",
            Self::UnlinkedGeneration => "unlinked_generation",
            Self::CursorStale => "cursor_stale",
            Self::AnnotationInvalid => "annotation_invalid",
            Self::BucketUnavailable => "bucket_unavailable",
            Self::EngineUnavailable => "engine_unavailable",
            Self::ConsoleNotConfigured => "console_not_configured",
        }
    }

    /// Every cause, for the `OpenAPI` document and for the test that pins each one to a producer.
    #[must_use]
    pub fn all() -> &'static [Cause] {
        &[
            Self::Unauthenticated,
            Self::SessionExpired,
            Self::UnknownHolder,
            Self::UnknownBlock,
            Self::UnknownMarker,
            Self::UnknownRoute,
            Self::RequestInvalid,
            Self::WindowInvalid,
            Self::LimitInvalid,
            Self::DimUnknown,
            Self::GrainUnavailable,
            Self::SubjectMissing,
            Self::OutsideRawHorizon,
            Self::SupersededSemantics,
            Self::UnlinkedGeneration,
            Self::CursorStale,
            Self::AnnotationInvalid,
            Self::BucketUnavailable,
            Self::EngineUnavailable,
            Self::ConsoleNotConfigured,
        ]
    }

    /// The status this cause answers with.
    ///
    /// **A bounded refusal is a RESULT, not an error status**, and that decision lives
    /// here rather than at each handler so it cannot be made twice differently:
    ///
    /// - a well-formed request whose subject exists, where a DATA BOUND prevents an answer,
    ///   is `200` with the refusal in the data position — the console renders it as a designed
    ///   state, which is what day.html already does for a day past the raw horizon;
    /// - a subject that never existed is `404`;
    /// - a malformed request is `400`;
    /// - no session is `401`;
    /// - an engine that cannot answer is `503`, never a zero.
    ///
    /// The bounded-refusal causes therefore never reach this function in an error body at all;
    /// they ride inside a 200. They are listed with `OK` so the mapping is total and so a future
    /// handler that does try to raise one as an error gets the honest status rather than a
    /// default.
    #[must_use]
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unauthenticated | Self::SessionExpired => StatusCode::UNAUTHORIZED,
            Self::UnknownHolder | Self::UnknownBlock | Self::UnknownMarker | Self::UnknownRoute => {
                StatusCode::NOT_FOUND
            }
            Self::RequestInvalid
            | Self::WindowInvalid
            | Self::LimitInvalid
            | Self::DimUnknown
            | Self::GrainUnavailable
            | Self::SubjectMissing
            | Self::AnnotationInvalid => StatusCode::BAD_REQUEST,
            // A missing console bundle is a DEPLOYMENT fact, the same class as an engine that
            // cannot answer: the thing asked for exists, and this instance cannot serve it yet.
            Self::EngineUnavailable | Self::BucketUnavailable | Self::ConsoleNotConfigured => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            // Bounded refusals: results, carried inside a 200 by the handler that produces them.
            //
            // **`CursorStale` is a result, not a 400.** Nothing about the request is malformed:
            // the token was correct when it was minted and the WORLD moved underneath it, which
            // is the definition of a bounded refusal one line up. As a 400 the whole response
            // would be an error body, so a console would have to BLANK the panel to say "your
            // page expired"; as a result it keeps the heading, the columns and the stamp, and
            // replaces only the rows. The bound `cursor.rs` attaches — the current tick and
            // generation — is what makes that render possible.
            Self::CursorStale
            | Self::OutsideRawHorizon
            | Self::SupersededSemantics
            | Self::UnlinkedGeneration => StatusCode::OK,
        }
    }
}

/// A refusal, with the facts that make it actionable.
///
/// **A refusal that does not state its bound is the vague 404 this shape exists to prevent.**
/// `bound` and `requested` are what let a console render "the local drill window is a byte budget and the bucket holds the rest;
/// is older than the oldest record this fleet holds" instead of "not found".
#[derive(Debug, Clone)]
pub struct ApiError {
    /// The named cause, from the closed set — what a consumer switches on.
    pub cause: Cause,
    /// The sentence for a human. A consumer never parses it.
    pub message: String,
    /// The facts the refusal is measured against — a horizon, a window, a pass. Absent only
    /// where the cause is self-describing.
    pub bound: Option<Value>,
    /// What was asked for, echoed beside the bound so the two read together.
    pub requested: Option<Value>,
}

impl ApiError {
    /// An error with its cause and message and no facts yet; `with_bound` and
    /// `with_requested` add them.
    #[must_use]
    pub fn new(cause: Cause, message: impl Into<String>) -> Self {
        Self {
            cause,
            message: message.into(),
            bound: None,
            requested: None,
        }
    }

    /// The engine could not answer. **Never a zero** — a figure this layer could not read is not
    /// a figure that measured nothing.
    #[must_use]
    pub fn engine(message: impl Into<String>) -> Self {
        Self::new(Cause::EngineUnavailable, message)
    }

    /// Attach the bound — the fact the refusal is measured against.
    #[must_use]
    pub fn with_bound(mut self, bound: Value) -> Self {
        self.bound = Some(bound);
        self
    }

    /// Attach what was asked for, to read beside the bound.
    #[must_use]
    pub fn with_requested(mut self, requested: Value) -> Self {
        self.requested = Some(requested);
        self
    }

    /// The body, shared with the bounded-refusal path so a refusal reads identically whether it
    /// arrives as a 200's `refused` or as a 4xx's `error`.
    #[must_use]
    pub fn body(&self) -> Value {
        let mut v = json!({ "cause": self.cause.as_str(), "message": self.message });
        if let Some(b) = &self.bound {
            v["bound"] = b.clone();
        }
        if let Some(r) = &self.requested {
            v["requested"] = r.clone();
        }
        v
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.cause.status(),
            axum::Json(json!({ "error": self.body() })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{ApiError, Cause};
    use axum::http::StatusCode;

    #[test]
    fn every_cause_has_a_distinct_token_and_all_of_them_are_listed() {
        let mut tokens: Vec<&str> = Cause::all().iter().map(|c| c.as_str()).collect();
        let total = tokens.len();
        tokens.sort_unstable();
        tokens.dedup();
        assert_eq!(tokens.len(), total, "two causes share a token");
        // `all()` is what the OpenAPI document enumerates, so a cause missing from it would be a
        // cause the contract never mentions. The count makes any change to the set a deliberate
        // diff, reviewed with the contract.
        assert_eq!(total, 20, "the closed set changed; that is a contract diff");
    }

    #[test]
    fn a_bounded_refusal_is_a_result_and_a_missing_subject_is_not() {
        // Read-api section 7.2's decision, pinned where it is made rather than at twenty call sites.
        assert_eq!(Cause::OutsideRawHorizon.status(), StatusCode::OK);
        assert_eq!(Cause::SupersededSemantics.status(), StatusCode::OK);
        assert_eq!(Cause::UnknownHolder.status(), StatusCode::NOT_FOUND);
        assert_eq!(Cause::WindowInvalid.status(), StatusCode::BAD_REQUEST);
        assert_eq!(Cause::Unauthenticated.status(), StatusCode::UNAUTHORIZED);
        // Never a zero, never a 200 with empty figures.
        assert_eq!(
            Cause::EngineUnavailable.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn a_refusal_carries_its_bound_so_it_is_actionable() {
        let e = ApiError::new(
            Cause::OutsideRawHorizon,
            "older than the oldest record held",
        )
        .with_bound(serde_json::json!({ "raw_from_day": "2026-07-21" }))
        .with_requested(serde_json::json!({ "day": "2026-06-02" }));
        let body = e.body();
        assert_eq!(body["cause"], "outside_raw_horizon");
        assert_eq!(body["bound"]["raw_from_day"], "2026-07-21");
        assert_eq!(body["requested"]["day"], "2026-06-02");
    }
}

/// What the shell sees when this crate cannot start or cannot keep its own files: the
/// token store, the bootstrap secret, the listener and its runtime, a lock a panic
/// poisoned, and the crates below forwarded unchanged. Distinct from [`ApiError`], which
/// is what a client sees on the wire. Every message is exactly what the string it
/// replaced said.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The token store or a secret file: unreadable, unwritable, or a line that does not
    /// parse; a token asked for by an id nothing active carries.
    #[error("{0}")]
    Auth(String),
    /// The read API listener: the address would not bind, the socket would not prepare,
    /// the runtime would not build, or serving ended in an error.
    #[error("{0}")]
    Listener(String),
    /// A lock a previous panic poisoned, named.
    #[error("{0}")]
    Poisoned(&'static str),
    /// The engine boundary failed; the error is the storage crate's.
    #[error(transparent)]
    Engine(#[from] analyzer_serving_clickhouse::engine::Error),
    /// The read layer or the declared-annotation write failed; the error is the dao's.
    #[error(transparent)]
    Dao(#[from] analyzer_serving_clickhouse::Error),
}
