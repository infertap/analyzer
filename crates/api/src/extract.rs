// SPDX-License-Identifier: AGPL-3.0-only

//! The request extractors, each refusing in the contract's own envelope.
//!
//! Axum's `Query`, `Json` and `Path` refuse a request they cannot parse with a plain-text
//! body and a status of their own choosing, outside the JSON error envelope every documented
//! response uses. A generated client then meets a failure the specification never mentioned.
//! These three wrap them and translate every rejection into `request_invalid`, a `400` in the
//! envelope, with the extractor's own sentence as the message, so a malformed query string, a
//! body that is not the declared shape, or a path segment of the wrong type is a documented
//! outcome.
//!
//! `Params` checks query names against the matched `OpenAPI` operation, then deserializes
//! into that operation's raw type. Semantic constructors in `requests` validate before I/O.
//! Even queryless operations extract `NoQuery`, so the same protection covers every route.

use crate::error::{ApiError, Cause};
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::request::Parts;
use serde::de::DeserializeOwned;

/// The query string, deserialised into the endpoint's parameter type.
#[derive(Debug, Clone, Copy, Default)]
pub struct Params<T>(pub T);

impl<S, T> FromRequestParts<S> for Params<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        let matched = parts
            .extensions
            .get::<axum::extract::MatchedPath>()
            .map_or("", axum::extract::MatchedPath::as_str);
        // Axum serves HEAD through the registered GET handler and removes its body.
        let method = if parts.method == axum::http::Method::HEAD {
            "GET"
        } else {
            parts.method.as_str()
        };
        let declared = crate::openapi::declared_query_parameters(method, matched);
        if let Some(undeclared) = query_names(parts.uri.query().unwrap_or(""))
            .into_iter()
            .find(|name| !declared.contains(name))
        {
            return Err(invalid(format!(
                "`{undeclared}` is not a parameter of {} {matched}",
                parts.method
            )));
        }
        axum::extract::Query::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Query(q)| Self(q))
            .map_err(|e| {
                invalid(format!(
                    "the query string does not parse: {}",
                    e.body_text()
                ))
            })
    }
}

/// The parameter names a query string carries, decoded as the deserializer decodes them
/// (`%XX` escapes and `+` for a space), empty pairs skipped.
fn query_names(query: &str) -> Vec<String> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| pair.split_once('=').map_or(pair, |(name, _)| name))
        .map(percent_decode)
        .collect()
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let escape = (bytes[i] == b'%' && i + 2 < bytes.len())
            .then(|| u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?, 16).ok())
            .flatten();
        match (bytes[i], escape) {
            (b'+', _) => out.push(b' '),
            (_, Some(byte)) => {
                out.push(byte);
                i += 2;
            }
            (byte, None) => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A JSON body, deserialised into the endpoint's body type.
#[derive(Debug, Clone, Copy, Default)]
pub struct Body<T>(pub T);

impl<S, T> FromRequest<S> for Body<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, ApiError> {
        axum::Json::<T>::from_request(req, state)
            .await
            .map(|axum::Json(b)| Self(b))
            .map_err(|e| invalid(format!("the body does not parse: {}", e.body_text())))
    }
}

/// The path's captured segments, deserialised into the endpoint's subject type.
#[derive(Debug, Clone, Copy, Default)]
pub struct Subject<T>(pub T);

impl<S, T> FromRequestParts<S> for Subject<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        axum::extract::Path::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Path(p)| Self(p))
            .map_err(|e| invalid(format!("the path does not parse: {}", e.body_text())))
    }
}

fn invalid(message: String) -> ApiError {
    ApiError::new(Cause::RequestInvalid, message)
}

#[cfg(test)]
mod tests {
    use super::{percent_decode, query_names, Params};
    use axum::routing::get;
    use axum::Router;
    use tower::ServiceExt as _;

    #[derive(serde::Deserialize)]
    struct P {
        window: Option<String>,
        class: Option<String>,
    }

    /// Two documented operations and one the document does not know, each taking the shared
    /// parameter shape, so what decides a request is the declaration and not the type.
    fn app() -> Router {
        let ok = |Params(p): Params<P>| async move {
            format!(
                "ok window={} class={}",
                p.window.is_some(),
                p.class.is_some()
            )
        };
        Router::new()
            .route("/v1/coverage", get(ok))
            .route("/v1/holders", get(ok))
            .route("/v1/nowhere", get(ok))
    }

    async fn status_and_cause(path: &str) -> (u16, String) {
        let request = axum::http::Request::get(path)
            .body(axum::body::Body::empty())
            .expect("request");
        let response = app().oneshot(request).await.expect("response");
        let status = response.status().as_u16();
        let body = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .expect("body");
        let cause = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v["error"]["cause"].as_str().map(str::to_string))
            .unwrap_or_default();
        (status, cause)
    }

    #[tokio::test]
    async fn a_parameter_another_endpoint_takes_is_refused_here() {
        // `class` is a holders filter; coverage declares no such thing, and accepting it would
        // answer the unfiltered question under a filtered request.
        assert_eq!(
            status_and_cause("/v1/coverage?window=14d&class=cold").await,
            (400, "request_invalid".to_string())
        );
        assert_eq!(
            status_and_cause("/v1/holders?window=14d&class=cold")
                .await
                .0,
            200
        );
        assert_eq!(status_and_cause("/v1/coverage?window=14d").await.0, 200);
    }

    #[tokio::test]
    async fn an_operation_the_document_does_not_declare_accepts_no_parameter() {
        assert_eq!(status_and_cause("/v1/nowhere").await.0, 200);
        assert_eq!(status_and_cause("/v1/nowhere?window=14d").await.0, 400);
    }

    #[tokio::test]
    async fn a_name_is_judged_as_the_deserializer_reads_it() {
        // Percent-encoded and with an empty pair: what serde would accept, this accepts.
        assert_eq!(status_and_cause("/v1/coverage?&wind%6fw=14d&").await.0, 200);
        assert_eq!(
            status_and_cause("/v1/coverage?wind%6fw=14d&cl%61ss=x")
                .await
                .0,
            400
        );
    }

    #[test]
    fn query_names_decode_and_skip_empties() {
        assert_eq!(
            query_names("a=1&&b%20c=2&+d&e"),
            vec!["a", "b c", " d", "e"]
        );
        assert_eq!(percent_decode("%zz%4"), "%zz%4");
        assert_eq!(percent_decode("x%"), "x%");
    }
}
