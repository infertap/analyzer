// SPDX-License-Identifier: AGPL-3.0-only

//! Shared pagination result translation.
use crate::error::ApiError;
use crate::wire;
pub(crate) fn refused_page(limit: u32) -> wire::Page {
    wire::Page {
        limit,
        returned: 0,
        total_rows: None,
        next_cursor: None,
    }
}
/// An expected availability result. Only the shared protocol can construct it.
pub(crate) struct Unavailable(ApiError);
impl Unavailable {
    pub fn into_wire(self) -> wire::Refusal {
        let e = self.0;
        wire::Refusal {
            cause: e.cause,
            message: e.message,
            bound: e.bound,
            requested: e.requested,
        }
    }
}

pub(crate) enum Outcome<T> {
    Available { data: T, paging: Completed },
    Unavailable(Unavailable),
}

/// Proof that the page read crossed the consistency boundary successfully.
/// A resource cannot obtain this by minting a cursor or constructing a struct literal.
pub(crate) struct Completed {
    stamp: crate::envelope::Stamp,
    question: crate::cursor::QueryIdentity,
    limit: u32,
}
impl Completed {
    pub fn page(
        &self,
        returned: usize,
        total_rows: Option<usize>,
        next: Option<String>,
    ) -> wire::Page {
        wire::Page {
            limit: self.limit,
            returned,
            total_rows,
            next_cursor: next
                .map(|p| crate::cursor::Cursor::mint(&self.stamp, &self.question, &p).encode()),
        }
    }
}

/// Validated size and original query spelling. Tokens bind explicit defaults distinctly.
/// This map is a token encoding detail, never an application request or parameter allowlist.
pub(crate) struct Input {
    limit: u32,
    token: Option<crate::cursor::Decoded>,
    spelling: serde_json::Value,
}
impl Input {
    pub fn limit(&self) -> u32 {
        self.limit
    }
    pub fn new(
        raw: &impl serde::Serialize,
        token: Option<&str>,
        limit: u32,
    ) -> Result<Self, ApiError> {
        Ok(Self {
            limit,
            token: token.map(crate::cursor::Decoded::parse).transpose()?,
            spelling: serde_json::to_value(raw).expect("query serializes"),
        })
    }
    pub fn is_first(&self) -> bool {
        self.token.is_none()
    }
    pub fn question(
        &self,
        surface: &str,
        window: &analyzer_analysis::Window,
        subject: &[&str],
    ) -> crate::cursor::QueryIdentity {
        // Transport spelling stays strict (omitted and explicit defaults remain distinct),
        // but relative windows are represented only by their resolved instants.
        let mut filters = self.spelling.clone();
        let fields = filters
            .as_object_mut()
            .expect("an operation query is an object");
        fields.remove("window");
        fields.remove("cursor");
        let identity = serde_json::to_string(&(
            surface,
            window.start_ms,
            window.end_ms,
            self.limit,
            filters,
            subject,
        ))
        .expect("validated finite query");
        crate::cursor::QueryIdentity::of(&[identity])
    }

    fn cursor(
        &self,
        stamp: &crate::envelope::Stamp,
        question: &crate::cursor::QueryIdentity,
    ) -> Result<Option<String>, ApiError> {
        self.token
            .as_ref()
            .map(|t| t.bind(stamp, question).map(|c| c.position))
            .transpose()
    }
}

impl Input {
    pub async fn execute<P, T, F>(
        &self,
        state: &crate::ApiState,
        stamp: &crate::envelope::Stamp,
        question: crate::cursor::QueryIdentity,
        decode: fn(&str) -> Result<P, ApiError>,
        fetch: F,
    ) -> Result<Outcome<T>, ApiError>
    where
        P: Send + 'static,
        T: Send + 'static,
        F: FnOnce(
                &analyzer_serving_clickhouse::Dao,
                Option<P>,
            ) -> Result<T, analyzer_serving_clickhouse::Error>
            + Send
            + 'static,
    {
        let position = self
            .cursor(stamp, &question)
            .and_then(|p| p.map(|s| decode(&s)).transpose());
        let read = match position {
            Ok(after) => state.read_at(stamp, move |dao| fetch(dao, after)).await,
            Err(e) => Err(e),
        };
        match read {
            Ok(data) => Ok(Outcome::Available {
                data,
                paging: Completed {
                    stamp: stamp.clone(),
                    question,
                    limit: self.limit,
                },
            }),
            Err(e) if e.cause == crate::error::Cause::CursorStale => {
                Ok(Outcome::Unavailable(Unavailable(e)))
            }
            Err(e) => Err(e),
        }
    }
}

impl crate::ApiState {
    /// Read against a durable execution revision, checked before and after the query.
    /// Active writers, unfinished scopes and asynchronous retirement withhold the claim.
    /// The same boundary applies with or without a daemon in this process.
    ///
    /// # Errors
    /// Engine failure or `cursor_stale` if the bound revision or tick changed.
    async fn read_at<T, F>(&self, stamp: &crate::envelope::Stamp, f: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(
                &analyzer_serving_clickhouse::Dao,
            ) -> Result<T, analyzer_serving_clickhouse::Error>
            + Send
            + 'static,
    {
        let Some(revision) = stamp.revision else {
            return Err(crate::cursor::stale(
                stamp,
                "execution output changed or has unfinished writes",
            ));
        };
        let bound = (
            stamp.tick_at_ms.map(f64::to_bits),
            stamp.classified_to_ms.map(f64::to_bits),
        );
        let (out, after) = self
            .read(move |dao| {
                let before = dao.revision()?;
                let out = f(dao)?;
                let edges = dao.edges()?;
                let after = (
                    edges.last_tick_at_ms.map(f64::to_bits),
                    edges.classified_to_ms.map(f64::to_bits),
                );
                let finished = dao.revision()?;
                Ok(((out, before, finished), after))
            })
            .await?;
        let (out, before, finished) = out;
        if finished != Some(revision) {
            return Err(crate::cursor::stale(
                stamp,
                "execution output changed or has unfinished writes",
            ));
        }
        if after != bound {
            return Err(crate::cursor::stale(
                stamp,
                "a tick was applied while this page was being read",
            ));
        }
        if before.is_none() || before != finished {
            return Err(crate::cursor::stale(
                stamp,
                "classification is unfinished or changed during the read",
            ));
        }
        Ok(out)
    }
}
