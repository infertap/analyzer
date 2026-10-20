// SPDX-License-Identifier: AGPL-3.0-only

//! What the derivation and its reads can fail with.
//!
//! Most of what fails here fails below — the engine refusing a statement, a source that
//! did not load, the store, a segment, the ingest ledger — and those pass through unchanged
//! as their own crates' errors. What is this crate's own: the stop request, which a caller
//! matches to tell a cancelled pass from a broken one; the lake spine's chunk format; an
//! engine row that does not read back as its declared shape; and the derivation's own account of
//! anything else, with the context it adds (`Pass`). Every message is exactly the string it
//! replaced, so the journal reads the same.

/// What the derivation and its reads can fail with.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// State belongs to a different namespace, module, semantic generation or schema.
    #[error(
        "incompatible execution state: namespace, module, semantic generation or schema differs"
    )]
    IncompatibleState,
    /// Required input membership is unavailable or cannot be reconstructed.
    #[error("required input history for day {day} is unavailable; output remains uncommitted")]
    UnavailableHistory {
        /// UTC output day whose required history cannot be established.
        day: i64,
    },
    /// The serving store failed; the error is its implementation's.
    #[error("serving store: {0}")]
    Serving(#[source] std::sync::Arc<dyn std::error::Error + Send + Sync + 'static>),
    /// An event source failed; the error is the analysis crate's.
    #[error(transparent)]
    Source(#[from] analyzer_analysis::Error),
    /// The object store failed; the error is the objectstore crate's.
    #[error(transparent)]
    Store(#[from] analyzer_objectstore::Error),
    /// A segment did not decode; the error is the segments crate's.
    #[error(transparent)]
    Segment(#[from] analyzer_segments::Error),
    /// The log partitioned by block, or a row in it, did not decode: a section with no footer,
    /// bytes past a row, an unknown kind — corruption in the bucket, refused rather than read.
    #[error("{0}")]
    Spine(String),
    /// A stored row that does not read back as its declared shape: a seed, a span, a frontier —
    /// the store and this crate have drifted.
    #[error("{0}")]
    Row(String),
    /// The derivation's own account of a failure, with the context it adds: which day,
    /// which stage, which object. The wrapped cause is in the text.
    #[error("{0}")]
    Derivation(String),
    /// Another writer holds this lineage's lease, or took it over since this writer last
    /// committed: this writer may not commit. See [`crate::lineage`].
    #[error("lineage {lineage}: this writer may not commit: {reason}")]
    Fenced {
        /// The lineage whose journal refused the commit.
        lineage: String,
        /// Who holds it, or how this writer was superseded.
        reason: String,
    },
}

/// The lake's failures in the pipeline's terms: a fenced writer stays fenced; a document that
/// does not decode is a row that does not read back; the rest keep their message.
impl From<analyzer_lake::Error> for Error {
    fn from(e: analyzer_lake::Error) -> Self {
        match e {
            analyzer_lake::Error::Fenced { lineage, reason } => Self::Fenced { lineage, reason },
            analyzer_lake::Error::Corrupt(m) => Self::Row(m),
            analyzer_lake::Error::Store(e) => Self::Store(e),
            other => Self::Derivation(other.to_string()),
        }
    }
}

/// The bridge the event source needs: an `EventSource` implementation returns the analysis
/// crate's error, and everything it calls in this crate returns this one. The same text, as
/// the source's own message.
impl From<Error> for analyzer_analysis::Error {
    fn from(e: Error) -> Self {
        Self::Source(e.to_string())
    }
}
