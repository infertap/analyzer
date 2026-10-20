// SPDX-License-Identifier: AGPL-3.0-only

//! What ingest can fail with.
//!
//! Most of what fails here fails below: a segment that does not decode, the store that
//! would not list or fetch, the input log refusing a commit. Those pass through unchanged,
//! each as its own crate's error, so a caller can still tell which boundary failed. Two
//! kinds are this crate's own: a row or an annotation that does not parse as the shape it
//! must have, and an annotation the validation refuses. Every message is exactly what the
//! string it replaced said.

/// What ingest can fail with.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A segment did not decode; the error is the segments crate's.
    #[error(transparent)]
    Segment(#[from] analyzer_segments::Error),
    /// The object store failed; the error is the objectstore crate's.
    #[error(transparent)]
    Store(#[from] analyzer_objectstore::Error),
    /// The input log refused or failed; the error is the lake crate's.
    #[error(transparent)]
    Lake(#[from] analyzer_lake::Error),
    /// A shaped row, a ledger state or an annotation that does not parse as the shape it
    /// must have — a defect in what was written, refused rather than guessed at.
    #[error("{0}")]
    Malformed(String),
    /// An annotation the validation refuses: no label, a label or scope over its cap, an
    /// instant that is not finite.
    #[error("{0}")]
    Refused(String),
    /// One file's failure, named by its key: the store would not fetch it, or its bytes did
    /// not prepare — the wrapped error says which.
    #[error("{key}: {source}")]
    File {
        /// The object key the worker was preparing.
        key: String,
        /// What failed under it.
        source: Box<Error>,
    },
}
