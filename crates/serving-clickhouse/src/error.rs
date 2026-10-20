// SPDX-License-Identifier: AGPL-3.0-only

//! What the read layer can fail with.
//!
//! Most of what fails here fails below: the engine refusing a statement, a pass adapter
//! or a ledger read, the declared-annotation write. Those pass through unchanged, each as
//! its own crate's error, so a caller can still tell which boundary failed. Two kinds are
//! this crate's own: a row the engine returned that does not parse as the shape its
//! statement declared — a defect between a statement and its reader, never the caller's —
//! and a request the read refuses by name: a cut with no table at the grain asked, a
//! quantile outside its range, a day that is not a date. Every message is exactly what
//! the string it replaced said.

/// What the read layer can fail with.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The engine boundary failed; the error is the storage crate's.
    #[error(transparent)]
    Engine(#[from] crate::engine::Error),
    /// A pass adapter, a ledger or a derived read failed; the error is the pipeline's.
    #[error(transparent)]
    Pipeline(#[from] analyzer_pipeline::Error),
    /// The declared-annotation write failed; the error is the ingest crate's.
    #[error(transparent)]
    Ingest(#[from] analyzer_ingest::Error),
    /// The event source refused a read; the error is the analysis crate's.
    #[error(transparent)]
    Source(#[from] analyzer_analysis::Error),
    /// A row the engine returned does not parse as the shape its statement declared.
    #[error("{0}")]
    Row(String),
    /// A request the read refuses by name, with the reason and what to ask instead.
    #[error("{0}")]
    Refused(String),
    /// A grain the read cannot serve for this cut, scope or window: an explicit grain whose
    /// table has no such dimensions, or whose measured reach does not cover the window. The
    /// message names the grain and what to ask instead.
    #[error("{0}")]
    GrainUnavailable(String),
}
