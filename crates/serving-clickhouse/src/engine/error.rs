// SPDX-License-Identifier: AGPL-3.0-only

//! What the engine boundary can fail with.
//!
//! Six kinds, because six different things go wrong here and a caller treats them
//! differently: a statement the engine refused or could not run, the connection or the
//! runtime under it, the server process itself, a refusal of a configuration or a posture,
//! a file the crate could not read or write, and the migration ledger. Each variant renders
//! exactly as the message it replaced, so an operator's journal reads the same; what changed
//! is that a caller can now match the kind instead of the text.

/// What the engine boundary can fail with.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The engine refused a statement, or running it failed; the message is the engine's.
    #[error("{0}")]
    Statement(String),
    /// The connection or the runtime under it: it could not be built, a call exceeded the
    /// ceiling, a stream stalled.
    #[error("{0}")]
    Connection(String),
    /// The server process: it would not start, did not become ready, would not stop.
    #[error("{0}")]
    Server(String),
    /// A refusal, decided here: a configuration outside its floor, an endpoint that is not
    /// loopback, a build that is not the pinned one, a directory another process owns, a
    /// script the statement splitter does not model.
    #[error("{0}")]
    Refused(String),
    /// A file or a device the crate could not read, write, create or secure.
    #[error("{0}")]
    Io(String),
    /// The migration ledger: an entry that does not parse, or a migration that refused.
    #[error("{0}")]
    Migration(String),
}
