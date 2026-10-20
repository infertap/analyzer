// SPDX-License-Identifier: AGPL-3.0-only

//! The crate's error: what the derivation and its event sources can fail with.
//!
//! Hand-written rather than derived. This crate depends on nothing, and the dependency
//! allowlist pins that; three variants and a `Display` are not worth a proc-macro dependency.
//! The text of every message is unchanged from the strings these errors replaced — journal
//! lines and refusal texts are built from it — so a caller that only prints sees nothing move,
//! and a caller that wants the kind can match on it now.

/// What a derivation or its event source can fail with.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// Invalid half-open event-time range or an overflowing boundary conversion.
    InvalidWindow(&'static str),
    /// Invalid semantic parameters.
    InvalidParameters(&'static str),
    /// A field that did not parse as the type its name promises — an engine row's column,
    /// read back typed.
    Parse {
        /// The field's name, as the row spells it.
        name: String,
        /// The parser's own message.
        message: String,
    },
    /// An event source failed while loading or streaming a window: a transport failure,
    /// bytes that did not decode, or the sink's own error. The message is the source's.
    Source(String),
    /// A module set that cannot run: a name no module declares, a module switched off that an
    /// enabled one needs, or a cycle among the needs. The message names which.
    Modules(String),
    /// A rendered row a module's table does not accept: offered to the wrong table, the
    /// wrong number of key or measure values, or a value of the wrong kind.
    Row {
        /// The table the row was offered to.
        table: &'static str,
        /// What was wrong with it.
        message: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidWindow(reason) => write!(f, "invalid time window: {reason}"),
            Self::InvalidParameters(reason) => write!(f, "invalid parameters: {reason}"),
            Self::Parse { name, message } => write!(f, "{name}: {message}"),
            Self::Row { table, message } => write!(f, "{table}: {message}"),
            Self::Source(message) | Self::Modules(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {}
