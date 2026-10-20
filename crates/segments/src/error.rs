// SPDX-License-Identifier: AGPL-3.0-only

//! What decoding a segment can fail with.
//!
//! Two kinds, because a caller treats them differently: the bytes are not a segment
//! (`Corrupt`), or the records are a segment but break the contract (`Malformed`).

/// What decoding a segment can fail with.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The bytes do not decode as a segment: not Parquet where the magic said so, a row
    /// group or a cell that does not read, a value of a type the schema has no word for.
    #[error("{0}")]
    Corrupt(String),
    /// The records decode but break the contract: a record with no kind or no instant, a
    /// sealed segment with no header or with two, a line that is not the final line and
    /// does not parse.
    #[error("{0}")]
    Malformed(String),
}
