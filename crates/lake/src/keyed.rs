// SPDX-License-Identifier: AGPL-3.0-only

//! A record of the fold's keyed relations (`crate::working`): an identity hash, a key (the
//! relation's own, as bytes), a version, a `gone` flag and an opaque payload, and what a
//! revision changed, counted by each record's [`term`].

/// One record of a keyed relation.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// The identity's hash: the record's stripe, and its first sort key.
    pub hash: u64,
    /// The relation's key within the identity, as bytes.
    pub key: Vec<u8>,
    /// The record's version: the highest wins its key.
    pub version: f64,
    /// A tombstone: the key is gone.
    pub gone: bool,
    /// The relation's row, as its encoder wrote it.
    pub payload: Vec<u8>,
}

/// A record's value as one number: FNV-1a over its key and payload, stable across builds and
/// hosts. Two states hold a key at the same value when its terms agree.
#[must_use]
pub fn term(record: &Record) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;
    let mut term = OFFSET;
    for byte in record
        .key
        .iter()
        .chain(std::iter::once(&0xff))
        .chain(record.payload.iter())
    {
        term ^= u64::from(*byte);
        term = term.wrapping_mul(PRIME);
    }
    term
}

/// What a revision wrote, by key: its latest record's term under `(hash, key)`, `None` where
/// that record is gone. A later part of a revision wins a key, as a read's merge does.
pub type Terms = std::collections::HashMap<(u64, Vec<u8>), Option<u64>>;

/// What a revision changed against the state it was committed over: records inserted,
/// updated to another value, deleted, and the keys it wrote in all. Iceberg's snapshot summary
/// counts added and deleted records; telling a key restated at its old value from one changed
/// takes the old value, as Delta's MERGE metrics do (`num_target_rows_updated`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Changes {
    /// Keys the revision wrote.
    pub written: u64,
    /// Keys live after it and not before.
    pub inserted: u64,
    /// Keys live before and after it, at another value.
    pub updated: u64,
    /// Keys live before it and not after.
    pub deleted: u64,
}

impl Changes {
    /// Records the revision changed, whichever way.
    #[must_use]
    pub fn changed(&self) -> u64 {
        self.inserted + self.updated + self.deleted
    }
}
