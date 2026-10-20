// SPDX-License-Identifier: AGPL-3.0-only

//! `RowBinary`, the engine's own row encoding: a `LEB128` length before each string, a null
//! byte before each nullable value, little-endian numbers, nothing else. `LowCardinality(T)`
//! travels as `T`. The writer this store encodes every insert with.

use std::fmt::Write as _;

/// A growing `RowBinary` body: the writer side of the wire.
#[derive(Default)]
pub(crate) struct Writer {
    pub(crate) bytes: Vec<u8>,
}

impl Writer {
    pub(crate) fn u8(&mut self, v: u8) {
        self.bytes.push(v);
    }

    pub(crate) fn u16(&mut self, v: u16) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn u32(&mut self, v: u32) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn u64(&mut self, v: u64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn i64(&mut self, v: i64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn f64(&mut self, v: f64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn f32(&mut self, v: f32) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    pub(crate) fn varint(&mut self, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                self.bytes.push(b);
                return;
            }
            self.bytes.push(b | 0x80);
        }
    }

    pub(crate) fn string(&mut self, s: &str) {
        self.varint(s.len() as u64);
        self.bytes.extend_from_slice(s.as_bytes());
    }

    pub(crate) fn nullable<T>(&mut self, v: Option<T>, write: impl FnOnce(&mut Self, T)) {
        match v {
            None => self.u8(1),
            Some(v) => {
                self.u8(0);
                write(self, v);
            }
        }
    }

    /// The column list an insert names, so the body's field order is the statement's and
    /// never the table's — a column added to the table cannot shift what a row means.
    pub(crate) fn columns(names: &[&str]) -> String {
        let mut out = String::new();
        for (i, n) in names.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let _ = write!(out, "{}", crate::engine::quote_ident(n));
        }
        out
    }
}
