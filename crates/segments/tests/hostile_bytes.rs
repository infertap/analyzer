// SPDX-License-Identifier: AGPL-3.0-only

//! Segments come from a customer's bucket, so the decoders must refuse damaged bytes with an
//! error, never a panic. Each test takes a valid segment, damages it every way a torn upload
//! or a flipped bit can — truncated at a spread of lengths, one byte inverted at a spread of
//! offsets — and decodes every variant through both the row and the columnar path.
//!
//! A damaged segment may still decode (a flipped value byte is a different valid value); the
//! property is only that no input makes a decoder panic.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;

use analyzer_segments::columnar::{decode_segment_projected, Projection};
use analyzer_segments::decode_segment;

fn delivery_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contract/inference-events/conformance/delivery")
}

fn parquet() -> Vec<u8> {
    std::fs::read(delivery_dir().join("seg-1785153670000-4242-0.parquet"))
        .expect("the committed twin (submodule checked out?)")
}

fn jsonl() -> Vec<u8> {
    let records: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(delivery_dir().join("records.json")).expect("records.json"),
    )
    .expect("records.json parses");
    let lines = records["segment"]["lines"]
        .as_array()
        .expect("segment.lines");
    let mut out = String::new();
    for line in lines {
        out.push_str(&line.to_string());
        out.push('\n');
    }
    out.into_bytes()
}

/// Every variant of `bytes` the tests decode: truncations and single inverted bytes, at every
/// offset within the last 512 bytes (where Parquet keeps its footer) and at every `stride`
/// elsewhere.
fn damaged(bytes: &[u8], stride: usize) -> Vec<(String, Vec<u8>)> {
    let tail = bytes.len().saturating_sub(512);
    let offsets: Vec<usize> = (0..bytes.len())
        .filter(|&i| i >= tail || i % stride == 0)
        .collect();
    let mut out = Vec::new();
    for &i in &offsets {
        out.push((format!("truncated at {i}"), bytes[..i].to_vec()));
        let mut flipped = bytes.to_vec();
        flipped[i] ^= 0xFF;
        out.push((format!("byte {i} inverted"), flipped));
    }
    out
}

fn assert_no_panic(name: &str, bytes: &[u8], stride: usize) {
    let mut panics = Vec::new();
    for (how, variant) in damaged(bytes, stride) {
        let row = catch_unwind(AssertUnwindSafe(|| {
            let _ = decode_segment(name, "f", &variant);
        }));
        let columnar = catch_unwind(AssertUnwindSafe(|| {
            let _ = decode_segment_projected(name, "f", variant.clone(), Projection::default());
        }));
        if row.is_err() {
            panics.push(format!("row decoder, {how}"));
        }
        if columnar.is_err() {
            panics.push(format!("columnar decoder, {how}"));
        }
    }
    assert!(
        panics.is_empty(),
        "{} damaged variants of {name} panicked a decoder: {panics:?}",
        panics.len()
    );
}

#[test]
fn a_damaged_parquet_segment_is_refused_without_a_panic() {
    assert_no_panic("seg-1785153670000-4242-0.parquet", &parquet(), 17);
}

#[test]
fn a_damaged_jsonl_segment_is_refused_without_a_panic() {
    assert_no_panic("seg-1785153670000-4242-0.jsonl", &jsonl(), 3);
}
