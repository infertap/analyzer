// SPDX-License-Identifier: AGPL-3.0-only

//! The form-equivalence MUST (spec 2.1): one record model, two
//! permanent encodings, and a reader proves it by decoding both forms of the same segment to
//! the same record sequence.
//!
//! The Parquet side is the corpus's committed pyarrow twin — a REFERENCE implementation's
//! bytes, not ours, so this is interop rather than a round trip with ourselves. The JSONL side
//! is rendered from the same `segment.lines` the twin was generated from. Both run through
//! `decode_segment`, the one decoder, and the shaped NDJSON must be identical: one shaping
//! path, two decoders, and nothing downstream can tell which form arrived.

use std::path::PathBuf;

use analyzer_segments::decode_segment;
use serde_json::Value;

fn delivery_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contract/inference-events/conformance/delivery")
}

/// The vendored delivery fixture, both forms: (JSONL bytes, Parquet bytes, record count).
fn fixture() -> (Vec<u8>, Vec<u8>, u64) {
    let corpus: Value = serde_json::from_str(
        &std::fs::read_to_string(delivery_dir().join("records.json"))
            .expect("delivery records.json (submodule checked out?)"),
    )
    .expect("records.json parses");
    let segment = &corpus["segment"];
    let lines = segment["lines"].as_array().expect("segment.lines");
    let mut jsonl = String::new();
    for line in lines {
        jsonl.push_str(&line.to_string());
        jsonl.push('\n');
    }
    let twin = segment["parquet_twin"].as_str().expect("parquet_twin");
    let parquet = std::fs::read(delivery_dir().join(twin)).expect("read the committed twin");
    (jsonl.into_bytes(), parquet, lines.len() as u64)
}

/// Every shaped row, with the one field that is a function of the FILE's bytes rather than of
/// the records removed: `batch_id` is the content hash, so the two forms differ there by
/// construction and must agree everywhere else.
fn rows_without_batch_id(shaped: &str) -> Vec<Value> {
    shaped
        .lines()
        .map(|line| {
            let mut row: Value = serde_json::from_str(line).expect("shaped row parses");
            row.as_object_mut()
                .expect("shaped row is an object")
                .remove("batch_id")
                .expect("every shaped row carries its batch_id");
            row
        })
        .collect()
}

/// Decoding the pyarrow twin and the JSONL it was generated from yields the SAME shaped rows —
/// and different batch ids, because batch identity is the content hash and the bytes differ.
#[test]
fn both_forms_of_one_segment_decode_to_the_same_shaped_rows() {
    let (jsonl, parquet, records) = fixture();
    assert!(parquet.starts_with(b"PAR1"), "the twin is not Parquet");
    let from_text = decode_segment("seg-1785153670000-4242-0.jsonl", "f", &jsonl).expect("jsonl");
    let from_parquet =
        decode_segment("seg-1785153670000-4242-0.parquet", "f", &parquet).expect("parquet");
    assert_eq!(
        rows_without_batch_id(&from_text.shaped),
        rows_without_batch_id(&from_parquet.shaped),
        "one record model, two encodings: the shaped output must not depend on the form"
    );
    assert_eq!(from_text.records, records);
    assert_eq!(from_parquet.records, records);
    assert_eq!(from_parquet.unknown_kinds, 0);
    assert_ne!(
        from_text.batch_id, from_parquet.batch_id,
        "batch identity is the content hash, and the two forms are different bytes"
    );
}

/// A torn Parquet file fails closed with no rows: there is no tail rule in the shipped form,
/// because a truncated columnar file is not recoverable and corruption is not data.
#[test]
fn a_truncated_twin_refuses_whole() {
    let (_, parquet, _) = fixture();
    let torn = &parquet[..parquet.len() / 2];
    assert!(torn.starts_with(b"PAR1"), "the tear must keep the magic");
    let err = decode_segment("seg-1785153670000-4242-0.parquet", "f", torn)
        .expect_err("a torn Parquet segment must refuse");
    assert!(
        matches!(&err, analyzer_segments::Error::Corrupt(m) if m.contains("Parquet")),
        "the refusal is corruption on the Parquet path, not a JSON mis-parse: {err}"
    );
}

/// The dispatch is the content's first bytes: PAR1 routes to the Parquet decoder even when
/// everything after the magic is garbage, so the error is Parquet-shaped rather than a JSON
/// parse failure three layers from the cause.
#[test]
fn the_magic_bytes_route_to_the_parquet_path() {
    let err = decode_segment("seg-x-0.jsonl", "f", b"PAR1 this is not a parquet file")
        .expect_err("corrupt-after-magic must refuse");
    assert!(
        matches!(&err, analyzer_segments::Error::Corrupt(m) if m.contains("Parquet")),
        "PAR1 must reach the Parquet decoder, whatever the filename says: {err}"
    );
}
