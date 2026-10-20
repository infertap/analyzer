// SPDX-License-Identifier: AGPL-3.0-only

//! The columnar decode against the row decode, over the corpus's committed pyarrow twin: one
//! file, two decoders, identical shaped rows. This is the two-decoders-drift guard for the
//! arrow path — `form_equivalence.rs` proves the row path against the JSONL form, and this
//! proves the columnar path against the row path on the same reference bytes, so the three
//! readings pin each other.

use std::path::PathBuf;

use analyzer_segments::columnar::{decode_segment_projected, Projection};
use analyzer_segments::decode_segment;
use serde_json::Value;

fn twin() -> Vec<u8> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("contract/inference-events/conformance/delivery");
    std::fs::read(dir.join("seg-1785153670000-4242-0.parquet"))
        .expect("the committed twin (submodule checked out?)")
}

const SOURCE: &str = "seg-1785153670000-4242-0.parquet";

fn row_path_rows(bytes: &[u8]) -> Vec<Value> {
    decode_segment(SOURCE, "f", bytes)
        .expect("row path decodes")
        .shaped
        .lines()
        .map(|l| serde_json::from_str(l).expect("shaped row parses"))
        .collect()
}

/// Unprojected, unwindowed, unfiltered: the columnar decode of the twin is byte-for-byte the
/// row decode of the twin — same shaping, same stamping, same batch identity.
#[test]
fn the_columnar_decode_equals_the_row_decode_on_the_reference_bytes() {
    let bytes = twin();
    let projected = decode_segment_projected(SOURCE, "f", bytes.clone(), Projection::default())
        .expect("columnar decodes");
    let rows = row_path_rows(&bytes);
    assert!(!rows.is_empty(), "an empty twin proves nothing");
    assert_eq!(
        projected.rows, rows,
        "one file, two decoders: the arrow path and the row path must not drift"
    );
    assert_eq!(
        projected.batch_id,
        analyzer_segments::content_batch_id(&bytes),
        "batch identity is the content hash on every path"
    );
}

/// A projection returns a subset of each row's fields and changes no value in it — plus the
/// stamped declarations, which arrive from the header whatever the projection says.
#[test]
fn a_projection_narrows_the_row_and_never_rewrites_it() {
    let bytes = twin();
    let projected = decode_segment_projected(
        SOURCE,
        "f",
        bytes.clone(),
        Projection {
            columns: &["instance_id", "block_id", "n_tokens"],
            ..Projection::default()
        },
    )
    .expect("columnar decodes");
    let full = row_path_rows(&bytes);
    assert_eq!(projected.rows.len(), full.len());
    for (narrow, wide) in projected.rows.iter().zip(&full) {
        let narrow = narrow.as_object().expect("row object");
        let wide = wide.as_object().expect("row object");
        for (key, value) in narrow {
            if key == "extra" {
                // `extra` bags whatever was decoded and not typed, so under a projection it
                // holds a SUBSET of the full row's entries — each present entry unchanged.
                let full_extra = wide["extra"].as_object().expect("extra object");
                for (k, v) in value.as_object().expect("extra object") {
                    assert_eq!(Some(v), full_extra.get(k), "extra entry {k} rewritten");
                }
                continue;
            }
            assert_eq!(
                Some(value),
                wide.get(key),
                "projected field {key} must carry the row-path value"
            );
        }
        // The declarations are not optional: they come from row zero, not from the projection.
        assert!(
            narrow.contains_key("incarnation"),
            "stamping survives projection"
        );
        assert!(narrow.contains_key("kind"), "`kind` always rides");
        assert!(narrow.contains_key("at_ms"), "`at_ms` always rides");
    }
}

/// The kind predicate is the loader's `kind IN (...)`, applied before the shaping — so a
/// projection need not carry the fields the shaping requires of kinds the caller never asked
/// about.
#[test]
fn the_kind_set_selects_before_the_shaping() {
    let bytes = twin();
    let projected = decode_segment_projected(
        SOURCE,
        "f",
        bytes.clone(),
        Projection {
            columns: &["instance_id"],
            kinds: &["segment_open"],
            window: None,
        },
    )
    .expect("a lifecycle-shaped read must not fail on store rows it never asked about");
    assert!(!projected.rows.is_empty(), "the twin carries its header");
    for row in &projected.rows {
        assert_eq!(row["kind"], "segment_open");
    }
}

/// A window that excludes the whole file decodes nothing; the JSONL dispatch of the same call
/// shape decodes whole, because there is nothing to prune in the write-ahead form.
#[test]
fn a_disjoint_window_yields_no_rows_from_the_columnar_path() {
    let bytes = twin();
    let projected = decode_segment_projected(
        SOURCE,
        "f",
        bytes.clone(),
        Projection {
            window: Some((0.0, 1.0)),
            ..Projection::default()
        },
    )
    .expect("pruned decode");
    assert!(
        projected.rows.is_empty(),
        "every row group is provably outside"
    );

    let jsonl = b"{\"kind\":\"store\",\"instance_id\":\"i\",\"block_id\":\"b\",\"at_ms\":5.0}\n";
    let from_text = decode_segment_projected(
        "archive.jsonl",
        "f",
        jsonl.to_vec(),
        Projection {
            window: Some((0.0, 1.0)),
            ..Projection::default()
        },
    )
    .expect("jsonl decode");
    assert_eq!(
        from_text.rows.len(),
        1,
        "the row path has no pruning; the caller applies its own window"
    );
}
