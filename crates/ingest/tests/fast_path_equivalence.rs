// SPDX-License-Identifier: AGPL-3.0-only

//! The equivalence pin, corpus half — the adoption condition: the projected walk and the full decode prepare a segment by two routes, a
//! drift pair by construction, so both run over the conformance corpus's Parquet fixtures
//! and what each commits (the input log's batch records, the stored-mass denominator, the
//! arrivals, the drops and the lifecycle records among them) must match, forever. Both routes fold cache facts through
//! one function; this pin holds everything around it.
//!
//! This half pins the CORPUS; the rig track's half re-measures at volume with a real
//! segment. Neither substitutes for the other: the corpus is the contract's own edge
//! cases, the real segment is cardinality and scale.

mod common;

use std::path::PathBuf;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every Parquet fixture the vendored corpus holds. The corpus is pinned by the contract
/// submodule, so this listing is stable per pin — and the test asserts it found some,
/// because an empty walk proving nothing must not read as green.
fn corpus_parquet() -> Vec<PathBuf> {
    fn walk(dir: &PathBuf, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("parquet") {
                out.push(path);
            }
        }
    }
    let mut found = Vec::new();
    walk(
        &workspace_root().join("crates/analyzer/contract/inference-events/conformance"),
        &mut found,
    );
    if found.is_empty() {
        // The submodule sits beside the crates when this test runs from the analyzer
        // repository root rather than the workspace parent.
        walk(
            &workspace_root().join("contract/inference-events/conformance"),
            &mut found,
        );
    }
    found.sort();
    found
}

#[test]
fn both_preparations_commit_identical_records_over_the_corpus() {
    let fixtures = corpus_parquet();
    assert!(
        !fixtures.is_empty(),
        "no Parquet fixtures found in the vendored corpus — an empty equivalence proves \
         nothing, so it fails instead"
    );
    // Two input logs: `reference` takes the full decode (`ingest_bytes_shaped`), `projected`
    // the fast path (`ingest_bytes`, the corpora's own entry).
    let reference_lake = common::fresh_lake("reference");
    let projected_lake = common::fresh_lake("projected");

    // The WHOLE ingest through both public entries: the fast path derives the cache facts,
    // the arrivals and the lifecycle records from a projected walk instead of the shaped
    // rows, so everything a commit writes is under the pin. `projected_files` is the
    // non-vacuity guard: if the fast path's admission ever declines the corpus, the
    // comparison silently becomes the reference against itself, and this pin must fail
    // rather than pass like that.
    let mut fast_files = 0u64;
    for fixture in &fixtures {
        let bytes = std::fs::read(fixture).expect("read fixture");
        let source = fixture.display().to_string();
        analyzer_ingest::ingest_bytes_shaped(&reference_lake, "fx", &source, &bytes)
            .expect("the reference path ingests the fixture");
        let auto = analyzer_ingest::ingest_bytes(&projected_lake, "fx", &source, &bytes)
            .expect("the projected path ingests the fixture");
        fast_files += auto.projected_files;
    }
    assert!(
        fast_files > 0,
        "no corpus fixture took the projected path — the comparison below would be \
         the reference against itself, which proves nothing about the fast path"
    );

    // The input log's records, less each side's own commit clock and position.
    let records = |lake: &analyzer_lake::Lake| {
        let mut batches = common::committed(lake);
        for b in &mut batches {
            b.ingested_at_ms = 0;
            b.input_position = 0;
        }
        batches.sort_by(|a, b| a.batch_id.cmp(&b.batch_id));
        batches
    };
    let reference = records(&reference_lake);
    assert_eq!(
        reference,
        records(&projected_lake),
        "input log: the fast path and the reference path diverged over the pinned corpus \
         — this is the drift the D3 ruling makes unshippable"
    );
    assert!(
        reference.iter().any(|b| !b.stored.is_empty()),
        "the corpus produced no cache rows — the equivalence would be vacuous, so it \
         fails instead"
    );
}
