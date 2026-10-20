// SPDX-License-Identifier: AGPL-3.0-only

//! **The derived-facts snapshot: one document per fixture case, over the whole read surface.**
//!
//! This is the artifact the store-free rewrite is accepted against, and it exists because the
//! differential harnesses are about to lose their reference. Nine suites proved the folds equal to
//! the SQL; once the SQL is deleted there is nothing left to be equal *to* — so the agreed output
//! is captured here first, from the INCUMBENT path, and becomes the thing the folds must
//! reproduce.
//!
//! **Captured from SQL, asserted against the fold.** The direction matters. A golden captured from
//! the fold would only prove the fold is stable; captured from the engine path it proves the fold
//! did not change any answer — a comparison against an incumbent rather than against
//! hand-written expectations.
//!
//! **One document per case, not one per derivation.** Every figure the product prints is in here,
//! so a change anywhere in the read surface shows up as a diff a reviewer reads rather than as a
//! count that moved. It is also the first concrete shape of the Classified Fact artifact the
//! egress seam needs a name for.
//!
//! Regenerate deliberately: `UPDATE_DERIVED_SNAPSHOTS=1 cargo test -p analyzer-analysis
//! --test derived_snapshot -- --ignored`. A snapshot that changes without a stated reason is the
//! review failing, not the test.

use analyzer_pipeline::derived::derive;

use serde_json::{json, Value};

/// Capture or verify one document per fixture case.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_derived_facts_match_their_snapshots() {
    let dir = harness::repo_root().join("fixtures/joins");
    let out_dir = harness::repo_root().join("conformance/derived");
    let updating = std::env::var_os("UPDATE_DERIVED_SNAPSHOTS").is_some();
    if updating {
        std::fs::create_dir_all(&out_dir).expect("mkdir conformance/derived");
    }

    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("fixtures/joins")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();

    let offset = harness::rebase_offset();
    let mut checked = 0usize;
    let mut missing: Vec<String> = Vec::new();
    let mut expected_files = std::collections::BTreeSet::new();

    // **One database for every case, emptied between them.** The cases are sequential, so
    // `harness::reset` gives each the isolation a fresh database would, without a migration per
    // case.
    let (engine, _db) = harness::fresh_engine();

    for path in files {
        let fixture: Value = serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
            .expect("fixture parses");
        let stem = path
            .file_stem()
            .expect("stem")
            .to_string_lossy()
            .to_string();

        for case in fixture["cases"].as_array().expect("cases") {
            let name = case["name"].as_str().expect("name");
            let records: Vec<Value> = case["records"].as_array().expect("records").clone();
            harness::reset(&engine);
            // The records are ingested as segments, and the derivation reads them through the
            // same source the deployment derives with.
            let source =
                harness::seed_source(&engine, "fx", &records, offset, &format!("snap-{name}"));
            let w = harness::cache_window(&engine, "fx");

            // **Rebased to zero before serialising.** Fixtures are shifted to a recent day so
            // they sit inside the TTL, so raw timestamps would differ on every run. Every
            // relationship in a fixture is a delta, so subtracting the offset makes the document
            // a function of the fixture rather than of the clock.
            let doc = rebase(derive(&source, "fx", &w).expect("derive"), offset);
            let file = out_dir.join(format!("{stem}--{name}.json"));
            expected_files.insert(format!("{stem}--{name}.json"));
            let rendered = format!("{}\n", serde_json::to_string_pretty(&doc).expect("render"));

            if updating {
                std::fs::write(&file, &rendered).expect("write snapshot");
            } else {
                match std::fs::read_to_string(&file) {
                    Ok(expected) => assert_eq!(
                        rendered, expected,
                        "derived snapshot changed for {stem}::{name} — if this is intended, \
                         state why and regenerate with UPDATE_DERIVED_SNAPSHOTS=1"
                    ),
                    Err(_) => missing.push(format!("{stem}::{name}")),
                }
            }
            checked += 1;
        }
    }

    assert!(checked > 0, "no cases executed");
    assert!(
        missing.is_empty(),
        "no snapshot for {} case(s): {}\nrun with UPDATE_DERIVED_SNAPSHOTS=1 to capture",
        missing.len(),
        missing.join(", ")
    );
    // A snapshot no case produces is a deleted or renamed case's leftover: it pins nothing.
    let orphans: Vec<String> = std::fs::read_dir(&out_dir)
        .expect("conformance/derived")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .map(|p| p.file_name().expect("name").to_string_lossy().to_string())
        .filter(|f| !expected_files.contains(f))
        .collect();
    assert!(
        orphans.is_empty(),
        "snapshots no fixture case produces: {}",
        orphans.join(", ")
    );
    println!("derived snapshots: {checked} cases");
}

/// Shift every engine-domain millisecond back by the fixture rebase, so a snapshot is a function
/// of the fixture and not of when it ran.
fn rebase(v: Value, offset: f64) -> Value {
    match v {
        Value::Number(n) => n.as_f64().map_or(Value::Number(n.clone()), |f| {
            // Only real instants are shifted. Timestamps are large; counts, masses and fractions
            // are not, and a blanket subtraction would corrupt them.
            if f > offset / 2.0 {
                json!(f - offset)
            } else {
                Value::Number(n)
            }
        }),
        Value::Array(a) => Value::Array(a.into_iter().map(|x| rebase(x, offset)).collect()),
        Value::Object(o) => {
            Value::Object(o.into_iter().map(|(k, x)| (k, rebase(x, offset))).collect())
        }
        other => other,
    }
}
