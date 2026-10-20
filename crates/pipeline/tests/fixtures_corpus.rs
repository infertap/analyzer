// SPDX-License-Identifier: AGPL-3.0-only

//! The join fixture corpus, executed end-to-end: every fixture under `fixtures/joins/` through
//! real ingest and the classifier. Implementations conform to these fixtures, never the
//! reverse; every case's expectations are asserted, with/without pairs included, and an
//! expectation this harness does not grade fails rather than passing unread.

use analyzer_analysis::Params;
use analyzer_pipeline::read::classify;
use serde_json::Value;

/// The expectation keys this harness asserts; `note` is prose for the reader.
const GRADED: &[&str] = &[
    "classifications",
    "eviction_waste_tokens",
    "post_clear_tokens",
    "note",
];

fn params_from(fixture: &Value, case: &Value) -> Params {
    let mut p = Params::default();
    for source in [fixture.get("params"), case.get("params")] {
        let Some(obj) = source.and_then(Value::as_object) else {
            continue;
        };
        if let Some(v) = obj.get("delta_skew_ms").and_then(Value::as_f64) {
            p.delta_skew_ms = v;
        }
        if let Some(v) = obj.get("examined_history_ms").and_then(Value::as_f64) {
            p.examined_history_ms = v;
        }
    }
    p
}

fn run_fixture(name: &str) {
    let path = harness::repo_root().join("fixtures/joins").join(name);
    let fixture: Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("fixture file"))
            .expect("fixture parses");
    let offset = harness::rebase_offset();

    for case in fixture["cases"].as_array().expect("cases") {
        let case_name = case["name"].as_str().expect("name");
        let (engine, _db) = harness::fresh_engine();
        let records: Vec<Value> = case["records"].as_array().expect("records").clone();
        let source = harness::seed_source(
            &engine,
            "fx",
            &records,
            offset,
            &format!("joins-{case_name}"),
        );
        let w = harness::cache_window(&engine, "fx");
        let params = params_from(&fixture, case);
        let expect = &case["expect"];
        for key in expect.as_object().expect("expect").keys() {
            assert!(
                GRADED.contains(&key.as_str()),
                "{name}/{case_name}: expectation {key:?} is not graded by this harness"
            );
        }

        if let Some(classes) = expect.get("classifications").and_then(Value::as_object) {
            let result = classify(&source, "fx", &w, &params).expect("classify");
            for (r, s) in records
                .iter()
                .filter_map(|r| r.get("ref").and_then(Value::as_str).map(|rf| (r, rf)))
            {
                let Some(want) = classes.get(s).and_then(Value::as_str) else {
                    continue;
                };
                let at = r["at_ms"].as_f64().expect("at_ms") + offset;
                let instance = r["instance_id"].as_str().expect("instance");
                let block = r["block_id"].as_str().expect("block");
                let got = result
                    .stores
                    .iter()
                    .find(|c| {
                        c.instance == instance && c.block == block && (c.at_ms - at).abs() < 1e-6
                    })
                    .unwrap_or_else(|| panic!("{name}/{case_name}: ref {s} not classified"));
                assert_eq!(
                    got.class.as_str(),
                    want,
                    "{name}/{case_name}: ref {s} classified {}, fixture says {want}",
                    got.class.as_str()
                );
            }
            if let Some(want) = expect.get("eviction_waste_tokens").and_then(Value::as_i64) {
                assert_eq!(
                    result.eviction_waste_tokens, want,
                    "{name}/{case_name}: eviction waste"
                );
            }
            if let Some(want) = expect.get("post_clear_tokens").and_then(Value::as_i64) {
                assert_eq!(
                    result.post_clear_tokens, want,
                    "{name}/{case_name}: post-clear tokens"
                );
            }
        }
    }
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e1_re_beats_etr() {
    run_fixture("E1-re-beats-etr.json");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e2_indeterminate_not_cold() {
    run_fixture("E2-indeterminate-not-cold.json");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e4_post_clear_vs_etr() {
    run_fixture("E4-post-clear-vs-etr.json");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e5_readmit() {
    run_fixture("E5-readmit.json");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e6_skew_preset() {
    run_fixture("E6-skew-preset.json");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e7_history_bounds_the_lookback() {
    run_fixture("E7-history-bounds-the-lookback.json");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e8_close_reason_decides() {
    run_fixture("E8-close-reason-decides.json");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn e11_lookback_taint() {
    run_fixture("E11-lookback-taint.json");
}
