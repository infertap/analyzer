// SPDX-License-Identifier: AGPL-3.0-only

//! The facts-helper pins the review named: the empty-fleet refusal actually happening
//! (wrong `--fleet-id` → `Ok(None)`, never a bogus 2 ms window) and the corpus-driven
//! `agent_version` extraction (the key is a wire dependency on the tap's exact field
//! spelling — a tap-side rename must break this test, not silently empty a provenance
//! field).

use analyzer_pipeline::facts::full_span;
use analyzer_pipeline::read::agent_versions;
use serde_json::Value;

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_empty_fleet_yields_none_never_a_bogus_window() {
    let (engine, _db) = harness::fresh_engine();
    harness::bind_source(&engine, "empty");
    // an input log with nothing committed finds nothing: the guard must actually fire
    // (a minimum over no days is no instant, never 0)
    let span = full_span(&harness::lake(&engine, "empty")).expect("the log reads");
    assert_eq!(
        span, None,
        "empty fleet must refuse, not fabricate a window"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn agent_version_extraction_is_pinned_against_the_tap_corpus() {
    // the tap's lifecycle fixture is the wire contract: its agent_start record carries
    // `agent_version` — feed it through the real reader and extract
    let corpus: Value = serde_json::from_str(
        &std::fs::read_to_string(
            harness::repo_root()
                .join("contract/inference-events/conformance/lifecycle/records.json"),
        )
        .expect("lifecycle corpus"),
    )
    .expect("corpus parses");
    let start = corpus["cases"]
        .as_array()
        .expect("cases")
        .iter()
        .find(|c| c["kind"] == "agent_start")
        .expect("agent_start case")["expect"]
        .clone();
    // The corpus record carries a REAL wall stamp; REPLACE it with a recent one rather
    // than shifting it — the additive rebase is for fixture-relative deltas, and adding
    // an absolute offset to an absolute stamp lands decades in the future, outside any
    // discovery horizon.
    let mut record = start.as_object().expect("record").clone();
    record.insert("at_ms".into(), harness::rebase_offset().into());
    // Through the real reader: the segment decode routes the unrecognised field into
    // `extra`, and the declaration slice folds it back out of the IR.
    let source = harness::source_over_bytes(
        "facts-agent-version",
        format!("{}\n", Value::Object(record)).as_bytes(),
    );
    let versions = agent_versions(
        &source,
        "f",
        &analyzer_analysis::Window {
            start_ms: 0.0,
            end_ms: 4.0e12,
        },
    )
    .expect("versions");
    assert_eq!(
        versions,
        vec!["0.0.0-fixture".to_string()],
        "the corpus's agent_version must survive reader shaping into extra and back out"
    );
}
