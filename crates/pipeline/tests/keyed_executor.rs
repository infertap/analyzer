// SPDX-License-Identifier: AGPL-3.0-only

//! The executor's tick under the journal intent, through a crash between publishing a batch and
//! marking it applied: the batch is published again, whole, and no figure doubles.

use analyzer_analysis::Params;
use serde_json::Value;

/// Fixture G1's records, both waves: an agent start, heartbeats, and stores with an evict and a
/// store again after it.
fn records() -> Vec<Value> {
    let path = harness::repo_root().join("fixtures/gait/G1-in-order-two-waves.json");
    let case: Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("G1")).expect("G1 parses");
    case["waves"]
        .as_array()
        .expect("waves")
        .iter()
        .flat_map(|wave| wave["records"].as_array().expect("records").clone())
        .collect()
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_lost_apply_publishes_the_batch_again_and_no_figure_doubles() {
    let off = harness::rebase_offset();
    let (clean, _clean_db) = harness::fresh_engine();
    let clean_source = harness::seed_ingested_source(&clean, "f", &records(), off, "exec-clean");
    harness::advance(&clean, &clean_source, "f", &Params::default()).expect("clean tick");

    let (engine, _db) = harness::fresh_engine();
    let source = harness::seed_ingested_source(&engine, "f", &records(), off, "exec-lost");
    let (faults, spine) = harness::journal_faults::faulty_default_spine(&engine, "f");
    harness::journal_faults::lose_apply(&faults, "lost_apply");
    let lost = harness::advance_on(&engine, spine.clone(), &source, &Params::default())
        .expect_err("the apply is refused");
    assert!(lost.to_string().contains("lost_apply"), "{lost}");
    assert_eq!(
        harness::revision(&engine, "f").expect("revision"),
        None,
        "a tick whose intent is unapplied states no revision"
    );

    harness::journal_faults::heal(&faults);
    let recovered =
        harness::advance_on(&engine, spine, &source, &Params::default()).expect("recovered tick");
    assert!(
        recovered.republished,
        "the unapplied batch is published again"
    );
    assert!(harness::revision(&engine, "f").expect("revision").is_some());
    assert_eq!(
        harness::product_dump(&engine),
        harness::product_dump(&clean),
        "the recovered tables differ from a run that never crashed"
    );
}
