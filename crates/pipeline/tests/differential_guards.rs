// SPDX-License-Identifier: AGPL-3.0-only

//! Evict-resolution audit guards: the five classes, and the law that they PARTITION.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite keeps a corpus loop of its own
//! because **the partition law is a property, not an equality.** An audit
//! that cannot account for every evict must refuse to report rather than report around the gap, so
//! a fold that dropped an evict into no class would inflate `agreed` by omission — a healthier-
//! looking tap, which is the flattering direction. That is checkable over every fixture without
//! any second implementation to compare to.

use analyzer_analysis::differential::audit_from_events;
use analyzer_analysis::events::EventSource;
use analyzer_analysis::Window;
use serde_json::Value;

/// The partition law: every audited evict lands in exactly one class.
fn assert_partitions(source: &dyn EventSource, fleet: &str, w: &Window, label: &str) {
    let outcome = audit_from_events(&source.cache_events(fleet, w).expect("cache IR"));
    for row in &outcome.instances {
        let accounted =
            row.agreed + row.mismatched + row.underresolved + row.indeterminate + row.unjoined;
        assert_eq!(
            accounted, row.audited,
            "{label}: classes must partition for {} — {accounted} accounted of {} audited",
            row.instance, row.audited
        );
    }
}

/// The partition law over every join fixture — the one corpus sweep worth keeping, because it
/// asserts a property of the output rather than an equality against a vanished reference.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn every_audited_evict_lands_in_exactly_one_class_over_every_join_fixture() {
    let dir = harness::repo_root().join("fixtures/joins");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("fixtures/joins")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();

    let offset = harness::rebase_offset();
    let mut cases_run = 0usize;

    for path in files {
        let fixture: Value = serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
            .expect("fixture parses");
        let file = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .to_string();

        for case in fixture["cases"].as_array().expect("cases") {
            let name = case["name"].as_str().expect("name");
            let records: Vec<Value> = case["records"].as_array().expect("records").clone();
            let (engine, _db) = harness::fresh_engine();
            let source =
                harness::seed_source(&engine, "fx", &records, offset, &format!("audit-{name}"));
            let w = harness::cache_window(&engine, "fx");

            assert_partitions(&source, "fx", &w, &format!("{file} :: {name}"));
            cases_run += 1;
        }
    }

    assert!(cases_run > 0, "no cases executed");
    println!("evict-audit partition: {cases_run} fixture cases accounted for");
}

/// Every class in one fleet, so the fold is exercised across the partition rather than on the
/// happy path.
///
/// `agreed` (both carry the same id), `agreed` again by MATCHING ABSENCE (neither carries one —
/// a store that could not chain makes an evict that cannot resolve, and that pair is honest),
/// `mismatched` (both carry, and differ — the silent-wrong class this audit exists to catch),
/// `underresolved` (the evict carries nothing where the SAME run's store did — a lost promise),
/// `indeterminate` (a different run wrote the store, so "same run" cannot be claimed), and
/// `unjoined` (no prior store at all).
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn every_audit_class_agrees() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 15_000_000.0;

    let ev = |kind: &str, inst: &str, run: &str, block: &str, content: Option<&str>, at: f64| {
        let mut v = serde_json::json!({"kind":kind,"instance_id":inst,"incarnation":run,
                                       "block_id":block,"n_tokens":8,"at_ms":at});
        if let Some(c) = content {
            v.as_object_mut()
                .expect("obj")
                .insert("content_id".to_string(), serde_json::json!(c));
        }
        v
    };

    let records: Vec<Value> = vec![
        // agreed — both sides carry the same content id
        ev("store", "i-agree", "r1", "b1", Some("c1"), base),
        ev("evict", "i-agree", "r1", "b1", Some("c1"), base + 100.0),
        // agreed by matching absence — neither side could chain
        ev("store", "i-absent", "r1", "b2", None, base),
        ev("evict", "i-absent", "r1", "b2", None, base + 100.0),
        // mismatched — both carry, and disagree
        ev("store", "i-mismatch", "r1", "b3", Some("c3"), base),
        ev(
            "evict",
            "i-mismatch",
            "r1",
            "b3",
            Some("c-other"),
            base + 100.0,
        ),
        // underresolved — same run, store resolved, evict did not
        ev("store", "i-under", "r1", "b4", Some("c4"), base),
        ev("evict", "i-under", "r1", "b4", None, base + 100.0),
        // indeterminate — a DIFFERENT run wrote the store, so no same-run claim is available
        ev("store", "i-indet", "r1", "b5", Some("c5"), base),
        ev("evict", "i-indet", "r2", "b5", None, base + 100.0),
        // unjoined — an evict with no prior store in the window
        ev("evict", "i-unjoined", "r1", "b6", Some("c6"), base + 100.0),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "audit-classes");
    let w = harness::cache_window(&engine, "fx");

    let outcome = audit_from_events(&source.cache_events("fx", &w).expect("ir"));
    let by = |name: &str| {
        outcome
            .instances
            .iter()
            .find(|i| i.instance == name)
            .unwrap_or_else(|| panic!("no audit row for {name}: {outcome:?}"))
            .clone()
    };
    assert_eq!(by("i-agree").agreed, 1);
    assert_eq!(
        by("i-absent").agreed,
        1,
        "matching absence is agreement, not a defect"
    );
    assert_eq!(by("i-mismatch").mismatched, 1);
    assert_eq!(by("i-under").underresolved, 1);
    assert_eq!(
        by("i-indet").indeterminate,
        1,
        "a store from a different run cannot support a same-run claim"
    );
    assert_eq!(by("i-unjoined").unjoined, 1);

    assert_partitions(&source, "fx", &w, "every class");
}

/// The strict-prior rule: a store and an evict sharing one timestamp must NOT join.
///
/// One batch stamps one timestamp on every event it carries, so an evict-before-store inside a
/// single batch would otherwise be graded against a store the tap had not yet seen. The honest
/// cost is that such a pair falls to `unjoined` — counted, so the audit's own coverage is declared
/// rather than implied.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_same_instant_store_does_not_resolve_its_evict() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 16_000_000.0;

    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"store","instance_id":"i1","incarnation":"r1","block_id":"b",
                           "content_id":"c","n_tokens":8,"at_ms":base}),
        serde_json::json!({"kind":"evict","instance_id":"i1","incarnation":"r1","block_id":"b",
                           "content_id":"c","at_ms":base}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "audit-instant");
    let w = harness::cache_window(&engine, "fx");

    let outcome = audit_from_events(&source.cache_events("fx", &w).expect("ir"));
    assert_eq!(
        outcome.unjoined(),
        1,
        "a same-instant pair must not join — the timestamps establish no order: {outcome:?}"
    );
    assert_eq!(
        outcome.instances.iter().map(|i| i.agreed).sum::<u64>(),
        0,
        "nothing may be graded agreed off an order the timestamps do not establish"
    );

    assert_partitions(&source, "fx", &w, "same-instant store and evict");
}
