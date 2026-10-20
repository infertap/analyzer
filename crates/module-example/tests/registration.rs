// SPDX-License-Identifier: AGPL-3.0-only

//! Acceptance from a separate crate: a public module registered beside the core, run by the
//! settled-day hook. Shared fixtures themselves use only public APIs.
use analyzer_analysis::{module::DerivationModule, Params, DAY_MS};
use analyzer_module_example::{CoverageGaps, GapAdapter};
use analyzer_pipeline::{adapter::Registration, modules::ModuleSet, Executor};
use serde_json::Value;

/// The example module, or none of it when `disabled`.
fn modules(disabled: bool) -> ModuleSet {
    let off: &[&str] = if disabled { &[CoverageGaps::NAME] } else { &[] };
    ModuleSet::from_registrations(&[Registration::of::<GapAdapter>()], off).unwrap()
}

/// Fixture P1's first wave, day-relative: two engines hold one prefix.
fn p1() -> Vec<Value> {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/producer/P1-two-engines-hold-the-same-prefix.json"
    ))
    .unwrap();
    fixture["waves"][0]["records"].as_array().unwrap().clone()
}

/// A heartbeat of P1's run naming no source, and a store, an hour into day `day` after the base:
/// what carries every edge past the previous day's end, so it settles. The heartbeat attests
/// the run and none of its sources, so their silence is final and the module has gaps to state.
fn settling(day: f64) -> Vec<Value> {
    let at = day * DAY_MS + 3_600_000.0;
    let mut heartbeat = p1().into_iter().find(|r| r["kind"] == "heartbeat").unwrap();
    heartbeat["at_ms"] = at.into();
    heartbeat["endpoints"] = serde_json::json!([]);
    vec![
        heartbeat,
        serde_json::json!({"kind": "store", "instance_id": "i1", "dp_rank": 0, "group_idx": 0,
            "block_id": format!("s{day}"), "content_id": format!("s{day}"), "at_ms": at,
            "incarnation": "run-1", "workload_class": "agentic", "n_tokens": 1}),
    ]
}

fn count(engine: &analyzer_serving_clickhouse::engine::Engine, sql: &str) -> u64 {
    engine.run(sql).unwrap().trim().parse().unwrap()
}

/// A failed settle leaves the tick unapplied; the next tick publishes its batch again and states
/// the day whole, a stale key included, whether the module's write or the apply failed.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine module-example"]
fn a_public_module_states_its_settled_day_whole_through_a_failed_tick() {
    for apply_fault in [false, true] {
        recover(apply_fault);
    }
}

fn recover(apply_fault: bool) {
    let (engine, _db) = harness::fresh_engine();
    analyzer_pipeline::adapter::install::<GapAdapter>(&*harness::serving(&engine)).unwrap();
    let base = harness::rebase_offset();
    let day = analyzer_analysis::emitted::day_index(base);
    let records = [p1(), settling(1.0)].concat();
    let source = harness::seed_ingested_source(&engine, "f", &records, base, "fifth-module");
    let params = Params::default();
    let (refusals, faulty) = harness::journal_faults::faulty_default_spine(&engine, "f");
    let executor = Executor::open(faulty, &params, harness::serving(&engine))
        .unwrap()
        .with_modules(modules(false));
    if apply_fault {
        harness::journal_faults::lose_apply(&refusals, "fifth_fault");
    } else {
        engine
            .run("ALTER TABLE example_gap_total ADD CONSTRAINT fifth_fault CHECK unobserved_ms = 0")
            .unwrap();
    }
    let failure = executor.advance(&source).expect_err("the settle fails");
    assert!(failure.to_string().contains("fifth_fault"), "{failure}");
    assert!(
        executor.revision().unwrap().is_none(),
        "the tick is unapplied"
    );
    assert_eq!(
        count(
            &engine,
            "SELECT count() FROM example_gap_sources FINAL WHERE gone = 0"
        ),
        2,
        "the per-source projection landed before the failure"
    );
    // A key the restatement does not produce must leave when the day is stated again.
    engine.run("INSERT INTO example_gap_sources SELECT fleet_id, semantics_version, discriminator, day, '__orphan__', 1, version, 0 FROM example_gap_sources LIMIT 1").unwrap();
    if apply_fault {
        harness::journal_faults::heal(&refusals);
    } else {
        engine
            .run("ALTER TABLE example_gap_total DROP CONSTRAINT fifth_fault")
            .unwrap();
    }

    let replacement = harness::executor(&engine, "f", &params)
        .unwrap()
        .with_modules(modules(false));
    let recovered = replacement.advance(&source).unwrap();
    assert!(recovered.republished, "{recovered:?}");
    harness::settled(&engine);
    assert_eq!(
        count(
            &engine,
            "SELECT count() FROM example_gap_sources FINAL WHERE gone = 0"
        ),
        2,
        "the stale key left with the restatement"
    );
    let total: f64 = engine
        .run("SELECT unobserved_ms FROM example_gap_total FINAL WHERE gone = 0")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let source_total: f64 = engine
        .run("SELECT sum(unobserved_ms) FROM example_gap_sources FINAL WHERE gone = 0")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(total > 0.0);
    assert!((total - source_total).abs() < 0.001);

    let output = replacement
        .availability()
        .unwrap()
        .into_iter()
        .find(|m| m.definition.name == CoverageGaps::NAME)
        .unwrap();
    assert!(output.enabled && output.revision.is_some());
    assert_eq!(
        (output.stated_from_day, output.stated_through_day),
        (Some(day), Some(day))
    );

    // Nothing new: nothing restated.
    let idle = replacement.advance(&source).unwrap();
    assert!(idle.settled_days.is_empty(), "{idle:?}");
    assert_eq!(
        count(
            &engine,
            "SELECT count() FROM example_gap_total FINAL WHERE gone = 0"
        ),
        1
    );
}

/// A module enabled after days settled states from the first day that settles while it is
/// enabled, never the days before; disabling it ends the span, and enabling it again starts one.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine module-example"]
fn a_module_enabled_later_states_only_the_days_that_settle_after() {
    let (engine, _db) = harness::fresh_engine();
    analyzer_pipeline::adapter::install::<GapAdapter>(&*harness::serving(&engine)).unwrap();
    let base = harness::rebase_offset() - 3.0 * DAY_MS;
    let day = analyzer_analysis::emitted::day_index(base);
    let params = Params::default();
    let root = harness::fresh_dir("enabled-later");
    let source = harness::source_over_tree("enabled-later", root.clone(), base);
    let ship = |wave: usize, records: &[Value]| {
        let (tree, _) = harness::plant_tree(records, base, &format!("enabled-later-w{wave}"));
        let planted = harness::planted_files(&tree);
        for (file, bytes) in harness::copy_planted(&planted, &tree, &root, &format!("w{wave}")) {
            harness::ingest_bytes(&engine, "f", &file, &bytes).unwrap();
        }
    };
    let tick = |disabled: bool| {
        harness::executor(&engine, "f", &params)
            .unwrap()
            .with_modules(modules(disabled))
            .advance(&source)
            .unwrap()
    };
    let span = |disabled: bool| {
        let gaps = harness::executor(&engine, "f", &params)
            .unwrap()
            .with_modules(modules(disabled))
            .availability()
            .unwrap()
            .into_iter()
            .find(|m| m.definition.name == CoverageGaps::NAME)
            .unwrap();
        (gaps.stated_from_day, gaps.stated_through_day)
    };

    // Day 0 settles with the module disabled.
    ship(0, &[p1(), settling(1.0)].concat());
    assert_eq!(tick(true).settled_days, [day]);
    assert_eq!(span(true), (None, None));
    // Day 1 settles with it enabled: its span starts there, and day 0 stays unstated.
    ship(1, &settling(2.0));
    assert_eq!(tick(false).settled_days, [day + 1]);
    assert_eq!(span(false), (Some(day + 1), Some(day + 1)));
    assert_eq!(
        count(
            &engine,
            &format!("SELECT count() FROM example_gap_total FINAL WHERE gone = 0 AND day = {day}")
        ),
        0,
        "a day settled before the module was enabled is never stated by it"
    );
    // Disabled for day 2, enabled again for day 3: a new span.
    ship(2, &settling(3.0));
    tick(true);
    ship(3, &settling(4.0));
    tick(false);
    assert_eq!(span(false), (Some(day + 3), Some(day + 3)));
}

#[test]
fn registration_validates_names_dependencies_and_toggles() {
    let own = Registration::of::<GapAdapter>();
    // Its one dependency is the core coverage register, which is always there.
    assert!(ModuleSet::from_registrations(std::slice::from_ref(&own), &[]).is_ok());
    assert!(
        ModuleSet::from_registrations(std::slice::from_ref(&own), &[CoverageGaps::NAME]).is_ok()
    );
    assert!(
        ModuleSet::from_registrations(std::slice::from_ref(&own), &["coverage"]).is_err(),
        "a core register cannot be disabled"
    );
    assert!(
        ModuleSet::from_registrations(&[own.clone(), own], &[]).is_err(),
        "a module registers once"
    );
}
