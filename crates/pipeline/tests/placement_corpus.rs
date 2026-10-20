// SPDX-License-Identifier: AGPL-3.0-only

//! Placement's conformance corpus. Each fixture under `fixtures/producer/` is a named claim
//! about co-residency, delivered in waves, and each is held to the admissibility law: after
//! every wave has been ticked through the executor, the served placement cells (all seven
//! `replication_*` tables) equal one global fold of every record (`Duplication` over the whole
//! log, no seeds), clipped to the emitted edge. The tick folds on block-keyed lanes from held
//! seeds, a wave at a time; the reference folds once. Beside the law, each case pins:
//!
//! - the fleet cell set: exactly the `(copies_band, locality)` pairs the claim names appear at
//!   fleet scope, so a fixture cannot silently stop exercising its shape;
//! - the in-order contract: no wave of an in-order fixture arrives behind placement's edge.
//!
//! The gait fixtures run through the same law with their late waves: placement corrects behind
//! its edge by re-folding the identities a late record touches, and the law holds after. A late
//! scope closer names no identity, so it is counted in the restated ledger and the span it
//! touches stays uncorrected: for a fixture with one, the count is asserted instead of the law.
//!
//! Fixture `at_ms` are day-relative; the driver rebases to a recent day like every fixture
//! suite here. A fixture's producer heartbeats within its liveness bound for as long as its
//! records run: a longer gap is a silence, which ends every residency on the instance there.

use analyzer_analysis::dup_cells::{DupCells, DupCtx, Duplication};
use analyzer_analysis::emitted::Meaning;
use analyzer_analysis::events::{EventSource, ScopeClosers};
use analyzer_analysis::module::{run_window, Persisted, Value as Cell};
use analyzer_analysis::{Params, Window};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn every_placement_fixture_holds_the_admissibility_law() {
    holds(corpus("fixtures/producer"), Mode::InOrder);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_gait_fixtures_hold_the_admissibility_law_through_late_correction() {
    holds(corpus("fixtures/gait"), Mode::Gait);
}

/// Every case run, the failures collected rather than the first one alone: a change of semantics
/// reports every fixture it moves in one run.
fn holds(cases: Vec<(String, Value)>, mode: Mode) {
    let failed: Vec<String> = cases
        .into_iter()
        .filter_map(|(name, case)| {
            std::panic::catch_unwind(|| run_case(&name, &case, mode))
                .err()
                .map(|_| name)
        })
        .collect();
    assert!(failed.is_empty(), "fixtures failed: {failed:?}");
}

fn corpus(dir: &str) -> Vec<(String, Value)> {
    let dir = harness::repo_root().join(dir);
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("fixture dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "the corpus is empty");
    files
        .into_iter()
        .map(|path| {
            let case: Value = serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                .expect("fixture parses");
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("stem")
                .to_string();
            (name, case)
        })
        .collect()
}

#[derive(PartialEq, Clone, Copy)]
enum Mode {
    /// Placement's own fixtures: in-order waves, the edge advancing at every one, and the
    /// claim's exact fleet cell set.
    InOrder,
    /// The gait fixtures: waves may arrive behind the edge; only the law runs, since the
    /// fixtures carry gait claims, not cell sets.
    Gait,
}

fn run_case(name: &str, case: &Value, mode: Mode) {
    let claim = case["claim"].as_str().expect("claim");
    let waves = case["waves"].as_array().expect("waves");
    let base = harness::rebase_offset();
    let params = Params::default();
    let meaning = Meaning::current(&analyzer_analysis::Discriminator::of(&params));

    let tree_root = harness::fresh_dir(&format!("{name}-placement"));
    let source = harness::source_over_tree(&format!("{name}-placement"), tree_root.clone(), base);
    let (engine, _db) = harness::fresh_engine();
    let mut last_emitted = f64::NEG_INFINITY;
    let mut late_closers = 0;
    for (i, wave) in waves.iter().enumerate() {
        let records: Vec<Value> = wave["records"].as_array().expect("records").clone();
        let (wave_root, _) = harness::plant_tree(&records, base, &format!("{name}-w{i}"));
        let planted = harness::planted_files(&wave_root);
        for (file, bytes) in
            harness::copy_planted(&planted, &wave_root, &tree_root, &format!("w{i}"))
        {
            harness::ingest_bytes(&engine, "f", &file, &bytes).expect("ingest");
        }
        let tick = harness::advance(&engine, &source, "f", &params)
            .unwrap_or_else(|e| panic!("{name} wave {i}: {e}"));
        let emitted = emitted_to_ms(&engine, &meaning)
            .unwrap_or_else(|| panic!("{name} wave {i}: no emitted edge"));
        if mode == Mode::InOrder {
            assert_eq!(
                tick.placement_late.records, 0,
                "{name} wave {i}: an in-order wave arrived behind placement's edge — {claim:?}"
            );
            assert!(
                emitted > last_emitted,
                "{name} wave {i}: the emitted edge did not advance — {claim:?}"
            );
        }
        late_closers += tick.placement_late.closers;
        last_emitted = emitted;
    }

    // Placement emits from the first record its first tick folds: before it, nothing was
    // observed, so a closer there opens no residency.
    let first = waves[0]["records"]
        .as_array()
        .expect("records")
        .iter()
        .filter(|r| r.get("instance_id").is_some())
        .filter_map(|r| r["at_ms"].as_f64())
        .reduce(f64::min)
        .expect("the first wave carries a cache record");
    let reference = global_fold(&source, &params, (base + first, last_emitted));
    if late_closers > 0 {
        // The ruled gap: a late scope closer names no identity, so placement counts it and
        // leaves the span it touches uncorrected. The law is not asked of such a case; the
        // count is.
        let counted: u64 = engine
            .run(
                "SELECT toUInt64(sum(closers)) FROM restated_hourly FINAL \
                 WHERE fleet_id = 'f' AND family = 'placement' AND gone = 0",
            )
            .expect("restated")
            .trim()
            .parse()
            .expect("count");
        assert_eq!(
            counted, late_closers,
            "{name}: the late scope closers are not counted in the restated ledger — {claim:?}"
        );
        eprintln!("placement corpus: {name} counts {late_closers} late scope closer(s) — {claim}");
        return;
    }
    let mut served = DupCells::default();
    analyzer_serving_clickhouse::persist::read_back(&engine, ("f", &meaning), &mut served, &[])
        .expect("read back");
    if mode == Mode::InOrder {
        assert!(
            !reference.days().is_empty(),
            "{name}: the fold emitted nothing"
        );
    }
    agree(name, claim, &reference, &served);

    if mode == Mode::InOrder {
        assert_claim_shape(&engine, case, name, claim, &served);
    }
    eprintln!("placement corpus: {name} holds — {claim}");
}

/// The emitted edge the last applied tick stated.
fn emitted_to_ms(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    meaning: &Meaning,
) -> Option<f64> {
    engine
        .run(&format!(
            "SELECT value FROM derivation_status FINAL WHERE fleet_id = 'f' \
               AND semantics_version = '{}' AND discriminator = '{}' \
               AND fact = 'emitted_to_ms' AND gone = 0",
            meaning.semantics_version, meaning.discriminator
        ))
        .expect("status")
        .trim()
        .parse()
        .ok()
}

/// One fold of every record the source holds, from no seeds, over `[from, emitted)`.
fn global_fold(source: &dyn EventSource, params: &Params, (from, emitted): (f64, f64)) -> DupCells {
    let all = Window::new(0.0, emitted + 365.0 * analyzer_analysis::DAY_MS).expect("window");
    let events = source.cache_events("f", &all).expect("events");
    let closers = ScopeClosers::of(&events);
    let mut grouped = BTreeMap::new();
    for e in events.iter() {
        if let Some(id) = e.identity() {
            grouped
                .entry(id.to_string())
                .or_insert_with(Vec::new)
                .push(e.clone());
        }
    }
    let ctx = DupCtx {
        closers: &closers,
        half_skew: params.delta_skew_ms / 2.0,
    };
    let window = Window::new(from, emitted).expect("emitted window");
    run_window(&Duplication, grouped, &BTreeMap::new(), window, &ctx).cells
}

/// A cell's measures as numbers, by its table and its key.
fn cells(state: &DupCells) -> BTreeMap<(String, String), Vec<f64>> {
    let number = |v: &Cell| match v {
        Cell::F64(x) => *x,
        #[allow(clippy::cast_precision_loss)] // test fixtures' sums stay far below 2^52
        Cell::I64(x) => *x as f64,
        Cell::U32(x) => f64::from(*x),
        other => panic!("a measure that is not a number: {other:?}"),
    };
    state
        .days()
        .into_iter()
        .flat_map(|day| state.render_day(day))
        .map(|row| {
            (
                (row.table.to_string(), format!("{:?}", row.key)),
                row.measures.iter().map(number).collect(),
            )
        })
        .collect()
}

/// Served and reference agree cell for cell, every table, to rounding: the tick sums a cell
/// across waves and lanes, the reference in one pass.
fn agree(name: &str, claim: &str, reference: &DupCells, served: &DupCells) {
    let (want, have) = (cells(reference), cells(served));
    let keys: BTreeSet<_> = want.keys().chain(have.keys()).collect();
    for key in keys {
        let (w, h) = (want.get(key), have.get(key));
        let (Some(w), Some(h)) = (w, h) else {
            panic!("{name} {key:?}: reference {w:?} vs served {h:?} — {claim:?}");
        };
        for (a, b) in w.iter().zip(h) {
            let scale = a.abs().max(b.abs()).max(1.0);
            assert!(
                ((a - b) / scale).abs() < 1e-6,
                "{name} {key:?}: reference {w:?} vs served {h:?} — {claim:?}"
            );
        }
    }
}

/// The fixture's own witness: the fleet `(band, locality)` cell set is exactly the claim's, and
/// (where declared) every per-class row stayed in band 1.
fn assert_claim_shape(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    case: &Value,
    name: &str,
    claim: &str,
    served: &DupCells,
) {
    let expect: BTreeSet<(String, String)> = case["expect_fleet_cells"]
        .as_array()
        .expect("expect_fleet_cells")
        .iter()
        .map(|pair| {
            (
                pair[0].as_str().expect("band").to_string(),
                pair[1].as_str().expect("locality").to_string(),
            )
        })
        .collect();
    let fleet_cells: BTreeSet<(String, String)> = served
        .minute
        .iter()
        .filter(|(k, sum)| k.class.is_empty() && sum.token_ms.abs() > 1e-9)
        .map(|(k, _)| (k.band.to_string(), k.locality.to_string()))
        .collect();
    assert_eq!(
        fleet_cells, expect,
        "{name}: the fleet cell set is not the claim's — {claim:?}"
    );

    if case["per_class_all_band_one"].as_bool() == Some(true) {
        let above = engine
            .run(
                "SELECT count() FROM replication_minute FINAL \
                 WHERE gone = 0 AND workload_class != '' AND copies_band != '1'",
            )
            .expect("class bands");
        assert_eq!(
            above.trim(),
            "0",
            "{name}: a per-class row left band 1 — the cut counted across classes — {claim:?}"
        );
    }
}
