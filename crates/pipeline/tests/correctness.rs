// SPDX-License-Identifier: AGPL-3.0-only

//! Correctness pins: the interval traces, clear expansion across interleaved instances, the
//! contracts-meet-in-the-middle run (tap emission corpus → reader → interval substrate), the
//! completeness clock, and the rollup properties through real ticks (a second tick never
//! doubles; every grain sums to the others).

use analyzer_analysis::classify::Class;
use analyzer_analysis::intervals::CloseReason;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::read::classify;
use analyzer_pipeline::read::residency_intervals;
use analyzer_pipeline::read::uncertainty_model;
use serde_json::{json, Value};

fn store(i: &str, p: &str, at: f64) -> Value {
    json!({"kind": "store", "instance_id": i, "block_id": p, "n_tokens": 4, "at_ms": at})
}

fn evict(i: &str, p: &str, at: f64) -> Value {
    json!({"kind": "evict", "instance_id": i, "block_id": p, "at_ms": at})
}

fn clear(i: &str, at: f64) -> Value {
    json!({"kind": "clear", "instance_id": i, "scope": "all", "at_ms": at})
}

fn store_on(i: &str, p: &str, tier: &str, at: f64) -> Value {
    json!({"kind": "store", "instance_id": i, "block_id": p, "n_tokens": 4,
           "tier": tier, "at_ms": at})
}

fn evict_on(i: &str, p: &str, tier: &str, at: f64) -> Value {
    json!({"kind": "evict", "instance_id": i, "block_id": p, "tier": tier, "at_ms": at})
}

/// The offload trace: one block, two tiers, independent lifetimes.
///
/// GPU 100→200 and 250→350, CPU 150→300. The GPU evict at 200 happens while the CPU copy is
/// still resident, and the GPU store at 250 is a promotion off that copy rather than a
/// recomputed prefill.
fn offload_trace(i: &str, p: &str) -> Vec<Value> {
    vec![
        store_on(i, p, "GPU", 100.0),
        store_on(i, p, "CPU", 150.0),
        evict_on(i, p, "GPU", 200.0),
        store_on(i, p, "GPU", 250.0),
        evict_on(i, p, "CPU", 300.0),
        evict_on(i, p, "GPU", 350.0),
    ]
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn each_tier_closes_the_interval_it_opened() {
    // Collapsed on holder alone, the GPU evict at 200 closed the block's ONE interval while a
    // CPU copy was still holding it, and the tiers' stores and evicts paired up arbitrarily.
    // Tier is in the interval key so each tier's residency stands on its own.
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(&engine, "f", &offload_trace("i1", "p"), off, "corr-1");
    let ivs =
        residency_intervals(&source, "f", &harness::cache_window(&engine, "f")).expect("intervals");

    let mut seen: Vec<(Option<String>, Option<f64>, Option<f64>)> = ivs
        .iter()
        .map(|iv| {
            (
                iv.key.tier.clone(),
                iv.open_ms.map(|t| t - off),
                iv.close_ms.map(|t| t - off),
            )
        })
        .collect();
    seen.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.partial_cmp(&b.1).expect("ordered")));
    assert_eq!(
        seen,
        vec![
            (Some("CPU".to_string()), Some(150.0), Some(300.0)),
            (Some("GPU".to_string()), Some(100.0), Some(200.0)),
            (Some("GPU".to_string()), Some(250.0), Some(350.0)),
        ],
        "each tier's store pairs with that tier's own evict"
    );
    assert!(
        ivs.iter()
            .all(|iv| iv.close_reason == Some(CloseReason::Evict)),
        "no interval is left open by a tier boundary"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_promotion_off_the_local_cpu_tier_is_not_recomputed_waste() {
    // The number this whole change is about. A tier is a location within a cache, not a cache,
    // so residency is asked of the HOLDER across its tiers: the store at 250 lands inside the
    // CPU interval and is a re-admit. Collapsed intervals closed that residency at 200, so the
    // same store read as a recompute -- charging a device-to-host copy as a recomputed prefill,
    // which overstates waste on the number a customer checks first.
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(&engine, "f", &offload_trace("i1", "p"), off, "corr-2");
    let w = harness::cache_window(&engine, "f");
    let c = classify(&source, "f", &w, &Params::default()).expect("classify");

    let promotion: Vec<Class> = c
        .stores
        .iter()
        .filter(|s| (s.at_ms - off - 250.0).abs() < 0.5)
        .map(|s| s.class)
        .collect();
    assert_eq!(
        promotion,
        vec![Class::ReAdmit],
        "a store while another tier of the same holder still holds the block is a re-admit"
    );
    assert_eq!(
        c.eviction_waste_tokens, 0,
        "nothing was recomputed, so nothing is charged as waste"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn pin1_synthetic_clear_before_store_is_a_filtered_noop() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[clear("i1", 100.0), store("i1", "p", 200.0)],
        off,
        "corr-3",
    );
    let ivs =
        residency_intervals(&source, "f", &harness::cache_window(&engine, "f")).expect("intervals");
    assert_eq!(
        ivs.len(),
        1,
        "the broadcast clear must not corrupt later intervals"
    );
    assert_eq!(ivs[0].open_ms, Some(200.0 + off));
    assert_eq!(ivs[0].close_ms, None); // censored, cleanly open
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn pin2_store_at_clear_instant_survives_and_opens_post_clear() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            store("i1", "p", 100.0),
            clear("i1", 200.0),
            store("i1", "p", 200.0),
        ],
        off,
        "corr-4",
    );
    let w = harness::cache_window(&engine, "f");
    let ivs = residency_intervals(&source, "f", &w).expect("intervals");
    assert_eq!(ivs.len(), 2);
    assert_eq!(ivs[0].close_ms, Some(200.0 + off));
    assert_eq!(ivs[0].close_reason, Some(CloseReason::Clear));
    assert_eq!(ivs[1].open_ms, Some(200.0 + off)); // the survivor opens
                                                   // and the classifier reads the same instant as post-clear (delta 0)
    let c = classify(&source, "f", &w, &Params::default()).expect("classify");
    let second = c
        .stores
        .iter()
        .find(|s| (s.at_ms - (200.0 + off)).abs() < 1e-6)
        .expect("second store");
    assert_eq!(second.class, Class::PostClear);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn pin3_the_evict_clear_asymmetry_left_open_vs_absent() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    // block a: evict with no store — evidence it existed — left-open interval;
    // the clear afterwards names no blocks and must conjure nothing
    let source = harness::seed_source(
        &engine,
        "f",
        &[evict("i1", "a", 100.0), clear("i1", 200.0)],
        off,
        "corr-5",
    );
    let ivs =
        residency_intervals(&source, "f", &harness::cache_window(&engine, "f")).expect("intervals");
    assert_eq!(
        ivs.len(),
        1,
        "exactly the left-open evict interval, nothing from the clear"
    );
    let iv = &ivs[0];
    assert_eq!(iv.open_ms, None, "left-open");
    assert_eq!(iv.close_ms, Some(100.0 + off));
    assert_eq!(iv.close_reason, Some(CloseReason::Evict));
    // pin 6 rides here: the maximal-nulls row parse (left-open, no epoch)
    assert_eq!(iv.key.key_epoch, None);
    assert_eq!(iv.readmits, 0);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn pin4_multi_readmit_count() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            store("i1", "p", 100.0),
            store("i1", "p", 150.0),
            store("i1", "p", 180.0),
        ],
        off,
        "corr-6",
    );
    let ivs =
        residency_intervals(&source, "f", &harness::cache_window(&engine, "f")).expect("intervals");
    assert_eq!(ivs.len(), 1);
    assert_eq!(ivs[0].readmits, 2);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn pin5_epochs_are_disjoint_and_nothing_maps_across_them() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            json!({"kind": "store", "instance_id": "i1", "block_id": "p", "n_tokens": 4,
                   "at_ms": 100.0, "key_epoch": 1}),
            json!({"kind": "evict", "instance_id": "i1", "block_id": "p",
                   "at_ms": 200.0, "key_epoch": 1}),
            json!({"kind": "store", "instance_id": "i1", "block_id": "p", "n_tokens": 4,
                   "at_ms": 300.0, "key_epoch": 2}),
        ],
        off,
        "corr-7",
    );
    let w = harness::cache_window(&engine, "f");
    let ivs = residency_intervals(&source, "f", &w).expect("intervals");
    assert_eq!(
        ivs.len(),
        2,
        "one interval per epoch — identity is (key_epoch, p)"
    );
    // cross-epoch joining would call the epoch-2 store ETR (evict 100 ms earlier); the
    // correct answer is cold — a rotation is a declared discontinuity (spec 3.3), so there
    // is no cross-epoch mapping for a consumer to reach for at all
    let c = classify(&source, "f", &w, &Params::default()).expect("classify");
    let e2_store = c
        .stores
        .iter()
        .find(|s| s.key_epoch == Some(2))
        .expect("epoch-2 store");
    assert_eq!(
        e2_store.class,
        Class::Cold,
        "cross-epoch joins must not happen"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn clear_expansion_is_instance_scoped_across_interleaved_instances() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            store("i1", "p", 100.0),
            store("i2", "q", 110.0),
            store("i1", "r", 120.0),
            clear("i1", 200.0),
            store("i2", "s", 250.0),
        ],
        off,
        "corr-8",
    );
    let ivs =
        residency_intervals(&source, "f", &harness::cache_window(&engine, "f")).expect("intervals");
    let closed: Vec<_> = ivs
        .iter()
        .filter(|iv| iv.close_reason == Some(CloseReason::Clear))
        .collect();
    assert_eq!(closed.len(), 2, "every open interval of i1, exactly");
    assert!(closed.iter().all(|iv| iv.key.instance == "i1"));
    assert!(
        ivs.iter()
            .filter(|iv| iv.key.instance == "i2")
            .all(|iv| iv.close_ms.is_none()),
        "i2 untouched by i1's clear"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn clear_scope_is_read_as_declared_never_defaulted_to_zero() {
    // A clear record carrying dp_rank/group_idx closes ITS holder and no other; one
    // carrying neither declared no scope — the producer emits exactly that shape when the
    // wire named none — and closes every holder of the instance. Coalescing the absent
    // scope onto holder zero would leave rank 1's residency open past a clear that emptied
    // it: overstated durations, in the confident-wrong direction.
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let scoped_clear = json!({"kind": "clear", "instance_id": "i1", "scope": "all",
                              "dp_rank": 0, "group_idx": 0, "at_ms": 200.0});
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            store("i1", "p", 100.0),
            json!({"kind": "store", "instance_id": "i1", "block_id": "q", "n_tokens": 4,
                   "dp_rank": 1, "at_ms": 110.0}),
            scoped_clear,
            clear("i1", 300.0),
        ],
        off,
        "corr-9",
    );
    let ivs =
        residency_intervals(&source, "f", &harness::cache_window(&engine, "f")).expect("intervals");
    let of = |block: &str| {
        ivs.iter()
            .find(|iv| iv.key.identity == block)
            .unwrap_or_else(|| panic!("interval for {block}"))
    };
    assert_eq!(
        (of("p").close_ms, of("p").close_reason),
        (Some(200.0 + off), Some(CloseReason::Clear)),
        "the scoped clear closes its own holder"
    );
    assert_eq!(
        (of("q").close_ms, of("q").close_reason),
        (Some(300.0 + off), Some(CloseReason::Clear)),
        "rank 1 survives holder 0's clear and closes at the UNSCOPED one"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn contracts_meet_in_the_middle_tap_corpus_through_reader_to_intervals() {
    // The tap's emission fixtures as reader input: what infertap pins as output, the
    // correctness core consumes — the two contracts meet at the wire.
    // one fleet per corpus file: both fixtures use instance "a", and merging them
    // would let the clear fixture's clear close prefix_chain's intervals. A database is
    // one fleet's namespace, so each fleet has its own.
    let corpus = harness::repo_root().join("contract/inference-events/conformance/telemetry");
    let mut engines = Vec::new();
    // Block ids are read from the fixtures rather than written here. The tap's corpus was
    // regenerated from captured traffic and its hashes are now realistic u64 — several above
    // i64::MAX — so a literal here would pin this test to whichever values that regeneration
    // happened to select, and the next one would break it for no reason that matters.
    let mut ids: Vec<Vec<String>> = Vec::new();
    let mut sources = Vec::new();
    for file in ["prefix_chain.json", "clear_is_scope_level.json"] {
        let fixture: Value =
            serde_json::from_str(&std::fs::read_to_string(corpus.join(file)).expect("corpus file"))
                .expect("corpus parses");
        // Rebase into the RECENT PAST: subtract the fixture's own day floor, add the harness
        // base. Adding the base to an already-absolute stamp lands decades in the future,
        // which no segment stream can be — a segment's dated key hour is its SHIP hour, and
        // nothing ships from the future — so dated discovery rightly finds nothing there.
        let fixture_floor = fixture["expect"]["records"]
            .as_array()
            .expect("records")
            .iter()
            .filter_map(|r| r.get("at_ms").and_then(Value::as_f64))
            .fold(f64::INFINITY, f64::min)
            .div_euclid(harness::DAY_MS)
            * harness::DAY_MS;
        let off = harness::rebase_offset() - fixture_floor;
        let mut jsonl = String::new();
        let mut file_ids = Vec::new();
        for r in fixture["expect"]["records"].as_array().expect("records") {
            if let Some(b) = r.get("block_id") {
                // identities are decimal strings on the wire; Value::to_string on a string
                // would keep its JSON quotes
                let id = b.as_str().map_or_else(
                    || b.as_u64().expect("id is a string or integer").to_string(),
                    str::to_string,
                );
                if !file_ids.contains(&id) {
                    file_ids.push(id);
                }
            }
            let mut r = r.as_object().expect("record").clone();
            if let Some(at) = r.get("at_ms").and_then(Value::as_f64) {
                r.insert("at_ms".into(), (at + off).into());
            }
            jsonl.push_str(&Value::Object(r).to_string());
            jsonl.push('\n');
        }
        let fleet = format!("f-{file}");
        let (engine, db) = harness::fresh_engine();
        let report =
            harness::ingest_bytes(&engine, &fleet, file, jsonl.as_bytes()).expect("ingest corpus");
        engines.push((engine, db));
        assert!(report.records_inserted > 0);
        sources.push(harness::source_over_bytes(
            &format!("corr-mid-{file}"),
            jsonl.as_bytes(),
        ));
        ids.push(file_ids);
    }

    // prefix_chain: a root and its chained child, both open on instance a
    let (root, child) = (&ids[0][0], &ids[0][1]);
    let fleet = "f-prefix_chain.json";
    let source = &sources[0];
    let w = harness::cache_window(&engines[0].0, fleet);
    let ivs = residency_intervals(source, fleet, &w).expect("intervals");
    assert!(ivs
        .iter()
        .any(|iv| iv.key.instance == "a" && &iv.key.identity == root && iv.close_ms.is_none()));
    // the tree substrate sees the corpus's parent edge (numeric wire ids stored as
    // strings by the reader)
    let tree = analyzer_pipeline::read::block_tree(source, fleet, &w).expect("tree");
    assert!(tree.is_root(root));
    assert_eq!(tree.proven_depth(child), 1);
    assert_eq!(
        tree.nodes[child].parent().map(String::as_str),
        Some(root.as_str())
    );

    // the clear fixture: its clear terminates every open interval of its instance
    let fleet = "f-clear_is_scope_level.json";
    let source = &sources[1];
    let w = harness::cache_window(&engines[1].0, fleet);
    let ivs = residency_intervals(source, fleet, &w).expect("intervals");
    assert_eq!(
        ivs.iter()
            .filter(|iv| iv.close_reason == Some(CloseReason::Clear))
            .count(),
        2,
        "the corpus clear must expand through the interval substrate"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_no_drop_span_is_provably_complete_and_a_drop_reduces_coverage() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let hb = |at: f64, dropped: i64| {
        json!({"kind": "heartbeat", "at_ms": at, "msgs_seen": 10, "dropped": dropped,
               "oversized": 0,
               "endpoints": [{"source": "i1", "endpoint": "tcp://e:1",
                              "msgs_seen": 10, "dropped": dropped}]})
    };
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            json!({"kind": "agent_start", "at_ms": 0.0}),
            hb(10_000.0, 0),
            hb(70_000.0, 0),
            json!({"kind": "store", "instance_id": "i1", "block_id": "p", "n_tokens": 4,
                   "at_ms": 30_000.0}),
            json!({"kind": "agent_stop", "at_ms": 80_000.0, "msgs_seen": 10,
                   "dropped": 0, "endpoints": []}),
        ],
        off,
        "corr-10",
    );
    let w = Window {
        start_ms: 10_000.0 + off,
        end_ms: 70_000.0 + off,
    };
    let model = uncertainty_model(&source, "f", &w, &Params::default()).expect("model");
    let clean = model.coverage(None);
    assert!(
        (clean.fraction - 1.0).abs() < 1e-9,
        "clean deltas, started and stopped: complete ({})",
        clean.fraction
    );

    // same shape with a drop delta: the span becomes a drop window and coverage falls
    let (engine2, _db2) = harness::fresh_engine();
    let source2 = harness::seed_source(
        &engine2,
        "f",
        &[
            json!({"kind": "agent_start", "at_ms": 0.0}),
            hb(10_000.0, 0),
            hb(70_000.0, 2),
            json!({"kind": "store", "instance_id": "i1", "block_id": "p", "n_tokens": 4,
                   "at_ms": 30_000.0}),
            json!({"kind": "agent_stop", "at_ms": 80_000.0, "msgs_seen": 10,
                   "dropped": 2, "endpoints": []}),
        ],
        off,
        "corr-11",
    );
    let model2 = uncertainty_model(&source2, "f", &w, &Params::default()).expect("model");
    let droppy = model2.coverage(None);
    assert!(droppy.fraction < 1.0, "a drop window must reduce coverage");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_second_tick_never_doubles() {
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let source = harness::seed_run(
        &engine,
        "f",
        &[store("i1", "p", 1000.0), store("i1", "q", 2000.0)],
        base,
        "corr-12",
    );
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("first tick");
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("second tick");
    let out = engine
        .run(
            "SELECT count(), sum(events), sum(tokens) FROM class_rollup_daily FINAL \
             WHERE fleet_id = 'f'",
        )
        .expect("rollup");
    assert_eq!(out.trim(), "1\t2\t8", "re-analysis replaces, never doubles");
    let hourly = engine
        .run("SELECT sum(events) FROM class_rollup_hourly FINAL WHERE fleet_id = 'f'")
        .expect("hourly");
    assert_eq!(hourly.trim(), "2", "the hourly grain replaces the same way");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn band_rollup_partitions_the_class_rollup_and_never_doubles() {
    // The thrash lens's table through the real tick: the band is a
    // pure function of the gap, so banding must SPLIT the class rollup's sums without moving
    // them — and a re-pass must replace, never double, which is the purge-then-emit law
    // extended to the new table.
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let source = harness::seed_run(
        &engine,
        "f",
        &[
            // p: cold store, then a close and two re-stores at measured gaps —
            // 20 s (10s-1m) and 700 s (10-60m)
            store("i1", "p", 1_000.0),
            evict("i1", "p", 11_000.0),
            store("i1", "p", 31_000.0),
            evict("i1", "p", 40_000.0),
            store("i1", "p", 740_000.0),
            // q: cold, no prior close ever observed — the gap-unobserved band
            store("i1", "q", 2_000.0),
        ],
        base,
        "corr-band",
    );
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("first tick");
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("second tick");

    let by_band = engine
        .run(
            "SELECT band, toUInt64(sum(events)), toUInt64(sum(tokens)) \
             FROM band_rollup_daily FINAL WHERE fleet_id = 'f' \
             GROUP BY band ORDER BY band",
        )
        .expect("bands");
    assert_eq!(
        by_band.trim(),
        "10-60m\t1\t4\n10s-1m\t1\t4\nnone\t2\t8",
        "each store lands in exactly one fixed band; no prior close is `none`, never zero \
         and never a duration claim"
    );

    let class_total = engine
        .run(
            "SELECT toUInt64(sum(events)), toUInt64(sum(tokens)) \
             FROM class_rollup_daily FINAL WHERE fleet_id = 'f'",
        )
        .expect("class total");
    let band_total = engine
        .run(
            "SELECT toUInt64(sum(events)), toUInt64(sum(tokens)) \
             FROM band_rollup_daily FINAL WHERE fleet_id = 'f'",
        )
        .expect("band total");
    assert_eq!(
        band_total.trim(),
        class_total.trim(),
        "banding partitions the register: collapsing the band away must reproduce the class \
         rollup's sums exactly"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_minute_spine_sums_to_the_daily_rollup_and_never_doubles() {
    // The thrash zoom's monoid: one fold, three
    // grains, so summing the minute rows over the day at the class cut must reproduce
    // `class_rollup_daily`'s same cut exactly — and a re-pass must replace, never
    // double, which is the purge-then-emit law extended to the new table.
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let source = harness::seed_run(
        &engine,
        "f",
        &[
            // Two stores in different minutes plus a re-store after an observed close,
            // so the minute rows span classes and buckets.
            store("i1", "p", 1_000.0),
            evict("i1", "p", 11_000.0),
            store("i1", "p", 31_000.0),
            store("i1", "q", 200_000.0),
        ],
        base,
        "corr-minute",
    );
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("first tick");
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("second tick");

    let minute_total = engine
        .run(
            "SELECT class, toUInt64(sum(events)), toUInt64(sum(tokens)) \
             FROM recompute_minute FINAL WHERE fleet_id = 'f' \
             GROUP BY class ORDER BY class",
        )
        .expect("minute totals");
    let daily_total = engine
        .run(
            "SELECT class, toUInt64(sum(events)), toUInt64(sum(tokens)) \
             FROM class_rollup_daily FINAL WHERE fleet_id = 'f' \
             GROUP BY class ORDER BY class",
        )
        .expect("daily totals");
    assert_eq!(
        minute_total.trim(),
        daily_total.trim(),
        "the minute spine and the daily rollup are one fold at two grains; their class \
         cuts may never tell different stories about the same day"
    );
    let buckets = engine
        .run("SELECT count(DISTINCT bucket) FROM recompute_minute FINAL WHERE fleet_id = 'f'")
        .expect("buckets");
    assert!(
        buckets.trim().parse::<u64>().expect("count") >= 2,
        "the fixture spans minutes, so a vacuously-single-bucket pass cannot fake the sum \
         equality: {buckets}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_banded_minute_cells_sum_to_the_daily_bands_and_replay_never_doubles() {
    // The band fold at two grains is one fold, so the minute cells' day-sum at the shared cut
    // must equal `band_rollup_daily`'s — and a second tick must replace, never double. The
    // fixture spans two bands and two minutes, so neither equality can hold vacuously.
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let source = harness::seed_run(
        &engine,
        "f",
        &[
            store("i1", "p", 1_000.0),
            evict("i1", "p", 11_000.0),
            store("i1", "p", 31_000.0),
            store("i1", "q", 200_000.0),
        ],
        base,
        "corr-band-minute",
    );
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("first tick");
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("replay");

    let cut = "class, band, workload_class, model";
    let minute_total = engine
        .run(&format!(
            "SELECT {cut}, toUInt64(sum(events)), toUInt64(sum(tokens)) \
             FROM band_minute FINAL WHERE fleet_id = 'f' \
             GROUP BY {cut} ORDER BY {cut}"
        ))
        .expect("minute band totals");
    let daily_total = engine
        .run(&format!(
            "SELECT {cut}, toUInt64(sum(events)), toUInt64(sum(tokens)) \
             FROM band_rollup_daily FINAL WHERE fleet_id = 'f' \
             GROUP BY {cut} ORDER BY {cut}"
        ))
        .expect("daily band totals");
    assert_eq!(
        minute_total.trim(),
        daily_total.trim(),
        "one band fold at two grains: the cells and the daily bands may never tell \
         different stories about the same day"
    );
    let spread = engine
        .run(
            "SELECT count(DISTINCT bucket), count(DISTINCT band) \
             FROM band_minute FINAL WHERE fleet_id = 'f'",
        )
        .expect("spread");
    let f: Vec<u64> = spread
        .split_whitespace()
        .map(|v| v.parse().expect("count"))
        .collect();
    assert!(
        f[0] >= 2 && f[1] >= 2,
        "the fixture spans minutes and bands, so a single-cell pass cannot fake either \
         equality: {spread}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_banded_hourly_rollup_sums_to_the_daily_bands_and_replay_never_doubles() {
    // One band fold, three grains, so the hourly rows summed over a
    // day must reproduce `band_rollup_daily`'s same cut exactly at the FULL holder cut —
    // hourly keeps every dimension, unlike the minute cells. A re-derivation must replace,
    // never double.
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let source = harness::seed_run(
        &engine,
        "f",
        &[
            // Two stores in different HOURS plus a re-store after an observed close, so the
            // hourly rows span buckets and bands rather than collapsing to one cell.
            store("i1", "p", 1_000.0),
            evict("i1", "p", 11_000.0),
            store("i1", "p", 31_000.0),
            store("i1", "q", 4_000_000.0),
        ],
        base,
        "corr-band-hourly",
    );
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("first tick");
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("replay");

    let cut = "class, band, workload_class, model, instance_id, dp_rank, group_idx";
    let totals = |table: &str| {
        engine
            .run(&format!(
                "SELECT {cut}, toUInt64(sum(events)), toUInt64(sum(tokens)) \
                 FROM {table} FINAL WHERE fleet_id = 'f' \
                 GROUP BY {cut} ORDER BY {cut}"
            ))
            .unwrap_or_else(|e| panic!("{table}: {e}"))
    };
    assert_eq!(
        totals("band_rollup_hourly").trim(),
        totals("band_rollup_daily").trim(),
        "one band fold at two holder grains: the hourly rows and the daily bands may never \
         tell different stories about the same day"
    );
    let buckets = engine
        .run("SELECT count(DISTINCT bucket) FROM band_rollup_hourly FINAL WHERE fleet_id = 'f'")
        .expect("buckets");
    assert!(
        buckets.trim().parse::<u64>().expect("count") >= 2,
        "the fixture spans hours, so a vacuously-single-bucket pass cannot fake the sum \
         equality: {buckets}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_late_eviction_moves_banded_minute_mass_out_of_unobserved() {
    // A recompute whose eviction record has not arrived bands `none` at emission; when the late
    // segment lands, the correction path re-derives the identity and the mass must MOVE to its
    // true band at the same minute, the stale `none` key tombstoned. If the correction left the
    // old key standing, this fails.
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    // First arrival: only the recompute's own segment — no prior close in view. The late half
    // lands as a NEW segment of the run in the SAME tree (the wave idiom): a shipper never
    // rewrites a shipped object, so a rewritten-in-place fixture would test a transport that
    // does not exist.
    let tree_root = harness::fresh_dir("corr-late-band");
    harness::ship_run_segment(
        &engine,
        "f",
        &tree_root,
        &[store("i1", "p", 200_000.0)],
        base,
        0,
        &[200_000.0, 260_000.0],
    );
    let source = harness::source_over_tree("corr-late-band", tree_root.clone(), base);
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("first tick");
    let cell = "SELECT band, toUInt64(sum(tokens)) FROM band_minute FINAL \
                WHERE fleet_id = 'f' GROUP BY band ORDER BY band";
    let before = engine.run(cell).expect("before");
    assert_eq!(
        before.trim(),
        "none\t4",
        "with no observed prior close the mass bands `none` at emission: {before}"
    );

    // The late segment: the eviction that precedes it, 189 s earlier — a 1-10m gap.
    let late = [store("i1", "p", 1_000.0), evict("i1", "p", 11_000.0)];
    // The run's next segment: the late records, and heartbeats that carry its clock on.
    harness::ship_run_segment(
        &engine,
        "f",
        &tree_root,
        &late,
        base,
        1,
        &[260_000.0, 320_000.0],
    );
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("second tick");
    // The stale `none` row is a departed KEY (the band moved), retired by the whole-day
    // write's asynchronous delete and not by `FINAL`: assert the settled day.
    harness::settled(&engine);
    let after = engine.run(cell).expect("after");
    assert_eq!(
        after.trim(),
        "1-10m\t4\nnone\t4",
        "the late eviction moves the recompute's mass into its true band, and the only \
         `none` mass left is the first store's own (a genuinely unobserved prior): {after}"
    );
}

/// Two UTC days of attested heartbeats with one drop window inside day 2, positioned so the
/// coverage bracket for that drop STRADDLES midnight: the last record attested before the drop
/// span sits 1000 ms before the day boundary, the first attested after it sits 1000 ms past
/// noon on day 2. Day 1 therefore loses exactly 1000 ms and day 2 exactly half a day plus 1000.
///
/// Shipped as the tap ships it, one sequenced segment per heartbeat interval: a source's
/// counters are exact at segment boundaries, and a drop inside one segment taints all of it.
fn seed_two_days_with_a_straddling_drop(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    off: f64,
) -> (
    Window,
    analyzer_pipeline::parquet_source::ParquetSource<analyzer_pipeline::parquet_source::FsStore>,
) {
    let day = harness::DAY_MS;
    let hb = |at: f64, dropped: i64| {
        // Beats twelve hours apart, so the run declares a twelve-hour liveness bound (spec 5.8).
        json!({"kind": "heartbeat", "incarnation": "run-1", "at_ms": at, "msgs_seen": 10,
               "dropped": dropped, "oversized": 0, "heartbeat_secs": 43_200,
               "max_segment_secs": 43_200,
               "endpoints": [{"source": "i1", "endpoint": "tcp://e:1",
                              "msgs_seen": 10, "dropped": dropped}]})
    };
    let stamped = |block: &str, at: f64| {
        json!({"kind": "store", "instance_id": "i1", "block_id": block, "n_tokens": 4,
               "at_ms": at})
    };
    let segments = [
        vec![hb(0.0, 0), hb(0.5 * day, 0), stamped("p", 1000.0)],
        vec![hb(day, 0), stamped("q", day - 1000.0)],
        // the drop: the span (day .. 1.5 day) becomes a drop window, wholly inside day 2
        vec![hb(1.5 * day, 2)],
        vec![
            stamped("r", 1.5 * day + 1000.0),
            hb(2.0 * day, 2),
            json!({"kind": "agent_stop", "incarnation": "run-1", "at_ms": 2.0 * day,
                   "msgs_seen": 10, "dropped": 2, "oversized": 0, "endpoints": []}),
        ],
    ];
    let root = harness::fresh_dir("corr-14");
    for (seq, records) in (0u32..).zip(&segments) {
        harness::ship_run_segment(engine, "f", &root, records, off, seq, &[]);
    }
    let source = harness::source_over_tree("corr-14", root, off);
    (
        Window {
            start_ms: off,
            end_ms: off + 2.0 * day,
        },
        source,
    )
}

/// `(day, covered_ms, window_ms)` for i1 under one parameter generation.
fn coverage_rows(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    disc: &str,
) -> Vec<(String, f64, f64)> {
    engine
        .run(&format!(
            "SELECT toString(day), covered_ms, window_ms FROM coverage_daily FINAL \
             WHERE fleet_id = 'f' AND instance_id = 'i1' AND discriminator = '{disc}' AND gone = 0 \
             ORDER BY day"
        ))
        .expect("coverage_daily")
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            (
                f[0].to_string(),
                f[1].parse().expect("covered"),
                f[2].parse().expect("window"),
            )
        })
        .collect()
}

/// Coverage gets a HISTORY, per day and per source, and the days account for the window figure
/// exactly.
///
/// The defect: `Coverage::per_source_covered_ms` is one number for the whole widened window, so
/// the 90-day trend could state the window's coverage but not each point's — the naked number
/// `Covered` exists to prevent, one axis over.
///
/// Coverage gets a HISTORY, per day and per source, and the days account for the window figure
/// exactly.
///
/// The defect: `Coverage::per_source_covered_ms` is one number for the whole widened window, so
/// the 90-day trend could state the window's coverage but not each point's — the naked number
/// `Covered` exists to prevent, one axis over.
///
/// The uncertainty spans have real time extents, so the slice is EXACT: a drop window straddling
/// midnight charges each day for the part inside it, and no day is given a share of the whole.
/// The assertions below are the whole claim — known per-day figures, and their sum equal to what
/// the tick itself computed for the window.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn coverage_lands_per_day_and_the_days_account_for_the_window_exactly() {
    let (engine, _db) = harness::fresh_engine();
    let day = harness::DAY_MS;
    let (w, source) = seed_two_days_with_a_straddling_drop(&engine, harness::rebase_offset());
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick");
    let reference = classify(&source, "f", &w, &Params::default()).expect("the reference");

    let landed = coverage_rows(&engine, "s1000x3600000");
    assert_eq!(landed.len(), 2, "one row per UTC day: {landed:?}");
    for (d, _, window_ms) in &landed {
        assert!(
            (window_ms - day).abs() < 0.5,
            "a partial day would replace the whole day on this key: {d} {window_ms}"
        );
    }
    // day 1 loses only the 1000 ms of the bracket that falls before midnight; day 2 loses the
    // rest of it. An apportioned reading would have split the bracket some other way and put
    // uncertainty on time that was observed.
    assert!(
        (landed[0].1 - (day - 1000.0)).abs() < 1e-6,
        "day 1: {landed:?}"
    );
    assert!(
        (landed[1].1 - (0.5 * day - 1000.0)).abs() < 1e-6,
        "day 2: {landed:?}"
    );

    let window_figure = reference
        .coverage
        .per_source_covered_ms
        .iter()
        .find(|(s, _)| s == "i1")
        .expect("i1 in the window figure")
        .1;
    let summed: f64 = landed.iter().map(|(_, covered, _)| covered).sum();
    assert!(
        (summed - window_figure).abs() < 1e-6,
        "the days must account for the window figure exactly: {summed} vs {window_figure}"
    );

    // idempotent, like everything a tick writes: replacement on the key, read FINAL
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("a second tick");
    assert_eq!(
        coverage_rows(&engine, "s1000x3600000"),
        landed,
        "a second tick duplicated the coverage rows"
    );
}

/// Coverage keeps its semantic generation. Unversioned resumable frontiers cannot
/// cross generations: an incompatible attempt must preserve prior output, while a
/// fresh execution derives the requested interval under the new parameters.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_changed_skew_derives_afresh_and_preserves_previous_coverage() {
    // The property is generation coexistence for ANY discriminating parameter; the
    // example is the skew allowance, which also widens the drop windows the coverage figure
    // counts.
    let (engine, _db) = harness::fresh_engine();
    let (_, source) = seed_two_days_with_a_straddling_drop(&engine, harness::rebase_offset());
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick");
    let landed = coverage_rows(&engine, "s1000x3600000");
    assert_eq!(landed.len(), 2);

    // One working state holds one meaning: a changed parameter starts it again from the log,
    // and the previous generation's rows stay as they were, under their own discriminator.
    let changed = Params::default().with_delta_skew_ms(5000.0);
    harness::executor(&engine, "f", &changed)
        .and_then(|executor| executor.advance(&source))
        .expect("the changed generation derives afresh from the log");
    assert_eq!(
        coverage_rows(&engine, "s1000x3600000"),
        landed,
        "the changed-skew tick overwrote the previous generation's coverage"
    );
    assert_eq!(
        coverage_rows(&engine, "s5000x3600000").len(),
        2,
        "the new generation's own days are missing"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn every_classified_store_reaches_the_rollup_so_the_denominator_federates() {
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    // one of each shape the classifier can bucket differently: a cold store, a re-store after
    // an evict (ETR), and a re-admit over an open interval
    let source = harness::seed_run(
        &engine,
        "f",
        &[
            store("i1", "p", 1000.0),
            evict("i1", "p", 2000.0),
            store("i1", "p", 3000.0),
            store("i1", "q", 4000.0),
        ],
        base,
        "corr-15",
    );
    let w = Window {
        start_ms: base,
        end_ms: base + harness::DAY_MS,
    };
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick");
    let reference = classify(&source, "f", &w, &Params::default()).expect("the reference");

    let rollup: i64 = engine
        .run("SELECT toInt64(sum(tokens)) FROM class_rollup_daily FINAL WHERE fleet_id = 'f'")
        .expect("rollup")
        .trim()
        .parse()
        .expect("int");
    let stores: i64 = reference.stores.iter().map(|s| s.n_tokens).sum();

    assert_eq!(
        rollup, stores,
        "the rollup's total across every class must equal the stored mass the report divides \
         by, or a federated avoidable-fraction is computed against the wrong denominator"
    );
    assert!(stores > 0, "the fixture classified nothing");
}

/// Two stores of one block in one millisecond on one holder collapse in the rollup, so the
/// rollup counts one where the reference classification counts two.
///
/// A fact's identity is `(holder, block, key_epoch, at_ms, meaning)`, which does not include
/// `class`, and the fold counts an identity once. The classifier deliberately models this case:
/// the first store at an interval's opening instant is the opener and the rest are re-admits,
/// because "ms-resolution duplicates are plausible under burst". So both events are real, the
/// reference carries both, and the rollup carries one.
///
/// The consequence is bounded and one-directional: the rollup UNDER-counts events and tokens for
/// same-instant bursts, so a trend built on it is a lower bound.
///
/// This test asserts the WRONG behaviour on purpose. Adding `class` to the identity would make
/// both facts count and break it, which is the signal to delete it and re-bless.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn same_instant_stores_still_collapse_in_the_rollup() {
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[store("i1", "q", 4000.0), store("i1", "q", 4000.0)],
        base,
        "corr-16",
    );
    let w = Window {
        start_ms: base,
        end_ms: base + harness::DAY_MS,
    };
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick");
    let reference = classify(&source, "f", &w, &Params::default()).expect("the reference");
    let rollup: i64 = engine
        .run("SELECT toInt64(sum(tokens)) FROM class_rollup_daily FINAL WHERE fleet_id = 'f'")
        .expect("rollup")
        .trim()
        .parse()
        .expect("int");
    let stores: i64 = reference.stores.iter().map(|s| s.n_tokens).sum();

    assert_eq!(reference.stores.len(), 2, "both events classified");
    assert!(
        rollup < stores,
        "the rollup kept the same-instant re-admit — `class` reached the identity. \
         Delete this test, and re-check every trend built on class_rollup_daily."
    );
}

/// A generation that never changes produces one baseline marker, not one per pass.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn re_passing_under_one_generation_leaves_exactly_one_seam() {
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let day = (base / harness::DAY_MS).floor() * harness::DAY_MS;
    let source = harness::seed_run(
        &engine,
        "f",
        // The second store, an hour into the next day, carries every edge past the first
        // day's end and its grace, so the day settles: markers are stated on settled days.
        &[
            store("i1", "p", day + 1000.0),
            store("i1", "q", day + harness::DAY_MS + 3_600_000.0),
        ],
        0.0,
        "corr-22",
    );

    for _ in 0..3 {
        harness::executor(&engine, "f", &Params::default())
            .and_then(|executor| executor.advance(&source))
            .expect("tick");
    }

    let count: u64 = engine
        .run("SELECT count() FROM markers FINAL WHERE gone = 0 AND fleet_id = 'f' AND kind = 'param_change'")
        .expect("markers")
        .trim()
        .parse()
        .expect("count");
    assert_eq!(
        count, 1,
        "the seam is the CHANGE, not the presence — three ticks under one generation are one \
         baseline, and a set that grew with the tick cadence would be a function of the schedule"
    );
}

/// **The rank cells summed over ranks are the fleet minute cells, key for key**: one fold, two collapses. Two instances store in
/// different minutes so the rank table holds two ranks and the equality is over a real
/// spread, not one row agreeing with itself.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_rank_cells_summed_over_ranks_are_the_fleet_minute_cells() {
    let (engine, _db) = harness::fresh_engine();
    let base = harness::rebase_offset();
    let source = harness::seed_run(
        &engine,
        "f",
        &[
            store("i1", "p", 1_000.0),
            evict("i1", "p", 11_000.0),
            store("i1", "p", 31_000.0),
            store("i2", "q", 200_000.0),
            store("i2", "r", 260_000.0),
        ],
        base,
        "corr-rank",
    );
    harness::executor(&engine, "f", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick");
    let cut = "class, workload_class, ifNull(model, '-'), toString(bucket)";
    let sums = |table: &str| {
        engine
            .run(&format!(
                "SELECT {cut}, toInt64(sum(events)), toInt64(sum(tokens)) \
                 FROM {table} FINAL WHERE fleet_id = 'f' AND gone = 0 GROUP BY {cut} \
                 HAVING sum(events) > 0 ORDER BY {cut}"
            ))
            .expect("sums")
    };
    assert_eq!(
        sums("recompute_rank_minute").trim(),
        sums("recompute_minute").trim(),
        "the rank cells summed over ranks are the fleet cells: one fold, two collapses"
    );
    let ranks = engine
        .run(
            "SELECT count(DISTINCT (instance_id, dp_rank)), max(group_idx_present) FROM \
             (SELECT instance_id, dp_rank, 0 AS group_idx_present FROM recompute_rank_minute \
              WHERE fleet_id = 'f')",
        )
        .expect("ranks");
    assert_eq!(
        ranks.trim(),
        "2\t0",
        "two ranks in the table, and no group column to speak of"
    );
    let band_cut = "class, band, workload_class, ifNull(model, '-'), toString(bucket)";
    let band_sums = |table: &str| {
        engine
            .run(&format!(
                "SELECT {band_cut}, toInt64(sum(events)), toInt64(sum(tokens)) \
                 FROM {table} FINAL WHERE fleet_id = 'f' AND gone = 0 GROUP BY {band_cut} \
                 HAVING sum(events) > 0 ORDER BY {band_cut}"
            ))
            .expect("band sums")
    };
    assert_eq!(
        band_sums("band_rank_minute").trim(),
        band_sums("band_minute").trim(),
        "and the banded rank cells summed over ranks are the banded fleet cells"
    );
}
