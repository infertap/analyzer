// SPDX-License-Identifier: AGPL-3.0-only

//! **A keyed tick's cost on a synthetic day, locally**: a first tick over a few hundred thousand
//! events, then an in-order tick over a tenth as many, each timed through the executor the
//! daemon runs. Run explicitly with Cargo:
//!
//!     BENCH_EVENTS=300000 cargo bench -p analyzer-pipeline --bench tick
//!
//! Not a pin: it asserts only that both ticks classified. What it prints is the measurement.

#![allow(clippy::cast_precision_loss)] // a bench's synthetic instants

use analyzer_analysis::Params;
use serde_json::{json, Value};

fn heartbeat(at: f64, run: &str, instances: &[String]) -> Value {
    let endpoints: Vec<Value> = instances
        .iter()
        .map(|i| json!({"source": i, "endpoint": format!("tcp://{i}:1"), "msgs_seen": 10, "dropped": 0, "last_msg_at_ms": at}))
        .collect();
    json!({"kind": "heartbeat", "at_ms": at, "incarnation": run, "canary": format!("k-{run}"),
        "msgs_seen": 10, "dropped": 0, "oversized": 0, "heartbeat_secs": 60, "max_segment_secs": 300,
        "endpoints": endpoints})
}

/// `events` cache events over `instances`, blocks cycling so re-stores, evictions and
/// readmits all occur, spread over `[from_ms, to_ms)` in time order.
fn wave(
    events: usize,
    instances: &[String],
    run: &str,
    from_ms: f64,
    to_ms: f64,
    seed: u64,
) -> Vec<Value> {
    let mut records = vec![
        json!({"kind": "agent_start", "at_ms": from_ms, "incarnation": run, "canary": format!("k-{run}"), "heartbeat_secs": 60, "max_segment_secs": 300}),
        heartbeat(from_ms + 1000.0, run, instances),
    ];
    let blocks = (events / 4).max(1);
    let step = (to_ms - from_ms) / events as f64;
    let mut x = seed;
    for n in 0..events {
        x = x
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let at = from_ms + step * n as f64;
        let instance = &instances[(x >> 33) as usize % instances.len()];
        let block = format!("b{}", (x >> 40) as usize % blocks);
        let kind = if (x >> 20).is_multiple_of(5) {
            "evict"
        } else {
            "store"
        };
        let mut r = json!({"kind": kind, "instance_id": instance, "dp_rank": 0, "group_idx": 0,
            "block_id": block, "content_id": block, "at_ms": at, "incarnation": run, "workload_class": "agentic"});
        if kind == "store" {
            r["n_tokens"] = json!(8 + ((x >> 50) % 64).cast_signed());
        }
        records.push(r);
    }
    records.push(heartbeat(to_ms - 1000.0, run, instances));
    records
}

fn main() {
    let events: usize = std::env::var("BENCH_EVENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300_000);
    let instances: Vec<String> = (0..8).map(|i| format!("i{i}")).collect();
    let base = harness::rebase_offset();
    let hour = 3_600_000.0;
    let root = harness::fresh_dir("bench-tick");
    let src = harness::source_over_tree("bench-tick", root.clone(), base);
    let (engine, _db) = harness::fresh_engine();
    harness::serving(&engine)
        .bind(&analyzer_pipeline::execution::Namespace::new("fixture-source", "f").unwrap())
        .unwrap();
    let waves = [
        (
            wave(
                events,
                &instances,
                "run-1",
                base + 1.0 * hour,
                base + 4.0 * hour,
                7,
            ),
            "first tick",
        ),
        (
            wave(
                events / 10,
                &instances,
                "run-1",
                base + 4.0 * hour,
                base + 4.3 * hour,
                11,
            ),
            "in-order tick",
        ),
    ];
    // The lineage's lake in a directory store, as the corpora run it.
    let lake = harness::fresh_dir("bench-lake");
    let spine = std::sync::Arc::new(
        analyzer_pipeline::spine::Spine::new(
            std::sync::Arc::new(analyzer_objectstore::DirStore::new(&lake)),
            "tap/",
            &analyzer_pipeline::execution::Namespace::new("fixture-source", "f").unwrap(),
            analyzer_pipeline::lineage::Lineage::new("bench", "bench").unwrap(),
        )
        .with_working_state(
            &lake.with_extension("working"),
            analyzer_lake::working::DEFAULT_MEMORY_BYTES,
        )
        .unwrap(),
    );
    for (i, (records, what)) in waves.iter().enumerate() {
        let planted_at = std::time::Instant::now();
        let (planted, _) = harness::plant_tree(records, 0.0, &format!("bench-w{i}"));
        let files = harness::planted_files(&planted);
        let copied = harness::copy_planted(&files, &planted, &root, &format!("w{i}"));
        for (file, bytes) in &copied {
            analyzer_ingest::ingest_bytes(&spine.lake(), "f", file, bytes).expect("ingest");
        }
        eprintln!(
            "bench: {what}: {} records planted and ingested in {:.1}s",
            records.len(),
            planted_at.elapsed().as_secs_f64()
        );
        let started = std::time::Instant::now();
        let tick = harness::advance_on(
            &engine,
            std::sync::Arc::clone(&spine),
            &src,
            &Params::default(),
        )
        .unwrap_or_else(|e| panic!("{what}: {e}"));
        assert!(tick.stores > 0, "{what}: nothing classified");
        eprintln!(
            "bench: {what}: tick {:.1}s, {} stores classified, settled {:?}, classified to {:?}",
            started.elapsed().as_secs_f64(),
            tick.stores,
            tick.settled_days,
            tick.classified_to_ms
        );
    }
}
