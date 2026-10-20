// SPDX-License-Identifier: AGPL-3.0-only

//! Roster guards: the per-endpoint fan-out and the model relay's precedence.
//!
//! Hand-built cases in the suite's usual sense — each pins a reading whose wrong version
//! would be invisible in aggregate. The real producer's liveness records carry no
//! `instance_id` of their own; the instances live in `endpoints[]`, and the model dimension
//! is declared at the deployment boundary and relayed verbatim.

use serde_json::Value;

/// **The roster fans a run out over its endpoint sources, and the model obeys section 5A.3:
/// recognized topic where present, else the declared value, else null.**
///
/// The seed is the REAL producer shape: `agent_start` names no instance and no endpoints
/// (its span still opens the run), heartbeats name instances only through `endpoints[]`,
/// and the `--provenance` declaration rides the segment header. The wrong readings this
/// pins against: a roster keyed by the liveness rows' own (absent) instance collapses the
/// fleet to one empty-labelled row; a declaration outranking a recognized topic relabels
/// the finer observation with the coarser one; an unrecognized topic "parsed anyway" is a
/// guess where section 5A.1 demands refusal.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_roster_fans_out_per_endpoint_and_the_topic_outranks_the_declaration() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 9_000_000.0;

    let records: Vec<Value> = vec![
        // The header's declaration: one `--provenance` model for the run, no pod.
        serde_json::json!({"kind":"segment_open","incarnation":"run-1","at_ms":base,
                           "model":"declared-y"}),
        serde_json::json!({"kind":"agent_start","incarnation":"run-1","at_ms":base,
                           "heartbeat_secs":60,"max_segment_secs":300}),
        // i0 publishes under the llm-d convention; i1 under a bare topic the recognizer
        // must refuse.
        serde_json::json!({"kind":"heartbeat","incarnation":"run-1","at_ms":base+1000.0,
                           "msgs_seen":10,"dropped":0,"oversized":0,
                           "endpoints":[
                               {"source":"i0","endpoint":"e0","msgs_seen":6,"dropped":0,
                                "last_msg_at_ms":base+900.0,
                                "topic":"kv@pod-a@model-x"},
                               {"source":"i1","endpoint":"e1","msgs_seen":4,"dropped":0,
                                "last_msg_at_ms":base+950.0,
                                "topic":"an-arbitrary-channel"}]}),
    ];
    // One segment, as the tap ships it: the header's declaration reaches the heartbeats the
    // roster reads from the input log.
    let source = harness::seed_segment(&engine, "fx", &records, offset, "roster-model");

    harness::executor(&engine, "fx", &analyzer_analysis::Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("roster operation");

    let out = engine
        .run(
            "SELECT instance_id, ifNull(model, '-'), ifNull(pod_identity, '-'), \
                    first_seen_ms \
             FROM fleet_inventory FINAL WHERE fleet_id = 'fx' \
             ORDER BY instance_id",
        )
        .expect("query");
    let rows: Vec<Vec<&str>> = out
        .trim()
        .lines()
        .map(|l| l.split('\t').collect())
        .collect();

    assert_eq!(
        rows.len(),
        2,
        "two instances from the endpoints, and no empty-labelled third row: {rows:?}"
    );
    assert_eq!(rows[0][0], "i0");
    assert_eq!(
        rows[0][1], "model-x",
        "the recognized topic outranks the declaration"
    );
    assert_eq!(
        rows[0][2], "pod-a",
        "the convention's pod segment rides beside the label"
    );
    assert_eq!(rows[1][0], "i1");
    assert_eq!(
        rows[1][1], "declared-y",
        "an unrecognized topic yields the declaration, never a guess"
    );
    assert_eq!(
        rows[1][2], "-",
        "no pod was declared and none was recognized: null"
    );
    for r in &rows {
        let first: f64 = r[3].parse().expect("first_seen_ms");
        assert!(
            (first - (base + offset)).abs() < 1e-6,
            "the span opens at agent_start even though it names no instance: {r:?}"
        );
    }
}

/// **The relay reaches the class rollup.** One tick over the real producer shape: the topic
/// names the model, and the classified store joins it per RUN, so the rollup's model cut
/// renders a dimension the producer already stated.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_model_relay_reaches_the_class_rollup() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 11_000_000.0;

    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"segment_open","incarnation":"run-1","at_ms":base,
                           "contract_version":"1.2","segment_seq":0}),
        serde_json::json!({"kind":"agent_start","incarnation":"run-1","at_ms":base,
                           "heartbeat_secs":60,"max_segment_secs":300}),
        serde_json::json!({"kind":"heartbeat","incarnation":"run-1","at_ms":base+500.0,
                           "msgs_seen":1,"dropped":0,"oversized":0,
                           "endpoints":[{"source":"i0","endpoint":"e0","msgs_seen":1,
                                         "dropped":0,"last_msg_at_ms":base+400.0,
                                         "topic":"kv@pod-a@model-x"}]}),
        serde_json::json!({"kind":"store","instance_id":"i0","incarnation":"run-1",
                           "block_id":"b1","n_tokens":8,"at_ms":base+1000.0}),
        // the run's clock a minute on: the tick classifies through it, less the skew band
        serde_json::json!({"kind":"heartbeat","incarnation":"run-1","at_ms":base+61_000.0,
                           "msgs_seen":2,"dropped":0,"oversized":0,
                           "endpoints":[{"source":"i0","endpoint":"e0","msgs_seen":2,
                                         "dropped":0,"topic":"kv@pod-a@model-x"}]}),
    ];
    let source = harness::seed_segment(&engine, "fx", &records, offset, "carry-model");

    harness::executor(&engine, "fx", &analyzer_analysis::Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick");

    let out = engine
        .run(
            "SELECT DISTINCT ifNull(model, '-') FROM class_rollup_hourly FINAL \
             WHERE fleet_id = 'fx' AND gone = 0",
        )
        .expect("query");
    assert_eq!(
        out.trim(),
        "model-x",
        "the run-scoped relay labels the rollup row"
    );
}

/// **The relay reaches the minute cells**: `recompute_minute` carries `model` and the minute
/// collapse keeps it, so the model cut answers at the 3 h zoom on the recompute chart as it does
/// on the banded one. The fixture declares a model and the pin asserts it ON the cells — a null
/// would agree with a null and prove nothing — then holds the cells to the daily rollup at
/// class × workload × model: one fold at two grains.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_model_relay_reaches_the_minute_cells() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 11_000_000.0;

    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"segment_open","incarnation":"run-1","at_ms":base,
                           "contract_version":"1.2","segment_seq":0}),
        serde_json::json!({"kind":"agent_start","incarnation":"run-1","at_ms":base,
                           "heartbeat_secs":60,"max_segment_secs":300}),
        serde_json::json!({"kind":"heartbeat","incarnation":"run-1","at_ms":base+500.0,
                           "msgs_seen":2,"dropped":0,"oversized":0,
                           "endpoints":[{"source":"i0","endpoint":"e0","msgs_seen":2,
                                         "dropped":0,"last_msg_at_ms":base+400.0,
                                         "topic":"kv@pod-a@model-x"}]}),
        serde_json::json!({"kind":"store","instance_id":"i0","incarnation":"run-1",
                           "block_id":"b1","n_tokens":8,"at_ms":base+1000.0}),
        // a second minute, so a single-bucket pass cannot fake the sum equality
        serde_json::json!({"kind":"store","instance_id":"i0","incarnation":"run-1",
                           "block_id":"b2","n_tokens":8,"at_ms":base+200_000.0}),
        // the run's clock a minute on: the tick classifies through it, less the skew band
        serde_json::json!({"kind":"heartbeat","incarnation":"run-1","at_ms":base+260_000.0,
                           "msgs_seen":2,"dropped":0,"oversized":0,
                           "endpoints":[{"source":"i0","endpoint":"e0","msgs_seen":2,
                                         "dropped":0,"topic":"kv@pod-a@model-x"}]}),
    ];
    let source = harness::seed_segment(&engine, "fx", &records, offset, "carry-model-minute");
    harness::executor(&engine, "fx", &analyzer_analysis::Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick");

    let models = engine
        .run(
            "SELECT DISTINCT ifNull(model, '-') FROM recompute_minute FINAL \
             WHERE fleet_id = 'fx'",
        )
        .expect("models on the cells");
    assert_eq!(
        models.trim(),
        "model-x",
        "the minute cells carry the relayed model; a null here is the collapse dropping it"
    );
    let cut = "class, workload_class, ifNull(model, '-')";
    let sums = |table: &str| {
        engine
            .run(&format!(
                "SELECT {cut}, toUInt64(sum(events)), toUInt64(sum(tokens)) \
                 FROM {table} FINAL WHERE fleet_id = 'fx' GROUP BY {cut} ORDER BY {cut}"
            ))
            .expect("sums")
    };
    assert_eq!(
        sums("recompute_minute").trim(),
        sums("class_rollup_daily").trim(),
        "one fold at two grains: the cells and the daily rollup agree at the model cut"
    );
    let buckets = engine
        .run("SELECT count(DISTINCT bucket) FROM recompute_minute FINAL WHERE fleet_id = 'fx'")
        .expect("buckets");
    assert!(
        buckets.trim().parse::<u64>().expect("count") >= 2,
        "two minutes, so the equality is over a real spread: {buckets}"
    );
}
