// SPDX-License-Identifier: AGPL-3.0-only

//! The lifecycle bank. The ingest records every lifecycle row a segment carries in the
//! input log, as the tap's own JSON, and the source's lifecycle read takes the log's rows for
//! its window and walks only segments the log never saw. The pin: one ingested wire segment
//! lands its lifecycle rows in the log; the rows re-parse to the same events the walk would
//! build; a source over an empty tree with the bank attached answers from the log alone.

use analyzer_analysis::events::EventSource;
use analyzer_pipeline::arrivals;
use analyzer_pipeline::parquet_source::lifecycle_event_of;
use analyzer_serving_clickhouse::engine::{migrate, RetentionConfig};

fn wire_segment() -> String {
    [
        r#"{"kind":"segment_open","at_ms":1000.0,"contract_version":"1.10","incarnation":"1700000000000-42","segment_seq":3,"heartbeat_secs":60,"max_segment_secs":300,"key_epoch":0,"gpu":"fixture","vllm_version":"0.27.1","model":"declared-model","pod_identity":"pod-1"}"#,
        r#"{"kind":"store","at_ms":1500.0,"instance_id":"e2c","block_id":"b1","n_tokens":16,"tier":"GPU","dp_rank":0,"group_idx":0,"seq":1}"#,
        r#"{"kind":"heartbeat","at_ms":2000.0,"msgs_seen":5,"dropped":1,"oversized":0,"unknown_types":0,"events_ingested":5,"content_unresolved":0,"content_bridge_entries":0,"content_bridge_evicted":0,"publisher_restarts":2,"endpoints":[{"source":"e2","endpoint":"tcp://h:5557","msgs_seen":5,"dropped":1,"last_msg_at_ms":1990.0,"publisher_restarts":2,"topic":"kv@pod-1@topic-model"},{"source":"e2b","endpoint":"tcp://h:6557","msgs_seen":0,"dropped":0}]}"#,
        r#"{"kind":"agent_stop","at_ms":3000.0,"incarnation":"1700000000000-42","reason":"fixture"}"#,
    ]
    .join("\n")
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_ingest_banks_a_segments_lifecycle_rows_and_the_read_takes_them_without_a_walk() {
    let (engine, _db) = harness::fresh_engine();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    let bytes = wire_segment().into_bytes();
    let report =
        harness::ingest_bytes(&engine, "f1", "segments/f1/one.jsonl", &bytes).expect("ingest");
    assert_eq!(report.duplicates_skipped, 0, "{report:?}");

    // The bank holds the three lifecycle rows and the segment's sentinel.
    let (rows, banked) =
        arrivals::lifecycle_bank_read(&harness::lake(&engine, "f1"), 0.0, 10_000.0)
            .expect("bank read");
    assert_eq!(rows.len(), 3, "the lifecycle rows banked: {rows:?}");
    assert_eq!(
        banked,
        std::iter::once("segments/f1/one.jsonl".to_string()).collect(),
        "the segment is banked by its key"
    );
    let mut kinds: Vec<String> = rows
        .iter()
        .map(|r| {
            let v: serde_json::Value = serde_json::from_str(r).expect("a banked row is JSON");
            lifecycle_event_of(&v)
                .expect("parses")
                .expect("a lifecycle event")
                .kind
        })
        .collect();
    kinds.sort();
    assert_eq!(kinds, ["agent_stop", "heartbeat", "segment_open"]);

    // A window that misses the rows still reports the segment as banked, and no rows.
    let (none, still_banked) =
        arrivals::lifecycle_bank_read(&harness::lake(&engine, "f1"), 50_000.0, 60_000.0)
            .expect("bank read");
    assert!(none.is_empty(), "{none:?}");
    assert_eq!(
        still_banked.len(),
        1,
        "the sentinel keeps the segment banked"
    );

    // A source over an empty tree with the bank attached answers from the bank alone: the
    // walk has nothing to walk and the events are the banked ones, in the read's order.
    let root = harness::fresh_dir("lifecycle-bank-empty");
    let bank_lake = harness::lake(&engine, "f1");
    let source = harness::source_over_tree("lifecycle-bank", root, 0.0).with_lifecycle_bank(
        Box::new(move |lo, hi| arrivals::lifecycle_bank_read(&bank_lake, lo, hi)),
    );
    let events = source
        .lifecycle_events("f1", 0.0, 10_000.0)
        .expect("lifecycle events from the bank");
    let got: Vec<(String, f64)> = events.iter().map(|e| (e.kind.clone(), e.at_ms)).collect();
    assert_eq!(
        got,
        [
            ("segment_open".to_string(), 1000.0),
            ("heartbeat".to_string(), 2000.0),
            ("agent_stop".to_string(), 3000.0),
        ]
    );
}
