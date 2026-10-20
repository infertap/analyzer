// SPDX-License-Identifier: AGPL-3.0-only

//! The arrivals derivation, through real ingest into an input log of its own (facts at
//! ingest, verdicts at read). One schema-valid wire segment goes through `ingest_bytes`, and
//! the batch's record answers what the roster and health reads will ask: the
//! run-level row carries the envelope, the fan-out carries one row per endpoint source and
//! per cache-record instance, the topic recognizer's model outranks nothing here (it is a
//! FACT column — precedence is the read's business), and a declared drop lands under the
//! incarnation whose data is gone. The second ingest of the same bytes is skipped whole by
//! the input log — the content hash is the idempotence boundary.

mod common;

fn wire_segment() -> String {
    [
        r#"{"kind":"segment_open","at_ms":1000.0,"contract_version":"1.10","incarnation":"1700000000000-42","segment_seq":3,"heartbeat_secs":60,"max_segment_secs":300,"key_epoch":0,"gpu":"fixture","vllm_version":"0.27.1","model":"declared-model","pod_identity":"pod-1"}"#,
        r#"{"kind":"store","at_ms":1500.0,"instance_id":"e2c","block_id":"b1","n_tokens":16,"tier":"GPU","dp_rank":0,"group_idx":0,"seq":1}"#,
        r#"{"kind":"heartbeat","at_ms":2000.0,"msgs_seen":5,"dropped":1,"oversized":0,"unknown_types":0,"events_ingested":5,"content_unresolved":0,"content_bridge_entries":0,"content_bridge_evicted":0,"publisher_restarts":2,"endpoints":[{"source":"e2","endpoint":"tcp://h:5557","msgs_seen":5,"dropped":1,"last_msg_at_ms":1990.0,"publisher_restarts":2,"topic":"kv@pod-1@topic-model"},{"source":"e2b","endpoint":"tcp://h:6557","msgs_seen":0,"dropped":0}]}"#,
        r#"{"kind":"heartbeat","at_ms":2600.0,"msgs_seen":6,"dropped":1,"oversized":0,"unknown_types":0,"events_ingested":6,"content_unresolved":0,"content_bridge_entries":0,"content_bridge_evicted":0,"publisher_restarts":2,"endpoints":[{"source":"e2","endpoint":"tcp://h:5557","msgs_seen":6,"dropped":1,"last_msg_at_ms":2590.0,"publisher_restarts":2,"topic":"kv@pod-1@topic-model"},{"source":"e2b","endpoint":"tcp://h:6557","msgs_seen":0,"dropped":0}]}"#,
        r#"{"kind":"segments_dropped","at_ms":2700.0,"incarnation":"1600000000000-7","count":3,"first":"seg-1600000000000-7-4.jsonl","last":"seg-1600000000000-7-6.jsonl","first_seq":4,"last_seq":6}"#,
        r#"{"kind":"agent_stop","at_ms":3000.0,"msgs_seen":6,"dropped":1,"oversized":0,"unknown_types":0,"events_ingested":6,"content_unresolved":0,"content_bridge_entries":0,"content_bridge_evicted":0,"publisher_restarts":2,"reason":"duration"}"#,
        "",
    ]
    .join("\n")
}

#[test]
fn arrivals_facts_land_fan_out_and_dedup() {
    let lake = common::fresh_lake("arrivals");
    let bytes = wire_segment().into_bytes();
    let first = analyzer_ingest::ingest_bytes(&lake, "f1", "test", &bytes).expect("ingest");
    assert_eq!(first.files_ingested, 1);

    // The fan-out: the run-level row, both endpoint sources, and the cache-record instance
    // that no endpoint named — four rows, one segment.
    let batches = common::committed(&lake);
    assert_eq!(batches.len(), 1);
    let mut rows: Vec<String> = batches[0]
        .segments
        .iter()
        .map(|a| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                a.instance.as_deref().unwrap_or(""),
                a.msgs_seen,
                a.publisher_restarts,
                a.hb_count,
                a.topic_model.as_deref().unwrap_or(""),
                a.declared_model.as_deref().unwrap_or(""),
                a.engine_version.as_deref().unwrap_or(""),
                u8::from(a.stop_at_ms.is_some())
            )
        })
        .collect();
    rows.sort();
    assert_eq!(rows.len(), 4, "run-level + e2 + e2b + e2c: {rows:?}");
    // Run-level: the envelope's cumulative marks are the LAST heartbeat's, and the segment
    // header's declarations ride every row.
    assert_eq!(rows[0], "\t6\t2\t2\t\tdeclared-model\t0.27.1\t1");
    // e2: the per-endpoint counters, and the recognized topic model AS A FACT — the section 5A
    // precedence over the declaration is decided at read, not here.
    assert_eq!(
        rows[1],
        "e2\t6\t2\t2\ttopic-model\tdeclared-model\t0.27.1\t1"
    );
    // e2b: a silent endpoint is still a member.
    assert_eq!(rows[2], "e2b\t0\t0\t2\t\tdeclared-model\t0.27.1\t1");
    // e2c: membership from a cache record alone — the store named it, no endpoint did.
    assert_eq!(rows[3], "e2c\t0\t0\t2\t\tdeclared-model\t0.27.1\t1");

    // The declared drop, under the incarnation whose DATA IS GONE — never the decoder's.
    let drops: Vec<(String, i64, i64)> = batches[0]
        .drops
        .iter()
        .map(|d| (d.incarnation.clone(), d.first_seq, d.last_seq))
        .collect();
    assert_eq!(drops, vec![("1600000000000-7".to_string(), 4, 6)]);

    // Same bytes again: the input log skips the whole file — content hash is the
    // idempotence boundary — and the log does not move.
    let second = analyzer_ingest::ingest_bytes(&lake, "f1", "test", &bytes).expect("re-ingest");
    assert_eq!(second.duplicates_skipped, 1);
    assert_eq!(common::committed(&lake).len(), 1);
}
