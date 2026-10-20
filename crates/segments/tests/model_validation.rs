// SPDX-License-Identifier: AGPL-3.0-only
//! Segment-boundary validation and full-width identity preservation.

use analyzer_segments::decode_segment;
use serde_json::{json, Value};

#[test]
fn rejects_out_of_range_fields_at_the_segment_boundary() {
    let record = json!({
        "kind": "store", "at_ms": 1.0, "instance_id": "i", "block_id": "b",
        "dp_rank": 4_294_967_296_u64, "group_idx": 0, "n_tokens": 16, "tier": "GPU"
    });
    let error = decode_segment("segment.jsonl", "f", record.to_string().as_bytes())
        .expect_err("dp_rank exceeds the contract's u32 range");
    assert!(error.to_string().contains("dp_rank"));
}

#[test]
fn preserves_identities_that_differ_only_above_64_bits() {
    let ids = [
        "00000000000000000000000000000007",
        "00000010000000000000000000000007",
    ];
    let mut records = String::new();
    for id in ids {
        let record = json!({
            "kind": "store", "at_ms": 1.0, "instance_id": "i", "block_id": "b",
            "content_id": id, "dp_rank": 0, "group_idx": 0, "n_tokens": 16, "tier": "GPU"
        });
        records.push_str(&record.to_string());
        records.push('\n');
    }
    let decoded = decode_segment("segment.jsonl", "f", records.as_bytes()).expect("decode");
    let rows: Vec<Value> = decoded
        .shaped
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows[0]["content_id"], ids[0]);
    assert_eq!(rows[1]["content_id"], ids[1]);
}

#[test]
fn rejects_invalid_nested_endpoint_counters() {
    let record = json!({
        "kind": "heartbeat", "at_ms": 1.0,
        "endpoints": [{"source": "i", "endpoint": "tcp://localhost:5557",
                       "msgs_seen": -1, "dropped": 0}]
    });
    let error = decode_segment("segment.jsonl", "f", record.to_string().as_bytes())
        .expect_err("negative cumulative counter");
    assert!(error.to_string().contains("msgs_seen"), "{error}");
}

#[test]
fn rejects_a_nested_endpoint_missing_a_required_field() {
    // An endpoint decodes as a whole struct, so the contract's required fields hold inside it.
    let record = json!({
        "kind": "heartbeat", "at_ms": 1.0,
        "endpoints": [{"source": "i", "endpoint": "tcp://localhost:5557", "msgs_seen": 3}]
    });
    let error = decode_segment("segment.jsonl", "f", record.to_string().as_bytes())
        .expect_err("an endpoint without its dropped counter");
    assert!(error.to_string().contains("dropped"), "{error}");
}
