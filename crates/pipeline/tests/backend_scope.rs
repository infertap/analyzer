// SPDX-License-Identifier: AGPL-3.0-only
//! Backend scope survives projection, interval folding, and frontier construction.
use analyzer_analysis::classify::frontiers;
use analyzer_analysis::intervals::residency_intervals_from_events;
use analyzer_pipeline::parquet_source::cache_event_of;
use serde_json::json;

#[test]
fn backend_eviction_does_not_close_another_backend() {
    let rows = [
        json!({"kind":"store","instance_id":"i0","block_id":"b","content_id":"c","tier":"STORAGE","backend_id":"a","at_ms":1,"n_tokens":16}),
        json!({"kind":"store","instance_id":"i0","block_id":"b","content_id":"c","tier":"STORAGE","backend_id":"b","at_ms":2,"n_tokens":16}),
        json!({"kind":"evict","instance_id":"i0","block_id":"b","content_id":"c","tier":"STORAGE","backend_id":"a","at_ms":3}),
    ];
    let mut events: Vec<_> = rows
        .iter()
        .map(|row| cache_event_of(row, None).unwrap().unwrap())
        .collect();
    let intervals = residency_intervals_from_events(&events);
    assert_eq!(intervals.len(), 2);
    let a = intervals
        .iter()
        .find(|i| i.key.backend_id.as_deref() == Some("a"))
        .unwrap();
    let b = intervals
        .iter()
        .find(|i| i.key.backend_id.as_deref() == Some("b"))
        .unwrap();
    assert_eq!(a.close_ms, Some(3.0));
    assert_eq!(b.close_ms, None);
    assert_eq!(a.holder(), b.holder());
    let banked = frontiers(&intervals, &std::collections::HashMap::new());
    assert_eq!(banked.len(), 2);
    assert!(banked
        .iter()
        .any(|f| f.key.backend_id.as_deref() == Some("b") && f.open));
    let clear = json!({"kind":"clear","instance_id":"i0","tier":"STORAGE","at_ms":4});
    events.push(cache_event_of(&clear, None).unwrap().unwrap());
    let intervals = residency_intervals_from_events(&events);
    assert!(intervals.iter().all(|i| i.close_ms.is_some()));
}

#[test]
fn an_absent_backend_preserves_tier_level_scope() {
    let rows = [
        json!({"kind":"store","instance_id":"i0","block_id":"b","tier":"CPU","at_ms":1,"n_tokens":16}),
        json!({"kind":"evict","instance_id":"i0","block_id":"b","tier":"CPU","at_ms":2}),
    ];
    let events: Vec<_> = rows
        .iter()
        .map(|row| cache_event_of(row, None).unwrap().unwrap())
        .collect();
    let intervals = residency_intervals_from_events(&events);
    assert_eq!(intervals.len(), 1);
    assert_eq!(intervals[0].key.backend_id, None);
    assert_eq!(intervals[0].close_ms, Some(2.0));
}
