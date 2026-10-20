// SPDX-License-Identifier: AGPL-3.0-only

//! Coverage and taint guards: a drop window, and an intact stream.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite holds what a corpus snapshot
//! cannot express: **hand-built cases where one
//! specific wrong reading would be invisible in aggregate.** Each is a flattering-direction guard
//! from `docs/error-direction.md` — a reading that looks reasonable and moves the headline number
//! the way you would like it to move.

use analyzer_analysis::completeness::{uncertainty_model_from_events, EVIDENCE_CLIP_MARGIN_MS};
use analyzer_analysis::events::EventSource;
use analyzer_analysis::{Params, Window};
use serde_json::Value;

/// A drop window built explicitly, so the suite is not only comparing clean streams.
///
/// Two heartbeats whose per-endpoint `dropped` counter moves between them: the span they bracket
/// is a drop window, coverage falls, and every record inside the skew-widened bracket is tainted.
/// A fold that mis-read the parallel `endpoints.*` arrays would report no drops at all — a fleet
/// that looks perfectly observed, which is the flattering direction.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_moving_drop_counter_makes_a_drop_window() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 7_000_000.0;

    let hb = |at: f64, dropped: i64| {
        serde_json::json!({
            "kind":"heartbeat","instance_id":"i1","incarnation":"run-1","at_ms":at,
            "dropped":dropped,"oversized":0,
            "endpoints":[{"source":"i1","endpoint":"e","msgs_seen":10,"dropped":dropped,
                          "last_msg_at_ms":at}]
        })
    };
    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"run-1",
                           "at_ms":base,"heartbeat_secs":60,"max_segment_secs":300}),
        hb(base + 1000.0, 0),
        // the counter moves: everything between these two heartbeats is a drop window
        hb(base + 2000.0, 5),
        hb(base + 3000.0, 5),
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"p","n_tokens":8,
                           "at_ms":base+2500.0}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "coverage-drop");

    let w = Window {
        start_ms: base + offset,
        end_ms: base + offset + 4000.0,
    };
    let params = Params::default();

    // The property, asserted on the IR path directly so a shared bug cannot pass: the moving
    // counter must actually taint the record that sits inside its bracket.
    let cache = source.cache_events("fx", &w).expect("cache IR");
    let lifecycle = source
        .lifecycle_events(
            "fx",
            w.start_ms - EVIDENCE_CLIP_MARGIN_MS,
            w.end_ms + EVIDENCE_CLIP_MARGIN_MS,
        )
        .expect("lifecycle IR");
    let model = uncertainty_model_from_events(&cache, &lifecycle, &w, &params).expect("model");
    assert!(
        model.is_record_tainted("i1", base + offset + 2500.0),
        "a record inside a drop window's bracket must be tainted, or absence gets read as \
         evidence and the store lands `cold` instead of `indeterminate`"
    );
}

/// A clean stream: no counter movement anywhere, so no span is a drop window and nothing is
/// tainted. The complement of the case above — without it, a fold that tainted EVERYTHING would
/// pass the drop-window test and be caught by nothing.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_intact_stream_taints_nothing() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 8_000_000.0;

    let hb = |at: f64| {
        serde_json::json!({
            "kind":"heartbeat","instance_id":"i1","incarnation":"run-1","at_ms":at,
            "dropped":0,"oversized":0,
            "endpoints":[{"source":"i1","endpoint":"e","msgs_seen":10,"dropped":0,
                          "last_msg_at_ms":at}]
        })
    };
    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"run-1",
                           "at_ms":base,"heartbeat_secs":60,"max_segment_secs":300}),
        hb(base + 1000.0),
        hb(base + 2000.0),
        serde_json::json!({"kind":"store","instance_id":"i1","block_id":"p","n_tokens":8,
                           "at_ms":base+1500.0}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "coverage-clean");

    let w = Window {
        start_ms: base + offset,
        end_ms: base + offset + 3000.0,
    };
    let params = Params::default();
    let cache = source.cache_events("fx", &w).expect("cache IR");
    let lifecycle = source
        .lifecycle_events(
            "fx",
            w.start_ms - EVIDENCE_CLIP_MARGIN_MS,
            w.end_ms + EVIDENCE_CLIP_MARGIN_MS,
        )
        .expect("lifecycle IR");
    let model = uncertainty_model_from_events(&cache, &lifecycle, &w, &params).expect("model");
    assert!(
        !model.is_record_tainted("i1", base + offset + 1500.0),
        "an intact stream must taint nothing, or every figure reads `indeterminate` and the \
         taxonomy stops distinguishing anything"
    );
}
