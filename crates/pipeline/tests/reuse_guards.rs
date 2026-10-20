// SPDX-License-Identifier: AGPL-3.0-only

//! Section 5.9 guards: the permit/refuse gate that keeps the inverted measurement out.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite holds what a corpus snapshot
//! cannot express: **hand-built cases where one
//! specific wrong reading would be invisible in aggregate.** Each is a flattering-direction guard
//! from `docs/error-direction.md` — a reading that looks reasonable and moves the headline number
//! the way you would like it to move.

use analyzer_analysis::completeness::LIFECYCLE_CLIP_MARGIN_MS;
use analyzer_analysis::events::EventSource;
use analyzer_analysis::reuse::{declaration_from_events, figures_from_events};
use serde_json::Value;

/// The gate itself: under `unlabelled`, two of the three figures must REFUSE.
///
/// A fold that counted them would present a number where the reader cannot answer, and the number
/// would flatter: it rises with cache effectiveness. The recompute figure stays countable under
/// every declaration, because a store following an OBSERVED close is qualified by that close
/// rather than by the producer's ability to label.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_unlabelled_stream_refuses_the_two_unqualified_figures() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 17_000_000.0;

    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"r1",
                           "at_ms":base,"heartbeat_secs":60,"max_segment_secs":300,
                           "reuse_reporting":"unlabelled"}),
        serde_json::json!({"kind":"store","instance_id":"i1","incarnation":"r1","block_id":"b",
                           "n_tokens":100,"at_ms":base+100.0}),
        serde_json::json!({"kind":"evict","instance_id":"i1","incarnation":"r1","block_id":"b",
                           "at_ms":base+200.0}),
        // a store AFTER an observed close: qualified, and therefore countable regardless
        serde_json::json!({"kind":"store","instance_id":"i1","incarnation":"r1","block_id":"b",
                           "n_tokens":100,"at_ms":base+300.0}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "reuse-unlabelled");
    let w = harness::cache_window(&engine, "fx");

    let cache = source.cache_events("fx", &w).expect("cache IR");
    let lifecycle = source
        .lifecycle_events(
            "fx",
            w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
            w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
        )
        .expect("lifecycle IR");
    let (unqualified, recompute, first_insertion) = figures_from_events(&cache, &lifecycle);

    assert!(
        !unqualified.countable,
        "an unqualified store count on an unlabelled stream is the inverted measurement section 5.9 \
         forbids: {unqualified:?}"
    );
    assert!(
        !first_insertion.countable,
        "first insertion is exactly where a reuse and an insertion are indistinguishable"
    );
    assert!(
        recompute.countable,
        "a store following an observed close is qualified by that close, not by the label"
    );
}

/// **The permitting side, which nothing could reach before the harness carried
/// `reuse_reporting`.** A producer declaring `none` never announces reuse as a store, so every
/// store IS an insertion and the unqualified count is permitted. Without this case a fold that
/// refused unconditionally would pass the whole suite.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_none_declaration_permits_the_unqualified_count() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 19_000_000.0;

    let source = harness::seed_source(
        &engine,
        "fx",
        &[
            serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"r1",
                               "at_ms":base,"heartbeat_secs":60,"max_segment_secs":300,
                               "reuse_reporting":"none"}),
            serde_json::json!({"kind":"store","instance_id":"i1","incarnation":"r1",
                               "block_id":"b","n_tokens":100,"at_ms":base+100.0}),
        ],
        offset,
        "reuse-none",
    );
    let w = harness::cache_window(&engine, "fx");
    let cache = source.cache_events("fx", &w).expect("cache IR");
    let lifecycle = source
        .lifecycle_events(
            "fx",
            w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
            w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
        )
        .expect("lifecycle IR");

    assert_eq!(
        declaration_from_events(&lifecycle),
        analyzer_analysis::reuse::Declaration::None_,
        "the declaration must actually reach the reader, or this suite only ever tests refusal"
    );
    let (unqualified, _, first_insertion) = figures_from_events(&cache, &lifecycle);
    assert!(
        unqualified.countable && unqualified.records == Some(1),
        "under `none` every store is an insertion: {unqualified:?}"
    );
    assert!(
        first_insertion.countable,
        "and so is a first-insertion count"
    );
}

/// **The weakest declaration in a window wins.** A fleet mixing a `none` producer with an
/// `unlabelled` one must refuse: applying one producer's `none` to another's records is exactly
/// the mix section 5.9's ordering exists to prevent.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_mixed_fleet_takes_the_weakest_declaration_it_contains() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 20_000_000.0;

    let source = harness::seed_source(
        &engine,
        "fx",
        &[
            serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"r1",
                               "at_ms":base,"heartbeat_secs":60,"max_segment_secs":300,
                               "reuse_reporting":"none"}),
            serde_json::json!({"kind":"agent_start","instance_id":"i2","incarnation":"r2",
                               "at_ms":base,"heartbeat_secs":60,"max_segment_secs":300,
                               "reuse_reporting":"unlabelled"}),
            serde_json::json!({"kind":"store","instance_id":"i1","incarnation":"r1",
                               "block_id":"b","n_tokens":100,"at_ms":base+100.0}),
        ],
        offset,
        "reuse-mixed",
    );
    let w = harness::cache_window(&engine, "fx");
    let lifecycle = source
        .lifecycle_events(
            "fx",
            w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
            w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
        )
        .expect("lifecycle IR");
    assert_eq!(
        declaration_from_events(&lifecycle),
        analyzer_analysis::reuse::Declaration::Unlabelled,
        "the weakest declaration governs the window"
    );
}

/// Silence is not `none`, and an unrecognized value is not `none` either.
///
/// A producer that does not declare has not told the reader its stores are insertions. Reading
/// absence as the safe-sounding case is, per section 2.3, "the one reading that turns an unknown into a
/// measurement" — so both must fall to `unlabelled`.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn silence_and_an_unrecognized_value_both_read_as_unlabelled() {
    let offset = harness::rebase_offset();
    let base = 18_000_000.0;

    for (label, declared) in [
        ("no declaration at all", None),
        (
            "a value this build does not define",
            Some("some-future-mode"),
        ),
    ] {
        let (engine, _db) = harness::fresh_engine();
        let mut start = serde_json::json!({"kind":"agent_start","instance_id":"i1",
                                           "incarnation":"r1","at_ms":base,
                                           "heartbeat_secs":60,"max_segment_secs":300});
        if let Some(d) = declared {
            start
                .as_object_mut()
                .expect("obj")
                .insert("reuse_reporting".to_string(), serde_json::json!(d));
        }
        let source = harness::seed_source(
            &engine,
            "fx",
            &[
                start,
                serde_json::json!({"kind":"store","instance_id":"i1","incarnation":"r1",
                                   "block_id":"b","n_tokens":100,"at_ms":base+100.0}),
            ],
            offset,
            "reuse-silence",
        );
        let w = harness::cache_window(&engine, "fx");
        let lifecycle = source
            .lifecycle_events(
                "fx",
                w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
                w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
            )
            .expect("lifecycle IR");

        let cache = source.cache_events("fx", &w).expect("cache IR");
        let (unqualified, _, _) = figures_from_events(&cache, &lifecycle);
        assert!(
            !unqualified.countable,
            "{label}: must not permit an unqualified count — silence has not told the reader \
             its stores are insertions"
        );
    }
}
