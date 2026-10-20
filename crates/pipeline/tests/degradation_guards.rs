// SPDX-License-Identifier: AGPL-3.0-only

//! Degradation-ledger guards: a hole, a refusal window, a boundary, and a retention-scoped bracket.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite holds what a corpus snapshot
//! cannot express: **hand-built cases where one
//! specific wrong reading would be invisible in aggregate.** Each is a flattering-direction guard
//! from `docs/error-direction.md` — a reading that looks reasonable and moves the headline number
//! the way you would like it to move.

use analyzer_analysis::completeness::LIFECYCLE_CLIP_MARGIN_MS;
use analyzer_analysis::degradation::{
    declared_drop_incidents_from_events, incarnation_boundaries_from_events,
    refusal_entries_from_events, sequence_holes_from_events, Cause,
};
use analyzer_analysis::events::EventSource;
use analyzer_analysis::Window;
use serde_json::Value;

fn lifecycle(
    source: &dyn EventSource,
    fleet: &str,
    w: &Window,
) -> Vec<analyzer_analysis::events::LifecycleEvent> {
    source
        .lifecycle_events(
            fleet,
            w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
            w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
        )
        .expect("lifecycle IR")
}

/// A real sequence hole, built explicitly.
///
/// Segments 1, 2, then 5 — so 3..4 are missing and the bracket runs from segment 2's open to
/// segment 5's. The first observed sequence reports nothing: missing BELOW observation is the
/// retention edge, not detected loss, and claiming it would manufacture an incident out of the
/// clip.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_missing_segment_range_is_a_hole_and_the_first_sequence_is_not() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 9_000_000.0;

    let open = |seq: u64, at: f64| {
        serde_json::json!({"kind":"segment_open","instance_id":"i1","incarnation":"run-1",
                           "segment_seq":seq,"at_ms":at})
    };
    // Deliberately starting at 1, not 0: the run's own first observed segment must not read as a
    // hole below itself.
    let records: Vec<Value> = vec![
        open(1, base),
        open(2, base + 1000.0),
        open(5, base + 4000.0),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "degr-hole");
    let w = Window {
        start_ms: base + offset,
        end_ms: base + offset + 5000.0,
    };

    let holes = sequence_holes_from_events(&lifecycle(&source, "fx", &w));
    assert_eq!(holes.len(), 1, "exactly one hole: {holes:?}");
    assert_eq!(holes[0].missing_from, 3);
    assert_eq!(holes[0].missing_to, 4);
    assert!(
        (holes[0].bracket_start_ms - (base + offset + 1000.0)).abs() < f64::EPSILON,
        "the bracket opens at the last observed segment before the hole"
    );
}

/// A refusal whose declared window sits outside the analysis window must not enter the ledger,
/// and one that touches it must. The bound is engine-domain on both sides — the declaration's
/// own window against the analysis window — and the record's producer-clock `at_ms` never enters.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_refusal_enters_only_where_its_declared_engine_window_touches_the_analysis_window() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 10_000_000.0;

    let refusal = |ws: f64, we: f64, refused: u64| {
        serde_json::json!({"kind":"identity_refused","instance_id":"i1","incarnation":"run-1",
                           "dp_rank":0,"group_idx":0,"at_ms":base,
                           "refused":refused,"window_start_ms":ws,"window_end_ms":we})
    };
    let records: Vec<Value> = vec![
        refusal(base, base + 1000.0, 7),               // inside
        refusal(base + 50_000.0, base + 60_000.0, 99), // wholly outside
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "degr-refusal");

    let w = Window {
        start_ms: base + offset,
        end_ms: base + offset + 2000.0,
    };
    let entries = refusal_entries_from_events(&lifecycle(&source, "fx", &w), &w);
    assert_eq!(
        entries.len(),
        1,
        "only the touching declaration: {entries:?}"
    );
    assert!(
        matches!(entries[0].cause, Cause::IdentityRefusal { refused: 7 }),
        "the wrong refusal survived: {:?}",
        entries[0].cause
    );
}

/// A real section 5.3 boundary, and the overlap that must NOT produce one.
///
/// Run 1 covers `i1` and stops; run 2 starts later covering the same instance — the gap between
/// them is an uncovered window, the ledger's strongest register. Then a third run that starts
/// BEFORE run 2 left: overlapping runs mean there was no moment nobody watched, so no boundary is
/// emitted. Without the second half, a fold that ignored the `from_ms < to_ms` filter would pass.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_gap_between_producer_runs_is_uncovered_and_an_overlap_is_not() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 11_000_000.0;

    let ep = |at: f64| {
        serde_json::json!([{"source":"i1","endpoint":"e","msgs_seen":1,"dropped":0,
                            "last_msg_at_ms":at}])
    };
    let records: Vec<Value> = vec![
        serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"run-1",
                           "at_ms":base,"heartbeat_secs":60,"max_segment_secs":300}),
        serde_json::json!({"kind":"heartbeat","instance_id":"i1","incarnation":"run-1",
                           "at_ms":base+1000.0,"dropped":0,"oversized":0,"endpoints":ep(base+1000.0)}),
        serde_json::json!({"kind":"agent_stop","instance_id":"i1","incarnation":"run-1",
                           "at_ms":base+2000.0,"dropped":0,"oversized":0,"endpoints":ep(base+2000.0)}),
        // run-2 starts a full second after run-1 left: nobody was watching in between
        serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"run-2",
                           "at_ms":base+3000.0,"heartbeat_secs":60,"max_segment_secs":300}),
        serde_json::json!({"kind":"heartbeat","instance_id":"i1","incarnation":"run-2",
                           "at_ms":base+4000.0,"dropped":0,"oversized":0,"endpoints":ep(base+4000.0)}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "degr-boundary");
    let w = Window {
        start_ms: base + offset,
        end_ms: base + offset + 5000.0,
    };

    let entries = incarnation_boundaries_from_events(&lifecycle(&source, "fx", &w));
    assert_eq!(entries.len(), 1, "one uncovered window: {entries:?}");
    let Cause::IncarnationBoundary { from, to } = &entries[0].cause else {
        panic!("wrong cause: {:?}", entries[0].cause);
    };
    assert_eq!((from.as_str(), to.as_str()), ("run-1", "run-2"));
    assert!(
        (entries[0].producer_bracket.start_ms.expect("start") - (base + offset + 2000.0)).abs()
            < f64::EPSILON,
        "the bracket opens at the departing run's stop"
    );

    // Now an OVERLAPPING run: run-3 enters before run-2 is last seen, so no blind spot exists.
    let overlap: Vec<Value> = vec![
        serde_json::json!({"kind":"agent_start","instance_id":"i1","incarnation":"run-3",
                           "at_ms":base+3500.0,"heartbeat_secs":60,"max_segment_secs":300}),
        serde_json::json!({"kind":"heartbeat","instance_id":"i1","incarnation":"run-3",
                           "at_ms":base+4500.0,"dropped":0,"oversized":0,"endpoints":ep(base+4500.0)}),
    ];
    let combined: Vec<Value> = records.iter().chain(overlap.iter()).cloned().collect();
    let source = harness::source_for_records(&combined, offset, "degr-overlap");
    let w2 = Window {
        start_ms: base + offset,
        end_ms: base + offset + 6000.0,
    };
    let entries = incarnation_boundaries_from_events(&lifecycle(&source, "fx", &w2));
    assert!(
        !entries.iter().any(|d| matches!(&d.cause,
            Cause::IncarnationBoundary { from, to } if from == "run-2" && to == "run-3")),
        "overlapping runs leave no blind spot, so no boundary may be emitted: {entries:?}"
    );
}

/// **The case retention scoping exists for.**
///
/// A `segments_dropped` declaration inside the analysis window, whose bracketing `segment_open`
/// records sit far outside it — a producer declaring a loss long after it happened. The bracket
/// must still come from those distant opens.
///
/// A fold that took its opens from the window-clipped lifecycle stream would find no neighbours,
/// report an open-ended bracket, and produce a NARROWER degraded span than reality. That is the
/// flattering direction, and it is invisible to every fixture that does not straddle the clip —
/// which is all of them.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_declared_drop_is_bracketed_by_opens_far_outside_the_analysis_window() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 12_000_000.0;
    // **Nine days: outside the 7-day lifecycle clip, inside the 14-day raw retention floor.**
    // That band is exactly where retention scoping earns its keep, and it is narrow by
    // construction — the true horizon is only about twice the clip, so "widen to retention" is a
    // modest, bounded widening rather than an open-ended one. The harness rebases fixtures to
    // three days ago, so this lands ~12 days back with two days of TTL margin.
    let far = 9.0 * 86_400_000.0;

    let open = |seq: u64, at: f64| {
        serde_json::json!({"kind":"segment_open","instance_id":"i1","incarnation":"run-1",
                           "segment_seq":seq,"at_ms":at})
    };
    let records: Vec<Value> = vec![
        // The neighbours, deliberately ancient relative to the window below.
        open(10, base - far),
        open(20, base - far + 1000.0),
        // The declaration, inside the window: segments 11..19 were reclaimed.
        serde_json::json!({"kind":"segments_dropped","instance_id":"i1","incarnation":"run-1",
                           "first_seq":11,"last_seq":19,"at_ms":base}),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "degr-drop");
    let w = Window {
        start_ms: base + offset - 1000.0,
        end_ms: base + offset + 1000.0,
    };

    let opens = source
        .segment_opens_at_retention_horizon("fx")
        .expect("opens");
    // Self-verifying: if raw retention ever moves under this fixture, fail with the reason rather
    // than with a confusing absent bracket.
    assert_eq!(
        opens.len(),
        2,
        "the bracketing opens must still be retained — if this fails, the fixture has drifted \
         outside the raw TTL rather than the fold being wrong"
    );
    let incidents = declared_drop_incidents_from_events(&lifecycle(&source, "fx", &w), &opens);
    assert_eq!(incidents.len(), 1, "one declaration: {incidents:?}");
    assert_eq!((incidents[0].first_seq, incidents[0].last_seq), (11, 19));
    assert!(
        incidents[0].bracket_start_ms.is_some() && incidents[0].bracket_end_ms.is_some(),
        "both brackets must resolve from the distant opens — a clipped load would leave them \
         absent and understate the degraded span: {incidents:?}"
    );
}
