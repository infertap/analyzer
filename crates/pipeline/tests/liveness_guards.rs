// SPDX-License-Identifier: AGPL-3.0-only

//! Liveness and narrowing guards: the four verdicts, and the three narrowing refusals.
//!
//! The corpus is pinned by `tests/derived_snapshot.rs`; this suite holds what a corpus snapshot
//! cannot express: **hand-built cases where one
//! specific wrong reading would be invisible in aggregate.** Each is a flattering-direction guard
//! from `docs/error-direction.md` — a reading that looks reasonable and moves the headline number
//! the way you would like it to move.

use analyzer_analysis::completeness::LIFECYCLE_CLIP_MARGIN_MS;
use analyzer_analysis::events::{EventSource, LifecycleEvent};
use analyzer_analysis::narrowing::gap_span_from_events;
use analyzer_analysis::staleness::{run_verdicts_from_events, RunStaleness};
use analyzer_analysis::Window;
use serde_json::Value;

fn lifecycle(source: &dyn EventSource, fleet: &str, w: &Window) -> Vec<LifecycleEvent> {
    source
        .lifecycle_events(
            fleet,
            w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
            w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
        )
        .expect("lifecycle IR")
}

/// All four staleness verdicts in one fleet, so the fold is exercised across the closed set.
///
/// `departed` — a stop as the run's last record. `unexplained` — silence past the declared bound
/// with no stop. `live` — silence inside it. `no basis` — heartbeats but no header declaration, so
/// the bounds were never observed and no verdict is reachable however long the silence.
///
/// That last one is the case worth having: inventing a threshold from observed cadence there would
/// grade a coverage gap as an anomaly, which is a wrong alarm rather than a missed one.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn every_staleness_verdict_agrees() {
    let (engine, _db) = harness::fresh_engine();
    let offset = harness::rebase_offset();
    let base = 13_000_000.0;
    let bounds = serde_json::json!({"heartbeat_secs":60,"max_segment_secs":300});

    let start = |run: &str, at: f64| {
        let mut v = serde_json::json!({"kind":"segment_open","instance_id":"i1",
                                       "incarnation":run,"segment_seq":0,"at_ms":at});
        v.as_object_mut()
            .expect("obj")
            .extend(bounds.as_object().expect("bounds").clone());
        v
    };
    let hb = |run: &str, src: &str, at: f64| {
        serde_json::json!({"kind":"heartbeat","instance_id":"i1","incarnation":run,"at_ms":at,
                           "dropped":0,"oversized":0,
                           "endpoints":[{"source":src,"endpoint":"e","msgs_seen":1,"dropped":0,
                                         "last_msg_at_ms":at}]})
    };

    let hour = 3_600_000.0;
    let records: Vec<Value> = vec![
        // departed: stop is its last record
        start("departed", base),
        hb("departed", "s-departed", base + 1000.0),
        serde_json::json!({"kind":"agent_stop","instance_id":"i1","incarnation":"departed",
                           "at_ms":base+2000.0,"dropped":0,"oversized":0,
                           "endpoints":[{"source":"s-departed","endpoint":"e","msgs_seen":1,
                                         "dropped":0,"last_msg_at_ms":base+2000.0}]}),
        // unexplained: declared a 300s bound, then silent for an hour, no stop
        start("unexplained", base),
        hb("unexplained", "s-unexplained", base + 1000.0),
        // live: declared, and still speaking at the horizon
        start("live", base),
        hb("live", "s-live", base + hour),
        hb("live", "s-live", base + hour + 60_000.0),
        // no basis: heartbeats, never a header declaration, so nothing declared a threshold
        hb("nobasis", "s-nobasis", base + 1000.0),
    ];
    let source = harness::seed_source(&engine, "fx", &records, offset, "liveness-verdicts");
    let w = Window {
        start_ms: base + offset,
        end_ms: base + offset + 2.0 * hour,
    };

    let verdicts = run_verdicts_from_events(&lifecycle(&source, "fx", &w));
    assert_eq!(verdicts.get("departed"), Some(&RunStaleness::Departed));
    assert_eq!(
        verdicts.get("unexplained"),
        Some(&RunStaleness::Unexplained)
    );
    assert_eq!(verdicts.get("live"), Some(&RunStaleness::Live));
    assert_eq!(
        verdicts.get("nobasis"),
        Some(&RunStaleness::NoBasis),
        "an unobserved declaration is outside the stream's knowledge — no verdict, however long \
         the silence, or a coverage gap gets graded as an anomaly"
    );
}

/// The three narrowing refusals beside the one acceptance.
///
/// Narrowing is the only weakening section 5.4 permits, so an unsound bracket exonerates a span nothing
/// observed. Each refusal closes a different way of being wrong, and all three must refuse through
/// all three — a fold that narrowed on any of them would be silently generous, which is the one
/// direction section 5.4 says a reader must never fail in.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn narrowing_accepts_a_sound_bracket_and_refuses_the_three_unsound_ones() {
    let offset = harness::rebase_offset();
    let base = 14_000_000.0;
    let store = |run: &str, seq: Option<u64>, at: f64, block: &str| {
        let mut v = serde_json::json!({"kind":"store","instance_id":"i1","incarnation":run,
                                       "block_id":block,"n_tokens":8,"at_ms":at});
        if let Some(s) = seq {
            v.as_object_mut()
                .expect("obj")
                .insert("seq".to_string(), serde_json::json!(s));
        }
        v
    };

    // 1. SOUND: same run, both numbered, adjacent, strict forward jump.
    let (engine, _db) = harness::fresh_engine();
    let source = harness::seed_source(
        &engine,
        "fx",
        &[
            store("run-1", Some(1), base, "a"),
            store("run-1", Some(5), base + 1000.0, "b"),
        ],
        offset,
        "narrow-sound",
    );
    let w = harness::cache_window(&engine, "fx");
    let span = gap_span_from_events(&source.cache_events("fx", &w).expect("ir"));
    assert!(span.narrowed, "a sound bracket must narrow: {span:?}");

    // 2. REFUSE: a restart between them — sequence is monotonic only within an incarnation, so
    //    the bracket would compare numbers drawn from two different sequences.
    let (engine, _db) = harness::fresh_engine();
    let source = harness::seed_source(
        &engine,
        "fx",
        &[
            store("run-1", Some(1), base, "a"),
            store("run-2", Some(5), base + 1000.0, "b"),
        ],
        offset,
        "narrow-restart",
    );
    let w = harness::cache_window(&engine, "fx");
    assert!(
        !gap_span_from_events(&source.cache_events("fx", &w).expect("ir")).narrowed,
        "a bracket spanning an incarnation boundary must refuse"
    );

    // 3. REFUSE: an unnumbered record in between — it cannot be placed on either side of the
    //    discontinuity, so the span it occupies cannot be excluded from the loss.
    let (engine, _db) = harness::fresh_engine();
    let source = harness::seed_source(
        &engine,
        "fx",
        &[
            store("run-1", Some(1), base, "a"),
            store("run-1", None, base + 500.0, "b"),
            store("run-1", Some(5), base + 1000.0, "c"),
        ],
        offset,
        "narrow-unnumbered",
    );
    let w = harness::cache_window(&engine, "fx");
    assert!(
        !gap_span_from_events(&source.cache_events("fx", &w).expect("ir")).narrowed,
        "an unnumbered record between two numbered ones must refuse the pair"
    );

    // 4. REFUSE: consecutive numbers — no discontinuity at all, so nothing to narrow.
    let (engine, _db) = harness::fresh_engine();
    let source = harness::seed_source(
        &engine,
        "fx",
        &[
            store("run-1", Some(1), base, "a"),
            store("run-1", Some(2), base + 1000.0, "b"),
        ],
        offset,
        "narrow-intact",
    );
    let w = harness::cache_window(&engine, "fx");
    assert!(
        !gap_span_from_events(&source.cache_events("fx", &w).expect("ir")).narrowed,
        "an intact sequence is not a gap (section 5.2)"
    );
}
