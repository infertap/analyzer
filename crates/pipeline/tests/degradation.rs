// SPDX-License-Identifier: AGPL-3.0-only

//! The degradation ledger against real ingested rows (`analyzer_analysis::degradation`):
//! detected sequence gaps, declared drops, and section 5.1's scope expansion — a
//! producer-level loss degrades every instance the incarnation covered, resolved from
//! declared coverage and surviving records both.

use analyzer_analysis::degradation::{Cause, Register, Scope, HULL_COMPACTION_BOUND};
use analyzer_analysis::events::EventSource as _;
use analyzer_analysis::intervals::Holder;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::degradation_io::ledger;
use serde_json::json;

fn open(run: &str, seq: u64, at: f64) -> serde_json::Value {
    json!({"kind": "segment_open", "incarnation": run, "segment_seq": seq, "at_ms": at})
}

fn hb(run: &str, source: &str, at: f64) -> serde_json::Value {
    json!({"kind": "heartbeat", "at_ms": at, "incarnation": run,
           "endpoints": [{"source": source, "endpoint": "e", "msgs_seen": 1, "dropped": 0}]})
}

fn start_at(run: &str, at: f64) -> serde_json::Value {
    json!({"kind": "agent_start", "at_ms": at, "incarnation": run, "endpoints": []})
}

fn stop_at(run: &str, source: &str, at: f64) -> serde_json::Value {
    json!({"kind": "agent_stop", "at_ms": at, "incarnation": run, "reason": "signal",
           "endpoints": [{"source": source, "endpoint": "e", "msgs_seen": 1, "dropped": 0}]})
}

fn holder(instance: &str) -> Holder {
    Holder {
        instance: instance.to_string(),
        dp_rank: 0,
        group_idx: 0,
    }
}

fn window(off: f64) -> Window {
    Window {
        start_ms: off,
        end_ms: off + 1_000_000.0,
    }
}

/// The section 5.1 trap, exactly: the incarnation declared covering i0 AND i1, the gap's
/// surviving records mention only i0 — and BOTH instances degrade, because segments
/// belong to the run, so the loss belongs to everything the run observed.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_sequence_hole_degrades_every_instance_the_run_covered() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            open("run-1", 0, 1_000.0),
            open("run-1", 1, 2_000.0),
            open("run-1", 3, 4_000.0), // seq 2 never arrived
            json!({"kind": "heartbeat", "at_ms": 1_500.0, "incarnation": "run-1",
                   "endpoints": [
                       {"source": "i0", "endpoint": "e0", "msgs_seen": 1, "dropped": 0},
                       {"source": "i1", "endpoint": "e1", "msgs_seen": 1, "dropped": 0}]}),
            json!({"kind": "store", "instance_id": "i0", "block_id": "b", "n_tokens": 4,
                   "at_ms": 2_500.0, "incarnation": "run-1"}),
        ],
        off,
        "degr-1",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1);
    let e = &l.entries[0];
    assert_eq!(
        e.cause,
        Cause::SequenceGap {
            first_missing: 2,
            last_missing: 2
        }
    );
    assert_eq!(e.register(), Register::Indeterminate);
    let Scope::Run {
        incarnation,
        instances,
    } = &e.scope
    else {
        panic!("a sequence hole is a run-scoped loss");
    };
    assert_eq!(incarnation, "run-1");
    assert_eq!(
        instances,
        &vec!["i0".to_string(), "i1".to_string()],
        "the instance whose records survived AND the one only the declaration names"
    );
    // the bracket is the neighboring PRESENT segments' opens, deliberately wide
    assert_eq!(e.producer_bracket.start_ms, Some(off + 2_000.0));
    assert_eq!(e.producer_bracket.end_ms, Some(off + 4_000.0));
    assert_eq!(
        l.register_for(&[holder("i1")], &w),
        Some(Register::Indeterminate),
        "i1 has no surviving records at all, and that is exactly why it degrades"
    );
}

/// A contiguous sequence, a restart at seq zero, and a sequence beginning
/// above zero at the retention edge: none is a gap.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn contiguity_restart_and_retention_edge_yield_an_empty_ledger() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            open("run-1", 0, 1_000.0),
            open("run-1", 1, 2_000.0),
            open("run-2", 0, 5_000.0), // fresh incarnation at zero: a restart
            open("run-3", 7, 6_000.0), // begins above zero: absence outside observation
            open("run-3", 8, 7_000.0),
        ],
        off,
        "degr-2",
    );
    let l = ledger(
        &source,
        "f",
        &window(off),
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &window(off), &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries, vec![], "no hole inside any observed sequence");
}

/// The declared loss finally has a reader: a `segments_dropped` record becomes an
/// indeterminate entry, its bracket honest about the side with no present neighbor.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_declared_drop_is_read_with_an_honest_half_open_bracket() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            // the oldest segments were reclaimed; only 3 and 4 survive
            open("run-1", 3, 4_000.0),
            open("run-1", 4, 5_000.0),
            json!({"kind": "segments_dropped", "at_ms": 6_000.0, "incarnation": "run-1",
                   "first_seq": 0, "last_seq": 2}),
            json!({"kind": "heartbeat", "at_ms": 4_500.0, "incarnation": "run-1",
                   "endpoints": [
                       {"source": "i0", "endpoint": "e0", "msgs_seen": 1, "dropped": 0}]}),
        ],
        off,
        "degr-drop-bracket",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1);
    let e = &l.entries[0];
    assert_eq!(
        e.cause,
        Cause::DeclaredDrop {
            first_seq: 0,
            last_seq: 2
        }
    );
    assert_eq!(e.register(), Register::Indeterminate);
    assert_eq!(
        e.producer_bracket.start_ms, None,
        "no present segment precedes the drop, and the bracket says so rather than \
         inventing a stamp"
    );
    assert_eq!(e.producer_bracket.end_ms, Some(off + 4_000.0));
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::Indeterminate)
    );
}

/// The agent-downtime blind spot: between run-1's stop and run-2's start,
/// nobody watched i0 — an UNCOVERED entry, the ledger's strongest register, scoped to
/// the instance. i9, watched continuously by its own run, is untouched.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_interval_between_runs_is_uncovered_for_the_instances_they_watched() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            hb("run-1", "i0", 1_000.0),
            stop_at("run-1", "i0", 2_000.0),
            start_at("run-2", 60_000.0),
            hb("run-2", "i0", 61_000.0),
            // a bystander instance, covered without interruption by a third run
            hb("run-9", "i9", 1_000.0),
            hb("run-9", "i9", 61_000.0),
        ],
        off,
        "degr-3",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1);
    let e = &l.entries[0];
    assert_eq!(e.scope, Scope::Instance("i0".into()));
    assert_eq!(
        e.cause,
        Cause::IncarnationBoundary {
            from: "run-1".into(),
            to: "run-2".into()
        }
    );
    assert_eq!(e.register(), Register::Uncovered);
    assert_eq!(e.producer_bracket.start_ms, Some(off + 2_000.0));
    assert_eq!(e.producer_bracket.end_ms, Some(off + 60_000.0));
    assert_eq!(e.incidents, 1);
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::Uncovered)
    );
    assert_eq!(l.register_for(&[holder("i9")], &w), None);
}

/// The bracket falls back honestly when its endpoints were not observed: no stop means
/// the departing run's LAST observation opens the blind spot, no start means the
/// arriving run's FIRST observation closes it — and runs that overlap leave no moment
/// unwatched, so they produce no entry at all.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn brackets_fall_back_to_observations_and_overlap_is_no_boundary() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            // i0: run-1 crashes (no stop), run-2's start record was lost (no start)
            hb("run-1", "i0", 1_000.0),
            hb("run-1", "i0", 2_000.0),
            hb("run-2", "i0", 60_000.0),
            // i1: blue-green handover — run-4 starts before run-3 stops
            hb("run-3", "i1", 1_000.0),
            start_at("run-4", 5_000.0),
            hb("run-4", "i1", 6_000.0),
            stop_at("run-3", "i1", 9_000.0),
        ],
        off,
        "degr-4",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1, "only i0's crash gap is a blind spot");
    let e = &l.entries[0];
    assert_eq!(e.scope, Scope::Instance("i0".into()));
    assert_eq!(e.producer_bracket.start_ms, Some(off + 2_000.0));
    assert_eq!(e.producer_bracket.end_ms, Some(off + 60_000.0));
    assert_eq!(l.register_for(&[holder("i1")], &w), None);
}

/// Worst-wins across causes: an instance with both a sequence hole (indeterminate)
/// and an agent blind spot (uncovered) reads uncovered.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn uncovered_outranks_indeterminate_for_the_same_instance() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            open("run-1", 0, 1_000.0),
            open("run-1", 2, 3_000.0), // seq 1 missing
            hb("run-1", "i0", 1_500.0),
            stop_at("run-1", "i0", 4_000.0),
            start_at("run-2", 90_000.0),
            hb("run-2", "i0", 91_000.0),
        ],
        off,
        "degr-5",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 2);
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::Uncovered)
    );
}

/// The crash-loop pathology stays bounded: past the compaction bound, same-class
/// entries on one instance coalesce to their hull — first departure to last arrival —
/// and the entry carries the incident count, so a summarized ledger cannot present as
/// a quiet one.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_crash_loop_compacts_to_one_counted_hull() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let runs = u32::try_from(HULL_COMPACTION_BOUND).unwrap() + 2; // boundaries > bound
    let mut records = Vec::new();
    for k in 0..runs {
        let base = 10_000.0 * f64::from(k);
        records.push(start_at(&format!("run-{k:03}"), base));
        records.push(hb(&format!("run-{k:03}"), "i0", base + 1_000.0));
        records.push(stop_at(&format!("run-{k:03}"), "i0", base + 2_000.0));
    }
    let source = harness::seed_source(&engine, "f", &records, off, "degr-6");
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1, "one hull, not one entry per restart");
    let e = &l.entries[0];
    assert_eq!(e.incidents, u64::from(runs - 1));
    assert_eq!(
        e.cause,
        Cause::IncarnationBoundary {
            from: "run-000".into(),
            to: format!("run-{:03}", runs - 1)
        },
        "the hull runs from the first departure to the last arrival"
    );
    assert_eq!(e.producer_bracket.start_ms, Some(off + 2_000.0));
    assert_eq!(
        e.producer_bracket.end_ms,
        Some(off + 10_000.0 * f64::from(runs - 1))
    );
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::Uncovered)
    );
}

/// A declared capacity loss degrades exactly the named holder over exactly the
/// declared engine window: the sibling worker on the same instance stands, and a
/// window past the declaration's end sees nothing — the first entry family whose
/// extent confinement runs through the real query rather than a constructed entry.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_refusal_is_a_lower_bound_for_one_holder_over_one_window() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            json!({"kind": "identity_refused", "at_ms": 99_000.0, "instance_id": "i0",
                   "dp_rank": 0, "group_idx": 0, "refused": 5,
                   "window_start_ms": 10_000.0, "window_end_ms": 20_000.0,
                   "incarnation": "run-1"}),
            // anchor cache facts so the window helper has a span to sit inside
            json!({"kind": "store", "instance_id": "i0", "block_id": "b", "n_tokens": 4,
                   "at_ms": 15_000.0, "incarnation": "run-1"}),
        ],
        off,
        "degr-7",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1);
    let e = &l.entries[0];
    assert_eq!(e.scope, Scope::Holder(holder("i0")));
    assert_eq!(e.cause, Cause::IdentityRefusal { refused: 5 });
    assert_eq!(e.register(), Register::LowerBound);
    assert_eq!(e.engine_extent, Some((off + 10_000.0, off + 20_000.0)));
    assert_eq!(
        e.producer_bracket.start_ms, None,
        "the declaration's evidence is its engine window; no producer stamp is invented"
    );
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::LowerBound)
    );
    let sibling = analyzer_analysis::intervals::Holder {
        dp_rank: 1,
        ..holder("i0")
    };
    assert_eq!(
        l.register_for(&[sibling], &w),
        None,
        "the sibling worker's figures stand — the signal is scoped so they can"
    );
    let after = Window {
        start_ms: off + 30_000.0,
        end_ms: off + 40_000.0,
    };
    assert_eq!(
        l.register_for(&[holder("i0")], &after),
        None,
        "past the declared window the holder's figures stand again"
    );
}

/// Refusal declarations whose windows lie wholly outside the analysis window are not
/// fetched at all, and a burst of them on one holder hulls to a summed total.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn refusals_are_window_bounded_and_hull_by_summing() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let bound = u64::try_from(HULL_COMPACTION_BOUND).unwrap();
    let mut records = vec![json!({"kind": "identity_refused", "at_ms": 1_000.0,
        "instance_id": "i0", "dp_rank": 0, "group_idx": 0, "refused": 7,
        "window_start_ms": 2_000_000.0, "window_end_ms": 2_100_000.0})];
    for k in 0..bound + 2 {
        #[allow(clippy::cast_precision_loss)]
        let base = 10_000.0 + 1_000.0 * k as f64;
        records.push(json!({"kind": "identity_refused", "at_ms": 99_000.0,
            "instance_id": "i0", "dp_rank": 0, "group_idx": 0, "refused": 2,
            "window_start_ms": base, "window_end_ms": base + 500.0}));
    }
    let source = harness::seed_source(&engine, "f", &records, off, "degr-8");
    let w = window(off); // ends at off + 1_000_000: the 2_000_000 declaration is outside
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(
        l.entries.len(),
        1,
        "the out-of-window declaration is not fetched; the burst hulls to one entry"
    );
    let e = &l.entries[0];
    assert_eq!(e.incidents, bound + 2);
    assert_eq!(
        e.cause,
        Cause::IdentityRefusal {
            refused: 2 * (bound + 2)
        },
        "the hull of declared losses is their total"
    );
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::LowerBound)
    );
}

/// A gap in one run leaves another run's instances untouched: the ledger names the
/// offender and only the offender.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn degradation_is_scoped_to_the_losing_run() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            open("run-1", 0, 1_000.0),
            open("run-1", 2, 3_000.0), // seq 1 missing
            json!({"kind": "heartbeat", "at_ms": 1_500.0, "incarnation": "run-1",
                   "endpoints": [
                       {"source": "i0", "endpoint": "e0", "msgs_seen": 1, "dropped": 0}]}),
            open("run-2", 0, 1_000.0),
            open("run-2", 1, 2_000.0),
            json!({"kind": "heartbeat", "at_ms": 1_500.0, "incarnation": "run-2",
                   "endpoints": [
                       {"source": "i9", "endpoint": "e9", "msgs_seen": 1, "dropped": 0}]}),
        ],
        off,
        "degr-9",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1);
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::Indeterminate)
    );
    assert_eq!(
        l.register_for(&[holder("i9")], &w),
        None,
        "the healthy run's figures stand"
    );
}

/// section 5.4's second detected-gap class, cited from the completeness clock: a positive
/// dropped delta between two heartbeats becomes an indeterminate entry scoped to the
/// span's source, and a zero-delta stream mints nothing.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_dropped_delta_between_heartbeats_is_an_indeterminate_drop_window() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            json!({"kind": "heartbeat", "at_ms": 60_000.0, "incarnation": "run-1",
                   "endpoints": [
                       {"source": "i0", "endpoint": "e0", "msgs_seen": 10, "dropped": 0}]}),
            json!({"kind": "heartbeat", "at_ms": 120_000.0, "incarnation": "run-1",
                   "endpoints": [
                       {"source": "i0", "endpoint": "e0", "msgs_seen": 20, "dropped": 3}]}),
        ],
        off,
        "degr-10",
    );
    let w = window(off);
    let l = ledger(
        &source,
        "f",
        &w,
        &analyzer_pipeline::read::uncertainty_model(&source, "f", &w, &Params::default())
            .expect("model"),
        &source
            .segment_opens_at_retention_horizon("f")
            .expect("opens"),
    )
    .expect("ledger");
    assert_eq!(l.entries.len(), 1);
    let e = &l.entries[0];
    assert_eq!(e.scope, Scope::Instance("i0".into()));
    assert_eq!(
        e.cause,
        Cause::DropWindow {
            dropped: 3,
            oversized: 0
        }
    );
    assert_eq!(e.register(), Register::Indeterminate);
    assert_eq!(e.producer_bracket.start_ms, Some(off + 60_000.0));
    assert_eq!(e.producer_bracket.end_ms, Some(off + 120_000.0));
    assert_eq!(
        l.register_for(&[holder("i0")], &w),
        Some(Register::Indeterminate)
    );
}

/// The section 5.6 absence register, all four shapes: exact within a counted bracket; a
/// lower bound before the producer started, past its last observation with no stop,
/// and across a drop window — the three MUST-NOT cases in the spec's own order.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn absence_is_exact_only_within_a_counted_bracket() {
    use analyzer_analysis::degradation::AbsenceRegister;
    use analyzer_pipeline::degradation_io::absence_register;
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            start_at("run-1", 10_000.0),
            hb("run-1", "i0", 70_000.0),
            hb("run-1", "i0", 130_000.0),
            stop_at("run-1", "i0", 190_000.0),
        ],
        off,
        "degr-11",
    );
    let reg = |a: f64, b: f64| {
        absence_register(&source, "f", (off + a, off + b), &Params::default()).expect("register")
    };
    assert_eq!(
        reg(10_000.0, 190_000.0),
        AbsenceRegister::Exact,
        "start to stop, zero deltas: the counters establish the absence"
    );
    assert_eq!(
        reg(1_000.0, 190_000.0),
        AbsenceRegister::LowerBound,
        "before the producer started, nothing may be stated"
    );
    assert_eq!(
        reg(10_000.0, 400_000.0),
        AbsenceRegister::LowerBound,
        "past the bracket is outside any observed window"
    );
}

/// A nonzero counter delta anywhere the ledger can see poisons exactness: the drop
/// window stands in the ledger, and the counted bracket cannot vouch past it.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_drop_window_makes_absence_a_lower_bound() {
    use analyzer_analysis::degradation::AbsenceRegister;
    use analyzer_pipeline::degradation_io::absence_register;
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let mut lossy = hb("run-1", "i0", 130_000.0);
    lossy["endpoints"][0]["dropped"] = 3.into();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            start_at("run-1", 10_000.0),
            hb("run-1", "i0", 70_000.0),
            lossy,
            stop_at("run-1", "i0", 190_000.0),
        ],
        off,
        "degr-12",
    );
    assert_eq!(
        absence_register(
            &source,
            "f",
            (off + 10_000.0, off + 190_000.0),
            &Params::default()
        )
        .expect("register"),
        AbsenceRegister::LowerBound
    );
}
