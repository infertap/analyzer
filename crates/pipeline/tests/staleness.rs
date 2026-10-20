// SPDX-License-Identifier: AGPL-3.0-only

//! Staleness fixtures: the three run states plus the horizon-rule pin. The corpus covers
//! none of section 7, so these are the authored cases. Wall-domain only; the
//! horizon rule's crash case is asserted together with its coverage-side accounting
//! (invisible to batch health, visible as tail-uncertainty — never silently fine).

use analyzer_analysis::{Params, Window};
use analyzer_pipeline::read::fleet_health;
use analyzer_pipeline::read::uncertainty_model;
use serde_json::{json, Value};

/// A heartbeat for one source, optionally carrying its last-message stamp.
fn hb(source: &str, at: f64, last_msg: Option<f64>) -> Value {
    let mut endpoint = json!({"source": source, "endpoint": format!("tcp://{source}:1"),
                              "msgs_seen": 10, "dropped": 0});
    if let Some(m) = last_msg {
        endpoint["last_msg_at_ms"] = m.into();
    }
    json!({"kind": "heartbeat", "at_ms": at, "msgs_seen": 10, "dropped": 0,
           "oversized": 0, "endpoints": [endpoint]})
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn stop_then_silence_is_an_expected_departure() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            hb("i1", 60_000.0, Some(55_000.0)),
            hb("i1", 120_000.0, Some(115_000.0)),
            hb("i1", 180_000.0, Some(175_000.0)),
            json!({"kind": "agent_stop", "at_ms": 200_000.0, "reason": "signal",
                   "msgs_seen": 10, "dropped": 0, "endpoints": []}),
        ],
        off,
        "stale-stop",
    );
    let w = Window {
        start_ms: off,
        end_ms: off + 300_000.0,
    };
    let health = fleet_health(&source, "f", &w).expect("health");
    assert_eq!(health.expected_departures, 1);
    assert_eq!(health.unexplained_departures, 0);
    assert!(health.observed_idle_or_stalled.is_empty());
}

/// A `segment_open` declaring the spec 5.8 bounds for one run.
///
/// The bounds ride the header rather than the start record, so a run is judgeable from any
/// segment of it rather than only from the one carrying its `agent_start`.
fn start(run: &str, at: f64) -> Value {
    json!({"kind": "segment_open", "at_ms": at, "incarnation": run, "segment_seq": 0,
           "heartbeat_secs": 60, "max_segment_secs": 120, "endpoints": []})
}

/// One tap run per instance, each declaring its bounds: run-b's heartbeats stop at
/// 120k while run-a's continue to 600k. The advancing horizon exposes run-b's silence
/// (gap 480k against a declared larger bound of 120k and a cadence allowance of 180k),
/// and no stop explains it. The threshold rests on the DECLARED bounds (spec 5.8) —
/// the observed cadence only ever widens it.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_gap_beyond_the_declared_bound_with_an_advancing_horizon_is_unexplained() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let mut records = vec![start("run-a", 1_000.0), start("run-b", 1_000.0)];
    for (at, msg) in [(60_000.0, 55_000.0), (120_000.0, 118_000.0)] {
        let mut r = hb("i2", at, Some(msg));
        r["incarnation"] = "run-b".into();
        records.push(r);
    }
    // 40 beats: the horizon lands 38 minutes past run-b's last record, decisively beyond
    // the declared bound (120 s) WIDENED BY the platform grace (30 min) — the fixture
    // stretched when the grace replaced the adaptive allowance, which had
    // let an 8-minute silence decide the same verdict.
    for k in 1..=40 {
        let at = 60_000.0 * f64::from(k);
        let mut r = hb("i1", at, Some(at - 5_000.0));
        r["incarnation"] = "run-a".into();
        records.push(r);
    }
    let source = harness::seed_source(&engine, "f", &records, off, "stale-gap");
    let w = Window {
        start_ms: off,
        end_ms: off + 2_500_000.0,
    };
    let health = fleet_health(&source, "f", &w).expect("health");
    assert_eq!(
        health.unexplained_departures, 1,
        "run-b's silence is the anomaly"
    );
    assert_eq!(health.expected_departures, 0);
    assert!(
        health.observed_idle_or_stalled.is_empty(),
        "i1 is current with recent messages"
    );
    assert_eq!(health.instances, 2);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn current_heartbeats_with_silent_messages_is_idle_or_stalled() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    // i1: heartbeats current through 600k, last message stuck at 100k (silence 500k >
    // threshold 180k); i3: never saw a message at all — both flagged, neither called
    // "healthy" and neither called dead
    let mut records = Vec::new();
    for k in 1..=10 {
        let at = 60_000.0 * f64::from(k);
        records.push(hb("i1", at, Some(100_000.0_f64.min(at - 5_000.0))));
        records.push(hb("i3", at, None));
    }
    let source = harness::seed_source(&engine, "f", &records, off, "stale-idle");
    let w = Window {
        start_ms: off,
        end_ms: off + 700_000.0,
    };
    let health = fleet_health(&source, "f", &w).expect("health");
    assert_eq!(health.unexplained_departures, 0);
    assert_eq!(
        health.observed_idle_or_stalled,
        vec!["i1".to_string(), "i3".to_string()],
        "stuck messages and never-a-message both read idle-or-stalled"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_horizon_rule_an_archive_ending_without_a_stop_shows_no_anomaly_but_costs_coverage() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();
    let source = harness::seed_source(
        &engine,
        "f",
        &[
            hb("i1", 60_000.0, Some(55_000.0)),
            hb("i1", 120_000.0, Some(115_000.0)),
            // a single-heartbeat source rides along: cadence unobservable — counted,
            // never flagged (the N=1 rule)
            hb("i9", 60_000.0, Some(55_000.0)),
            json!({"kind": "store", "instance_id": "i1", "block_id": "p", "n_tokens": 4,
                   "at_ms": 100_000.0}),
        ],
        off,
        "stale-horizon",
    );
    let w = Window {
        start_ms: off,
        end_ms: off + 300_000.0,
    };
    // batch health: horizon equals the last heartbeat — end-of-file is
    // indistinguishable from end-of-capture, so no anomaly is claimed
    let health = fleet_health(&source, "f", &w).expect("health");
    assert_eq!(health.unexplained_departures, 0);
    assert_eq!(health.expected_departures, 0);
    assert_eq!(health.instances, 2);
    // ...but the crash window is never silently fine: absent a stop, the tail is
    // uncertain and costs coverage on every number (the three-layered accounting)
    let coverage = uncertainty_model(&source, "f", &w, &Params::default())
        .expect("model")
        .coverage(None);
    assert!(
        coverage.fraction < 1.0,
        "tail-uncertainty must surface in coverage ({})",
        coverage.fraction
    );
}

/// A run is judgeable from any segment of it, not only the one carrying its start record.
///
/// This is the property the bounds moved to the header for. `agent_start` is written once per
/// run, into whichever segment was open at process start; the records it would have qualified
/// outlive that segment, and whether it survives is a property of a retention policy. With the
/// bounds on `agent_start` only, a producer running longer than a reader keeps records would be
/// permanently unjudgeable — which is exactly the producer worth judging.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_run_whose_start_record_is_gone_is_still_judgeable() {
    let (engine, _db) = harness::fresh_engine();
    let off = harness::rebase_offset();

    // No `agent_start` anywhere: this is the state of a long-lived run whose first segment has
    // aged out of raw retention while later segments are still arriving.
    let mut records = vec![start("run-a", 1_000.0)];
    for (at, msg) in [(60_000.0, 55_000.0), (120_000.0, 118_000.0)] {
        let mut r = hb("i1", at, Some(msg));
        r["incarnation"] = "run-a".into();
        records.push(r);
    }
    // Silence past the declared larger bound (120 s) AND the 30-minute platform grace,
    // with the horizon advanced well beyond both: run-a last spoke at 120 s, the far
    // record stands at 40 min — 38 minutes of silence, decisive under the grace.
    let mut far = hb("i1", 2_400_000.0, Some(2_398_000.0));
    far["incarnation"] = "run-b".into();
    records.push(start("run-b", 2_390_000.0));
    records.push(far);

    let source = harness::seed_source(&engine, "fx", &records, off, "stale-headless");
    let w = Window {
        start_ms: off,
        end_ms: off + 2_500_000.0,
    };
    let verdicts = analyzer_pipeline::read::run_verdicts(&source, "fx", &w).expect("verdicts");

    let run_a = verdicts
        .iter()
        .find(|(r, _)| r.as_str() == "run-a")
        .map(|(_, v)| format!("{v:?}"))
        .expect("run-a judged");
    assert!(
        !run_a.contains("NoBasis"),
        "the header declared the bounds, so the run is judgeable with no start record: {run_a}"
    );
    assert!(
        run_a.contains("Unexplained"),
        "silence past the declared bound with an advancing horizon: {run_a}"
    );
}
