// SPDX-License-Identifier: AGPL-3.0-only

//! Two analyzers over one collection converge whatever order the segments reached them in: the
//! failover drill's segments derived whole in one tick, and in two waves with a tick between,
//! publish the same tables.

use analyzer_analysis::Params;
use std::fmt::Write as _;

/// The failover drill's segment `seq`: one run, one instance (`i{seq % 3}`), two heartbeats
/// bracketing `records` stores a half-second apart, and a drop on every third segment, counted
/// cumulatively over the run.
fn drill_segment(seq: u64, base_ms: f64, records: usize) -> String {
    let incarnation = "1785500000000-42";
    let mut body = format!(
        "{{\"kind\":\"segment_open\",\"at_ms\":{base_ms},\"incarnation\":\"{incarnation}\",\
         \"segment_seq\":{seq},\"contract_version\":\"1.0\"}}\n"
    );
    // Cumulative over the run, as a tap states them: source i{seq % 3}'s `seq / 3`-th segment
    // carries ten messages and, on i0, drops one. i0 also drops one in the quiet before segment
    // 12, so the late wave opens a drop window over stores the first wave classified.
    let (own, drops) = (seq / 3, u64::from(seq.is_multiple_of(3)));
    let gap = u64::from(seq >= 12);
    let (msgs, dropped) = (
        (own * 10, own * 10 + 10),
        (own * drops + gap * drops, own * drops + gap * drops + drops),
    );
    let (run_msgs, run_dropped) = (
        (seq * 10, seq * 10 + 10),
        (seq.div_ceil(3) + gap, seq.div_ceil(3) + gap + drops),
    );
    let _ = writeln!(
        body,
        "{{\"kind\":\"heartbeat\",\"at_ms\":{base_ms},\"msgs_seen\":{},\"dropped\":{},\
         \"endpoints\":[{{\"source\":\"i{}\",\"endpoint\":\"tcp://e:1\",\"msgs_seen\":{},\
         \"dropped\":{}}}]}}",
        run_msgs.0,
        run_dropped.0,
        seq % 3,
        msgs.0,
        dropped.0,
    );
    #[allow(clippy::cast_precision_loss)] // a few hundred records
    let last = base_ms + (records as f64) * 500.0;
    let _ = writeln!(
        body,
        "{{\"kind\":\"heartbeat\",\"at_ms\":{last},\"msgs_seen\":{},\"dropped\":{},\
         \"endpoints\":[{{\"source\":\"i{}\",\"endpoint\":\"tcp://e:1\",\"msgs_seen\":{},\
         \"dropped\":{}}}]}}",
        run_msgs.1,
        run_dropped.1,
        seq % 3,
        msgs.1,
        dropped.1,
    );
    for i in 0..records {
        #[allow(clippy::cast_precision_loss)]
        let at = base_ms + (i as f64) * 500.0;
        let _ = writeln!(
            body,
            "{{\"kind\":\"store\",\"instance_id\":\"i{}\",\"dp_rank\":0,\"group_idx\":0,\
             \"block_id\":\"{}\",\"n_tokens\":16,\"tier\":\"GPU\",\"at_ms\":{at}}}",
            seq % 3,
            seq * 1_000_000 + i as u64,
        );
    }
    body
}

/// The drill's timeline: twelve segments 25 minutes apart, then six more after a gap.
fn drill_base(seq: u64) -> f64 {
    let base = analyzer_serving_clickhouse::engine::testing::recent_day_base_ms();
    #[allow(clippy::cast_precision_loss)]
    let seq_f = seq as f64;
    if seq < 12 {
        base + seq_f * 1.5e6
    } else {
        base + 18_000_000.0 + seq_f * 1.5e6
    }
}

/// Plant and ingest `seqs` under `root` for `engine`'s fleet `f`.
fn ship(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    root: &std::path::Path,
    seqs: std::ops::Range<u64>,
) {
    for seq in seqs {
        let base = drill_base(seq);
        let body = drill_segment(seq, base, 300);
        let name = format!("seg-1785500000000-42-{seq}.jsonl");
        harness::plant_dated(root, &harness::dated_hour_of(base), &name, body.as_bytes());
        harness::ingest_bytes(engine, "f", &name, body.as_bytes()).expect("ingest");
    }
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_drill_segments_converge_whole_or_in_two_waves() {
    let params = Params::default();

    let (whole, _whole_db) = harness::fresh_engine();
    let whole_root = harness::fresh_dir("wave-whole");
    ship(&whole, &whole_root, 0..18);
    let source = harness::source_over_tree("wave-whole", whole_root, drill_base(0));
    harness::advance(&whole, &source, "f", &params).expect("whole tick");

    let (waves, _waves_db) = harness::fresh_engine();
    let waves_root = harness::fresh_dir("wave-split");
    ship(&waves, &waves_root, 0..12);
    let source = harness::source_over_tree("wave-split", waves_root.clone(), drill_base(0));
    harness::advance(&waves, &source, "f", &params).expect("first wave");
    ship(&waves, &waves_root, 12..18);
    harness::advance(&waves, &source, "f", &params).expect("second wave");
    // The second wave re-taints what the first classified: without it this test holds nothing
    // about a late drop window.
    let corrected = waves
        .run("SELECT sum(corrected) FROM restated_hourly FINAL WHERE gone = 0 AND family = 'classes'")
        .expect("restated");
    assert!(
        corrected.trim().parse::<i64>().unwrap_or(0) > 0,
        "the late drop window restated nothing: {corrected:?}"
    );

    harness::settled(&whole);
    harness::settled(&waves);
    assert_eq!(
        harness::product_dump(&waves),
        harness::product_dump(&whole),
        "two waves published other tables than one"
    );
}

/// A working state rolled back past ticks serving already holds converges with a run that
/// never crashed: the replay restates its open days whole, so a key only a lost tick published
/// leaves. The drill's shape: the second-to-last segments' stores publish as cold, the state
/// rolls back to a checkpoint before them, and the replay reads them with the late segments that
/// taint them.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_rollback_past_applied_ticks_retracts_what_only_they_published() {
    let params = Params::default();

    let (whole, _whole_db) = harness::fresh_engine();
    let whole_root = harness::fresh_dir("rollback-whole");
    ship(&whole, &whole_root, 0..18);
    let source = harness::source_over_tree("rollback-whole", whole_root, drill_base(0));
    harness::advance(&whole, &source, "f", &params).expect("whole tick");

    let (crashed, _crashed_db) = harness::fresh_engine();
    let root = harness::fresh_dir("rollback-crashed");
    let source = harness::source_over_tree("rollback-crashed", root.clone(), drill_base(0));
    ship(&crashed, &root, 0..10);
    harness::advance(&crashed, &source, "f", &params).expect("first tick");
    let lake = harness::lake(&crashed, "f");
    let working = lake.working().expect("a working state");
    working.checkpoint(&lake).expect("checkpoint");
    ship(&crashed, &root, 10..12);
    harness::advance(&crashed, &source, "f", &params).expect("the tick the crash loses");
    // The crash: a unit begun and never ended, so the next unit rolls back to the checkpoint.
    working.begin(&lake).expect("a unit the crash leaves open");
    ship(&crashed, &root, 12..18);
    harness::advance(&crashed, &source, "f", &params).expect("the replaying tick");

    assert_eq!(
        harness::product_dump(&crashed),
        harness::product_dump(&whole),
        "a rollback left keys only its lost tick published"
    );
}
