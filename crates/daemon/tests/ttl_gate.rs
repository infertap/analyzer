// SPDX-License-Identifier: AGPL-3.0-only

//! Backlog discovery and verified retirement against a live bucket: its own binary, because `loop.rs` poisons
//! `AWS_ENDPOINT` for its dead-bucket premise, env is process-global, and a live-bucket
//! test in that process would inherit the poison however it built its own config.
//!
//! Skips loudly without `INFERTAP_TEST_S3_URL` (+ `_KEY`, `_SECRET`; a local endpoint rides
//! `AWS_ENDPOINT`/`AWS_ALLOW_HTTP`, the objectstore's own env route), like every
//! live-bucket test.

use analyzer_daemon::{Daemon, DaemonConfig, Due};
use analyzer_objectstore::testing::env_store;

mod common;
use common::{horizon_days, Scratch, FLEET};

/// This binary's slice of the port inventory (see `loop.rs`): 28 000–28 198.
fn live_config(scratch: &Scratch, bucket: analyzer_daemon::BucketConfig) -> DaemonConfig {
    let mut cfg = DaemonConfig::new(
        FLEET,
        bucket,
        scratch.path(),
        analyzer_serving_clickhouse::engine::testing::engine_bin(),
    );
    let base = analyzer_serving_clickhouse::engine::testing::port_block(4);
    cfg.http_port = base;
    cfg.tcp_port = base + 1;
    // The suite runs many daemons on this host by design; none may take the host claim.
    cfg.host_claim = false;
    // The backfilled segment lives in the LIVE bucket, so the derivation reads it through
    // the engine-fenced collection transport — the appliance shape, not the rig directory.
    cfg.segment_transport = None;
    // The test plants write-ahead JSONL; the corpus seam admits it (test rigs only).
    cfg.admit_corpus_forms = true;
    // The test declares its horizon rather than inheriting the default: its whole premise
    // is a segment one day past a 14-day horizon.
    cfg.retention.ledger_retention_days = 14;
    cfg
}

/// Discovery widens before ingest so classification can reconstruct a backlog beyond the
/// declared horizon. After classification the target returns; local sources remain until
/// verified export retirement, independently of that target.
#[test]
#[ignore = "live bucket: INFERTAP_TEST_S3_URL/_KEY/_SECRET; skips loudly when absent"]
fn ttl_cannot_outrun_the_tick_the_horizon_widens_before_ingest_and_returns_after() {
    use analyzer_objectstore::ObjectStore as _;
    let Some(live) = env_store() else {
        eprintln!("SKIPPED: no INFERTAP_TEST_S3_URL — the TTL gate ran against nothing");
        return;
    };
    let prefix = format!("ttl-gate-{}/", std::process::id());
    let dir = Scratch::new("analyzer-ttl-gate", "ttl-gate");
    let cfg = live_config(
        &dir,
        analyzer_daemon::BucketConfig {
            url: live.url.clone(),
            credentials: live.credentials.clone(),
            key_prefix: prefix.clone(),
        },
    );
    let mut daemon = Daemon::start(cfg).expect("start");

    // A backfilled segment from 15 days ago — one day PAST the declared 14-day horizon —
    // under its own dated hour prefix, exactly as the shipper writes it.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    let at = now - 15.0 * analyzer_analysis::DAY_MS;
    let day = daemon
        .engine()
        .run("SELECT toString(toDate(now() - 15 * 86400))")
        .expect("date");
    let segment = |seq: u32, at: f64, block: &str| {
        let heartbeat = |hb: f64| {
            format!(
                "{{\"kind\":\"heartbeat\",\"at_ms\":{hb},\"incarnation\":\"a\",\
                 \"msgs_seen\":1,\"dropped\":0,\"oversized\":0,\"heartbeat_secs\":60,\
                 \"max_segment_secs\":300,\"endpoints\":[{{\"source\":\"i1\",\
                 \"endpoint\":\"tcp://e:1\",\"msgs_seen\":1,\"dropped\":0,\
                 \"last_msg_at_ms\":{hb}}}]}}"
            )
        };
        format!(
            "{{\"kind\":\"segment_open\",\"at_ms\":{open},\"incarnation\":\"a\",\
             \"segment_seq\":{seq},\"heartbeat_secs\":60,\"max_segment_secs\":300}}\n{}\n\
             {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"{block}\",\
             \"n_tokens\":4,\"at_ms\":{at}}}\n{}",
            heartbeat(at - 100.0),
            // Ten seconds past the store, so the watermark (this heartbeat) less the skew band
            // still passes it and the tick classifies it.
            heartbeat(at + 10_000.0),
            open = at - 1000.0,
        )
    };
    let body = segment(0, at, "pold");
    live.store
        .put(
            &format!("{prefix}{}/00/n1/seg-a-0.jsonl", day.trim()),
            format!("{body}\n").as_bytes(),
        )
        .expect("put segment");

    // The ingest cycle widens from the listing, then lands the rows.
    let first = daemon.cycle(Due::INGEST);
    assert_eq!(first.ingested_files, 1, "the backfilled segment landed");
    let rows = daemon
        .engine()
        .run(
            "SELECT sum(records) FROM facts_rollup_daily FINAL \
             WHERE fleet_id = 'daemon-fleet' AND gone = 0",
        )
        .expect("count");
    // ONE record in the denominator: the segment carries one cache fact (the store), and its
    // lifecycle records land in the arrivals, never in the denominator.
    assert_eq!(rows.trim(), "1", "the store's denominator landed");
    let held = horizon_days(&daemon);
    assert!(
        (15..=17).contains(&held),
        "the horizon widened past the backfilled day (declared 14, held {held})"
    );

    // The same cycle ticked: the backfilled store was classified under the widened horizon.
    let classified = daemon
        .engine()
        .run(
            "SELECT toInt64(sum(events)) FROM class_rollup_daily FINAL \
             WHERE fleet_id = 'daemon-fleet' AND gone = 0",
        )
        .expect("classified");
    assert_eq!(
        classified.trim(),
        "1",
        "the backfilled store was classified under the widened horizon"
    );
    // The horizon holds while the driver's edges sit on that day. Today's segment moves them,
    // and the declared horizon returns.
    let today = daemon
        .engine()
        .run("SELECT toString(today())")
        .expect("date");
    live.store
        .put(
            &format!("{prefix}{}/00/n1/seg-a-1.jsonl", today.trim()),
            format!("{}\n", segment(1, now - 60_000.0, "pnew")).as_bytes(),
        )
        .expect("put segment");
    assert!(daemon.cycle(Due::INGEST).ticked, "today's segment ticks");
    // The hold is set at the start of a cycle, before its ingest, so the cycle after the tick
    // that read past the backfilled day is the one that returns it.
    daemon.cycle(Due::INGEST);
    assert_eq!(
        horizon_days(&daemon),
        14,
        "once the driver has read past the backfilled day, the declared horizon returns"
    );
}
