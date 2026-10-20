// SPDX-License-Identifier: AGPL-3.0-only

//! The loop against a real server (daemon).
//!
//! These tests start their OWN server rather than using the suite's shared fixture, and
//! that is required rather than tidy: the daemon's whole premise is that it EXCLUSIVELY
//! owns a data directory — it claims it, supervises the engine in it, and measures the
//! filesystem under it. A shared server would make every one of those claims untestable.
//!
//! Cycles are driven directly instead of through `run`, so nothing here waits on a
//! wall-clock cadence. A loop whose only entry point is `run` can only be tested by
//! sleeping, and a suite that sleeps is a suite people stop running.

use std::path::{Path, PathBuf};

use analyzer_analysis::DAY_MS;
use analyzer_daemon::counters::Cadences;
use analyzer_daemon::{Daemon, DaemonConfig, Due};
use analyzer_serving_clickhouse::engine::DEFAULT_LEDGER_RETENTION_DAYS;

mod common;
use common::{horizon_days, Scratch, FLEET};
use harness::journal_faults;

/// An executor over `spine`, the daemon's own unless a test substitutes one, publishing to
/// the daemon's serving store.
fn executor_on<'a>(
    daemon: &'a Daemon,
    params: &analyzer_analysis::Params,
    spine: Option<std::sync::Arc<analyzer_pipeline::Spine>>,
) -> analyzer_pipeline::Executor<'a> {
    analyzer_pipeline::Executor::open(
        spine.unwrap_or_else(|| daemon.spine()),
        params,
        daemon.serving(),
    )
    .expect("executor")
}

/// The daemon's journal through a faulty store over the same directory and prefix, and a
/// spine over it: executors given the spine share the daemon's journal and see the refusals.
fn faulty_journal(
    daemon: &Daemon,
    cfg: &DaemonConfig,
) -> (
    journal_faults::FaultyStore,
    std::sync::Arc<analyzer_pipeline::Spine>,
) {
    let store = journal_faults::FaultyStore::over(std::sync::Arc::new(
        analyzer_objectstore::DirStore::new(segments_root(cfg)),
    ));
    // The daemon's working state: one per directory and process, so this spine shares it.
    let spine = analyzer_pipeline::Spine::new(
        std::sync::Arc::new(store.clone()),
        &cfg.bucket.key_prefix,
        daemon.spine().namespace(),
        daemon.spine().lineage().clone(),
    )
    .with_working_state(
        &daemon
            .engine()
            .data_dir()
            .join(analyzer_pipeline::spine::WORKING_STATE),
        analyzer_lake::working::DEFAULT_MEMORY_BYTES,
    )
    .expect("the daemon's working state");
    (store, std::sync::Arc::new(spine))
}

/// The shipped cadences, for the counter snapshots the assertions read.
const CADENCES: Cadences = Cadences {
    tick: std::time::Duration::from_mins(1),
    ingest: std::time::Duration::from_mins(1),
};

/// Three days of a tap's segments, ending three days back, ingested as a shipper delivers them.
fn seed(daemon: &Daemon, segments: &Path) {
    ship(daemon, segments, &tap_segments(3.0, 3.0, 0));
}

/// Plant each of `days`' segments into the rig's tree at its own dated hour and ingest the same
/// bytes, so the input log and the source name every batch alike.
fn ship(daemon: &Daemon, segments: &Path, days: &[(f64, String)]) {
    for (day_start, body) in days {
        harness::plant_segment(segments, "", *day_start, "records.jsonl", body.as_bytes());
        analyzer_ingest::ingest_bytes(
            &daemon.spine().lake(),
            FLEET,
            &format!("records-{day_start}.jsonl"),
            body.as_bytes(),
        )
        .expect("ingest");
    }
}

/// Where [`rig_defaults`] rooted a config's segment directory.
fn segments_root(cfg: &DaemonConfig) -> PathBuf {
    cfg.data_dir.join("rig-segments")
}

/// A tap's segments over `span_days`, ending `ends_days_ago` days back: one per UTC day, each
/// opened in sequence from `first_seq` with heartbeats bracketing its stores, the run's first
/// segment after the agent's start.
/// Returns each day's start and segment.
fn tap_segments(ends_days_ago: f64, span_days: f64, first_seq: usize) -> Vec<(f64, String)> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    // day-aligned and inside the raw TTL — the harness's own rule
    let base = (now / DAY_MS).floor() * DAY_MS - (ends_days_ago + span_days) * DAY_MS;
    let mut by_day: std::collections::BTreeMap<i64, Vec<serde_json::Value>> =
        std::collections::BTreeMap::new();
    for k in 0..400 {
        let at = base + f64::from(k) * (span_days * DAY_MS / 400.0);
        by_day
            .entry(analyzer_analysis::emitted::day_index(at))
            .or_default()
            .push(serde_json::json!({
                "kind": "store", "instance_id": format!("i{}", k % 4),
                "block_id": format!("b{k}"), "n_tokens": 16, "at_ms": at,
                "incarnation": "run-1",
            }));
    }
    let heartbeat = |at: f64| {
        serde_json::json!({"kind": "heartbeat", "at_ms": at, "incarnation": "run-1",
            "canary": "k1", "msgs_seen": 100, "dropped": 0, "oversized": 0,
            "heartbeat_secs": 60, "max_segment_secs": 300,
            "endpoints": (0..4).map(|i| serde_json::json!({"source": format!("i{i}"),
                "endpoint": format!("tcp://e:{i}"), "msgs_seen": 25, "dropped": 0,
                "last_msg_at_ms": at})).collect::<Vec<_>>()})
    };
    by_day
        .into_values()
        .enumerate()
        .map(|(k, stores)| {
            let seq = first_seq + k;
            let first = stores[0]["at_ms"].as_f64().expect("at_ms");
            let last = stores[stores.len() - 1]["at_ms"].as_f64().expect("at_ms");
            let mut records = vec![serde_json::json!({"kind": "segment_open",
                "at_ms": first - 1000.0, "contract_version": "1.10", "incarnation": "run-1",
                "segment_seq": seq, "heartbeat_secs": 60, "max_segment_secs": 300})];
            if seq == 0 {
                records.push(
                    serde_json::json!({"kind": "agent_start", "at_ms": first - 500.0,
                    "incarnation": "run-1", "canary": "k1", "heartbeat_secs": 60,
                    "max_segment_secs": 300}),
                );
            }
            records.push(heartbeat(first - 100.0));
            records.extend(stores);
            records.push(heartbeat(last + 100.0));
            let body: String = records.iter().map(|r| r.to_string() + "\n").collect();
            ((first / DAY_MS).floor() * DAY_MS, body)
        })
        .collect()
}

/// A bucket no one answers on: connection-refused is instant, the loop's ingest arm
/// errors and retries, and every lifecycle property under test is ingest-free. The rig
/// is where daemon-driven ingest is exercised for real.
///
/// The dead endpoint rides the object-store builder's own env route: `s3://` parses, the
/// endpoint points at a port nobody holds, and the static pair keeps the credential chain
/// (and its IMDS timeout) out of a test about lifecycle. Env is process-global and
/// identical for every test in this binary, so the set is idempotent rather than racy, and
/// that is a boundary: a test needing a real bucket cannot live in this binary at all,
/// because it inherits this poison however it builds its own config (`ttl_gate.rs` is its
/// own binary for exactly that reason).
fn dead_bucket() -> analyzer_daemon::BucketConfig {
    for (k, v) in [
        ("AWS_ENDPOINT", "http://127.0.0.1:1"),
        ("AWS_ALLOW_HTTP", "true"),
        ("AWS_DEFAULT_REGION", "us-east-1"),
    ] {
        std::env::set_var(k, v);
    }
    analyzer_daemon::BucketConfig {
        url: "s3://nowhere/".to_string(),
        credentials: Some(analyzer_daemon::StaticCredentials {
            access_key_id: "dead".to_string(),
            secret_access_key: "dead".to_string(),
            session_token: None,
        }),
        key_prefix: String::new(),
    }
}
/// A rig daemon on a fresh scratch directory and its own port pair.
fn config(scratch: &Scratch) -> DaemonConfig {
    let mut cfg = DaemonConfig::new(
        FLEET,
        dead_bucket(),
        scratch.path(),
        analyzer_serving_clickhouse::engine::testing::engine_bin(),
    );
    // Four: the engine's two, and the restore check's two above them.
    let base = analyzer_serving_clickhouse::engine::testing::port_block(4);
    cfg.http_port = base;
    cfg.tcp_port = base + 1;
    rig_defaults(&mut cfg);
    cfg
}

/// Re-open a daemon on an existing configuration's directory and ports: the restart cases,
/// which come back to the same directory rather than allocating a fresh one.
///
/// It exists so the rig settings are applied in one place rather than two. A restart site
/// that built its `DaemonConfig` by hand once inherited the production default
/// `host_claim: true` and lost the race against a neighbouring test, refusing at the door
/// with the one-per-VM sentence.
fn restart_config(previous: &DaemonConfig) -> DaemonConfig {
    let mut cfg = DaemonConfig::new(
        FLEET,
        dead_bucket(),
        &previous.data_dir,
        &previous.clickhouse_bin,
    );
    cfg.http_port = previous.http_port;
    cfg.tcp_port = previous.tcp_port;
    rig_defaults(&mut cfg);
    cfg
}

/// What makes a `DaemonConfig` a RIG config rather than a deployment one.
///
/// The suite runs many daemons on this host, concurrently, by design — so none of them may
/// take the host claim. The two tests that assert the claim's behaviour opt back IN
/// explicitly, which is the only correct direction for this switch: forgetting to opt in
/// weakens one test, forgetting to opt out breaks every other test that happens to be
/// running at the time.
fn rig_defaults(cfg: &mut DaemonConfig) {
    cfg.host_claim = false;
    // The rig derives from a directory, like the job form: the loop's ingest arm points at a
    // dead bucket, so a collection transport would fail every pass on a listing nothing
    // answers. Rooted beside the data directory so a restart over the same directory finds
    // the same segments.
    cfg.segment_transport = Some(
        analyzer_pipeline::parquet_source::SegmentTransport::Directory {
            root: cfg.data_dir.join("rig-segments"),
        },
    );
    // The rig plants write-ahead JSONL; the corpus seam admits it (test rigs only).
    cfg.admit_corpus_forms = true;
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn one_analyzer_per_vm_is_a_decided_refusal_at_the_door() {
    // The second analyzer on a host refuses with the one-per-VM sentence before any
    // directory or port symptom can fire.
    let first_dir = Scratch::new("analyzer-daemon", "host-claim-a");
    let mut cfg = config(&first_dir);
    cfg.host_claim = true;
    let _first = Daemon::start(cfg).expect("first daemon takes the host claim");

    let rival_dir = Scratch::new("analyzer-daemon", "host-claim-b");
    let mut rival = config(&rival_dir);
    rival.http_port += 2;
    rival.tcp_port += 2;
    rival.host_claim = true;
    let Err(refusal) = Daemon::start(rival) else {
        panic!("a second analyzer on the same host must refuse")
    };
    assert!(
        matches!(&refusal, analyzer_daemon::Error::HostClaimed { .. }),
        "the refusal states the rule, not a symptom: {refusal}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn the_loop_ingests_ticks_and_reports_with_no_operator_invocation() {
    // The headline claim, stated as a test: no operator invocation anywhere in the steady
    // state. One cycle ingests and ticks. The sampler is the only writer of the health
    // artifacts; `Daemon::run` spawns it, and this test drives `cycle` directly, so it emits
    // explicitly below.
    let dir = Scratch::new("analyzer-daemon", "full");
    let cfg = config(&dir);
    let out = cfg.out_dir.clone();
    let segments = segments_root(&cfg);
    let mut daemon = Daemon::start(cfg).expect("the daemon starts and owns its directory");
    seed(&daemon, &segments);

    let first = daemon.cycle(Due::BOTH);
    assert!(first.ticked, "the tick ran on the cycle, not by a person");
    // NOT asserted: the pressure RUNG. It is a property of whatever volume this runs on,
    // and a CI runner with under 20% free runs at Reclaim. What the
    // test actually depends on is that the volume was measurable and intake was permitted;
    // asserting Normal was asserting the weather.
    assert_ne!(
        first.pressure,
        analyzer_daemon::pressure::Pressure::Unmeasured,
        "the volume must be measurable, or nothing below is a real test"
    );
    assert!(
        first.pressure.allows_ingest(),
        "this host is too full to exercise the ingest path (pressure: {})",
        first.pressure
    );

    // the artifacts a customer actually consumes, written by the path that writes them
    daemon
        .emit_health(&daemon.sample())
        .expect("the sampler's writer");
    for artifact in ["health.json", "health.prom"] {
        let path = out.join(artifact);
        assert!(
            path.exists(),
            "{artifact} must exist after one cycle: {}",
            path.display()
        );
    }

    // and the counters a dashboard binds to advanced
    let status = daemon.counters().snapshot(0, CADENCES);
    assert_eq!(status.ticks_total, 1);
    assert_eq!(status.tick_failures_total, 0);
    assert!(status.tick_lag_seconds.is_some(), "a tick completed");

    // a SECOND cycle re-ingests nothing (content-hash dedup) and ticks again idempotently
    let second = daemon.cycle(Due::BOTH);
    assert_eq!(
        second.ingested_files, 0,
        "the same file is not ingested twice — replay is recovery, not duplication"
    );
    assert!(second.ticked);
    assert_eq!(daemon.counters().snapshot(0, CADENCES).ticks_total, 2);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn the_maintenance_cadence_alone_ticks_nothing() {
    // The tick rides the ingest cadence; maintenance keeps the input log to its retention and
    // classifies nothing.
    let dir = Scratch::new("analyzer-daemon", "maintain");
    let cfg = config(&dir);
    let segments = segments_root(&cfg);
    let mut daemon = Daemon::start(cfg).expect("the daemon starts and owns its directory");
    seed(&daemon, &segments);

    let maintained = daemon.cycle(Due::MAINTAIN);
    assert!(!maintained.ticked, "no tick without the ingest cadence");
    assert_eq!(daemon.counters().snapshot(0, CADENCES).ticks_total, 0);
    let ticked = daemon.cycle(Due::INGEST);
    assert!(ticked.ticked, "the ingest cadence ticks");
    assert_eq!(daemon.counters().snapshot(0, CADENCES).ticks_total, 1);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn a_serving_layer_gets_a_third_engine_and_reads_touch_neither_of_the_daemons() {
    // `run` exits the process when the sampler's publication goes stale, naming "wedged on
    // the engine mutex" as the case it catches, so a console read holding the sampler's
    // mutex for three minutes would be indistinguishable from a dead sampler. The property
    // is not "a third engine exists" but "serving dispatches nothing on either of the other
    // two"; a sum cannot show that, which is why the counts come as a pair. The serving
    // connection's statements do reach the exported total.
    let dir = Scratch::new("analyzer-daemon", "api-engine");
    let cfg = config(&dir);
    let segments = segments_root(&cfg);
    let mut daemon = Daemon::start(cfg).expect("start");
    seed(&daemon, &segments);
    daemon.cycle(Due::BOTH);

    // The exported total first: a sample dispatches on the sampler's own connection, so it
    // is taken before the window this test measures, which holds the serving reads alone.
    let total_before = daemon
        .sample()
        .value("infertap_analyzer_engine_queries_total");
    let before = daemon.engine_query_counts();
    let api = daemon.api_engine().expect("a third connection");

    for _ in 0..3 {
        api.lock()
            .expect("api engine")
            .run("SELECT 1")
            .expect("the serving connection answers");
    }

    let after = daemon.engine_query_counts();
    assert_eq!(
        after.loop_engine, before.loop_engine,
        "a console read must not dispatch on the loop's connection, or it queues behind a pass"
    );
    assert_eq!(
        after.sampler, before.sampler,
        "and it must not dispatch on the SAMPLER's, or a slow read starves the liveness \
         publication and the daemon exits itself"
    );
    assert!(
        api.lock().expect("api engine").queries_executed() >= 3,
        "the reads went somewhere: they went to the third connection"
    );
    let total_after = daemon
        .sample()
        .value("infertap_analyzer_engine_queries_total");
    assert!(
        total_after
            .zip(total_before)
            .is_some_and(|(a, b)| a >= b + 3.0),
        "the serving connection's statements reach the exported total: {total_before:?} -> \
         {total_after:?}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn the_sampler_holds_its_own_engine_and_sampling_never_touches_the_loops() {
    // An Engine is one connection and one current-thread runtime, so a sampler sharing the
    // loop's Engine would queue behind a running pass rather than observe it. The property
    // is sharper than "two engines exist": sampling must not advance the loop's counter at
    // all, and a cycle must not advance the sampler's, or a wedge in the loop could hold
    // the lock the sampler needs.
    let dir = Scratch::new("analyzer-daemon", "engines");
    let cfg = config(&dir);
    let segments = segments_root(&cfg);
    let mut daemon = Daemon::start(cfg).expect("start");
    seed(&daemon, &segments);

    daemon.cycle(Due::BOTH);
    let after_cycle = daemon.engine_query_counts();
    assert!(
        after_cycle.loop_engine > 0,
        "the loop's engine did the work"
    );
    assert_eq!(
        after_cycle.sampler, 0,
        "the cycle must not touch the sampler's connection"
    );
    let _ = daemon.sample();
    let sampled_once = daemon.engine_query_counts();
    assert!(
        sampled_once.sampler > 0,
        "the sampler's engine did the sampling"
    );

    // sample three more times and watch ONLY the sampler move
    for _ in 0..3 {
        let _ = daemon.sample();
    }
    let sampled_more = daemon.engine_query_counts();
    assert_eq!(
        sampled_more.loop_engine, after_cycle.loop_engine,
        "sampling must not dispatch a single statement on the loop's connection — if it \
         did, the sampler would queue behind a running pass instead of observing it"
    );
    assert!(
        sampled_more.sampler >= sampled_once.sampler + 3,
        "each sample costs its own round trip on the sampler's connection: {} -> {}",
        sampled_once.sampler,
        sampled_more.sampler
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn the_daemons_sample_carries_the_server_and_the_loop_so_no_series_flaps() {
    // The sample and the artifact on disk carry the server and loop series alike: a
    // writer that left either absent would make those series flap between a value and
    // nothing, and a dashboard would read "the loop stopped" once an hour.
    let dir = Scratch::new("analyzer-daemon", "flap");
    let cfg = config(&dir);
    let out = cfg.out_dir.clone();
    let segments = segments_root(&cfg);
    let mut daemon = Daemon::start(cfg).expect("start");
    seed(&daemon, &segments);
    daemon.cycle(Due::BOTH);

    let sampled = daemon.sample();
    assert_eq!(
        sampled.value("infertap_analyzer_server_up"),
        Some(1.0),
        "the daemon owns a live server and says so"
    );
    assert!(sampled
        .value("infertap_analyzer_server_uptime_seconds")
        .is_some_and(|v| v >= 0.0));
    assert_eq!(sampled.value("infertap_analyzer_ticks_total"), Some(1.0));

    // the artifact on disk carries the same series, not an absent one
    daemon.emit_health(&sampled).expect("emit");
    let from_disk: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(out.join("health.json")).expect("health"))
            .expect("valid json");
    assert!(
        from_disk["metrics"]["infertap_analyzer_ticks_total"].is_number(),
        "the emitted artifact must not leave the loop series null: {}",
        from_disk["metrics"]
    );
    assert!(
        from_disk["metrics"]["infertap_analyzer_server_up"].is_number(),
        "nor the server series"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn a_second_daemon_on_the_same_directory_is_a_decided_refusal() {
    // Exactly one daemon may own a data directory. The refusal names the rival's pid, so
    // an operator is told which case they have rather than left to guess.
    let dir = Scratch::new("analyzer-daemon", "rival");
    let cfg = config(&dir);
    let mut rival = cfg.clone();
    let rival_base = analyzer_serving_clickhouse::engine::testing::port_block(4);
    rival.http_port = rival_base;
    rival.tcp_port = rival_base + 1;

    let _first = Daemon::start(cfg).expect("the first daemon claims the directory");
    let Err(refusal) = Daemon::start(rival) else {
        panic!("a second daemon on the same directory must refuse")
    };
    assert!(
        matches!(
            &refusal,
            analyzer_daemon::Error::Engine(analyzer_serving_clickhouse::engine::Error::Refused(m))
                if m.contains("already owns") && m.contains(&std::process::id().to_string())
        ),
        "the refusal names the live owner: {refusal}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn the_declared_horizon_is_reasserted_at_every_start() {
    // A horizon widened for unread input is runtime state: a fresh owner starts from its
    // declared policy and widens again from what it finds unread.
    let dir = Scratch::new("analyzer-daemon", "horizon");
    let cfg = config(&dir);
    let restarted = restart_config(&cfg);
    {
        let daemon = Daemon::start(cfg).expect("start");
        daemon.source_config().horizon_hours.set(4 * 24);
        assert_eq!(horizon_days(&daemon), 4);
    }
    let daemon = Daemon::start(restarted).expect("restart");
    assert_eq!(horizon_days(&daemon), DEFAULT_LEDGER_RETENTION_DAYS);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn a_signal_stops_the_loop_through_its_ordinary_ordered_path() {
    // `systemctl stop` must produce an ORDERED engine shutdown, not the crash-recovery
    // path. Relying on orphan adoption for ordinary stops would make recovery the steady
    // state.
    //
    // The handler is deliberately dumb: it sets a flag and returns. Everything that makes
    // stopping safe already lives on the loop's normal path, so a signal takes that path
    // rather than a second one written for signals.
    let shutdown = analyzer_daemon::Shutdown::new();
    shutdown.listen().expect("handlers install");
    assert!(!shutdown.requested(), "nothing has asked us to stop yet");

    // raise SIGTERM at ourselves; the handler must flip the flag the loop reads
    unsafe_free_raise_sigterm();
    // the flag is set from a signal handler, so give the kernel a moment to deliver
    for _ in 0..100 {
        if shutdown.requested() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        shutdown.requested(),
        "SIGTERM must reach the flag the loop checks between cycles"
    );

    // and a clone observes it too: the daemon thread and the signal source hold separate
    // handles to one flag
    let observer = shutdown.clone();
    assert!(observer.requested());
}

/// Send ourselves SIGTERM without `libc` — the workspace forbids it in our sources, and
/// this is a test, not a reason for an exception. `kill` is a process the test may spawn.
fn unsafe_free_raise_sigterm() {
    std::process::Command::new("kill")
        .arg("-TERM")
        .arg(std::process::id().to_string())
        .status()
        .expect("kill");
}

/// A tick whose apply was lost is published again by a replacement daemon, with no new
/// arrival, and its figures do not double.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine daemon"]
fn a_lost_apply_is_recovered_by_a_replacement_daemon_without_new_arrivals() {
    let dir = Scratch::new("analyzer-daemon", "durable-apply");
    let cfg = config(&dir);
    let params = cfg.params.clone();
    let segments = segments_root(&cfg);
    let mut daemon = Daemon::start(cfg.clone()).expect("start");
    seed(&daemon, &segments);
    assert!(daemon.cycle(Due::BOTH).ticked);
    let mass = |daemon: &Daemon| {
        daemon
            .engine()
            .run(&format!(
                "SELECT toInt64(sum(tokens)) FROM class_rollup_daily FINAL \
                 WHERE fleet_id = '{FLEET}' AND gone = 0"
            ))
            .expect("mass")
    };
    let before = mass(&daemon);
    // A tick on more input finishes its serving writes, then loses its durable apply.
    // The run continues past the seed's three segments.
    ship(&daemon, &segments, &tap_segments(1.0, 1.0, 3));
    let (journal, faulty) = faulty_journal(&daemon, &cfg);
    journal_faults::lose_apply(&journal, "lose_apply");
    let lost = executor_on(&daemon, &params, Some(faulty))
        .advance(&daemon.source_config().source())
        .expect_err("apply lost");
    assert!(lost.to_string().contains("lose_apply"), "{lost}");
    journal_faults::heal(&journal);
    assert!(
        analyzer_pipeline::execution::revision(&*daemon.serving(), &daemon.spine())
            .expect("withheld")
            .is_none()
    );
    let lost_mass = mass(&daemon);
    // No process-local state carries the unapplied tick into the next daemon.
    drop(daemon);
    let mut replacement = Daemon::start(cfg).expect("replacement daemon");
    assert!(replacement.cycle(Due::INGEST).ticked);
    assert!(
        analyzer_pipeline::execution::revision(&*replacement.serving(), &replacement.spine())
            .expect("recovered")
            .is_some()
    );
    assert_ne!(lost_mass, before, "the lost tick wrote its cells");
    assert_eq!(
        mass(&replacement),
        lost_mass,
        "publishing the batch again doubles nothing"
    );
}
