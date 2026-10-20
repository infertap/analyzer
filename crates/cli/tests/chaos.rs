// SPDX-License-Identifier: AGPL-3.0-only

//! The section 3 chaos drill, scaled: kill the analyzer ungracefully mid-deployment, feed it a
//! simulated multi-day backlog, and assert it comes back live and heals completely.
//!
//! **What the scale proves and what it does not.** The mechanisms under test — kill -9
//! survival, orphaned-engine takeover, idempotent re-ingest across the crash, health
//! artifacts live shortly after restart, a backlog healed whole — are scale-independent,
//! so a small representative backlog exercises the identical code paths in CI minutes.
//! The ABSOLUTE recovery numbers (dashboard ≤15 min, 72 h outage healed ≤12 h) are claims
//! about envelope-scale volume, and only the section 3 soak — a standing fleet over weeks — can
//! measure those. This drill asserts scaled bounds and prints the measured real-time
//! multiple, so a throughput regression still moves a number somebody sees.

#![allow(clippy::cast_precision_loss)] // drill-scale counts and offsets, nowhere near 2^52

use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use harness::journal_faults;

/// Ports for drill slot `n`, scoped to THIS test process.
///
/// **Fixed ports were a cascading flake, and the asymmetry is the tell:** `scratch()` below
/// already scopes directories by pid, but the ports were constants. So a drill that timed out
/// left its daemons alive holding those ports, and every subsequent run failed deterministically
/// on a bind — one bad run poisoning the suite until somebody went looking at `ps`. Measured, not
/// theorised: two engines and a daemon from a timed-out failover run were still holding
/// 19930/19931 an hour later, and the retry could not have passed. Directory isolation without
/// port isolation is half an isolation.
///
/// **Slots are spaced by four, not two**, because each daemon's scheduled restore-verify spawns
/// a scratch server at `VERIFY_PORT_OFFSET` (+2) above its own ports. Two would put one drill's
/// verify server on the next drill's daemon.
fn ports(slot: u16) -> (u16, u16) {
    static BASE: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    let base = *BASE.get_or_init(|| analyzer_serving_clickhouse::engine::testing::port_block(24));
    (base + slot * 4, base + slot * 4 + 1)
}

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("chaos-drill-{name}-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
}

/// The drill's bucket, from the environment — the same gate `bucket_live` uses. Absent,
/// the drill skips loudly rather than failing: the rig is what makes it unconditional.
fn bucket_env() -> Option<(String, String, String)> {
    let url = std::env::var("INFERTAP_TEST_S3_URL").ok()?;
    Some((
        url,
        std::env::var("INFERTAP_TEST_S3_KEY").unwrap_or_default(),
        std::env::var("INFERTAP_TEST_S3_SECRET").unwrap_or_default(),
    ))
}

fn clickhouse() -> String {
    std::env::var("CLICKHOUSE_BIN").unwrap_or_else(|_| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(".tools/clickhouse")
            .display()
            .to_string()
    })
}

/// One sealed segment of synthetic telemetry, PUT to the bucket under a dated hour
/// prefix (the delivery layout, `conformance/delivery/layout.json`): a
/// `segment_open` header (ingest refuses a headerless segment as corruption) plus store
/// records spread across the simulated span.
/// **No engine parameter: a segment writer needs no database connection.** It took one to run
/// `INSERT INTO FUNCTION s3(records_bucket, …)` through the engine's client. Writing an object is
/// object storage's business now.
/// Every drill timestamp derives from here — three days back, day-aligned, the same
/// base the whole suite seeds from. The literals this replaces were
/// a lit fuse: the drills would have started failing the day those records crossed the
/// deltas TTL, in an environment that had only just started running them at all.
fn drill_base_ms() -> f64 {
    analyzer_serving_clickhouse::engine::testing::recent_day_base_ms()
}

/// The drill run's cumulative counters around segment `seq`, each as (first heartbeat, last
/// heartbeat): segment `seq` is source `i{seq % 3}`'s `seq / 3`-th, carries ten messages, and
/// drops one when it is `i0`'s.
struct Counters {
    msgs: (u64, u64),
    dropped: (u64, u64),
    run_msgs: (u64, u64),
    run_dropped: (u64, u64),
}

impl Counters {
    fn at(seq: u64) -> Self {
        let own = seq / 3;
        let drops = u64::from(seq.is_multiple_of(3));
        // i0's segments before `seq`: 0, 3, 6, ... below it.
        let run_drops_before = seq.div_ceil(3);
        Self {
            msgs: (own * 10, own * 10 + 10),
            dropped: (own * drops, own * drops + drops),
            run_msgs: (seq * 10, seq * 10 + 10),
            run_dropped: (run_drops_before, run_drops_before + drops),
        }
    }
}

fn put_segment(prefix: &str, seq: u64, base_ms: f64, records: usize) {
    use std::fmt::Write as _;
    let incarnation = "1785500000000-42";
    let mut body = format!(
        "{{\"kind\":\"segment_open\",\"at_ms\":{base_ms},\"incarnation\":\"{incarnation}\",\
         \"segment_seq\":{seq},\"contract_version\":\"1.0\"}}\n"
    );
    // Heartbeats bracket every segment, and every third one carries a DROP: without drop
    // windows the taint machinery never runs, and a divergence in it cannot fail the
    // convergence assertion. The tainted classes must converge too.
    // The counters are cumulative over the run, as a tap states them: each source's and the
    // envelope's at the segment's first heartbeat, and after its stores at the last.
    let c = Counters::at(seq);
    let _ = writeln!(
        body,
        "{{\"kind\":\"heartbeat\",\"at_ms\":{base_ms},\"msgs_seen\":{},\"dropped\":{},\
         \"endpoints\":[{{\"source\":\"i{}\",\"endpoint\":\"tcp://e:1\",\"msgs_seen\":{},\"dropped\":{}}}]}}",
        c.run_msgs.0,
        c.run_dropped.0,
        seq % 3,
        c.msgs.0,
        c.dropped.0,
    );
    let _ = writeln!(
        body,
        "{{\"kind\":\"heartbeat\",\"at_ms\":{},\"msgs_seen\":{},\"dropped\":{},\
         \"endpoints\":[{{\"source\":\"i{}\",\"endpoint\":\"tcp://e:1\",\"msgs_seen\":{},\"dropped\":{}}}]}}",
        base_ms + (records as f64) * 500.0,
        c.run_msgs.1,
        c.run_dropped.1,
        seq % 3,
        c.msgs.1,
        c.dropped.1,
    );
    for i in 0..records {
        let at = base_ms + (i as f64) * 500.0; // one store every simulated half-second
        let _ = writeln!(
            body,
            "{{\"kind\":\"store\",\"instance_id\":\"i{}\",\"dp_rank\":0,\"group_idx\":0,\
             \"block_id\":\"{}\",\"n_tokens\":16,\"tier\":\"GPU\",\"at_ms\":{at}}}",
            seq % 3,
            seq * 1_000_000 + i as u64,
        );
    }
    // The key's date and hour derive from base_ms — the layout's own arithmetic — so
    // keys stay lexicographically monotonic because the callers' base_ms is monotonic.
    // A literal date stood here beside literal record timestamps: the
    // fixed-fuse shape the fixture law bans, discovered the day the drills first ran.
    let date_hour = analyzer_lake::dated::hour_prefixes(base_ms, base_ms)
        .pop()
        .expect("a dated hour for the plant");
    let key = format!("{prefix}{date_hour}/n1/seg-{incarnation}-{seq}.jsonl");
    put_object(&key, &format!("{body}\n"));
}

fn spawn_daemon(root: &std::path::Path, prefix: &str) -> Child {
    let (http, tcp) = ports(0);
    spawn_daemon_over(root, prefix, http, tcp)
}

/// Spawn a daemon over a key prefix in the drill bucket: the failover drill runs two of
/// these over ONE prefix, each owning its own database, engine and artifacts.
fn spawn_daemon_over(root: &std::path::Path, prefix: &str, http: u16, tcp: u16) -> Child {
    let (url, key, secret) = bucket_env().expect("gated before spawn");
    Command::new(env!("CARGO_BIN_EXE_infertap-analyzer"))
        .env("INFERTAP_S3_ACCESS_KEY_ID", key)
        .env("INFERTAP_S3_SECRET_ACCESS_KEY", secret)
        .env("INFERTAP_HOST_CLAIM", "off") // drills run several daemons: a rig, not a deployment
        .env("INFERTAP_ADMIT_CORPUS_FORMS", "on") // drills plant write-ahead JSONL
        .args([
            "run",
            "--bucket-url",
            &url,
            "--bucket-prefix",
            prefix,
            "--fleet-id",
            "chaos",
            "--data-dir",
            &root.join("db").display().to_string(),
            "--out",
            &root.join("out").display().to_string(),
            "--metrics-addr",
            "off",
            // The drills predate the read API's default-on listener: every daemon bound
            // 127.0.0.1:8088, and whichever came second — the failover drill runs two at
            // once by design — died at the door on the conflict. await_heal reads the
            // health artifacts, so the drills need no API at all.
            "--api-addr",
            "off",
            // Seconds, not the minute default: the convergence tail compares what the ticks
            // published, and at the default cadence the spare would compare a stale tick
            // against a fresh one and call the pipeline divergent.
            "--ingest-interval",
            "2",
            "--http-port",
            &http.to_string(),
            "--tcp-port",
            &tcp.to_string(),
            "--clickhouse",
            &clickhouse(),
        ])
        .spawn()
        .expect("spawn daemon")
}

/// How long a drill waits for a daemon to reach a state before calling it hung. A guard against
/// a hang, not a budget: the drills claim that the daemons heal and converge, and a debug build
/// on a shared runner takes as long as it takes. A hang trips one guard well inside the
/// release battery's own timeout.
const HANG_GUARD: Duration = Duration::from_mins(10);

/// One figure from the health artifact (the dashboard-live proxy: the same sample
/// `/metrics` serves); `None` until the artifact exists and carries it.
fn health_metric(root: &std::path::Path, name: &str) -> Option<f64> {
    let text = std::fs::read_to_string(root.join("out").join("health.json")).ok()?;
    let v = serde_json::from_str::<serde_json::Value>(&text).ok()?;
    // metrics render as a map of name -> value in the JSON artifact
    v["metrics"][name].as_f64()
}

/// Wait until the health artifact is written and its ledger shows `expected` files
/// ingested, or fail at `limit`.
fn await_heal(root: &std::path::Path, expected: u64, limit: Duration) -> Duration {
    let started = Instant::now();
    while started.elapsed() < limit {
        #[allow(clippy::float_cmp)] // a file count round-trips f64 exactly at drill scale
        if health_metric(root, "infertap_analyzer_ledger_rows") == Some(expected as f64) {
            return started.elapsed();
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!(
        "not healed within {limit:?}: {} never reported {expected} ledger rows",
        root.join("out").join("health.json").display()
    );
}

/// Wait past [`await_heal`] until a tick has STARTED after the healed sample, so an
/// ordered stop cannot land between the last commit and the tick that accounts for it.
/// TWO increments, not one: a tick already in flight when the healed sample was taken
/// incremented the counter before that sample and proves nothing about what it examined;
/// the second start is necessarily after it. The daemons tick at a seconds cadence, so this
/// waits out one sampler publication rather than hoping at a scheduling.
fn await_settled(root: &std::path::Path, expected: u64, limit: Duration) {
    await_heal(root, expected, limit);
    let p0 = health_metric(root, "infertap_analyzer_ticks_total").unwrap_or(0.0);
    let started = Instant::now();
    while started.elapsed() < limit {
        if health_metric(root, "infertap_analyzer_ticks_total").unwrap_or(0.0) >= p0 + 2.0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("no tick started after the heal within {limit:?}");
}

#[test]
#[ignore = "chaos drill: it spawns real daemons against a live bucket. Run on demand: \
           cargo test -p infertap-analyzer --test chaos -- --ignored"]
fn kill_dash_nine_then_a_backlog_heals_whole_and_the_dashboard_comes_back() {
    if bucket_env().is_none() {
        eprintln!("SKIPPED: no INFERTAP_TEST_S3_URL — the kill-9 drill ran against nothing");
        return;
    }
    let root = scratch("kill9");
    let prefix = format!("chaos-{}-kill9/", std::process::id());
    // Half the backlog before the crash: 12 segments over a simulated ~5 hours.
    for seq in 0..12u64 {
        put_segment(&prefix, seq, drill_base_ms() + (seq as f64) * 1.5e6, 300);
    }
    let mut first = spawn_daemon(&root, &prefix);
    let live_after_start = await_heal(&root, 12, HANG_GUARD);

    // The chaos: no SIGTERM, no ordered stop — the process is simply gone, leaving the
    // engine child orphaned and the pid file claiming ownership.
    first.kill().expect("kill -9");
    first.wait().expect("reap");

    // The outage backlog lands while nobody is running: 12 more segments.
    for seq in 12..24u64 {
        put_segment(
            &prefix,
            seq,
            drill_base_ms() + 18_000_000.0 + (seq as f64) * 1.5e6,
            300,
        );
    }

    let mut second = spawn_daemon(&root, &prefix);
    let healed_in = await_heal(&root, 24, HANG_GUARD);
    // ~10 simulated hours of telemetry over the measured wall time: the real-time multiple
    // catch-up is stated in. Printed, not asserted: a debug build on a shared runner is not
    // where throughput is measured, and the drill's claim is that it heals whole.
    let simulated_hours = 10.0;
    let multiple = simulated_hours * 3_600.0 / healed_in.as_secs_f64().max(0.001);
    println!(
        "chaos drill: live {live_after_start:?} after first start; healed 24 segments in \
         {healed_in:?} after kill -9 (~{multiple:.0}x real time at drill scale)"
    );

    // Ordered stop still works after a lifetime that began with a takeover: SIGTERM the
    // way the init system sends it (a spawned `kill`, since the libc crate is forbidden
    // workspace-wide and this is exactly what systemd does anyway).
    let _ = Command::new("kill")
        .args(["-TERM", &second.id().to_string()])
        .status();
    let status = second.wait().expect("reap second");
    assert!(
        status.success(),
        "SIGTERM after takeover is an ordered exit"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// SIGTERM as the init system sends it, asserting the ordered exit.
fn ordered_stop(child: &mut Child, who: &str) {
    let _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status();
    let status = child.wait().expect("reap");
    assert!(status.success(), "{who}: SIGTERM is an ordered exit");
}

/// The convergence surface: the ANALYSIS tables, each with its one canonical dump
/// statement: the tables themselves are the strong claim, where a summary of them could agree
/// by accident.
///
/// `FINAL` on every replacing table, because two databases at different MERGE states can
/// hold identical logical content. `ORDER BY ALL`, because a dump is canonical or it is
/// nothing. The exclusions are the columns that are provenance of a RUN rather than
/// analysis of the fleet: `ingested_at_ms` is this analyzer's own arrival clock, and an
/// input position may carry different gaps after faults.
const ANALYSIS_DUMP: &[(&str, &str)] = &[
    (
        "input_batches",
        "SELECT * EXCEPT (ingested_at_ms, input_position, version) FROM input_batches FINAL \
         WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "input_segments",
        "SELECT * EXCEPT (ingested_at_ms, version) FROM input_segments FINAL WHERE gone = 0 \
         ORDER BY ALL",
    ),
    (
        "input_drops",
        "SELECT * EXCEPT (ingested_at_ms, version) FROM input_drops FINAL WHERE gone = 0 \
         ORDER BY ALL",
    ),
    (
        "class_rollup_hourly",
        "SELECT * EXCEPT (version, gone) FROM class_rollup_hourly FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "class_rollup_daily",
        "SELECT * EXCEPT (version, gone) FROM class_rollup_daily FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "band_rollup_daily",
        "SELECT * EXCEPT (version, gone) FROM band_rollup_daily FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "fleet_inventory",
        "SELECT * EXCEPT (version, gone) FROM fleet_inventory FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "coverage_daily",
        "SELECT * EXCEPT (version, gone) FROM coverage_daily FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "facts_rollup_daily",
        // One row per batch: read as sums, since the batches are each box's own split of the
        // same input.
        "SELECT fleet_id, instance_id, kind, day, sum(records), sum(tokens), sum(own), \
         sum(comparable) FROM facts_rollup_daily FINAL WHERE gone = 0 \
         GROUP BY fleet_id, instance_id, kind, day ORDER BY ALL",
    ),
    (
        "input_key_spaces",
        "SELECT * EXCEPT (version, gone) FROM input_key_spaces FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "coverage_minute",
        "SELECT * EXCEPT (version, gone) FROM coverage_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "block_mass_daily",
        "SELECT * EXCEPT (version, gone) FROM block_mass_daily FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "block_mass_day",
        "SELECT * EXCEPT (version, gone) FROM block_mass_day FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "tag_minute",
        "SELECT * EXCEPT (version, gone) FROM tag_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "tag_intervals",
        "SELECT * EXCEPT (version, gone) FROM tag_intervals FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "markers",
        // `observed` markers are derived from the records and must converge; `recorded`
        // ones (deploy, param_change, analyzer_upgrade) are this analyzer's own diary
        "SELECT * EXCEPT (version, gone) FROM markers FINAL WHERE provenance = 'observed' AND gone = 0 ORDER BY ALL",
    ),
            // The grain ladder's tables (landings 3–10): the figure rollups at every grain, the
    // and the producer's serving tables — Summing tables read as sums
    // with rounding, the way `product_dump` reads them, since a sum of deltas converges
    // rather than matching to the byte. Missing from this list since the hourly rollups: the
    // drill runs on tags only, so nothing here fired.
    (
        "band_minute",
        "SELECT fleet_id, semantics_version, discriminator, class, band, workload_class, model, bucket, toInt64(events), toInt64(tokens) FROM band_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "band_rank_minute",
        "SELECT fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, band, workload_class, model, toInt64(events), toInt64(tokens) FROM band_rank_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "band_rollup_hourly",
        "SELECT fleet_id, semantics_version, discriminator, class, band, tier, workload_class, model, instance_id, dp_rank, group_idx, bucket, toInt64(events), toInt64(tokens) FROM band_rollup_hourly FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "recompute_minute",
        "SELECT fleet_id, semantics_version, discriminator, class, workload_class, model, bucket, toInt64(events), toInt64(tokens) FROM recompute_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "recompute_rank_minute",
        "SELECT fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, workload_class, model, toInt64(events), toInt64(tokens) FROM recompute_rank_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_daily",
        "SELECT * EXCEPT (version, gone) FROM replication_daily FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_hourly",
        "SELECT * EXCEPT (version, gone) FROM replication_hourly FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_whale_day",
        "SELECT * EXCEPT (version, gone) FROM replication_whale_day FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_whale_day_total",
        "SELECT * EXCEPT (version, gone) FROM replication_whale_day_total FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_instance_hourly",
        "SELECT * EXCEPT (version, gone) FROM replication_instance_hourly FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_instance_minute",
        "SELECT * EXCEPT (version, gone) FROM replication_instance_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_instance_spans_day",
        "SELECT * EXCEPT (version, gone) FROM replication_instance_spans_day FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_minute",
        "SELECT * EXCEPT (version, gone) FROM replication_minute FINAL WHERE gone = 0 ORDER BY ALL",
    ),
    (
        "replication_pairs_day",
        "SELECT * EXCEPT (version, gone) FROM replication_pairs_day FINAL WHERE gone = 0 ORDER BY ALL",
    ),
];

/// Tables two independent analyzers legitimately DISAGREE on — provenance of a run, not
/// analysis of the fleet. Every schema table must be on this list or in
/// [`ANALYSIS_DUMP`]; the dump asserts that against `system.tables`, so a new table
/// cannot join the schema without somebody deciding which claim it carries.
const RUN_LOCAL_TABLES: &[&str] = &[
    // the schema pin's own record: deployment state, not analysis
    "schema_migrations",
    // Ownership, recovery and allocation records are validated per run.
    "execution_namespace",
    // the derivation's standing facts: this analyzer's last tick and its edges — where THIS
    // analyzer got to, not a claim about the fleet
    "derivation_status",
    // late arrivals counted against this analyzer's own edges: when its ticks ran, not what
    // the fleet did
    "restated_hourly",
];

/// Canonical dump of the analysis one STOPPED database holds, on explicit ports so
/// nothing in the parallel test suite is disturbed. `Session` is the same shipped
/// machinery every one-shot command runs on: an engine this process owns, which is
/// possible exactly when nobody else owns the directory — the drill stops its daemons
/// first, and a leaked claim would fail here rather than read stale.
fn analysis_dump(root: &std::path::Path, prefix: &str, http: u16, tcp: u16) -> String {
    use std::fmt::Write as _;
    let mut config =
        analyzer_serving_clickhouse::engine::server::ServerConfig::new(root.join("db"));
    config.http_port = http;
    config.tcp_port = tcp;
    let session =
        analyzer_serving_clickhouse::engine::server::Session::open_with(clickhouse(), &config)
            .expect("a session over the stopped data-dir");
    let tables = session
        .engine()
        .run(
            "SELECT name FROM system.tables WHERE database = currentDatabase() \
             AND engine != 'MaterializedView' ORDER BY name",
        )
        .expect("list tables");
    for table in tables.lines().filter(|l| !l.is_empty()) {
        assert!(
            ANALYSIS_DUMP.iter().any(|(name, _)| *name == table)
                || RUN_LOCAL_TABLES.contains(&table),
            "table `{table}` is on neither the convergence list nor the run-local list: \
             decide whether two independent analyzers must agree on it and add it to one"
        );
    }
    // The daemon's journal is its lineage's, in the drill bucket.
    let (url, key, secret) = bucket_env().expect("gated before spawn");
    let credentials = (!key.is_empty()).then_some(analyzer_objectstore::StaticCredentials {
        access_key_id: key,
        secret_access_key: secret,
        session_token: None,
    });
    let bucket = std::sync::Arc::new(
        analyzer_objectstore::S3Store::open(&url, credentials.as_ref()).expect("bucket"),
    );
    let lineage = analyzer_pipeline::lineage::Lineage::read(&root.join("db"))
        .expect("lineage")
        .expect("the daemon recorded its lineage");
    let namespace = journal_faults::namespace(session.engine(), "chaos");
    let head = journal_faults::read_journal(&*bucket, prefix, &namespace, &lineage);
    // PRECONDITION, asserted per database rather than compared across them: this analyzer
    // read every record it ingested, and its last tick is applied. Without it the comparison
    // can hold a stale verdict against a fresh one and blame the pipeline. The driver
    // classifies through the newest record less its watermark delay.
    let spine = analyzer_pipeline::Spine::new(bucket, prefix, &namespace, lineage.clone());
    let span = analyzer_pipeline::facts::full_span(&spine.lake())
        .expect("input span")
        .expect("the daemon ingested");
    let classified: f64 = session
        .engine()
        .run(
            "SELECT value FROM derivation_status FINAL \
             WHERE fact = 'classified_to_ms' AND gone = 0",
        )
        .expect("classified edge")
        .trim()
        .parse()
        .expect("this database was stopped before any tick was applied");
    let delay = analyzer_analysis::Params::default().delta_skew_ms;
    assert!(
        classified >= span.end_ms - delay,
        "this database was stopped before its ticks read every record ({classified} against \
         {}) — the convergence comparison would be over a stale verdict, which proves nothing \
         about the pipeline",
        span.end_ms
    );
    assert_eq!(
        head["keyed"]["applied"], true,
        "an unapplied tick cannot establish convergence"
    );
    let mut dump = String::new();
    for (table, sql) in ANALYSIS_DUMP {
        let rows = session
            .engine()
            .run(sql)
            .unwrap_or_else(|e| panic!("dumping {table}: {e}"));
        let _ = writeln!(dump, "== {table} ==\n{rows}");
    }
    session.close().expect("close the dump session");
    dump
}

/// The section 4 failover drill: two analyzers, one collection, zero coordination.
///
/// The claim under test is the HA pattern verbatim — the pipeline is deterministic and
/// idempotent over append-only input, so two independent analyzers reading the same
/// collection converge to the same analysis with no channel between them. Failover is
/// repointing the dashboard; a returning analyzer converges on the delta it slept
/// through. The drill kills the primary without ceremony mid-stream, lets the spare
/// absorb the outage segments alone, brings the primary back to catch up, stops both in
/// order, and then holds their databases to ONE answer: every analysis table dumped
/// canonically ([`ANALYSIS_DUMP`]) and compared EQUAL, with only run-local provenance
/// excluded.
#[test]
#[ignore = "chaos drill: it spawns real daemons against a live bucket. Run on demand: \
           cargo test -p infertap-analyzer --test chaos -- --ignored"]
fn failover_two_analyzers_one_collection_converge_without_coordination() {
    if bucket_env().is_none() {
        eprintln!("SKIPPED: no INFERTAP_TEST_S3_URL — the failover drill ran against nothing");
        return;
    }
    let root = scratch("failover");
    let prefix = format!("chaos-{}-failover/", std::process::id());
    for seq in 0..12u64 {
        put_segment(&prefix, seq, drill_base_ms() + (seq as f64) * 1.5e6, 300);
    }
    let a = root.join("a");
    let b = root.join("b");
    std::fs::create_dir_all(&a).expect("mkdir a");
    std::fs::create_dir_all(&b).expect("mkdir b");

    let (p_http, p_tcp) = ports(1);
    let mut primary = spawn_daemon_over(&a, &prefix, p_http, p_tcp);
    let (s_http, s_tcp) = ports(2);
    let mut spare = spawn_daemon_over(&b, &prefix, s_http, s_tcp);
    await_heal(&a, 12, HANG_GUARD);
    await_heal(&b, 12, HANG_GUARD);

    // The failover event: the primary is simply gone, and nobody tells the spare,
    // because there is nothing to tell — it reads the same collection it always read.
    primary.kill().expect("kill -9 primary");
    primary.wait().expect("reap primary");
    for seq in 12..18u64 {
        put_segment(
            &prefix,
            seq,
            drill_base_ms() + 18_000_000.0 + (seq as f64) * 1.5e6,
            300,
        );
    }
    await_heal(&b, 18, HANG_GUARD);

    // The blue-green half: the old primary returns with a stale database and converges
    // on the delta it slept through, taking over its own orphaned engine on the way.
    let (p_http, p_tcp) = ports(1);
    let mut primary = spawn_daemon_over(&a, &prefix, p_http, p_tcp);
    // Settled, not merely healed, on BOTH sides: the comparison below is of what the ticks
    // published, so each daemon must have started a tick after its last commit landed —
    // otherwise the drill compares a stale tick and blames the pipeline for the schedule.
    await_settled(&a, 18, HANG_GUARD);
    await_settled(&b, 18, HANG_GUARD);

    ordered_stop(&mut primary, "primary");
    ordered_stop(&mut spare, "spare");

    let (a_http, a_tcp) = ports(3);
    let dump_a = analysis_dump(&a, &prefix, a_http, a_tcp);
    let (b_http, b_tcp) = ports(4);
    let dump_b = analysis_dump(&b, &prefix, b_http, b_tcp);
    assert_eq!(
        dump_a, dump_b,
        "two analyzers over one collection diverged: the zero-coordination convergence \
         claim is broken somewhere between ingest and the verdict"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **Writes through the object store, because the engine has no bucket to write to.**
fn put_object(key: &str, body: &str) {
    use analyzer_objectstore::ObjectStore as _;
    let url = std::env::var("INFERTAP_TEST_S3_URL").expect("INFERTAP_TEST_S3_URL");
    let store = analyzer_objectstore::S3Store::open(
        &url,
        Some(&analyzer_objectstore::StaticCredentials {
            access_key_id: std::env::var("INFERTAP_TEST_S3_KEY").unwrap_or_default(),
            secret_access_key: std::env::var("INFERTAP_TEST_S3_SECRET").unwrap_or_default(),
            session_token: None,
        }),
    )
    .expect("a store over the test endpoint");
    store.put(key, body.as_bytes()).expect("put segment");
}
