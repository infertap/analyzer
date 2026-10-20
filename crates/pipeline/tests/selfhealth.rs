// SPDX-License-Identifier: AGPL-3.0-only

//! Self-health against a real database. The unit tests hold the
//! rendering rules; these hold the two things only a live engine can prove: that the
//! collector actually reads engine internals, and that the export surface a customer's
//! alarms bind to does not drift.
//!
//! Why a spawn-count pin rides along: this module is called on every report and, at
//! server posture, on the maintenance loop's cadence. A collector that quietly cost one
//! round trip per metric would make the sampler itself the thing that degrades with time, in
//! the component whose whole job is to notice that class of failure.

use analyzer_analysis::Params;
use analyzer_serving_clickhouse::selfhealth::{self, Paths, Status, METRIC_NAMES};

fn paths<'a>(data_dir: &'a std::path::Path, out_dir: &'a std::path::Path) -> Paths<'a> {
    Paths {
        data_dir,
        out_dir,
        ledger_retention_days: 30,
        analyzer_id: None,
        server: None,
        loop_status: None,
        fleet: None,
        runs: None,
    }
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_collector_reads_real_engine_internals_not_placeholders() {
    let (engine, db) = harness::fresh_engine();
    let out = harness::fresh_dir("health-out");
    let vitals = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &paths(db.data_dir(), &out),
    )
    .expect("collect");

    // the migrated schema exists, so parts and a real volume must be visible — a
    // collector wired to nothing would report zeros here and look healthy forever
    assert!(
        vitals
            .value("infertap_analyzer_disk_total_bytes")
            .is_some_and(|v| v > 0.0),
        "the engine's own view of the volume must reach the metric"
    );
    assert!(
        vitals
            .value("infertap_analyzer_active_parts")
            .is_some_and(|v| v >= 0.0),
        "part accounting must be present"
    );
    assert_eq!(
        vitals.value("infertap_analyzer_raw_retention_days"),
        Some(30.0)
    );

    // an empty fleet cannot prove TTL enforcement either way, and says so rather than
    // reporting ok
    let retention = vitals
        .checks
        .iter()
        .find(|c| c.name == "raw_retention_enforced")
        .expect("retention check");
    assert_eq!(retention.status, Status::Unknown);
    assert!(
        vitals
            .unmeasured()
            .contains(&"infertap_analyzer_raw_oldest_age_days"),
        "no committed batch means an absent series, never a zero"
    );

    std::fs::remove_dir_all(out).ok();
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn every_declared_metric_name_is_actually_emitted() {
    // METRIC_NAMES is the export contract an alarm in an unreachable environment binds
    // to. This is the ratchet: the list and the emission must agree exactly, so a
    // rename, a drop, or a silently-added metric fails here rather than in a customer's
    // dashboard six months later.
    let (engine, db) = harness::fresh_engine();
    let out = harness::fresh_dir("health-names");
    let vitals = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &paths(db.data_dir(), &out),
    )
    .expect("collect");

    let emitted: Vec<&str> = vitals.metrics.iter().map(|m| m.name).collect();
    let declared: Vec<&str> = METRIC_NAMES
        .iter()
        .copied()
        // produced by the renderer rather than the collector: the identity series,
        // the meaning-pair carrier, and the unmeasured count
        .filter(|n| {
            *n != "infertap_analyzer_metrics_unmeasured"
                && *n != "infertap_analyzer_info"
                && *n != "infertap_fleet_meaning_info"
        })
        .collect();
    assert_eq!(
        emitted, declared,
        "the emitted metric set and METRIC_NAMES have drifted — update both \
         deliberately, and never by renaming"
    );

    // and the rendering carries every declared name with its OWN kind. A `_total` series
    // typed `gauge` is a promtool lint failure and misleads anything reasoning about
    // resets, so the kind is asserted rather than assumed uniform.
    let text = vitals.to_prometheus();
    for m in &vitals.metrics {
        assert_eq!(
            text.contains(&format!("# TYPE {} {}", m.name, m.kind.as_str())),
            m.value.is_some(),
            "{}: a measured metric declares its own kind, an unmeasured one is absent",
            m.name
        );
    }
    for renderer_owned in [
        "infertap_analyzer_info",
        "infertap_analyzer_metrics_unmeasured",
    ] {
        assert!(
            text.contains(&format!("# TYPE {renderer_owned} gauge")),
            "{renderer_owned} is emitted by the renderer and must still be declared"
        );
    }
    // every `_total` in the contract is a counter, and nothing else is
    for m in &vitals.metrics {
        assert_eq!(
            m.kind == selfhealth::MetricKind::Counter,
            m.name.ends_with("_total"),
            "{}: the _total suffix and the counter kind must agree",
            m.name
        );
    }

    std::fs::remove_dir_all(out).ok();
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_loopless_process_says_nothing_about_a_loop_and_the_daemon_says_everything() {
    // Same discipline as `server_up`, for the same reason: a one-shot `report` and the
    // build harness run no cadence, so a zero pass count would read as "the loop is
    // wedged" on every harness run. Absent, not zero.
    let (engine, db) = harness::fresh_engine();
    let out = harness::fresh_dir("health-loop");

    let quiet = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &paths(db.data_dir(), &out),
    )
    .expect("collect without a loop");
    for name in [
        "infertap_analyzer_ticks_total",
        "infertap_analyzer_tick_failures_total",
        "infertap_analyzer_files_ingested_total",
        "infertap_analyzer_reclaimed_bytes_total",
        "infertap_analyzer_engine_queries_total",
        "infertap_analyzer_tick_duration_seconds",
        "infertap_analyzer_tick_lag_seconds",
    ] {
        assert!(
            quiet.unmeasured().contains(&name),
            "{name} must be absent in a process that runs no loop"
        );
    }
    assert!(
        !quiet.checks.iter().any(|c| c.name == "tick_cadence_kept"),
        "no loop, no cadence check"
    );

    let mut running = paths(db.data_dir(), &out);
    running.loop_status = Some(selfhealth::LoopStatus {
        ticks_total: 9,
        tick_failures_total: 1,
        files_ingested_total: 412,
        bytes_ingested_total: 1_048_576,
        records_ingested_total: 52_000,
        ingest_backlog_events_estimate: None,
        files_projected_total: 0,
        reclaimed_bytes_total: 8_192,
        engine_queries_total: 77,
        last_tick_duration_seconds: Some(31.5),
        tick_lag_seconds: Some(120.0),
        tick_interval_seconds: 3_600.0,
        effective_retention_days: 14.0,
        ingest_rows_per_second: None,
        ingest_duty_cycle: None,
        ingest_duty_samples: 0,
        ingest_catchup_cycles: 0,
        ingest_backlog_files: 0,
        ingest_backlog_bytes: 0,
        ingest_refused_total: 0,
        ingest_refusing: false,
        audit_blocks_total: 0,
        audit_disagreed_at_ms: None,
        evict_audited_total: 0,
        evict_mismatched_total: 0,
        evict_underresolved_total: 0,
    });
    let vitals = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &running,
    )
    .expect("collect with a loop");
    assert_eq!(vitals.value("infertap_analyzer_ticks_total"), Some(9.0));
    assert_eq!(
        vitals.value("infertap_analyzer_tick_failures_total"),
        Some(1.0)
    );
    assert_eq!(
        vitals.value("infertap_analyzer_engine_queries_total"),
        Some(77.0)
    );
    assert_eq!(
        vitals.value("infertap_analyzer_tick_lag_seconds"),
        Some(120.0)
    );
    let cadence = vitals
        .checks
        .iter()
        .find(|c| c.name == "tick_cadence_kept")
        .expect("the cadence check exists once a loop is running");
    assert_eq!(
        cadence.status,
        Status::Ok,
        "two minutes of lag against an hourly cadence is keeping up"
    );

    std::fs::remove_dir_all(out).ok();
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_refusal_reaches_the_export_and_a_lost_source_file_is_critical() {
    // A refusal is a first-class outcome, surfaced, never silent: stderr in a journal nobody
    // tails is not a surface — these three series are.
    let (engine, db) = harness::fresh_engine();
    let out = harness::fresh_dir("health-refuse");

    let mut refusing = paths(db.data_dir(), &out);
    refusing.loop_status = Some(selfhealth::LoopStatus {
        ticks_total: 4,
        tick_failures_total: 0,
        files_ingested_total: 100,
        bytes_ingested_total: 0,
        records_ingested_total: 0,
        ingest_backlog_events_estimate: None,
        files_projected_total: 0,
        reclaimed_bytes_total: 0,
        engine_queries_total: 40,
        last_tick_duration_seconds: Some(2.0),
        tick_lag_seconds: Some(30.0),
        tick_interval_seconds: 3_600.0,
        effective_retention_days: 5.0,
        ingest_rows_per_second: None,
        ingest_duty_cycle: None,
        ingest_duty_samples: 0,
        ingest_catchup_cycles: 0,
        ingest_backlog_files: 0,
        ingest_backlog_bytes: 0,
        ingest_refused_total: 7,
        ingest_refusing: true,
        audit_blocks_total: 0,
        audit_disagreed_at_ms: None,
        evict_audited_total: 0,
        evict_mismatched_total: 0,
        evict_underresolved_total: 0,
    });
    let vitals = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &refusing,
    )
    .expect("collect");
    assert_eq!(
        vitals.value("infertap_analyzer_ingest_refused_total"),
        Some(7.0),
        "how often it has happened"
    );
    assert_eq!(
        vitals.value("infertap_analyzer_ingest_refusing"),
        Some(1.0),
        "and whether it is happening NOW — an alarm needs the second, a rate the first"
    );
    // the horizon discovery holds is exported as the loop reports it
    assert_eq!(
        vitals.value("infertap_analyzer_effective_retention_days"),
        Some(5.0)
    );

    std::fs::remove_dir_all(out).ok();
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn an_unreachable_engine_still_emits_a_sample_rather_than_leaving_a_stale_file() {
    // the requirement, and the failure it prevents: if a dead engine meant no health
    // file was written, the artifact a customer scrapes would simply stop changing — and
    // a stale file reads as a healthy one until somebody checks the timestamp. In an
    // airgapped estate nobody does.
    let (engine, db) = harness::fresh_engine();
    let out = harness::fresh_dir("health-unreachable");

    let mut down = paths(db.data_dir(), &out);
    down.server = Some(selfhealth::ServerStatus {
        up: false,
        uptime_seconds: None,
    });
    let vitals = selfhealth::unreachable("fleet-x", &down, "connection refused");

    // the export surface is IDENTICAL in shape — same names, same order — so a scraper
    // sees absent series rather than a different document
    let live = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &paths(db.data_dir(), &out),
    )
    .expect("a live sample to compare against");
    let names =
        |v: &selfhealth::SelfHealth| -> Vec<&str> { v.metrics.iter().map(|m| m.name).collect() };
    assert_eq!(
        names(&vitals),
        names(&live),
        "an unreachable sample emits the same contract, with values absent"
    );

    // engine-derived figures are absent, not zero
    for name in [
        "infertap_analyzer_active_part_bytes",
        "infertap_analyzer_unreclaimed_bytes",
        "infertap_analyzer_disk_free_bytes",
        "infertap_analyzer_ledger_rows",
        "infertap_analyzer_max_parts_per_partition",
    ] {
        assert!(
            vitals.unmeasured().contains(&name),
            "{name} must be absent when the engine did not answer, never zero"
        );
    }
    assert!(
        vitals
            .value("infertap_analyzer_data_dir_bytes")
            .is_some_and(|v| v > 0.0),
        "the filesystem is still readable with the engine down"
    );

    // and the failure is stated, carrying the engine's own words
    let reachable = vitals
        .checks
        .iter()
        .find(|c| c.name == "engine_reachable")
        .expect("the unreachable sample names its failure");
    assert_eq!(reachable.status, Status::Critical);
    assert!(
        reachable.message.contains("connection refused"),
        "the engine's own error text reaches the operator: {}",
        reachable.message
    );
    assert_eq!(vitals.status(), Status::Critical);
    // the engine-dependent checks are omitted rather than reported Unknown — six
    // degraded-looking lines saying the same thing would bury the one honest one
    for absent in [
        "dead_parts_reclaimed",
        "disk_headroom",
        "parts_per_partition",
    ] {
        assert!(
            !vitals.checks.iter().any(|c| c.name == absent),
            "{absent} cannot be evaluated without the engine and must not pretend to be"
        );
    }

    std::fs::remove_dir_all(out).ok();
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn a_dead_owned_server_is_critical_and_owning_none_is_silent() {
    // The server posture's failure mode, stated as a test: a batch invocation that fails announces
    // itself to whoever ran it, while a daemon whose engine died stays up, keeps
    // answering `health`, and quietly stops ingesting. In an airgapped estate nobody
    // sees that until a report is months stale, so the sensor carries it — and carries
    // the difference between "owns a dead server" (0, critical) and "owns no server"
    // (unmeasured, no check at all).
    let (engine, db) = harness::fresh_engine();
    let out = harness::fresh_dir("health-server");

    let mut up = paths(db.data_dir(), &out);
    up.server = Some(selfhealth::ServerStatus {
        up: true,
        uptime_seconds: Some(7200.0),
    });
    let vitals = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &up,
    )
    .expect("collect");
    assert_eq!(vitals.value("infertap_analyzer_server_up"), Some(1.0));
    assert_eq!(
        vitals.value("infertap_analyzer_server_uptime_seconds"),
        Some(7200.0)
    );
    let alive = vitals
        .checks
        .iter()
        .find(|c| c.name == "server_alive")
        .expect("the check exists when a server is owned");
    assert_eq!(alive.status, Status::Ok);

    let mut down = paths(db.data_dir(), &out);
    down.server = Some(selfhealth::ServerStatus {
        up: false,
        uptime_seconds: None,
    });
    let vitals = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &down,
    )
    .expect("collect");
    assert_eq!(
        vitals.value("infertap_analyzer_server_up"),
        Some(0.0),
        "owned and dead is ZERO, never absent — absence would read as 'not applicable'"
    );
    assert!(vitals
        .unmeasured()
        .contains(&"infertap_analyzer_server_uptime_seconds"));
    assert_eq!(
        vitals
            .checks
            .iter()
            .find(|c| c.name == "server_alive")
            .expect("check")
            .status,
        Status::Critical
    );
    assert_eq!(
        vitals.status(),
        Status::Critical,
        "a dead engine makes the whole sample critical, so the health exit code carries it"
    );

    let vitals = selfhealth::collect(
        &engine,
        harness::lake_if_bound(&engine, "fleet-x").as_ref(),
        "fleet-x",
        &paths(db.data_dir(), &out),
    )
    .expect("collect");
    assert!(vitals.unmeasured().contains(&"infertap_analyzer_server_up"));
    assert!(
        !vitals.checks.iter().any(|c| c.name == "server_alive"),
        "no observation, no check: a process owning no server says nothing about one"
    );

    std::fs::remove_dir_all(out).ok();
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn one_sample_costs_exactly_one_engine_query() {
    // The maintenance loop samples on a cadence forever, so a per-metric round trip would make
    // the health check the next thing that degrades with time. The unit is a QUERY, read from
    // `Engine::queries_executed` — the counter the daemon exports.
    let (engine, db) = harness::fresh_engine();
    let dir = db.data_dir().to_path_buf();
    let out = harness::fresh_dir("health-queries");
    let lake = harness::lake_if_bound(&engine, "fleet-x");
    let before = engine.queries_executed();
    selfhealth::collect(&engine, lake.as_ref(), "fleet-x", &paths(&dir, &out)).expect("collect");
    assert_eq!(
        engine.queries_executed() - before,
        1,
        "every engine figure arrives in one round trip"
    );
    std::fs::remove_dir_all(out).ok();
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
#[allow(clippy::too_many_lines)] // one narrative: two ticks, two counts, one sample
fn the_sensor_sees_replacement_hold_and_reclamation_happen_over_repeated_ticks() {
    // Two properties of a living engine, read from the sensor a customer's alarm binds to
    // rather than from a shell: `ReplacingMergeTree` collapses only at merge, so a restated
    // key holds two rows until one happens, and dead parts stay on disk until the cleanup
    // task runs. `server.rs` closes both at the MECHANISM level against a synthetic table;
    // this closes them over real ticks.
    //
    // **Its own server, because the sensor measures ENGINE-WIDE state** — `system.parts`
    // and the data directory are not scoped to a database, so a shared server would have
    // this test reading other tests' bytes.
    //
    // **Its own Engine for sampling, which is the constraint made concrete**: an
    // Engine is one connection and one current-thread runtime, so a sampler sharing the
    // ticks' Engine would queue behind a tick rather than observe it. The loop inherits
    // exactly this.
    let dir = harness::fresh_dir("sensor-close");
    let mut config = analyzer_serving_clickhouse::engine::server::ServerConfig::new(&dir);
    let base = analyzer_serving_clickhouse::engine::testing::port_block(2);
    config.http_port = base;
    config.tcp_port = base + 1;
    let handle = analyzer_serving_clickhouse::engine::server::ServerHandle::start(
        analyzer_serving_clickhouse::engine::testing::engine_bin(),
        &config,
    )
    .expect("the sensor's own server");

    let tick_engine =
        analyzer_serving_clickhouse::engine::Engine::for_handle(&handle).expect("tick engine");
    let sensor_engine =
        analyzer_serving_clickhouse::engine::Engine::for_handle(&handle).expect("sensor engine");
    analyzer_serving_clickhouse::engine::migrate(
        &tick_engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig::default(),
    )
    .expect("migrate");

    // A window of real store facts, then a tick over it; then a second wave with one more
    // fresh block in every holder-hour, so the second tick restates every hourly row.
    let base = harness::rebase_offset();
    // Each wave is one tap segment of the run: its open, a heartbeat attesting the four
    // sources at each end, and its stores between them.
    let heartbeat = |at: f64| {
        let endpoints: Vec<_> = (0..4)
            .map(|i| serde_json::json!({"source": format!("i{i}"), "endpoint": format!("e{i}"), "msgs_seen": 1, "dropped": 0}))
            .collect();
        serde_json::json!({"kind": "heartbeat", "incarnation": "run-a", "at_ms": at,
            "msgs_seen": 4, "dropped": 0, "oversized": 0, "endpoints": endpoints})
    };
    let wave = |seq: u32, shift: f64| -> Vec<u8> {
        let mut lines = vec![
            serde_json::json!({
                "kind": "segment_open", "at_ms": base + shift, "contract_version": "1.2",
                "incarnation": "run-a", "segment_seq": seq,
            }),
            heartbeat(base + shift),
        ];
        lines.extend((0..400).map(|k| {
            serde_json::json!({
                "kind": "store",
                "instance_id": format!("i{}", k % 4),
                "block_id": format!("p{k}-{seq}"),
                "n_tokens": 8,
                "at_ms": base + f64::from(k) * 1000.0 + shift,
            })
        }));
        lines.push(heartbeat(base + 400_000.0 + shift));
        lines
            .iter()
            .map(|l| l.to_string() + "\n")
            .collect::<String>()
            .into_bytes()
    };
    let root = harness::fresh_dir("sensor-segments");
    let source = harness::source_over_tree("sensor", root.clone(), base);
    let ship = |seq: u32, shift: f64| {
        let bytes = wave(seq, shift);
        let name = format!("seg-{seq}.jsonl");
        harness::plant_dated(&root, &harness::dated_hour_of(base), &name, &bytes);
        harness::ingest_bytes(&tick_engine, "sensor", &name, &bytes).expect("ingest");
    };

    // TWO counts, because the finding lives in the gap between them: `FINAL` is what the
    // analysis reads (replacement semantics, deduplicated at read time), the bare count is
    // what the STORAGE holds. the measured 2x was exactly this gap left open.
    let count = |sql: &str, label: &str| -> u64 {
        tick_engine
            .run(sql)
            .unwrap_or_else(|e| panic!("{label}: {e}"))
            .trim()
            .parse()
            .expect("a count")
    };
    let logical = |label: &str| {
        count(
            "SELECT count() FROM class_rollup_hourly FINAL WHERE fleet_id = 'sensor'",
            label,
        )
    };
    let physical = |label: &str| {
        count(
            "SELECT count() FROM class_rollup_hourly WHERE fleet_id = 'sensor'",
            label,
        )
    };
    let events = |label: &str| {
        count(
            "SELECT sum(events) FROM class_rollup_hourly FINAL WHERE fleet_id = 'sensor'",
            label,
        )
    };
    let out = harness::fresh_dir("sensor-close-out");
    let sample = || {
        selfhealth::collect(
            &sensor_engine,
            harness::lake_if_bound(&sensor_engine, "sensor").as_ref(),
            "sensor",
            &Paths {
                data_dir: &dir,
                out_dir: &out,
                ledger_retention_days: 30,
                analyzer_id: None,
                server: None,
                loop_status: None,
                fleet: None,
                runs: None,
            },
        )
        .expect("sample")
    };

    // Everything below is about what the TICKS leave behind, so the database's pre-existing
    // dead parts (the migration ledger merges its own once there are enough) are subtracted out.
    let baseline_unreclaimed = sample()
        .value("infertap_analyzer_unreclaimed_bytes")
        .expect("unreclaimed measured before any tick");

    // Merges held while the ticks write, so the pre-merge duplication below is the ticks' own
    // and not the engine's timing.
    tick_engine
        .run("SYSTEM STOP MERGES class_rollup_hourly")
        .expect("merges held");
    ship(0, 0.0);
    harness::executor(&tick_engine, "sensor", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick 1");
    let first_logical = logical("after the first tick");
    let first_events = events("after the first tick");
    assert!(
        first_logical > 0,
        "the tick classified something to measure"
    );
    assert_eq!(
        physical("after the first tick"),
        first_logical,
        "one tick states each row once"
    );

    ship(1, 500.0);
    harness::executor(&tick_engine, "sensor", &Params::default())
        .and_then(|executor| executor.advance(&source))
        .expect("tick 2");

    // FINDING ONE, at the pipeline level, in both of its halves.
    //
    // What the ANALYSIS sees is stable: the second tick restates every row, so the
    // deduplicated count does not move and each row carries both waves. Every figure a
    // reader sees reads `FINAL`, so no number a customer sees is ever wrong.
    assert_eq!(
        logical("after the second tick"),
        first_logical,
        "a restating tick must replace its rows, never add a copy"
    );
    // The newest store waits at the live edge on each tick, so the second tick classifies
    // the first wave's newest and all but the second wave's: 400 more.
    assert_eq!(
        events("after the second tick"),
        first_events + 400,
        "the restated rows carry both waves"
    );
    // What the STORAGE holds is not stable, and that is the finding rather than a bug:
    // the replacing merge retires the older version, so until one happens the table carries
    // the first statement and the restatement — two rows per key, a gap only a living
    // process closes.
    let duplicated = physical("after the second tick");
    assert_eq!(
        duplicated,
        first_logical * 2,
        "the pre-merge duplication is real and measured, not assumed"
    );

    // The second property, read from the sensor rather than from a shell, in the order the
    // engine produces it.
    //
    // Before any merge there are no dead parts, so the honest reading is ZERO: two ticks
    // wrote two live parts and nothing has been superseded on disk yet. The dead parts
    // appear when the merge collapses them, and THAT is the moment a sensor must not be
    // blind to. Under the old definition this figure was ~27 KB here and ~8.4 MB after,
    // dominated by the engine's own logs — it moved for reasons that had nothing to do
    // with reclamation, which is worse than not reporting at all.
    //
    // Measured as a DELTA from before the ticks, not as an absolute. The absolute belongs to
    // the whole database, and the migration ledger merges its own parts once enough migrations
    // exist, which has nothing to do with what this test
    // is about. An absolute assertion here fails on the next schema change and names the wrong
    // cause; the delta says exactly what is claimed: the TICKS have reclaimed nothing yet.
    let unreclaimed = |when: &str| {
        sample()
            .value("infertap_analyzer_unreclaimed_bytes")
            .unwrap_or_else(|| panic!("unreclaimed measured {when}"))
    };
    // Only the held table is held: every other table a tick writes merges as usual, and a
    // merge there leaves kilobytes of superseded parts the sensor sees. Bounded well under a
    // part's worth of data; the claim is the scale. Under a concurrent build the engine's
    // merges lag and the residue read 1.48 and 1.49 MB on two green trees.
    let after_second = unreclaimed("after the second tick");
    assert!(
        after_second - baseline_unreclaimed < 2_000_000.0,
        "residue is merge-scale, not data-scale \
         (baseline {baseline_unreclaimed}, now {after_second})"
    );

    tick_engine
        .run("SYSTEM START MERGES class_rollup_hourly")
        .expect("merges released");
    // The restatement's older-version delete was submitted under the held merges and is still
    // pending here; a part with a pending mutation is not a merge candidate, so the OPTIMIZE
    // below is a no-op until the mutation lands. A living process lets it land first.
    for _ in 0..600 {
        let pending = tick_engine
            .run(
                "SELECT count() FROM system.mutations \
                 WHERE database = currentDatabase() AND table = 'class_rollup_hourly' \
                   AND is_done = 0",
            )
            .expect("pending mutations");
        if pending.trim() == "0" {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    tick_engine
        .run("OPTIMIZE TABLE class_rollup_hourly FINAL")
        .expect("optimize");
    assert_eq!(
        physical("after the merge"),
        first_logical,
        "the duplication collapses under a living process — the storage now holds what \
         the analysis always read"
    );

    let dead = sample()
        .value("infertap_analyzer_unreclaimed_bytes")
        .expect("measured");
    assert!(
        dead > 0.0,
        "the merge left superseded parts on disk and the sensor did not see them — the \
         metric a customer's alarm binds to would be blind to exactly what it is for"
    );

    // The return to zero is NOT asserted here, and the reason is a decision rather than a
    // gap: reclamation of superseded parts is bounded by `old_parts_lifetime` (the
    // engine's default is eight minutes), and shortening it for the rollups is
    // the flagged `min_age_to_force_merge_seconds` question, which this test is not
    // allowed to pre-empt. the mechanism pin already proved the fall reaches zero on a
    // table that sets it.

    // and the sampler's own connection did the sampling, not the ticks'
    assert!(
        sensor_engine.queries_executed() > 0 && tick_engine.queries_executed() > 0,
        "two engines, two connections — the constraint the daemon's loop inherits"
    );

    handle.shutdown().expect("ordered shutdown");
    std::fs::remove_dir_all(dir).ok();
    std::fs::remove_dir_all(out).ok();
}
