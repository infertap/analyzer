// SPDX-License-Identifier: AGPL-3.0-only

//! The self-health tests.

use super::{
    check_audit, check_disk, check_evict_resolution, check_parts, check_retention, check_tick_lag,
    push_opt, Check, FleetFigures, LoopStatus, Metric, MetricKind, SelfHealth, Status,
    DISK_FREE_FRACTION_CRITICAL, METRIC_NAMES, PARTS_PER_PARTITION_CRITICAL,
    PARTS_PER_PARTITION_WARN,
};

fn sample() -> SelfHealth {
    SelfHealth {
        analyzer_id: None,
        fleet: "f".to_string(),
        meaning: None,
        metrics: vec![Metric {
            name: "infertap_analyzer_dead_part_ratio",
            help: "h",
            value: Some(3.75),
            kind: MetricKind::Gauge,
        }],
        checks: vec![
            Check {
                name: "a",
                status: Status::Ok,
                message: "fine".into(),
            },
            Check {
                name: "b",
                status: Status::Warn,
                message: "not fine".into(),
            },
        ],
    }
}

#[test]
fn status_is_worst_wins_and_unknown_is_never_ok() {
    assert_eq!(sample().status(), Status::Warn);
    assert!(Status::Unknown > Status::Ok, "unmeasured never reads as ok");
    assert!(Status::Critical > Status::Warn);
}

#[test]
fn thresholds_come_from_the_engines_own_verified_limits() {
    // not taste: 1000 = parts_to_delay_insert, 3000 = parts_to_throw_insert
    assert_eq!(check_parts(999.0).status, Status::Ok);
    assert_eq!(check_parts(PARTS_PER_PARTITION_WARN).status, Status::Warn);
    assert_eq!(
        check_parts(PARTS_PER_PARTITION_CRITICAL).status,
        Status::Critical
    );
    assert_eq!(check_disk(0.5, 1.0).status, Status::Ok);
    assert_eq!(
        check_disk(DISK_FREE_FRACTION_CRITICAL, 1.0).status,
        Status::Critical
    );
    // a fleet with no residency deltas cannot prove TTL enforcement either way
    assert_eq!(check_retention(None, 30.0).status, Status::Unknown);
    assert_eq!(check_retention(Some(31.0), 30.0).status, Status::Ok);
    assert_eq!(check_retention(Some(60.0), 30.0).status, Status::Warn);
}

#[test]
fn a_disagreement_is_critical_until_derived_afresh_and_no_sample_is_unknown() {
    let base = LoopStatus {
        ticks_total: 1,
        tick_failures_total: 0,
        files_ingested_total: 0,
        bytes_ingested_total: 0,
        records_ingested_total: 0,
        ingest_backlog_events_estimate: None,
        files_projected_total: 0,
        reclaimed_bytes_total: 0,
        engine_queries_total: 0,
        last_tick_duration_seconds: None,
        tick_lag_seconds: None,
        tick_interval_seconds: 60.0,
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
    };
    assert_eq!(check_audit(base).status, Status::Unknown);
    let sampled = LoopStatus {
        audit_blocks_total: 12,
        ..base
    };
    assert_eq!(check_audit(sampled).status, Status::Ok);
    // A restarted process has sampled nothing yet, and the held edge still speaks.
    let held = LoopStatus {
        audit_disagreed_at_ms: Some(1_785_000_000_000.0),
        ..base
    };
    assert_eq!(check_audit(held).status, Status::Critical);
    assert_eq!(check_evict_resolution(base).status, Status::Unknown);
    let graded = LoopStatus {
        evict_audited_total: 40,
        ..base
    };
    assert_eq!(check_evict_resolution(graded).status, Status::Ok);
    let lost = LoopStatus {
        evict_underresolved_total: 1,
        ..graded
    };
    assert_eq!(check_evict_resolution(lost).status, Status::Warn);
    let wrong = LoopStatus {
        evict_mismatched_total: 1,
        ..lost
    };
    assert_eq!(check_evict_resolution(wrong).status, Status::Critical);
}

#[test]
fn tick_lag_is_judged_against_the_declared_cadence_not_an_absolute() {
    // An absolute threshold would alarm on a fleet that ingests hourly
    // and stay silent on one that ingests every minute. The cadence is a deployment input,
    // so it is the denominator.
    let at = |lag: Option<f64>, interval: f64| {
        check_tick_lag(LoopStatus {
            ticks_total: 1,
            tick_failures_total: 0,
            files_ingested_total: 0,
            bytes_ingested_total: 0,
            records_ingested_total: 0,
            ingest_backlog_events_estimate: None,
            files_projected_total: 0,
            reclaimed_bytes_total: 0,
            engine_queries_total: 0,
            last_tick_duration_seconds: Some(1.0),
            tick_lag_seconds: lag,
            tick_interval_seconds: interval,
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
        })
        .status
    };
    // hourly cadence: an hour of lag is one cycle, still inside the warn line
    assert_eq!(at(Some(3_600.0), 3_600.0), Status::Ok);
    // past two cadences the miss is a trend, not a slow run
    assert_eq!(at(Some(7_201.0), 3_600.0), Status::Warn);
    // the SAME lag against a weekly cadence is nothing at all — the point of the rule
    assert_eq!(at(Some(7_201.0), 604_800.0), Status::Ok);
    // and a loop that has not completed a tick says so rather than reporting ok
    assert_eq!(at(None, 3_600.0), Status::Unknown);
}

#[test]
fn counters_render_as_counters_and_gauges_as_gauges() {
    // The `# TYPE` line is part of the contract: a `_total` declared `gauge` is a
    // promtool lint failure and misleads anything reasoning about resets.
    let mut metrics = Vec::new();
    super::push(&mut metrics, "infertap_analyzer_a_gauge", "h", 1.0);
    super::push_counter(
        &mut metrics,
        "infertap_analyzer_a_total",
        "h",
        Some(super::counter_value(5)),
    );
    let text = SelfHealth {
        analyzer_id: None,
        fleet: "f".to_string(),
        meaning: None,
        metrics,
        checks: vec![],
    }
    .to_prometheus();
    assert!(
        text.contains("# TYPE infertap_analyzer_a_gauge gauge"),
        "{text}"
    );
    assert!(
        text.contains("# TYPE infertap_analyzer_a_total counter"),
        "{text}"
    );
}

#[test]
fn the_prometheus_rendering_carries_help_type_and_check_gauges() {
    let text = sample().to_prometheus();
    assert!(text.contains("# HELP infertap_analyzer_dead_part_ratio h"));
    assert!(text.contains("# TYPE infertap_analyzer_dead_part_ratio gauge"));
    assert!(text.contains("infertap_analyzer_dead_part_ratio 3.75"));
    assert!(text.contains("infertap_analyzer_check_status{check=\"b\"} 2"));
}

#[test]
fn the_exposition_format_is_conformed_to_not_approximated() {
    // Checked against the Prometheus text format 0.0.4 spec, not from memory: the
    // three rules a hand-written exporter gets wrong, each of which fails silently
    // in a customer environment we cannot see.
    let vitals = SelfHealth {
        analyzer_id: None,
        fleet: "f".to_string(),
        meaning: None,
        metrics: vec![
            Metric {
                name: "infertap_analyzer_a",
                help: "back\\slash and\nnewline",
                value: Some(2.5),
                kind: MetricKind::Gauge,
            },
            Metric {
                name: "infertap_analyzer_b",
                help: "plain",
                value: Some(f64::INFINITY),
                kind: MetricKind::Gauge,
            },
        ],
        checks: vec![],
    };
    let text = vitals.to_prometheus();
    // 1. help text escapes backslashes and newlines
    assert!(
        text.contains("# HELP infertap_analyzer_a back\\\\slash and\\nnewline"),
        "{text}"
    );
    // 2. a non-finite that reached the renderer still cannot print Rust's `inf`
    assert!(
        !text
            .lines()
            .any(|l| l.ends_with(" inf") || l.ends_with(" -inf") || l.ends_with(" NaN ")),
        "Rust's inf fails every scraper: {text}"
    );
    assert!(text.contains("infertap_analyzer_b +Inf"), "{text}");
    // 3. the last line ends with a line feed
    assert!(
        text.ends_with('\n'),
        "the format requires a trailing newline"
    );
}

#[test]
fn a_non_finite_value_can_never_reach_either_rendering() {
    // The silent-corruption pair this type exists to prevent: serde_json turns NaN
    // and both infinities into `null` and returns Ok, while the Prometheus format
    // rejects Rust's `inf` outright. One bad division would otherwise put a hole in
    // the only telemetry an airgapped deployment has.
    let mut metrics = Vec::new();
    super::push(&mut metrics, "infertap_analyzer_nan", "h", f64::NAN);
    super::push(&mut metrics, "infertap_analyzer_inf", "h", f64::INFINITY);
    super::push(&mut metrics, "infertap_analyzer_ok", "h", 7.0);
    let vitals = SelfHealth {
        analyzer_id: None,
        fleet: "f".to_string(),
        meaning: None,
        metrics,
        checks: vec![],
    };
    assert_eq!(
        vitals.unmeasured(),
        vec!["infertap_analyzer_nan", "infertap_analyzer_inf"],
        "non-finite becomes explicitly unmeasured, never a number"
    );
    let text = vitals.to_prometheus();
    assert!(!text.contains("infertap_analyzer_nan"), "{text}");
    assert!(
        text.contains("infertap_analyzer_metrics_unmeasured 2"),
        "the gap is itself alarmable: {text}"
    );
    let json = vitals.to_json_string().expect("json");
    assert!(json.contains("\"infertap_analyzer_nan\": null"), "{json}");
    assert!(json.contains("\"unmeasured\""), "{json}");
    // the strong form: re-parse and require every exported value to be null or a
    // finite number — the property, not a substring search
    let doc: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    for (name, value) in doc["metrics"].as_object().expect("metrics object") {
        assert!(
            value.is_null() || value.as_f64().is_some_and(f64::is_finite),
            "{name} exported a value JSON cannot carry: {value}"
        );
    }
}

#[test]
fn fleet_figures_render_as_gauges_and_the_meaning_rides_the_info_series() {
    // The phase-2b surface: figures present render as bare gauges with the meaning
    // pair on `infertap_fleet_meaning_info` (the `_info` join pattern — the figure
    // series themselves carry no labels, by this module's own law), and figures
    // absent render as ABSENT series, never zeros wearing a measurement's clothes.
    let figures = FleetFigures {
        semantics_version: "v3",
        discriminator: "abc\"123".to_string(),
        recompute_tokens: 4_182_996.0,
        stored_tokens: 1_393_904_247.0,
        coverage_fraction: Some(0.941),
        capture_fraction: Some(1.0),
        segments_missing_unexplained: 0.0,
        frontier_end_seconds: 1_787_000_000.0,
    };
    let mut metrics = Vec::new();
    push_opt(
        &mut metrics,
        "infertap_fleet_recompute_fraction",
        "test",
        (figures.stored_tokens > 0.0).then(|| figures.recompute_tokens / figures.stored_tokens),
    );
    let vitals = SelfHealth {
        analyzer_id: None,
        fleet: "f".to_string(),
        meaning: Some((
            figures.semantics_version.to_string(),
            figures.discriminator.clone(),
        )),
        metrics,
        checks: Vec::new(),
    };
    let text = vitals.to_prometheus();
    assert!(
        text.contains(
            "infertap_fleet_meaning_info{semantics_version=\"v3\",discriminator=\"abc\\\"123\"} 1"
        ),
        "the meaning pair rides the _info series, labels escaped: {text}"
    );
    assert!(text.contains("infertap_fleet_recompute_fraction 0.003"));

    // And a sample with no figures asserts no meaning at all.
    let bare = SelfHealth {
        analyzer_id: None,
        fleet: "f".to_string(),
        meaning: None,
        metrics: Vec::new(),
        checks: Vec::new(),
    };
    assert!(!bare.to_prometheus().contains("infertap_fleet_meaning_info"));
}

#[test]
fn metric_names_are_unique_and_prefixed() {
    let mut seen = std::collections::BTreeSet::new();
    for name in METRIC_NAMES {
        // Two subsystems, one namespace: `analyzer` describes this process,
        // `fleet` carries the stamped figures the console's document states.
        // A third prefix is a review, not a typo to wave through.
        assert!(
            name.starts_with("infertap_analyzer_") || name.starts_with("infertap_fleet_"),
            "{name} breaks the export prefix"
        );
        assert!(seen.insert(*name), "{name} is listed twice");
    }
}

#[test]
fn a_sample_states_when_it_was_taken_even_when_the_engine_did_not_answer() {
    // **The failure this closes: a dead or wedged SAMPLER.** A dead engine is already
    // legible — its series go absent and a check carries the reason. A dead sampler is
    // not: every value holds its last reading, the scrape endpoint keeps answering,
    // and the export looks exactly like a healthy system that happens to be idle.
    // Nothing in the surface could distinguish them, because every other series
    // describes the system rather than the sample.
    //
    // The unreachable path is the sharp case and the reason the stamp is taken from the
    // HOST clock: engine down, emitter alive is precisely when an operator must be able
    // to tell that somebody is still watching.
    // An empty directory of its own, never the system temp root: the sample walks
    // its data directory, and the root of a build machine's temp is minutes of walk
    // under load — the one flake this pin ever had.
    let dir = std::env::temp_dir().join(format!(
        "infertap-selfhealth-unreachable-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let paths = super::Paths {
        data_dir: &dir,
        out_dir: &dir,
        ledger_retention_days: 30,
        analyzer_id: None,
        server: None,
        loop_status: None,
        fleet: None,
        runs: None,
    };
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("host clock is after the epoch")
        .as_secs_f64();
    let vitals = super::unreachable("fleet-x", &paths, "the engine did not answer");
    let stamp = vitals
        .value("infertap_analyzer_sample_timestamp_seconds")
        .expect("a sample always states when it was taken, engine or no engine");
    assert!(
        stamp >= before && stamp <= before + 60.0,
        "the stamp is this sample's own wall time, not a stored or engine-derived one"
    );
    // and it survives rendering, because the artifact is what a scraper actually reads
    assert!(
        vitals
            .to_prometheus()
            .contains("infertap_analyzer_sample_timestamp_seconds "),
        "the stamp must reach the exposition format, not only the struct"
    );
}
