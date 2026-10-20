// SPDX-License-Identifier: AGPL-3.0-only

//! The read methods against the pinned engine, over data a tick actually wrote.
//!
//! Not a mock: a tick runs, writes the rollups and the cells, and the DAO
//! reads them back. A DAO tested against hand-inserted rows proves only that its SQL parses —
//! what matters is that it reads what the WRITE path emits, which is where a column rename or
//! a key change actually bites.

// Token counts are integers that ride as f64 through the export contract, so exact comparison
// is the correct assertion here — an epsilon would hide a real off-by-one in a mass figure.
#![allow(clippy::float_cmp)]

use harness::journal_faults;

use analyzer_analysis::{Params, Window};
use analyzer_pipeline::parquet_source::{SegmentTransport, SourceConfig};
use analyzer_serving_clickhouse::{Dao, Dim, Grain, MinuteTier, Scope};
use serde_json::Value;

fn fixture() -> (
    analyzer_serving_clickhouse::engine::Engine,
    analyzer_serving_clickhouse::engine::testing::TestDatabase,
) {
    analyzer_serving_clickhouse::engine::testing::database("dao_reads")
}

static SEGMENT_SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// A fresh segment tree and cache for one seeded fleet — the derivation source every read and
/// every pass in these tests opens, over the same records the SQL seeds write.
fn segment_sources(label: &str) -> SourceConfig {
    let n = SEGMENT_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let scratch =
        std::env::temp_dir().join(format!("dao-reads-{label}-{}-{n}", std::process::id()));
    SourceConfig {
        transport: SegmentTransport::Directory {
            root: scratch.join("segments"),
        },
        key_prefix: "tap/".to_string(),
        cache_dir: scratch.join("cache"),
        horizon_hours: analyzer_pipeline::parquet_source::HorizonHours::fixed(24 * 30),
        admit_corpus_forms: true,
        cache_budget_bytes: analyzer_pipeline::parquet_source::DEFAULT_SEGMENT_CACHE_BYTES,
    }
}
/// Ship `records` as one segment, as a tap and its shipper do: written into the source's tree
/// at the dated key its first record names, then ingested under that same key, so the input log
/// and the source agree on what each batch is.
fn ship_segment(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
    sources: &SourceConfig,
    records: &[Value],
) {
    let SegmentTransport::Directory { root } = &sources.transport else {
        panic!("the test source is a directory");
    };
    let min_at = records
        .iter()
        .filter_map(|r| r.get("at_ms").and_then(Value::as_f64))
        .fold(f64::INFINITY, f64::min);
    let n = SEGMENT_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let key = analyzer_lake::dated::segment_key("tap/", min_at, "n1", &format!("{n:04}.jsonl"))
        .expect("a finite stamp names a key");
    let mut body = String::new();
    for r in records {
        body.push_str(&r.to_string());
        body.push('\n');
    }
    let path = root.join(&key);
    std::fs::create_dir_all(path.parent().expect("a directory")).expect("segment tree");
    std::fs::write(&path, &body).expect("write segment");
    journal_faults::serving(engine)
        .bind(&analyzer_pipeline::execution::Namespace::new("dao-fixture-source", fleet).unwrap())
        .unwrap();
    let lake = journal_faults::spine(engine, fleet).lake();
    analyzer_ingest::ingest_bytes(&lake, fleet, &key, body.as_bytes()).expect("ingest");
    publish_inputs(engine, fleet);
}

/// Settle the day holding `day_ms`: one more segment of run `run` at sequence `seq`, carrying a
/// heartbeat and a store an hour into the next day, past the settle delay, then a tick. Every
/// edge passes the day's end, so the day settles and its markers are stated.
fn settle(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
    sources: &SourceConfig,
    (day_ms, run, seq): (f64, &str, i64),
) {
    let at = (day_ms / analyzer_analysis::DAY_MS).floor() * analyzer_analysis::DAY_MS
        + analyzer_analysis::DAY_MS
        + 3_600_000.0;
    ship_segment(
        engine,
        fleet,
        sources,
        &[
            // Its one heartbeat comes a day after the run's last, so the segment declares a
            // liveness bound past that gap (spec 5.8): the beat attests the run's silence since.
            serde_json::json!({"kind": "segment_open", "at_ms": at - 1000.0,
                "contract_version": "1.10", "incarnation": run, "segment_seq": seq,
                "heartbeat_secs": 60, "max_segment_secs": 90_000}),
            serde_json::json!({"kind": "heartbeat", "at_ms": at, "incarnation": run,
                "canary": "k1", "msgs_seen": 10, "dropped": 0, "oversized": 0,
                "heartbeat_secs": 60, "max_segment_secs": 300,
                "endpoints": [{"source": "i0", "endpoint": "tcp://e:1", "msgs_seen": 10,
                               "dropped": 0, "last_msg_at_ms": at}]}),
            serde_json::json!({"kind": "store", "instance_id": "i0", "dp_rank": 0,
                "group_idx": 0, "block_id": "settle", "content_id": "settle", "n_tokens": 1,
                "at_ms": at, "incarnation": run}),
        ],
    );
    journal_faults::executor(engine, fleet, &Params::default())
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("the settling tick");
}

/// A fleet bound and migrated with no tick: for a read pinned over rows the test writes itself.
fn bare(engine: &analyzer_serving_clickhouse::engine::Engine, fleet: &str) -> SourceConfig {
    analyzer_serving_clickhouse::engine::migrate(
        engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig::default(),
    )
    .expect("migrate");
    journal_faults::serving(engine)
        .bind(&analyzer_pipeline::execution::Namespace::new("dao-fixture-source", fleet).unwrap())
        .unwrap();
    segment_sources(fleet)
}

/// Publish the served input tables through everything the input log committed, as the daemon
/// does after each ingest cycle.
fn publish_inputs(engine: &analyzer_serving_clickhouse::engine::Engine, fleet: &str) {
    journal_faults::executor(engine, fleet, &Params::default())
        .and_then(|executor| executor.publish_inputs())
        .expect("publish the input tables");
}

/// The fixture seeds RECORDS and lets the write path derive the rows, never the rows
/// themselves: a hand-written derived row can assert a shape the write path cannot produce.
/// **`b1` sits on two holders at once, and a store of a block another holder is already
/// holding is `RE`, by the rule right above `Class::Etr`.** The eviction at `+2500` below is what
/// makes the second `b1` store honest.
#[allow(clippy::too_many_lines)] // one seed, both halves: the SQL rows and their segment twins
fn seed(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> (Window, SourceConfig) {
    analyzer_serving_clickhouse::engine::migrate(
        engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig::default(),
    )
    .expect("migrate");
    let at = analyzer_serving_clickhouse::engine::testing::recent_day_base_ms(); // day-aligned and inside the raw TTL — see recent_day_base
    let day_ms = 86_400_000.0;
    // **The priming pair sits half an hour back, and both halves of that are load-bearing.**
    // Inside the examined history (an hour), so each store below finds a prior close and reads
    // `ETR` rather than `cold`; on the day before the window, so it emits no rows of its own and
    // the day under test stays the only bucket.
    let half_hour = 1_800_000.0;
    let primed = at - half_hour;

    // The same records as segment twins, in the wire model: heartbeats and starts are
    // agent-scoped (the sources ride `endpoints`), cache rows carry their holder and class,
    // and the run-constant facts ride the segment's header.
    let sources = segment_sources(fleet);
    let wire_hb = |at: f64| {
        serde_json::json!({"kind": "heartbeat", "at_ms": at, "incarnation": "run-1",
        "canary": "k1", "msgs_seen": 10, "dropped": 0, "oversized": 0,
        "heartbeat_secs": 60, "max_segment_secs": 300,
        "endpoints": [
            {"source": "i0", "endpoint": "tcp://e:1", "msgs_seen": 10, "dropped": 0,
             "last_msg_at_ms": at},
            {"source": "i1", "endpoint": "tcp://e:1", "msgs_seen": 10, "dropped": 0,
             "last_msg_at_ms": at},
        ]})
    };
    let wire_cache =
        |kind: &str, inst: &str, dp: u32, block: &str, tokens: Option<i64>, at: f64| {
            let mut r = serde_json::json!({"kind": kind, "instance_id": inst, "dp_rank": dp,
            "group_idx": 0, "block_id": block, "content_id": block, "at_ms": at,
            "incarnation": "run-1", "workload_class": "agentic"});
            if let Some(n) = tokens {
                r["n_tokens"] = n.into();
            }
            r
        };
    let mut wire = vec![
        serde_json::json!({"kind": "segment_open", "at_ms": primed - 1000.0,
            "contract_version": "1.10", "incarnation": "run-1", "segment_seq": 0,
            "heartbeat_secs": 60, "max_segment_secs": 300, "key_epoch": 0,
            "workload_class": "agentic"}),
        serde_json::json!({"kind": "agent_start", "at_ms": at, "incarnation": "run-1",
            "canary": "k1", "heartbeat_secs": 60, "max_segment_secs": 300}),
        wire_hb(at),
        wire_hb(at + day_ms / 2.0),
    ];
    wire.extend([
        wire_cache("store", "i0", 0, "b1", Some(4), primed),
        wire_cache("store", "i0", 0, "b2", Some(4), primed),
        wire_cache("store", "i1", 1, "b1", Some(4), primed),
        wire_cache("evict", "i0", 0, "b1", None, primed + 1000.0),
        wire_cache("evict", "i0", 0, "b2", None, primed + 1000.0),
        wire_cache("evict", "i1", 1, "b1", None, primed + 1000.0),
        wire_cache("store", "i0", 0, "b1", Some(16), at + 1000.0),
        wire_cache("store", "i0", 0, "b2", Some(32), at + 2000.0),
        wire_cache("evict", "i0", 0, "b1", None, at + 2500.0),
        wire_cache("store", "i1", 1, "b1", Some(8), at + 3000.0),
    ]);
    ship_segment(engine, fleet, &sources, &wire);

    // **Starts ON the day boundary, not a second before it.** `run_pass` widens to whole days, so
    // a window opening at `at - 1000` led with an entire empty day — no stores, so no rows, so no
    // bucket. Harmless to the mass figures and NOT harmless to the marker, which is derived from
    // the buckets its generation covers: it lands on the first bucket that exists, while
    // `widen_to_whole_days` still reports the empty day as the start. The fixture was
    // manufacturing that gap.
    let w = Window {
        start_ms: at,
        end_ms: at + 3_600_000.0,
    };
    journal_faults::executor(engine, fleet, &Params::default())
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("tick");
    (w, sources)
}

/// A seed with real `records`, which the roster, coverage and residual shapes all read through.
///
/// The classified-store seed above writes derived rows directly and never touches `records`, so
/// it cannot exercise the shapes whose substrate is the record stream. Wall and engine time are
/// deliberately the same base here: this is a READ-layer test, and the clock-domain mapping it
/// would otherwise exercise belongs to `analysis`'s own suite.
#[allow(clippy::too_many_lines)] // one seed, both halves: the SQL rows and their segment twins
fn seed_records(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> (Window, SourceConfig) {
    analyzer_serving_clickhouse::engine::migrate(
        engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig::default(),
    )
    .expect("migrate");
    let day = analyzer_serving_clickhouse::engine::testing::recent_day_base_ms(); // a UTC midnight inside the raw TTL — see recent_day_base
                                                                                  // **`i0` is agentic and `i1` declares nothing**, which is what makes the workload reading
                                                                                  // assertable: agentic has exactly ONE holder, so its zero is `SingleHolder` rather than a
                                                                                  // measured absence of duplication. This was a recorded glob map; the class
                                                                                  // rides the record now, so it is a column.

    // The segment twins, in the wire model — lifecycle rows agent-scoped, the class on the
    // partition header.
    let sources = segment_sources(fleet);
    let wire_hb = |at: f64| {
        serde_json::json!({"kind": "heartbeat", "at_ms": at, "incarnation": "run-1",
            "canary": "k1", "msgs_seen": 10, "dropped": 0, "oversized": 0,
            "heartbeat_secs": 60, "max_segment_secs": 300,
            "endpoints": [{"source": "i0", "endpoint": "tcp://e:1", "msgs_seen": 10,
                           "dropped": 0, "last_msg_at_ms": at}]})
    };
    let wire_store = |inst: &str, block: &str, class: &str, at: f64| {
        let mut r = serde_json::json!({"kind": "store", "instance_id": inst,
            "block_id": block, "content_id": block, "n_tokens": 4, "at_ms": at,
            "incarnation": "run-1"});
        if !class.is_empty() {
            r["workload_class"] = class.into();
        }
        r
    };
    let stop = day + 3_600_000.0;
    let records = [
        serde_json::json!({"kind": "agent_start", "at_ms": day, "incarnation": "run-1",
            "canary": "k1", "heartbeat_secs": 60, "max_segment_secs": 300}),
        wire_hb(day),
        wire_hb(day + 1_800_000.0),
        wire_hb(day + 3_600_000.0),
        serde_json::json!({"kind": "agent_stop", "at_ms": stop, "incarnation": "run-1",
            "canary": "k1", "msgs_seen": 10, "dropped": 0, "oversized": 0}),
        wire_store("i0", "p", "agentic", day + 1000.0),
        wire_store("i1", "p", "", day + 2000.0),
        wire_store("i0", "q", "agentic", day + 3000.0),
    ];
    // Proper segments: the roster read is arrivals-backed, so the records are ingested as
    // production ingests them. `pod_identity` rides each header as a `--provenance`
    // declaration, the DECLARED half of section 5A's pod pair, which the roster serves beside
    // the slot the operator named. The class rides a header too, so i0's agentic stores travel
    // in their own segment. Neither header declares an epoch.
    let open = |seq: i64, class: Option<&str>| {
        let mut header = serde_json::json!({"kind": "segment_open", "at_ms": day - 1000.0,
            "contract_version": "1.10", "incarnation": "run-1", "segment_seq": seq,
            "heartbeat_secs": 60, "max_segment_secs": 300, "pod_identity": "pod-declared"});
        if let Some(class) = class {
            header["workload_class"] = class.into();
        }
        header
    };
    let (agentic, rest): (Vec<Value>, Vec<Value>) = records
        .into_iter()
        .partition(|r| r["workload_class"] == "agentic");
    ship_segment(
        engine,
        fleet,
        &sources,
        &[vec![open(0, None)], rest].concat(),
    );
    ship_segment(
        engine,
        fleet,
        &sources,
        &[vec![open(1, Some("agentic"))], agentic].concat(),
    );
    journal_faults::executor(engine, fleet, &Params::default())
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("tick");
    (
        Window {
            start_ms: day,
            end_ms: day + 86_400_000.0,
        },
        sources,
    )
}

/// A tick under `params`: a changed meaning starts the working state again from the log, its
/// generations carried, so the new generation covers every day the log holds.
fn tick_under(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    sources: &SourceConfig,
    fleet: &str,
    params: &Params,
) {
    journal_faults::executor(engine, fleet, params)
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("a tick under the changed parameters");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_roster_is_one_row_per_run_carrying_its_own_staleness_verdict() {
    // Shape 2. Keyed on incarnation, never instance: a holder restarted three times is three
    // rows, and grading them together is what makes a rolling deploy read as an outage.
    let (engine, _db) = fixture();
    let fleet = "roster";
    let (w, sources) = seed_records(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let roster = dao.roster(&w, None).expect("roster");
    // One producer RUN, fanned out over the instances it covered: i0 from the
    // heartbeat's endpoints, i1 from its cache records alone — membership a lifecycle-only
    // view would miss. Both rows are the same run and carry the same verdict; the
    // run-keying this test exists for is that a restart would DOUBLE these rows, never
    // merge them.
    assert_eq!(roster.len(), 2, "one run over two instances: {roster:?}");
    assert_eq!(
        roster
            .iter()
            .map(|r| r.instance.as_str())
            .collect::<Vec<_>>(),
        ["i0", "i1"]
    );
    for run in &roster {
        assert_eq!(run.incarnation, "run-1");
        assert!(run.stop_observed, "the stop record was observed");
        // **The slot's occupant reaches the roster**. `instance` is the label
        // the operator gave the tap; `pod` is the process filling it, relayed through
        // `--provenance` and carried on the arrivals path the roster reads. Served because
        // a slot outlives its pods, and a slot whose pod churns is a naming choice the
        // operator can only fix if they can see it. The topic half of the precedence is
        // the same expression the model pair uses one line above, pinned by
        // `roster_guards::the_roster_fans_out_per_endpoint_and_the_topic_outranks_the_declaration`.
        assert_eq!(
            run.pod.as_deref(),
            Some("pod-declared"),
            "the declared pod reaches the roster row: {run:?}"
        );
        assert_eq!(
            run.staleness,
            analyzer_serving_clickhouse::RunStaleness::Departed,
            "an announced stop is an EXPECTED departure — collapsing it with Unexplained \
             discards the distinction spec section 2.4 exists to make"
        );
    }
    let run = &roster[0];
    // observed identity is absent because the tap does not emit it yet, and absent is never
    // defaulted — `unobserved` is a state, not a missing string
    assert_eq!(run.model, None);
    assert_eq!(run.engine_version, None);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn coverage_reads_back_per_day_at_both_groupings_and_they_agree() {
    // Shape 3, over the table this morning's pass began filling. One method with a grouping
    // parameter: S1 wants the fleet's day, S5 wants the instance's, and two methods would drift
    // on the weighting.
    let (engine, _db) = fixture();
    let fleet = "cov";
    let (w, sources) = seed_records(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let fleet_rows = dao
        .coverage(&w, analyzer_serving_clickhouse::CoverageBy::Fleet)
        .expect("fleet coverage");
    assert_eq!(fleet_rows.len(), 1, "one day: {fleet_rows:?}");
    let day = &fleet_rows[0];
    assert!(day.instance.is_none(), "the fleet row names no instance");
    assert!(
        day.fraction > 0.0 && day.fraction <= 1.0,
        "a fraction outside 0..=1: {day:?}"
    );
    assert!(day.instances >= 1, "the denominator rides the row: {day:?}");

    let per_instance = dao
        .coverage(&w, analyzer_serving_clickhouse::CoverageBy::Instance)
        .expect("per instance");
    assert!(!per_instance.is_empty());
    assert!(per_instance.iter().all(|r| r.instance.is_some()));
    // instance-time weighted, so the fleet row is the SUM over instances rather than a mean of
    // their fractions — an unweighted average would let a briefly-seen instance carry the same
    // weight as one seen all day
    let covered: f64 = per_instance.iter().map(|r| r.covered_ms).sum();
    let observed: f64 = per_instance.iter().map(|r| r.window_ms).sum();
    assert!((covered - day.covered_ms).abs() < 1e-6, "{per_instance:?}");
    assert!((observed - day.window_ms).abs() < 1e-6, "{per_instance:?}");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn markers_read_back_with_both_sides_of_the_change() {
    // Shape 7. The first settled day states a BASELINE so the earliest trend point has a stated
    // basis.
    let (engine, _db) = fixture();
    let fleet = "marks-read";
    let (w, sources) = seed_records(&engine, fleet);
    // Markers are stated as a day settles.
    settle(&engine, fleet, &sources, (w.start_ms, "run-2", 0));
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let baseline = dao.markers(&w).expect("markers");
    // **TWO axes of meaning, two baselines**: the parameter generation and
    // the join-semantics generation move independently and for different reasons, so each gets
    // its own seam. `semantics_version` is in the rollups' sort keys now, so a `js-` bump
    // coexists with prior history rather than overwriting it — and coexisting history with an
    // unexplained step is exactly what the marker exists to prevent.
    assert_eq!(baseline.len(), 2, "{baseline:?}");
    let param = baseline
        .iter()
        .find(|m| m.kind == "param_change")
        .expect("the parameter baseline");
    assert_eq!(param.provenance, "recorded");
    assert_eq!(
        param.from_value, None,
        "a baseline has no previous generation to name"
    );
    let semantics = baseline
        .iter()
        .find(|m| m.kind == "semantics_change")
        .expect("the semantics baseline");
    assert_eq!(semantics.provenance, "recorded");
    assert_eq!(semantics.from_value, None);
    assert_eq!(
        semantics.to_value.as_deref(),
        Some(analyzer_analysis::SEMANTICS_VERSION),
        "the semantics seam names the token it is a seam for"
    );

    // **Re-analysing the SAME days under new parameters is not a transition**.
    // Both generations describe the same buckets, so the trend does not step — it FORKS, and a
    // reader picks a generation. The marker states the coverage and names no prior side, because
    // there is no chronology in the rollups to know which side came first, and a guess (the
    // tokens' alphabetical order, say) records the seam backwards half the time.
    let changed = Params::default().with_delta_skew_ms(5000.0);
    tick_under(&engine, &sources, fleet, &changed);
    let after = dao.markers(&w).expect("markers again");
    // three: the two baselines, plus the second parameter generation's coverage. The semantics
    // token did not move, so its axis writes nothing.
    assert_eq!(after.len(), 3, "{after:?}");
    assert_eq!(
        after
            .iter()
            .filter(|m| m.kind == "semantics_change")
            .count(),
        1,
        "an unchanged semantics token must not re-mark itself: {after:?}"
    );
    let forked = after
        .iter()
        .find(|m| m.to_value.as_deref() == Some("s5000x3600000"))
        .expect("the second generation");
    assert_eq!(
        forked.from_value, None,
        "an overlapping generation names no prior side — claiming one is a direction the rows \
         cannot support: {forked:?}"
    );
    assert!(
        !forked.breaks(),
        "a fork must not split the time axis: both generations cover these days, so a span walk \
         that broke here would put one generation's rows under the other's name"
    );
    // The VALUES ride the marker rather than the opaque token, and they are DECODED from that
    // token rather than remembered — the discriminator is a lossless encoding of the
    // discriminating set, so no pass has to record what it was configured with.
    assert!(
        forked.label.contains("delta_skew_ms=5000")
            && forked.label.contains("examined_history_ms=3600000"),
        "the values ride the marker, not just their hash: {forked:?}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_holder_timeline_names_its_own_stores_and_nobody_else_s() {
    // Shape 5. Event grain and raw-window bounded, which the panel says in the UI — a drill that
    // silently returns nothing past the horizon reads as "no problem here".
    let (engine, _db) = fixture();
    let fleet = "tl";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let holder = analyzer_serving_clickhouse::Holder {
        instance: "i0".to_string(),
        dp_rank: 0,
        group_idx: 0,
    };
    let events = dao.timeline(&holder, &w, 100, None).expect("timeline").rows;
    assert_eq!(events.len(), 2, "i0 stored b1 and b2: {events:?}");
    let blocks: Vec<&str> = events.iter().map(|e| e.block_id.as_str()).collect();
    assert_eq!(blocks, vec!["b1", "b2"]);
    assert!(
        events.windows(2).all(|p| p[0].at_ms <= p[1].at_ms),
        "a timeline that is not ordered in time is not a timeline"
    );

    let other = dao
        .timeline(
            &analyzer_serving_clickhouse::Holder {
                instance: "i1".to_string(),
                dp_rank: 1,
                group_idx: 0,
            },
            &w,
            100,
            None,
        )
        .expect("i1")
        .rows;
    assert_eq!(other.len(), 1, "the holder triple scopes it: {other:?}");

    // **The walk, at limit 1** — the whole table fits one page at this fixture's size, so a
    // default limit would never exercise the cursor at all. That is the same argument the
    // goldens page at 1 on: a path nobody runs is a path that breaks when the envelope lifts.
    let mut seen: Vec<String> = Vec::new();
    let mut after = None;
    for _ in 0..10 {
        let page = dao.timeline(&holder, &w, 1, after.as_ref()).expect("page");
        assert!(page.rows.len() <= 1, "a page never exceeds its limit");
        seen.extend(page.rows.iter().map(|e| e.block_id.clone()));
        match page.next {
            Some(c) => after = Some(c),
            None => break,
        }
    }
    assert_eq!(
        seen,
        vec!["b1", "b2"],
        "walking one row at a time visits every row EXACTLY once, in the same order -- a keyset boundary that repeated or dropped a row would show here and nowhere else"
    );
}

/// **One value, two surfaces**: the gap the classifier computes is served per event on the
/// drill row AND on the timeline, and the two must be the same measurement. Before
/// the per-event value was computed and discarded,
/// so the drill could not show the magnitude the
/// class's close-reason was decided beside.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn an_evidence_row_carries_the_gap_the_timeline_serves() {
    let (engine, _db) = fixture();
    let fleet = "gapdrill";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let half_hour = 1_800_000.0;

    // The seed primes each identity with a close half an hour back, so every store on the day is
    // ETR and its gap is exact arithmetic over the seed's own instants.
    let i0 = analyzer_serving_clickhouse::Holder {
        instance: "i0".to_string(),
        dp_rank: 0,
        group_idx: 0,
    };
    let out = dao
        .evidence(
            &analyzer_serving_clickhouse::EvidenceOf::Holder(i0.clone()),
            &w,
            100,
        )
        .expect("evidence");
    let mut gaps: Vec<f64> = out
        .records
        .iter()
        .map(|r| {
            r.gap_ms
                .unwrap_or_else(|| panic!("every store here follows an observed close: {r:?}"))
        })
        .collect();
    gaps.sort_by(f64::total_cmp);
    assert_eq!(
        gaps,
        vec![half_hour, half_hour + 1000.0],
        "the gap is the store's own instant minus its identity's prior close: {:?}",
        out.records
    );

    // The timeline serves the same column for the same events.
    let tl = dao.timeline(&i0, &w, 100, None).expect("timeline").rows;
    assert_eq!(
        tl.iter().map(|t| t.gap_ms).collect::<Vec<_>>(),
        vec![Some(half_hour), Some(half_hour + 1000.0)],
        "the timeline and the evidence drill serve one column: {tl:?}"
    );
}

/// A first-sight store has no observed prior close, and the drill says so with a null — never
/// a zero, which would read as an instant re-fetch. The epoch column takes the same treatment:
/// undeclared is absent.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_first_sight_store_serves_a_null_gap_not_a_zero() {
    let (engine, _db) = fixture();
    let fleet = "gapnull";
    let (w, sources) = seed_records(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let out = dao
        .evidence(
            &analyzer_serving_clickhouse::EvidenceOf::Block("p".to_string()),
            &w,
            100,
        )
        .expect("evidence");
    assert_eq!(out.records.len(), 2, "the seed stored p on two holders");
    for r in &out.records {
        assert_eq!(
            r.gap_ms, None,
            "no close was ever observed for this identity, and absence is not a value: {r:?}"
        );
        assert_eq!(
            r.key_epoch, None,
            "this seed declares no epoch, and undeclared is absent: {r:?}"
        );
    }
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn one_shape_serves_every_grouping_the_panels_ask_for() {
    let (engine, _db) = fixture();
    let fleet = "dao";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    // the headline: no dimensions at all. 56 tokens over three stores.
    let total = dao
        .waste_by(&w, Grain::Day, &[], &Scope::default())
        .expect("headline");
    assert_eq!(total.len(), 1, "one bucket: {total:?}");
    assert_eq!(total[0].tokens.value, 56.0);
    assert_eq!(total[0].events, 3);
    // **An open day is stated through the live edge**: the day's coverage runs to the
    // covered edge, the last attested instant, so while the day is open everything up to it was
    // observed.
    assert!(
        (total[0].tokens.coverage - 1.0).abs() < 1e-9,
        "an open day's denominator stops at the covered edge: {:?}",
        total[0]
    );
    // **Settled, a source's silence is final unless its run attests it.** The settling heartbeat
    // continues the run and names i0 with nothing dropped, so i0's whole day is covered. It
    // names no i1, so i1's silence after the last heartbeat is final: half its day, a SKEW SHORT,
    // because the tail gap opens `delta_skew_ms` before the last heartbeat (`completeness.rs`,
    // the tail arm) and coverage will not claim an instant it cannot place. The fleet's figure
    // is the two days' covered time over both days.
    settle(&engine, fleet, &sources, (w.start_ms, "run-1", 1));
    let total = dao
        .waste_by(&w, Grain::Day, &[], &Scope::default())
        .expect("headline, settled");
    let day_ms = 86_400_000.0;
    let expected = (1.5 * day_ms - Params::default().delta_skew_ms) / (2.0 * day_ms);
    assert!(
        (total[0].tokens.coverage - expected).abs() < 1e-9,
        "{:?} vs {expected}",
        total[0]
    );

    // S3's worst-holders: the same call, one dimension added. The figures must partition the
    // headline exactly — if a grouping loses or duplicates mass, the panels disagree with the
    // number above them and the customer is the one who notices.
    let by_holder = dao
        .waste_by(&w, Grain::Day, &[Dim::Holder], &Scope::default())
        .expect("by holder");
    assert_eq!(by_holder.len(), 2, "two holders: {by_holder:?}");
    let summed: f64 = by_holder.iter().map(|r| r.tokens.value).sum();
    assert_eq!(
        summed, total[0].tokens.value,
        "a grouping must not move mass"
    );
    assert_eq!(
        by_holder[0].dims.len(),
        1,
        "the holder triple is ONE dimension"
    );

    // and asking for the same dimensions in a different order is the same query
    let a = dao
        .waste_by(
            &w,
            Grain::Day,
            &[Dim::Class, Dim::Workload],
            &Scope::default(),
        )
        .expect("a");
    let b = dao
        .waste_by(
            &w,
            Grain::Day,
            &[Dim::Workload, Dim::Class, Dim::Class],
            &Scope::default(),
        )
        .expect("b");
    assert_eq!(
        a, b,
        "dimension order and repeats must not change the answer"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn both_grains_answer_and_agree_on_the_mass() {
    // Hourly exists for the incident mode and cannot be backfilled, so it is written from the
    // first pass. The two grains bucket differently and must still account for the same tokens
    // — a divergence here means one grain's emission drifted from the other's.
    let (engine, _db) = fixture();
    let fleet = "grains";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let daily = dao
        .waste_by(&w, Grain::Day, &[], &Scope::default())
        .expect("daily");
    let hourly = dao
        .waste_by(&w, Grain::Hour, &[], &Scope::default())
        .expect("hourly");
    assert!(!hourly.is_empty(), "the hourly tier was not written");
    let d: f64 = daily.iter().map(|r| r.tokens.value).sum();
    let h: f64 = hourly.iter().map(|r| r.tokens.value).sum();
    assert_eq!(d, h, "the grains disagree about the same window's mass");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_repassed_window_is_read_once_not_twice() {
    // **The flake the no-non-FINAL law exists for.** The rollups replace on their key, and the
    // merge selector will not merge two small idle parts within any bounded time — so a read
    // without FINAL double-counts whenever a restatement has not merged yet, passing or failing
    // on background timing. Ticking twice and reading once is the direct test.
    let (engine, _db) = fixture();
    let fleet = "repass";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let once = dao
        .waste_by(&w, Grain::Day, &[], &Scope::default())
        .expect("first read");

    journal_faults::executor(&engine, fleet, &Params::default())
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("repass");
    let twice = dao
        .waste_by(&w, Grain::Day, &[], &Scope::default())
        .expect("second read");
    assert_eq!(
        once, twice,
        "a re-pass doubled the figures — the read is not deduplicating replaced rows"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_block_names_its_holders_in_both_directions() {
    // Shape 4, the panel that names an object instead of an aggregate: b1 sits on two holders
    // and is the "stored on 78 of your 80 caches this week" figure in miniature — STORED, not
    // resident: this counts holders that stored the block in the window, and simultaneity is
    // shape 6's job.
    let (engine, _db) = fixture();
    let fleet = "blocks";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let days = dao.block_days(&w, 10).expect("blocks");
    assert_eq!(days.len(), 1, "one day of stores: {days:?}");
    let top = &days[0].blocks;
    assert_eq!(top.len(), 2, "two distinct blocks: {top:?}");
    let b1 = top.iter().find(|b| b.block_id == "b1").expect("b1");
    assert_eq!(
        (b1.tokens_at_least, b1.tokens_at_most),
        (24, 24),
        "16 + 8 across two holders, exact while the summary monitors every block"
    );
    assert_eq!(
        b1.holders_at_least, 2,
        "the holder count is the actionable half"
    );

    let holders = dao
        .holders_of("b1", &w, 100, None)
        .expect("holders_of")
        .rows;
    assert_eq!(holders.len(), 2);
    assert!(holders.iter().any(|h| h.instance == "i1" && h.dp_rank == 1));

    // The same walk on shape 4's other direction. Both rows share a block, so the cursor is
    // resuming inside one instant's group -- which is exactly where a cursor keyed on `at_ms`
    // alone would loop forever or skip the rest of the group.
    let first = dao.holders_of("b1", &w, 1, None).expect("first");
    assert_eq!(first.rows.len(), 1);
    let cursor = first.next.expect("two rows means a second page");
    let second = dao.holders_of("b1", &w, 1, Some(&cursor)).expect("second");
    assert_eq!(second.rows.len(), 1);
    assert_ne!(
        (
            second.rows[0].instance.clone(),
            second.rows[0].dp_rank,
            second.rows[0].at_ms
        ),
        (
            first.rows[0].instance.clone(),
            first.rows[0].dp_rank,
            first.rows[0].at_ms
        ),
        "the second page must not repeat the first"
    );
    assert!(
        second.next.is_none(),
        "two rows, two pages, then the walk ends"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_parameter_change_leaves_a_marker_and_running_again_does_not_add_another() {
    // The trend needs a stated cause for its step, and the marker roster must grow with EVENTS
    // rather than with the tick cadence — so the stamp is the first bucket under the new
    // parameters, not "now". An unchanged run writes nothing at all.
    let (engine, _db) = fixture();
    let fleet = "marks";
    let (_, sources) = seed(&engine, fleet);
    let count = |engine: &analyzer_serving_clickhouse::engine::Engine| -> u64 {
        engine
            .run(&format!(
                "SELECT count() FROM markers FINAL WHERE gone = 0 AND fleet_id = '{fleet}'"
            ))
            .expect("count")
            .trim()
            .parse()
            .expect("parse")
    };
    assert_eq!(
        count(&engine),
        2,
        "the first pass writes a BASELINE ON EACH AXIS — parameters and join semantics — so the \
         earliest trend point has a stated basis for both"
    );

    // **The assertion whose absence let the code contradict the comment above it.** The stamp was
    // `now64(3)` — the engine's WALL clock at insert — while this comment has said "the first
    // bucket under the new parameters, not now" since the marker landed. Nothing checked the
    // value, so the two disagreed silently for as long as both existed.
    //
    // It is a correctness property, not a tidiness one: a marker exists to sit on a trend whose
    // axis is engine-domain buckets, so a wall stamp placed there is a cross-domain comparison —
    // and an invisible one, because both are milliseconds and land somewhere plausible.
    let stamp: f64 = engine
        .run(&format!(
            "SELECT at_ms FROM markers FINAL WHERE gone = 0 AND fleet_id = '{fleet}' LIMIT 1"
        ))
        .expect("stamp")
        .trim()
        .parse()
        .expect("parse stamp");
    let first_bucket: f64 = engine
        .run(&format!(
            "SELECT toFloat64(toUnixTimestamp(min(bucket))) * 1000 FROM class_rollup_daily FINAL \
             WHERE gone = 0 AND fleet_id = '{fleet}'"
        ))
        .expect("first bucket")
        .trim()
        .parse()
        .expect("parse bucket");
    assert!(
        (stamp - first_bucket).abs() < f64::EPSILON,
        "the marker is stamped at the first bucket its generation is keyed under, not at the \
         wall clock: {stamp} vs {first_bucket}"
    );

    // a different examined-history horizon is a different discriminator, so this tick's rows
    // land beside the previous generation rather than replacing it
    let changed = Params::default().with_examined_history_ms(1_800_000.0);
    tick_under(&engine, &sources, fleet, &changed);
    assert_eq!(
        count(&engine),
        3,
        "the generation went unmarked (two baselines plus this one's coverage)"
    );

    // Re-analysing the same days is a FORK, not a transition — see
    // `markers_read_back_with_both_sides_of_the_change` for the ruling and
    // `generations::tests::a_handoff_is_a_change_and_an_overlap_is_a_fork` for the case that does
    // carry a direction. What must survive either way is that the marker states the VALUES.
    let row = engine
        .run(&format!(
            "SELECT from_value, to_value, label FROM markers FINAL \
             WHERE gone = 0 AND fleet_id = '{fleet}' AND to_value = 's1000x1800000'"
        ))
        .expect("marker");
    assert!(
        row.contains("examined_history_ms=1800000"),
        "the VALUES ride the marker, not just their hash — otherwise a reader has to go find \
         out which parameter moved and to what: {row}"
    );

    // and running again under the SAME parameters adds nothing
    tick_under(&engine, &sources, fleet, &changed);
    assert_eq!(
        count(&engine),
        3,
        "markers grew with the cadence rather than with the events — the table is supposed to \
         be its own memory, so an unchanged generation writes nothing, on either axis"
    );
}

// ===========================================================================================
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_baseline_marker_is_an_origin_and_does_not_break_the_series() {
    // A fleet's first settled day carries a baseline marker on each axis, with
    // `from_value: None`. Counting them as seams would split every window
    // at the fleet's own beginning -- an `Unlinked` segment over the span BEFORE any data
    // existed, with the real figures displaced into a second segment.
    //
    // A discontinuity is a TRANSITION, and a transition needs two sides.
    let (engine, _db) = fixture();
    let fleet = "baseline-marks";
    let (w, sources) = seed_records(&engine, fleet);
    // Markers are stated as a day settles.
    settle(&engine, fleet, &sources, (w.start_ms, "run-2", 0));
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let markers = dao.markers(&w).expect("markers");
    assert!(!markers.is_empty(), "a settled day states its baselines");
    assert!(
        markers.iter().all(|m| !m.breaks()),
        "a baseline names no prior generation, so it breaks nothing: {markers:?}"
    );

    let segments = dao
        .waste_by_segmented(&w, Grain::Day, &[Dim::Class], &Scope::default())
        .expect("segmented");
    assert_eq!(
        segments.len(),
        1,
        "a window whose only markers are baselines is ONE clean segment: {segments:?}"
    );
    assert!(
        segments[0].figures.is_some(),
        "and it carries figures rather than a refusal"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
#[allow(clippy::too_many_lines)] // one ledger through every partition of it
fn the_delivery_ledger_partitions_expected_into_captured_declared_and_missing() {
    // The delivery read is arrivals-backed and needs no pass: ingest writes the facts at
    // commit, and the ledger answers at ingest freshness — that is the ruling it implements.
    let (engine, _db) = fixture();
    analyzer_serving_clickhouse::engine::migrate(
        &engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig::default(),
    )
    .expect("migrate");
    let fleet = "delivery";
    let sources = segment_sources(fleet);

    let hb = |at: f64, seen: u64, dropped: u64| {
        serde_json::json!({"kind": "heartbeat", "at_ms": at, "msgs_seen": seen,
            "dropped": dropped, "oversized": 0, "unknown_types": 0, "events_ingested": seen,
            "content_unresolved": 0, "content_bridge_entries": 0,
            "content_bridge_evicted": 0, "publisher_restarts": 0,
            "endpoints": [{"source": "i0", "endpoint": "tcp://h:5557", "msgs_seen": seen,
                           "dropped": dropped, "last_msg_at_ms": at - 100.0}]})
    };
    let open = |seq: i64, at: f64| {
        serde_json::json!({"kind": "segment_open", "at_ms": at, "contract_version": "1.10",
            "incarnation": "run-a", "segment_seq": seq, "heartbeat_secs": 60,
            "max_segment_secs": 300, "key_epoch": 0})
    };
    let ship = |label: &str, records: &[serde_json::Value]| {
        let mut seg = String::new();
        for r in records {
            seg.push_str(&r.to_string());
            seg.push('\n');
        }
        journal_faults::serving(&engine)
            .bind(
                &analyzer_pipeline::execution::Namespace::new("dao-fixture-source", fleet).unwrap(),
            )
            .unwrap();
        let lake = journal_faults::spine(&engine, fleet).lake();
        analyzer_ingest::ingest_bytes(&lake, fleet, label, seg.as_bytes()).expect(label);
        publish_inputs(&engine, fleet);
    };

    // Run A ships sequences 0, 1 and 3 — a gap at 2 nobody declared. Each segment is its own
    // ingest so each is its own batch, like production. The counters are cumulative, so the
    // run's accounting is the maxima, not any one segment's word.
    for (seq, at, seen, dropped) in [
        (0, 1_000.0, 3, 0),
        (1, 61_000.0, 7, 1),
        (3, 181_000.0, 12, 2),
    ] {
        ship(
            &format!("seg-{seq}"),
            &[open(seq, at), hb(at + 500.0, seen, dropped)],
        );
    }

    // A liveness-less segment: run A's seq 4 carrying only a `segments_dropped` confession
    // about run B (sequences 4..=6). Ingest writes NO arrival row for it — the row would
    // state a clock the segment does not have, and widening `last_record_at_ms` past the
    // liveness kinds would make arrivals a second staleness reader. So the drop declaration
    // LANDS while seq 4 stays invisible to run A's ledger: the conservative direction, an
    // understated delivery, never an overstated one.
    ship(
        "seg-4",
        &[
            open(4, 200_000.0),
            serde_json::json!({"kind": "segments_dropped", "at_ms": 200_500.0,
                "incarnation": "run-b", "count": 3, "first": "seg-run-b-4.jsonl",
                "last": "seg-run-b-6.jsonl", "first_seq": 4, "last_seq": 6}),
        ],
    );

    let dao = Dao::new(
        &engine,
        sources,
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let report = dao.delivery().expect("delivery");
    assert_eq!(report.orphan_batches, 0);
    assert_eq!(report.rows.len(), 2, "run-a and run-b: {report:?}");

    // Run A: sequences 0..=3 expected (seq 4 left no arrival row), three captured, nothing
    // declared, the gap at 2 missing unexplained. The envelope marks are the cumulative
    // maxima.
    let a = &report.rows[0];
    assert_eq!(a.incarnation, "run-a");
    assert_eq!(
        (
            a.segments_expected,
            a.segments_captured,
            a.segments_declared_dropped,
            a.segments_missing_unexplained,
        ),
        (4, 3, 0, 1),
        "{a:?}"
    );
    assert_eq!(
        (
            a.records_seen,
            a.records_dropped_at_source,
            a.records_oversized
        ),
        (12, 2, 0)
    );
    assert_eq!(a.instances, vec!["i0".to_string()]);
    assert!(a.first_seen_ms.is_some() && a.last_seen_ms.is_some());

    // Run B: everything known about it is the confession. section 4.2 makes sequences gap-free
    // from zero, so the declaration itself proves 0..=6 existed — three confessed, four
    // missing unexplained. And NO instants: the only clock near it belongs to run A's
    // segment, and borrowing it would stamp one run with another's time.
    let b = &report.rows[1];
    assert_eq!(b.incarnation, "run-b");
    assert_eq!(
        (
            b.segments_expected,
            b.segments_captured,
            b.segments_declared_dropped,
            b.segments_missing_unexplained,
        ),
        (7, 0, 3, 4),
        "{b:?}"
    );
    assert_eq!((b.first_seen_ms, b.last_seen_ms), (None, None));
    assert!(b.instances.is_empty());

    // A different batch re-shipping an already-captured sequence adds a row under a new
    // batch_id; `uniqExact` counts the sequence once, so the ledger does not double.
    ship("seg-0-again", &[open(0, 1_000.0), hb(1_600.0, 3, 0)]);
    let again = dao.delivery().expect("delivery, again");
    assert_eq!(again.rows[0].segments_captured, 3, "dedup held: {again:?}");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_two_band_sources_state_one_lens() {
    // The thrash lens's one drift seam: day grain reads
    // `band_rollup_daily`, hour grain `band_rollup_hourly`. Two tables, one truth — over the
    // cells a REAL tick wrote, so a boundary or column drifting between them fails here
    // rather than on glass at day 31.
    let (engine, _db) = fixture();
    let fleet = "bands-read";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let day = dao
        .bands_by(&w, Grain::Day, &[], &Scope::default())
        .expect("day bands");
    // The seed's three stores all re-store blocks closed half an hour earlier, so one band holds
    // the whole day: 10-60m, 56 tokens, 3 events.
    assert_eq!(day.rows.len(), 1, "{:?}", day.rows);
    assert_eq!(
        (
            day.rows[0].band.as_str(),
            day.rows[0].events,
            day.rows[0].tokens.value
        ),
        ("10-60m", 3, 56.0),
        "{:?}",
        day.rows[0]
    );
    assert_eq!(day.evidence, analyzer_serving_clickhouse::RawReach::Covers);

    let hour = dao
        .bands_by(&w, Grain::Hour, &[], &Scope::default())
        .expect("hour bands");
    assert!(hour.evidence.covers(), "the seed sits inside the horizon");
    let day_total: (u64, f64) = day
        .rows
        .iter()
        .fold((0, 0.0), |a, r| (a.0 + r.events, a.1 + r.tokens.value));
    let hour_total: (u64, f64) = hour
        .rows
        .iter()
        .fold((0, 0.0), |a, r| (a.0 + r.events, a.1 + r.tokens.value));
    assert_eq!(
        day_total, hour_total,
        "day {:?} hour {:?}",
        day.rows, hour.rows
    );
    for r in &hour.rows {
        assert_eq!(
            r.band, "10-60m",
            "the scan's CASE must band exactly as the fold did: {r:?}"
        );
    }

    // The banded read collapses to the un-banded one: banding partitions the register.
    let waste = dao
        .waste_by(&w, Grain::Day, &[], &Scope::default())
        .expect("waste");
    let waste_total: f64 = waste.iter().map(|r| r.tokens.value).sum();
    assert_eq!(day_total.1, waste_total);

    // Cut by workload: the dims ride exactly as waste_by's do.
    let cut = dao
        .bands_by(&w, Grain::Day, &[Dim::Workload], &Scope::default())
        .expect("cut bands");
    assert!(
        cut.rows
            .iter()
            .all(|r| r.dims == vec![(Dim::Workload, "agentic".to_string())]),
        "{:?}",
        cut.rows
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_declared_deploy_survives_repass_and_never_breaks_the_series() {
    // Markers-direct annotations: a deploy is a marker row,
    // and the three properties that made a separate table unnecessary are each pinned here —
    // the generation purge is kind-scoped so the deploy survives a re-pass; `breaks()` says
    // the series continues across it; and the replace key makes re-declaring idempotent.
    let (engine, _db) = fixture();
    let fleet = "deploy-marks";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let at = w.start_ms + 1_500.0;
    let deploy = |scope: Option<&str>, source, declared_at_ms| {
        analyzer_serving_clickhouse::DeployAnnotation {
            at_ms: at,
            label: "affinity v2 rollout".to_string(),
            scope: scope.map(str::to_string),
            source,
            declared_at_ms,
        }
    };
    record_declaration(
        &engine,
        fleet,
        "ann:a@1",
        &deploy(
            Some("agentic"),
            analyzer_serving_clickhouse::DeploySource::Cli,
            None,
        ),
    );
    // Idempotent: the same instant and label replace, never accumulate.
    record_declaration(
        &engine,
        fleet,
        "ann:a@2",
        &deploy(
            None,
            analyzer_serving_clickhouse::DeploySource::Api,
            Some(at + 250_000.0),
        ),
    );

    // The purge that must NOT take it: a re-pass re-derives the generation markers.
    journal_faults::executor(&engine, fleet, &Params::default())
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("re-pass");
    engine
        .run("OPTIMIZE TABLE markers FINAL")
        .expect("optimize");

    let marks = dao.markers(&w).expect("markers");
    let deploys: Vec<_> = marks.iter().filter(|m| m.kind == "deploy").collect();
    assert_eq!(deploys.len(), 1, "exactly one deploy row: {marks:?}");
    let d = deploys[0];
    assert_eq!(d.label, "affinity v2 rollout");
    assert_eq!(d.provenance, "recorded");
    assert_eq!(d.source, "api", "the re-declare replaced the first row");
    assert_eq!(
        d.declared_at_ms,
        Some(at + 250_000.0),
        "the surviving declaration's own instant rides the row"
    );
    assert!(
        !d.breaks(),
        "a deploy continues the series with the marker drawn on it"
    );

    // The refusals a declaration states.
    assert!(
        deploy(None, analyzer_serving_clickhouse::DeploySource::Cli, None)
            .validate()
            .is_ok()
    );
    let mut bad = deploy(None, analyzer_serving_clickhouse::DeploySource::Cli, None);
    bad.at_ms = f64::NAN;
    assert!(bad.validate().is_err());
    bad.at_ms = at;
    bad.label = "   ".to_string();
    assert!(bad.validate().is_err());
}

/// A declaration as the annotation walk records it: its object's batch in the input log,
/// then the input publication that states its marker.
fn record_declaration(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
    identity: &str,
    declaration: &analyzer_serving_clickhouse::DeployAnnotation,
) {
    let log = journal_faults::spine(engine, fleet)
        .lake()
        .ledger(analyzer_pipeline::ledger::SHARD)
        .expect("input log");
    let position = log.next_position().expect("input position");
    log.commit_cycle(
        position,
        vec![analyzer_pipeline::ledger::BatchRecord {
            batch_id: identity.to_string(),
            carrier: "annotation".into(),
            source: identity.to_string(),
            object_key: identity.to_string(),
            etag: String::new(),
            input_position: position,
            ingested_at_ms: 1,
            record_count: 1,
            unknown_kinds: 0,
            first_at_ms: None,
            last_at_ms: None,
            days: Vec::new(),
            segments: Vec::new(),
            drops: Vec::new(),
            lifecycle: Vec::new(),
            stored: Vec::new(),
            annotations: vec![declaration.record()],
            tags: Vec::new(),
        }],
    )
    .expect("record the declaration");
    publish_inputs(engine, fleet);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn stored_mass_counts_store_arrivals_and_only_store_arrivals() {
    // The anchor's denominator: `facts_rollup_daily` banked at INSERT by the MV,
    // read back per day, store rows only — an evict carries no tokens and a clear is not mass
    // arriving. The seed writes 56 store tokens on the day under test and closes three blocks;
    // the evicts must not move this number.
    let (engine, _db) = fixture();
    let fleet = "stored-mass";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    let rows = dao.stored_mass(&w).expect("stored mass");
    assert_eq!(rows.len(), 1, "one day in the window: {rows:?}");
    assert_eq!(
        (rows[0].records, rows[0].tokens),
        (3, 56),
        "three stores, 56 tokens — evicts counted nowhere: {rows:?}"
    );

    // Widened to cover the priming day, the denominator gains exactly the primed stores.
    let wide = Window {
        start_ms: w.start_ms - 3.0 * 86_400_000.0,
        end_ms: w.end_ms,
    };
    let wide_rows = dao.stored_mass(&wide).expect("stored mass, widened");
    let total: u64 = wide_rows.iter().map(|r| r.tokens).sum();
    assert_eq!(total, 68, "56 on the day + 12 primed: {wide_rows:?}");
}

/// **The remedy split is a PARTITION, and a fourth locality cannot vanish into it**
/// (the cache-group split).
///
/// `cross` + `within` + `group` must reconcile to the measured duplicated total. The
/// defect this pin exists for is silent: a handler that DERIVED the total by adding the
/// remedies it knew about would leave a new locality out of every member's share denominator
/// with nothing anywhere reporting an error. Reading the total from the engine makes the
/// shortfall arithmetic, and this asserts it.
///
/// It also fails on the NEXT unwired locality value, which is the point: a value the
/// producer writes and the reader does not sum breaks the partition rather than
/// quietly shrinking a denominator.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_remedy_split_partitions_the_duplicated_mass() {
    let (engine, _db) = fixture();
    let fleet = "remedy-split";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());

    // Minute cells at fleet scope, one per locality, plus a band-`1` row that is HELD
    // mass and not duplicated — so a reader that forgot the band filter also fails here.
    #[allow(clippy::cast_possible_truncation)] // a fixture instant fits i64 ms
    let minute = (w.start_ms as i64 / 60_000) * 60_000;
    let mut rows = String::new();
    for (locality, band, mass) in [
        ("cross", "2", 300.0_f64),
        ("within", "3-4", 200.0),
        ("group", "2", 100.0),
        ("within", "1", 900.0),
    ] {
        use std::fmt::Write as _;
        if !rows.is_empty() {
            rows.push_str(", ");
        }
        let _ = write!(
            rows,
            "('{fleet}', '{sem}', '{disc}', '', '{band}', '{locality}', {minute}, {mass}, 0.0)",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
        );
    }
    engine
        .run(&format!(
            "INSERT INTO replication_minute (fleet_id, semantics_version, discriminator, \
             workload_class, copies_band, locality, minute_ms, token_ms, copies_token_ms) \
             VALUES {rows}"
        ))
        .expect("seed replication_minute");

    // The fixture seeds minute cells, and since the hourly rollups each grain serves from its own
    // table — so the read asks the tier it seeded. The partition law is the same at every
    // width; which table answers is the tick's business, pinned in the producer corpus.
    let data = dao
        .copies(&w, analyzer_serving_clickhouse::Grain::Minute, None)
        .expect("copies");
    let split =
        data.cross_engine_token_ms + data.within_engine_token_ms + data.cache_group_token_ms;
    assert!(
        (split - data.dup_total_token_ms).abs() < 1e-6,
        "the three remedies must partition the duplicated total: \
         cross {} + within {} + group {} = {split}, total {}",
        data.cross_engine_token_ms,
        data.within_engine_token_ms,
        data.cache_group_token_ms,
        data.dup_total_token_ms
    );
    assert!(
        (data.cache_group_token_ms - 100.0).abs() < 1e-6,
        "the architectural bucket carries its own mass rather than folding into `within`: {}",
        data.cache_group_token_ms
    );
    assert!(
        (data.dup_total_token_ms - 600.0).abs() < 1e-6,
        "band `1` is held mass, never duplicated mass: {}",
        data.dup_total_token_ms
    );
}

/// **Scope narrows the ANSWER, and `Dim::Instance` rolls a node's ranks up**.
///
/// A group-by-only read draws one node's page by fetching every node and filtering in the
/// client — fleet-proportional work for a single-entity page, which is the cost law the
/// monoid line exists to hold. This pins the
/// two halves that make an entity page cheap: a scoped read returns only that slice, and
/// grouping by instance sums a node's holders without reopening the triple.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_scoped_read_answers_about_one_entity_and_instance_rolls_up_its_ranks() {
    let (engine, _db) = fixture();
    let fleet = "scoped";
    let (w, sources) = seed_records(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );

    // The seed puts stores on i0 (agentic) and i1 (undeclared). Fleet scope sees both.
    let fleet_rows = dao
        .waste_by(&w, Grain::Day, &[Dim::Instance], &Scope::default())
        .expect("fleet");
    let instances: std::collections::BTreeSet<String> = fleet_rows
        .iter()
        .flat_map(|r| r.dims.iter())
        .filter(|(d, _)| *d == Dim::Instance)
        .map(|(_, v)| v.clone())
        .collect();
    assert!(
        instances.len() >= 2,
        "the fixture spans instances, so a scoped read has something to exclude: {instances:?}"
    );

    // Scoped to one instance: only that instance comes back, and its mass is the same mass
    // the fleet read attributed to it — narrowing the question never changes the answer.
    let scoped = dao
        .waste_by(
            &w,
            Grain::Day,
            &[Dim::Instance],
            &Scope {
                instance: Some("i0".to_string()),
                ..Scope::default()
            },
        )
        .expect("scoped");
    let only: std::collections::BTreeSet<String> = scoped
        .iter()
        .flat_map(|r| r.dims.iter())
        .filter(|(d, _)| *d == Dim::Instance)
        .map(|(_, v)| v.clone())
        .collect();
    assert_eq!(
        only,
        ["i0".to_string()].into_iter().collect(),
        "a scoped read answers about ONE entity: {only:?}"
    );
    let mass = |rows: &[analyzer_serving_clickhouse::WasteRow], inst: &str| -> f64 {
        rows.iter()
            .filter(|r| r.dims.iter().any(|(d, v)| *d == Dim::Instance && v == inst))
            .map(|r| r.tokens.value)
            .sum()
    };
    let (fleet_i0, scoped_i0) = (mass(&fleet_rows, "i0"), mass(&scoped, "i0"));
    assert!(
        (fleet_i0 - scoped_i0).abs() < 1e-9,
        "scoping is a narrower question, not a different one: fleet said {fleet_i0}, \
         scoped said {scoped_i0}"
    );

    // And the roll-up: instance-grain mass equals the sum of that instance's holder rows.
    let by_holder = dao
        .waste_by(&w, Grain::Day, &[Dim::Holder], &Scope::default())
        .expect("by holder");
    let holder_sum: f64 = by_holder
        .iter()
        .filter(|r| {
            r.dims
                .iter()
                .any(|(d, v)| *d == Dim::Holder && v.starts_with("i0"))
        })
        .map(|r| r.tokens.value)
        .sum();
    assert!(
        (holder_sum - fleet_i0).abs() < 1e-9,
        "an instance is its ranks summed: holders {holder_sum} vs instance {fleet_i0}"
    );
}
/// **Per-instance duplication is INTERNAL only, and cross-engine mass is absent by
/// construction**.
///
/// Two data-parallel ranks of one node holding the same prefix is unambiguous waste — it was
/// prefilled twice and is stored twice on one machine — and vLLM's own DP balancer scores by
/// load with no prefix term, so it happens by default and nothing measures it. That is the
/// mass this table exists to name. Copies SPANNING engines belong to no single node, so
/// attributing them per instance would double the mass on any sum across instances; the
/// producer never writes them here, which this pin holds by seeding one of each and finding
/// only the internal ones.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn per_instance_duplication_carries_the_internal_half_and_never_the_cross_engine_one() {
    let (engine, _db) = fixture();
    let fleet = "inst-dup";
    let sources = bare(&engine, fleet);
    let base = analyzer_serving_clickhouse::engine::testing::recent_day_base_ms();
    let w = Window {
        start_ms: base,
        end_ms: base + 3_600_000.0,
    };
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());

    #[allow(clippy::cast_possible_truncation)] // a fixture instant fits i64 seconds
    let hour = (w.start_ms as i64 / 3_600_000) * 3_600;
    let mut rows = String::new();
    for (instance, band, locality, mass) in [
        ("i0", "2", "within", 400.0_f64),
        ("i0", "3-4", "group", 150.0),
        ("i1", "2", "within", 90.0),
        // Band `1` is HELD mass, not duplicated: a read that forgot the filter fails here.
        ("i0", "1", "within", 7_000.0),
    ] {
        use std::fmt::Write as _;
        if !rows.is_empty() {
            rows.push_str(", ");
        }
        let _ = write!(
            rows,
            "('{fleet}', '{sem}', '{disc}', '{instance}', '{band}', '{locality}', \
             toDateTime({hour}, 'UTC'), {mass}, 0.0)",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
        );
    }
    engine
        .run(&format!(
            "INSERT INTO replication_instance_hourly (fleet_id, semantics_version, \
             discriminator, instance_id, copies_band, locality, bucket, token_ms, \
             copies_token_ms) VALUES {rows}"
        ))
        .expect("seed instance duplication");

    let i0 = dao.instance_duplication(&w, "i0", Grain::Hour).expect("i0");
    assert!(
        (i0.within_token_ms - 400.0).abs() < 1e-6,
        "the rank-crossing half is the one with a lever: {i0:?}"
    );
    assert!(
        (i0.group_token_ms - 150.0).abs() < 1e-6,
        "the architectural half rides beside it rather than vanishing: {i0:?}"
    );
    assert!(
        i0.rows.iter().all(|r| r.copies_band != "1"),
        "band `1` is held mass, never duplicated mass: {:?}",
        i0.rows
    );

    // Another node's mass is another node's: scoping is a narrower question, not a share.
    let i1 = dao.instance_duplication(&w, "i1", Grain::Hour).expect("i1");
    assert!(
        (i1.within_token_ms - 90.0).abs() < 1e-6,
        "each node carries its own internal duplication: {i1:?}"
    );

    // **The denominator rides with the numerators.** Band `1` is held-once mass — excluded
    // from the duplication rows on purpose, and counted here, because "how much of what this
    // node holds is held twice inside it" cannot be formed without it. The console could
    // otherwise only state the duplication's own composition, which gives its character and
    // never its size; borrowing the recompute read's token total instead would ratio across
    // registers, which `never_summed_with` exists to forbid.
    assert!(
        (i0.held_token_ms - 7_550.0).abs() < 1e-6,
        "held is every band including `1` — i0 holds 7000 held-once + 400 across ranks + \
         150 across groups = 7550; the 90 is i1's and stays there: {i0:?}"
    );

    // And a node that duplicated nothing internally reads as zero rather than as absent —
    // the read looked, and found none.
    let quiet = dao
        .instance_duplication(&w, "i-none", Grain::Hour)
        .expect("absent node");
    assert!(quiet.rows.is_empty() && quiet.within_token_ms == 0.0);

    // The minute cut: its own table, read at `Grain::Minute`, with its
    // own denominator — the hourly rows above are not consulted, so a minute ask cannot be
    // served hour-wide bars by accident.
    engine
        .run(&format!(
            "INSERT INTO replication_instance_minute (fleet_id, semantics_version, \
             discriminator, instance_id, copies_band, locality, bucket, token_ms, \
             copies_token_ms) VALUES \
             ('{fleet}', '{sem}', '{disc}', 'i0', '2', 'within', toDateTime({hour}, 'UTC'), 30.0, 60.0), \
             ('{fleet}', '{sem}', '{disc}', 'i0', '1', 'within', toDateTime({hour} + 60, 'UTC'), 500.0, 0.0)",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
        ))
        .expect("seed instance minute");
    let minute = dao
        .instance_duplication(&w, "i0", Grain::Minute)
        .expect("i0 minute");
    assert!(
        (minute.within_token_ms - 30.0).abs() < 1e-6 && (minute.held_token_ms - 530.0).abs() < 1e-6,
        "the minute cut is its own table and its own denominator: {minute:?}"
    );
    assert_eq!(
        minute.rows.len(),
        1,
        "one duplicated minute cell: {:?}",
        minute.rows
    );
}

/// **Whales sum each identity's settled days and bound what a day's cut left out**: day 0's cut
/// was full and left `c` out at no more than its floor of 4; day 1's cut held every duplicated
/// identity. Over both days `a` is exact, `c` is short by at most day 0's floor, `b` by nothing
/// (day 1 left nobody out), and the totals are the day rows' sums.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn whales_sum_each_identity_s_settled_days_and_bound_what_a_cut_left_out() {
    const DAY: f64 = 86_400_000.0;
    let (engine, _db) = fixture();
    let fleet = "whales";
    let (_w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources,
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    let d0 = analyzer_analysis::emitted::day_index(now_ms) - 3;
    let (sem, disc) = (analyzer_analysis::SEMANTICS_VERSION, meaning.as_str());
    engine
        .run(&format!(
            "INSERT INTO replication_whale_day (fleet_id, semantics_version, discriminator, day, \
             identity, n_tokens, dup_token_ms, held_token_ms) VALUES \
             ('{fleet}', '{sem}', '{disc}', toDate({d0}), 'a', 16, 6.0, 8.0), \
             ('{fleet}', '{sem}', '{disc}', toDate({d0}), 'b', 16, 5.0, 7.0), \
             ('{fleet}', '{sem}', '{disc}', toDate({d1}), 'a', 16, 3.0, 4.0), \
             ('{fleet}', '{sem}', '{disc}', toDate({d1}), 'c', 32, 6.0, 6.0)",
            d1 = d0 + 1,
        ))
        .expect("seed whale rows");
    engine
        .run(&format!(
            "INSERT INTO replication_whale_day_total (fleet_id, semantics_version, discriminator, \
             day, held_identities, duplicated_identities, dup_token_ms, held_token_ms, cut, \
             floor_dup_token_ms) VALUES \
             ('{fleet}', '{sem}', '{disc}', toDate({d0}), 4, 3, 15.0, 30.0, 2, 4.0), \
             ('{fleet}', '{sem}', '{disc}', toDate({d1}), 2, 2, 9.0, 12.0, 2, 0.0)",
            d1 = d0 + 1,
        ))
        .expect("seed whale totals");
    #[allow(clippy::cast_precision_loss)] // day indices are small
    let w = analyzer_analysis::Window {
        start_ms: d0 as f64 * DAY,
        end_ms: (d0 + 2) as f64 * DAY,
    };
    let whales = dao.whales(&w, 10).expect("whales");
    assert_eq!(
        (
            whales.days_stated,
            whales.cut,
            whales.duplicated_identity_days
        ),
        (2, 2, 5)
    );
    assert!((whales.window_dup_token_ms - 24.0).abs() < 1e-9);
    assert!((whales.window_held_token_ms - 42.0).abs() < 1e-9);
    let rows: Vec<(&str, f64, f64, u32, i64)> = whales
        .rows
        .iter()
        .map(|r| {
            (
                r.identity.as_str(),
                r.dup_token_ms,
                r.dup_token_ms_bound,
                r.days_in_cut,
                r.n_tokens,
            )
        })
        .collect();
    assert_eq!(
        rows,
        vec![
            ("a", 9.0, 0.0, 2, 16),
            ("c", 6.0, 4.0, 1, 32),
            ("b", 5.0, 0.0, 1, 16)
        ]
    );
}

/// **A minute cell past the horizon cannot fake reach**.
///
/// The minute tables expire by bucket age at the NEXT MERGE, so a row past the horizon can
/// sit unmerged for a while, and a re-derived old day writes exactly such rows. An unbounded
/// `min(bucket)` probe would read that island as coverage, and a short window between it and
/// the live edge would resolve to minute grain and draw zeros. The probe bounds itself by the
/// horizon the TTL reads. This pin plants
/// the island, PROVES it is present at read time (so the pin cannot pass by the row having
/// merged away), and asks the resolver about a window next to it.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_minute_cell_past_the_horizon_cannot_fake_reach() {
    const HOUR: f64 = 3_600_000.0;
    const DAY: f64 = 86_400_000.0;
    let (engine, _db) = fixture();
    let fleet = "minute-island";
    let (_w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    // Fresh cells over the last two hours, and one island cell thirty days back — the shape a
    // re-pass of an old day leaves behind.
    let row = |bucket_sql: &str| {
        format!(
            "('{fleet}', '{sem}', '{disc}', 'ETR', 'undeclared', {bucket_sql}, 10, 100)",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
        )
    };
    // The island lingers only until a TTL merge, and nothing bounds when one runs: stopped, the
    // precondition below holds by construction rather than by the merge scheduler's timing.
    engine
        .run("SYSTEM STOP TTL MERGES recompute_minute")
        .expect("hold the island");
    engine
        .run(&format!(
            "INSERT INTO recompute_minute (fleet_id, semantics_version, discriminator, class, \
             workload_class, bucket, events, tokens) VALUES \
             {}, {}, {}",
            row("toStartOfMinute(now() - INTERVAL 2 HOUR)"),
            row("toStartOfMinute(now() - INTERVAL 1 HOUR)"),
            row("toStartOfMinute(now() - INTERVAL 30 DAY)"),
        ))
        .expect("seed minute cells");
    let island = engine
        .run(&format!(
            "SELECT count() FROM recompute_minute WHERE fleet_id = '{fleet}' \
             AND bucket < now() - INTERVAL 20 DAY"
        ))
        .expect("count island");
    assert_eq!(
        island.trim(),
        "1",
        "the island must be present at read time, or this pin proves nothing"
    );

    // Next to the live edge: the fresh cells cover it, minute is honest.
    let recent = Window {
        start_ms: now_ms - 3.0 * HOUR,
        end_ms: now_ms,
    };
    assert_eq!(
        dao.resolve_grain(&recent, None, MinuteTier::Fleet)
            .expect("resolve recent"),
        Grain::Minute,
        "fresh cells inside the horizon serve minute grain"
    );
    // Next to the island: an unbounded probe reached back to it and answered minute here.
    let beside_island = Window {
        start_ms: now_ms - 20.0 * DAY,
        end_ms: now_ms - 20.0 * DAY + 3.0 * HOUR,
    };
    assert_ne!(
        dao.resolve_grain(&beside_island, None, MinuteTier::Fleet)
            .expect("resolve beside the island"),
        Grain::Minute,
        "a cell past the horizon is not reach: the probe is bounded by the same horizon the \
         TTL reads, so an unmerged island cannot make an empty window resolve to minute"
    );
}

/// **The minute tier serves the model cut**: `recompute_minute` carries `model`, so the
/// recompute figures answer `dims=model` at minute grain as the banded read does. Two models'
/// cells in
/// the last hours, one read, two rows keyed by model.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_minute_tier_serves_the_model_cut() {
    const HOUR: f64 = 3_600_000.0;
    let (engine, _db) = fixture();
    let fleet = "minute-model";
    let (_w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    let row = |model: &str, bucket_sql: &str| {
        format!(
            "('{fleet}', '{sem}', '{disc}', 'ETR', 'undeclared', '{model}', {bucket_sql}, \
             10, 100)",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
        )
    };
    engine
        .run(&format!(
            "INSERT INTO recompute_minute (fleet_id, semantics_version, discriminator, class, \
             workload_class, model, bucket, events, tokens) \
             VALUES {}, {}",
            row("m1", "toStartOfMinute(now() - INTERVAL 2 HOUR)"),
            row("m2", "toStartOfMinute(now() - INTERVAL 1 HOUR)"),
        ))
        .expect("seed two models' minute cells");
    let w = Window {
        start_ms: now_ms - 3.0 * HOUR,
        end_ms: now_ms,
    };
    let rows = dao
        .waste_by(&w, Grain::Minute, &[Dim::Model], &Scope::default())
        .expect("the model cut at minute grain");
    let mut models: Vec<String> = rows
        .iter()
        .map(|r| {
            assert_eq!(
                r.dims.len(),
                1,
                "one dimension asked, one carried: {:?}",
                r.dims
            );
            assert_eq!(r.dims[0].0, Dim::Model);
            r.dims[0].1.clone()
        })
        .collect();
    models.sort();
    models.dedup();
    assert_eq!(
        models,
        vec!["m1", "m2"],
        "one row per model, keyed by model"
    );
}

/// **The hour tier of the copies read serves from the hourly rollup.** Hourly rows alone, no
/// minute cell anywhere, and the read answers at hour grain bucket for bucket.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
#[allow(clippy::cast_possible_truncation)] // epoch millis, floored to a bucket: exact in i64
fn the_copies_read_serves_the_hour_tier_from_the_hourly_rollup() {
    const HOUR: f64 = 3_600_000.0;
    let (engine, _db) = fixture();
    let fleet = "copies-hourly";
    let (_w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    let hour_start = |ms: f64| ((ms / HOUR).floor() * HOUR) as i64;
    let (h2, h1) = (hour_start(now_ms - 2.0 * HOUR), hour_start(now_ms - HOUR));
    engine
        .run(&format!(
            "INSERT INTO replication_hourly (fleet_id, semantics_version, discriminator, \
             workload_class, copies_band, locality, bucket_ms, token_ms, copies_token_ms) VALUES \
             ('{fleet}', '{sem}', '{disc}', '', '2', 'cross', {h2}, 10.0, 20.0), \
             ('{fleet}', '{sem}', '{disc}', '', '2', 'cross', {h1}, 5.0, 10.0)",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
        ))
        .expect("seed hourly rows");
    let w = Window {
        start_ms: now_ms - 3.0 * HOUR,
        end_ms: now_ms,
    };
    let data = dao
        .copies(&w, Grain::Hour, None)
        .expect("copies at hour grain");
    let buckets: Vec<(i64, f64)> = data
        .rows
        .iter()
        .map(|b| (b.bucket_ms, b.bands_token_ms[0]))
        .collect();
    assert_eq!(
        buckets,
        vec![(h2, 10.0), (h1, 5.0)],
        "the hourly rows are the buckets"
    );
    assert!(
        (data.window_dup_token_ms - 15.0).abs() < 1e-9,
        "the window's duplicated mass sums the same rows: {}",
        data.window_dup_token_ms
    );
}

/// **A repaired old day cannot fake the copies minute reach**. The repair
/// writes a day's whole difference onto that day's first minute; with the minute cells on
/// the seven-day knob such a cell is past the horizon and lingers until a merge, and the
/// reach probe must not read it as coverage. The hour tier's reach is its own.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
#[allow(clippy::cast_possible_truncation)] // epoch millis, floored to a bucket: exact in i64
fn a_repaired_old_day_does_not_fake_the_copies_minute_reach() {
    const HOUR: f64 = 3_600_000.0;
    const DAY: f64 = 86_400_000.0;
    let (engine, _db) = fixture();
    let fleet = "copies-reach";
    let sources = bare(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    let minute = |ms: f64| ((ms / 60_000.0).floor() * 60_000.0) as i64;
    // The island lingers only until a TTL merge, and nothing bounds when one runs: stopped, the
    // precondition below holds by construction rather than by the merge scheduler's timing.
    engine
        .run("SYSTEM STOP TTL MERGES replication_minute")
        .expect("hold the island");
    engine
        .run(&format!(
            "INSERT INTO replication_minute (fleet_id, semantics_version, discriminator, \
             workload_class, copies_band, locality, minute_ms, token_ms, copies_token_ms) VALUES \
             ('{fleet}', '{sem}', '{disc}', '', '2', 'cross', {fresh}, 10.0, 20.0), \
             ('{fleet}', '{sem}', '{disc}', '', '2', 'cross', {island}, 1.0, 2.0)",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
            fresh = minute(now_ms - HOUR),
            island = minute(now_ms - 30.0 * DAY),
        ))
        .expect("seed a fresh cell and a repaired-old-day island");
    let count = engine
        .run(&format!(
            "SELECT count() FROM replication_minute WHERE fleet_id = '{fleet}' \
             AND minute_ms < {}",
            (now_ms - 20.0 * DAY) as i64
        ))
        .expect("island present");
    assert_eq!(count.trim(), "1", "the island must be present at read time");
    let reach = dao
        .replication_reach_ms(Grain::Minute)
        .expect("reach")
        .expect("a fresh cell is reach");
    assert!(
        reach >= now_ms - 8.0 * DAY,
        "a cell past the minute horizon is not reach: {reach}"
    );
    assert!(reach <= now_ms - 0.5 * HOUR, "the fresh cell is: {reach}");
    assert_eq!(
        dao.replication_reach_ms(Grain::Hour).expect("hour reach"),
        None,
        "no hourly rows, no hour reach — each tier's reach is its own table's"
    );
}

/// **The minute tier serves a node and a rank from the rank cells**. Three ranks across two nodes in the last hour,
/// no fleet cell anywhere: an instance scope sums the node's ranks, a holder scope reads the
/// rank whatever cache group it names, the instance cut is a row per node, a holder cut is
/// refused by name, and the resolver picks minute from the rank table's own reach.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_minute_tier_serves_a_node_and_a_rank_from_the_rank_cells() {
    const HOUR: f64 = 3_600_000.0;
    let (engine, _db) = fixture();
    let fleet = "rank-minute";
    let (_w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let meaning = analyzer_analysis::Discriminator::of(&Params::default());
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    let row = |inst: &str, rank: u32, events: u32| {
        format!(
            "('{fleet}', '{sem}', '{disc}', '{inst}', {rank}, \
             toStartOfMinute(now() - INTERVAL 1 HOUR), 'ETR', 'undeclared', {events}, \
             {tokens})",
            sem = analyzer_analysis::SEMANTICS_VERSION,
            disc = meaning.as_str(),
            tokens = events * 10,
        )
    };
    engine
        .run(&format!(
            "INSERT INTO recompute_rank_minute (fleet_id, semantics_version, discriminator, \
             instance_id, dp_rank, bucket, class, workload_class, events, tokens) \
             VALUES {}, {}, {}",
            row("i1", 0, 4),
            row("i1", 1, 6),
            row("i2", 0, 5),
        ))
        .expect("seed three ranks across two nodes");
    let w = Window {
        start_ms: now_ms - 3.0 * HOUR,
        end_ms: now_ms,
    };
    let events = |rows: &[analyzer_serving_clickhouse::WasteRow]| -> u64 {
        rows.iter().map(|r| r.events).sum()
    };
    let node = dao
        .waste_by(
            &w,
            Grain::Minute,
            &[Dim::Class],
            &Scope {
                instance: Some("i1".to_string()),
                ..Scope::default()
            },
        )
        .expect("instance scope at minute");
    assert_eq!(events(&node), 10, "an instance scope sums the node's ranks");
    let rank = dao
        .waste_by(
            &w,
            Grain::Minute,
            &[Dim::Class],
            &Scope {
                holder: Some(analyzer_analysis::intervals::Holder {
                    instance: "i1".to_string(),
                    dp_rank: 1,
                    group_idx: 7,
                }),
                ..Scope::default()
            },
        )
        .expect("holder scope at minute");
    assert_eq!(
        events(&rank),
        6,
        "a holder scope reads the rank whatever group it names"
    );
    let by_node = dao
        .waste_by(&w, Grain::Minute, &[Dim::Instance], &Scope::default())
        .expect("the instance cut at minute");
    let mut nodes: Vec<(String, u64)> = by_node
        .iter()
        .map(|r| (r.dims[0].1.clone(), r.events))
        .collect();
    nodes.sort();
    assert_eq!(nodes, vec![("i1".to_string(), 10), ("i2".to_string(), 5)]);
    assert!(
        dao.waste_by(&w, Grain::Minute, &[Dim::Holder], &Scope::default())
            .is_err(),
        "a holder cut has no minute table and is refused by name"
    );
    assert_eq!(
        dao.resolve_grain(&w, None, MinuteTier::Rank)
            .expect("resolve"),
        Grain::Minute,
        "the resolver picks minute from the rank table's own reach"
    );
}

/// The tag grouped read: each declared label grouped byte for byte, its stores attributed, its
/// exposure from the declared intervals clipped to the classified edge, and its two figures.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_tag_read_groups_by_label_with_exposure_and_both_figures() {
    let (engine, _db) = fixture();
    let fleet = "tags";
    let sources = bare(&engine, fleet);
    let at = analyzer_serving_clickhouse::engine::testing::recent_day_base_ms();
    let hb = |t: f64| {
        serde_json::json!({"kind": "heartbeat", "at_ms": t, "incarnation": "run-1",
            "canary": "k1", "msgs_seen": 10, "dropped": 0, "oversized": 0,
            "heartbeat_secs": 60, "max_segment_secs": 300,
            "endpoints": [{"source": "i0", "endpoint": "tcp://e:1", "msgs_seen": 10,
                           "dropped": 0, "last_msg_at_ms": t}]})
    };
    let cache = |kind: &str, block: &str, tokens: Option<i64>, t: f64| {
        let mut r = serde_json::json!({"kind": kind, "instance_id": "i0", "dp_rank": 0,
            "group_idx": 0, "block_id": block, "content_id": block, "at_ms": t,
            "incarnation": "run-1"});
        if let Some(n) = tokens {
            r["n_tokens"] = n.into();
        }
        r
    };
    // "A" over b1 for a day, "B" over b2 and still open; both begin a minute before the window,
    // so no store sits on an interval's edge.
    let tag = |id: &str, identity: &str, label: &str, until: Option<f64>| {
        let mut r = serde_json::json!({"kind": "tag_interval", "declaration_id": id,
            "identity": identity, "tag": label, "from_ms": at - 60_000.0, "at_ms": at});
        if let Some(until) = until {
            r["until_ms"] = until.into();
        }
        r
    };
    ship_segment(
        &engine,
        fleet,
        &sources,
        &[
            serde_json::json!({"kind": "segment_open", "at_ms": at - 1000.0,
                "contract_version": "1.10", "incarnation": "run-1", "segment_seq": 0,
                "heartbeat_secs": 60, "max_segment_secs": 300}),
            serde_json::json!({"kind": "agent_start", "at_ms": at, "incarnation": "run-1",
                "canary": "k1", "heartbeat_secs": 60, "max_segment_secs": 300}),
            tag("d1", "b1", "A", Some(at + 86_400_000.0)),
            tag("d2", "b2", "B", None),
            hb(at),
            cache("store", "b1", Some(4), at + 10_000.0),
            cache("evict", "b1", None, at + 20_000.0),
            // a refetch: b1 stored again after its eviction
            cache("store", "b1", Some(16), at + 60_000.0),
            cache("store", "b2", Some(32), at + 120_000.0),
            hb(at + 600_000.0),
        ],
    );
    journal_faults::executor(&engine, fleet, &Params::default())
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("tick");
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let w = Window {
        start_ms: at,
        end_ms: at + 3_600_000.0,
    };
    let read = dao.tags(&w).expect("tags");
    let through = read.through_ms.expect("the tick classified");
    assert!(
        through > at + 120_000.0 && through < w.end_ms,
        "exposure stops at the classified edge, inside the window: {through}"
    );
    let minutes = (through - at) / 60_000.0;
    let labels: Vec<&str> = read.tags.iter().map(|(t, _)| t.as_str()).collect();
    assert_eq!(labels, ["A", "B"], "grouped byte for byte: {read:?}");

    let a = &read.tags[0].1;
    assert_eq!(a.identities, 1);
    assert!((a.identity_minutes - minutes).abs() < 1e-9, "{a:?}");
    assert_eq!(
        (a.sums.stores, a.sums.prefill_tokens, a.sums.refetch_tokens),
        (2, 20, 16),
        "both of b1's stores, the second a refetch: {a:?}"
    );
    let primary = a.refetch_per_identity_minute.value.expect("exposure");
    assert!((primary - 16.0 / minutes).abs() < 1e-9, "{a:?}");
    assert_eq!(a.refetch_per_stored_token.value, Some(0.8));
    assert_eq!(
        a.refetch_per_identity_minute.interval_90, None,
        "one identity states no spread"
    );

    let b = &read.tags[1].1;
    assert!(
        (b.identity_minutes - minutes).abs() < 1e-9,
        "an open interval runs to the edge: {b:?}"
    );
    assert_eq!((b.sums.stores, b.sums.refetch_tokens), (1, 0), "{b:?}");
    assert_eq!(b.refetch_per_identity_minute.value, Some(0.0));
    assert!(
        read.window_ms > 0.0,
        "coverage rides beside the figures: {read:?}"
    );
}

/// Every read names its semantics version, so none sums across versions: a copy of every served
/// row under another version changes no read.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn no_read_sums_across_semantics_versions() {
    let (engine, _db) = fixture();
    let fleet = "versions";
    let (w, sources) = seed(&engine, fleet);
    let dao = Dao::new(
        &engine,
        sources.clone(),
        &Params::default(),
        journal_faults::spine(&engine, fleet),
        journal_faults::serving(&engine),
    );
    let day = analyzer_lake::dated::date_of_day(analyzer_analysis::emitted::day_index(w.start_ms));
    // Every read must succeed: two failures alike would compare equal and prove nothing.
    let reads = |dao: &Dao| {
        [
            format!(
                "{:?}",
                dao.waste_by(&w, Grain::Day, &[Dim::Workload], &Scope::default())
                    .expect("dao.waste_by")
            ),
            format!(
                "{:?}",
                dao.waste_by(&w, Grain::Minute, &[], &Scope::default())
                    .expect("dao.waste_by")
            ),
            format!(
                "{:?}",
                dao.bands_by(&w, Grain::Day, &[Dim::Workload], &Scope::default())
                    .expect("dao.bands_by")
            ),
            format!(
                "{:?}",
                dao.coverage(&w, analyzer_serving_clickhouse::CoverageBy::Instance)
                    .expect("dao.coverage")
            ),
            format!("{:?}", dao.block_days(&w, 10).expect("dao.block_days")),
            format!("{:?}", dao.tags(&w).expect("dao.tags")),
            format!("{:?}", dao.markers(&w).expect("dao.markers")),
            format!("{:?}", dao.placement(&w, 10).expect("dao.placement")),
            format!("{:?}", dao.whales(&w, 10).expect("dao.whales")),
            format!(
                "{:?}",
                dao.instance_duplication(&w, "i0", Grain::Hour)
                    .expect("dao.instance_duplication")
            ),
            format!(
                "{:?}",
                dao.copies(&w, Grain::Hour, None).expect("dao.copies")
            ),
            format!(
                "{:?}",
                dao.day_detail(&day, None, 10, None)
                    .expect("dao.day_detail")
            ),
            format!("{:?}", dao.edges().expect("dao.edges")),
        ]
        .join("\n")
    };
    let before = reads(&dao);
    let versioned: Vec<String> = engine
        .run(
            "SELECT DISTINCT table FROM system.columns \
             WHERE database = currentDatabase() AND name = 'semantics_version' ORDER BY table",
        )
        .expect("versioned tables")
        .lines()
        .map(str::to_string)
        .collect();
    assert!(versioned.len() > 10, "{versioned:?}");
    let mut copied = 0;
    for table in &versioned {
        engine
            .run(&format!(
                // Filtered in a subquery: in the outer select the name is the replacement's.
                "INSERT INTO {table} SELECT * REPLACE ('js-other' AS semantics_version) \
                 FROM (SELECT * FROM {table} WHERE semantics_version = '{}')",
                analyzer_analysis::SEMANTICS_VERSION
            ))
            .unwrap_or_else(|e| panic!("{table}: {e}"));
        copied += engine
            .run(&format!(
                "SELECT count() FROM {table} WHERE semantics_version = 'js-other'"
            ))
            .expect("count")
            .trim()
            .parse::<u64>()
            .expect("count");
    }
    assert!(copied > 0, "the other version's rows are in place");
    assert_eq!(
        reads(&dao),
        before,
        "a read summed across semantics versions"
    );
}
