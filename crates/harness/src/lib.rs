// SPDX-License-Identifier: AGPL-3.0-only

// A harness panics on every fixture failure by design: a fixture that limps on produces
// failures that look like product bugs. Its builders are called for their effects as often as
// for their values.
#![allow(clippy::missing_panics_doc, clippy::must_use_candidate)]

//! The test harness every suite shares: the pinned-engine fixture, fixture-authored streams
//! planted under a dated tree as a shipper delivers them and ingested byte for byte, and the
//! execution journal's fault store.
//!
//! Timestamps are REBASED to a recent day boundary before ingest (a constant shift: every
//! fixture relationship is a delta, so nothing changes semantically) per the TTL-race rule:
//! expired data belongs only where expiry is the assertion.

/// The naive segment-reading `EventSource`, kept in the TEST tree on purpose.
///
/// **Nothing in production reads it.** It exists to be the second implementation the production
/// source can disagree with. The production reader is `pipeline::parquet_source` — dated
/// discovery, the content cache, the pruned columnar decode — and this stays here as the plain
/// whole-prefix reading it is differentialled against.
pub mod bucket_source;

pub mod journal_faults;
/// The test-tree Parquet writer: the acceptance corpus's shipped-form twins, dev-only.
pub mod parquet;

use analyzer_serving_clickhouse::engine::testing::{self, TestDatabase};
use analyzer_serving_clickhouse::engine::{migrate, Engine, RetentionConfig};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

static DIR_SEQ: AtomicU32 = AtomicU32::new(0);

/// A day in milliseconds, for fixture arithmetic.
pub use analyzer_analysis::DAY_MS;
/// The workspace root, where the contract corpora live.
pub use analyzer_serving_clickhouse::engine::testing::repo_root;

/// A fresh scratch directory of this process's own, named for `label`.
pub fn fresh_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "analyzer-pipeline-test-{label}-{}-{}",
        std::process::id(),
        DIR_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).expect("mkdir");
    dir
}

/// A migrated database of this test's own, on the suite's shared server — the
/// SHIPPED backend. The guard keeps the server alive and takes the database with it, so
/// a test's cleanup is dropping it rather than deleting a directory.
///
/// **One per TEST, which is not the same as one per CASE.** Test functions in a binary run on
/// parallel threads, so a test that shares its database with another test is a race. A test that
/// loops over fixture cases is the opposite situation: the cases are sequential, so they can share
/// one database and [`reset`] between them. Two suites paid a database and a migration per case
/// before anyone measured it.
pub fn fresh_engine() -> (Engine, TestDatabase) {
    let (engine, db) = testing::database("analysis");
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    (engine, db)
}

/// Empty every table, DISCOVERED rather than enumerated — the isolation a case-loop uses in
/// place of a fresh database.
///
/// A hand-written truncate list is a second copy of the schema, and it fails silently when the
/// two drift: a table nobody listed keeps the previous case's rows, the next case derives over
/// both, and the golden that catches it is whichever one happens to read that table. The list
/// comes from `system.tables`, so a table added by a migration is reset the day it exists.
///
/// The row count afterwards is what makes this a check rather than an intention. It reads
/// `system.parts` instead of counting each table, so it cannot be satisfied by the same
/// enumeration it is meant to verify.
///
/// # Panics
/// If the database has no tables (the migration did not run), or if rows survive.
pub fn reset(engine: &Engine) {
    let listed = engine
        .run(
            "SELECT name FROM system.tables WHERE database = currentDatabase() \
             AND engine NOT LIKE '%View' AND name != 'schema_migrations' ORDER BY name",
        )
        .expect("list the tables");
    let tables: Vec<&str> = listed
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        !tables.is_empty(),
        "no tables to reset — the migration did not run against this database"
    );
    for table in &tables {
        engine
            .run(&format!(
                "TRUNCATE TABLE {}",
                analyzer_serving_clickhouse::engine::quote_ident(table)
            ))
            .unwrap_or_else(|e| panic!("truncate {table}: {e}"));
    }
    let remaining = engine
        .run(
            "SELECT sum(rows) FROM system.parts WHERE database = currentDatabase() \
             AND active AND table != 'schema_migrations'",
        )
        .expect("count what survived");
    assert_eq!(
        remaining.trim(),
        "0",
        "rows survived the reset: the previous case leaks into the next one"
    );
    // The lake beside the tables: the database's spine, its chunks, journal and input log.
    let dir = spine_dir(engine);
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("clear the spine {}: {e}", dir.display()),
    }
}

pub use journal_faults::{executor, namespace, revision, serving, spine, spine_dir};

/// Wait for every mutation on this test's database to apply.
///
/// A whole-day write retires the day's older cell rows — a previous version, a key the fold
/// no longer produces, a row nothing produced — with a lightweight delete the tick does NOT
/// wait for (`retire_older_tables` in `analyzer_serving_clickhouse::persist`: the served rows
/// are eventually consistent by design, and a tick that waited on its own mutations under held
/// merges would stall). So a read of the served rows the instant a tick returns can still see
/// a departed key. The suite's server is shared by every test in the binary, and the
/// mutation pool's latency is the other tests' load: milliseconds on a quiet server, seconds
/// under twenty-eight parallel tests. A test that asserts the SETTLED day — a departed
/// key gone, two databases' dumps equal — waits here first; a test that asserts a key a tick
/// re-stated needs nothing, since `FINAL` retires the older version of a key that stays.
///
/// # Panics
/// If a mutation is still pending after a minute — the pending list is the message, since a
/// mutation that failed stays pending forever and a spin would hide it.
pub fn settled(engine: &Engine) {
    const PENDING: &str = "SELECT count() FROM system.mutations \
                           WHERE database = currentDatabase() AND is_done = 0";
    for _ in 0..1200 {
        let pending = engine.run(PENDING).expect("count the pending mutations");
        if pending.trim() == "0" {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let listed = engine
        .run(
            "SELECT table, command, latest_fail_reason FROM system.mutations \
             WHERE database = currentDatabase() AND is_done = 0",
        )
        .unwrap_or_default();
    panic!("mutations still pending after 60 s — the retire never applied:\n{listed}");
}

/// The dated hour prefix (`<yyyy-mm-dd>/<hh>`) a stamp lands under — the bucket layout the
/// segment-reading source discovers by.
pub fn dated_hour_of(ms: f64) -> String {
    analyzer_lake::dated::hour_prefixes(ms, ms)
        .pop()
        .expect("a finite stamp names an hour")
}

/// One segment into a dated tree under `root`: `tap/<hour>/n1/<name>`, the bucket's own layout.
pub fn plant_dated(root: &std::path::Path, hour: &str, name: &str, bytes: &[u8]) {
    plant_at(root, &format!("tap/{hour}/n1/{name}"), bytes);
}

/// One segment under `root` at the key a shipper would give it: `prefix` and the dated hour of
/// `at_ms`, node `n1`, then `name`. Returns the key.
pub fn plant_segment(
    root: &std::path::Path,
    prefix: &str,
    at_ms: f64,
    name: &str,
    bytes: &[u8],
) -> String {
    let key = analyzer_lake::dated::segment_key(prefix, at_ms, "n1", name)
        .expect("a finite stamp names a dated key");
    plant_at(root, &key, bytes);
    key
}

fn plant_at(root: &std::path::Path, key: &str, bytes: &[u8]) {
    let path = root.join(key);
    std::fs::create_dir_all(path.parent().expect("a key names a directory")).expect("mkdir dated");
    std::fs::write(path, bytes).expect("write segment");
}

/// The wall clock, epoch milliseconds.
pub fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

/// A recent, day-aligned rebase offset keeping all fixture times inside the raw TTL.
pub fn rebase_offset() -> f64 {
    (now_ms() / DAY_MS).floor() * DAY_MS - 3.0 * DAY_MS
}

/// Bind the harness's source identity for `fleet`, once.
pub fn bind_source(engine: &Engine, fleet: &str) {
    // The harness owns this source identity. Namespace-isolation cases bind their
    // explicit tenants before inserting the same records through this helper.
    let serving = serving(engine);
    if serving.bound().expect("binding").is_none() {
        serving
            .bind(&analyzer_pipeline::execution::Namespace::new("fixture-source", fleet).unwrap())
            .unwrap();
    }
}

/// Ingest one segment's bytes into `fleet`'s input log and publish the input tables, as the
/// daemon does after each ingest cycle.
///
/// # Errors
/// The ingest refused the bytes.
pub fn ingest_bytes(
    engine: &Engine,
    fleet: &str,
    source: &str,
    bytes: &[u8],
) -> Result<analyzer_ingest::IngestReport, analyzer_ingest::Error> {
    bind_source(engine, fleet);
    let report = analyzer_ingest::ingest_bytes(&lake(engine, fleet), fleet, source, bytes)?;
    publish_inputs(engine, fleet);
    Ok(report)
}

/// Publish the served input tables through everything the input log committed, as the
/// daemon does after each ingest cycle.
pub fn publish_inputs(engine: &Engine, fleet: &str) {
    executor(engine, fleet, &analyzer_analysis::Params::default())
        .and_then(|executor| executor.publish_inputs())
        .expect("publish the input tables");
}

/// Ingest into the input log of a spine the caller built: the log that spine's passes read.
///
/// # Errors
/// The ingest refused the bytes.
pub fn ingest_bytes_into(
    engine: &Engine,
    lake: &analyzer_pipeline::Lake,
    fleet: &str,
    source: &str,
    bytes: &[u8],
) -> Result<analyzer_ingest::IngestReport, analyzer_ingest::Error> {
    bind_source(engine, fleet);
    analyzer_ingest::ingest_bytes(lake, fleet, source, bytes)
}

/// The input log `engine`'s default spine reads, for the bound namespace of `fleet`: where
/// ingest writes so a tick finds it.
pub fn lake(engine: &Engine, fleet: &str) -> analyzer_pipeline::Lake {
    spine(engine, fleet).lake()
}

/// The input log of `fleet`'s bound namespace, or `None` before one is bound.
pub fn lake_if_bound(engine: &Engine, fleet: &str) -> Option<analyzer_pipeline::Lake> {
    serving(engine)
        .bound()
        .ok()
        .flatten()
        .filter(|namespace| namespace.fleet() == fleet)
        .map(|namespace| {
            analyzer_pipeline::spine::local(&spine_dir(engine), &namespace)
                .expect("local spine")
                .lake()
        })
}

/// Expire what arrived: the input log drops every day it holds, as retention would; its
/// positions stand.
pub fn expire_input(engine: &Engine, fleet: &str) {
    lake(engine, fleet)
        .ledger(analyzer_pipeline::ledger::SHARD)
        .expect("input log")
        .retire_before(i64::MAX)
        .expect("retire the input log");
}

/// A `ParquetSource` over one raw segment's bytes, planted under a dated tree keyed by the
/// records' own earliest stamp — the harness shape for corpora that arrive as whole files,
/// where byte identity with the ingested copy is the point.
pub fn source_over_bytes(label: &str, bytes: &[u8]) -> ParquetSource<FsStore> {
    let min_at = min_at_ms_of_jsonl(bytes).expect("a segment carries at least one at_ms");
    let root = fresh_dir(&format!("{label}-segments"));
    plant_dated(&root, &dated_hour_of(min_at), "records.jsonl", bytes);
    source_over_tree(label, root, min_at)
}

/// A `ParquetSource` over many raw segments, each planted at its own records' earliest hour.
pub fn source_over_segments(label: &str, entries: &[(String, Vec<u8>)]) -> ParquetSource<FsStore> {
    let root = fresh_dir(&format!("{label}-segments"));
    let mut min_at = f64::INFINITY;
    for (name, bytes) in entries {
        let at = min_at_ms_of_jsonl(bytes).expect("a segment carries at least one at_ms");
        min_at = min_at.min(at);
        plant_dated(&root, &dated_hour_of(at), name, bytes);
    }
    source_over_tree(label, root, min_at)
}

/// A `ParquetSource` over the dated tree at `root`, reaching stamps as old as `min_at`. The
/// corpora are JSONL, the write-ahead form the corpus seam admits.
pub fn source_over_tree(label: &str, root: PathBuf, min_at: f64) -> ParquetSource<FsStore> {
    ParquetSource::new(
        FsStore::new(root),
        fresh_dir(&format!("{label}-cache")),
        "tap/",
        analyzer_pipeline::parquet_source::horizon_reaching(min_at),
    )
    .admitting_corpus_forms()
}

/// The same source as [`source_over_tree`], as the configuration a daemon or an API fixture
/// opens its sources from.
pub fn directory_sources(root: PathBuf, cache_dir: PathBuf, min_at: f64) -> SourceConfig {
    SourceConfig {
        transport: SegmentTransport::Directory { root },
        key_prefix: "tap/".to_string(),
        cache_dir,
        horizon_hours: HorizonHours::fixed(analyzer_pipeline::parquet_source::horizon_reaching(
            min_at,
        )),
        admit_corpus_forms: true,
        cache_budget_bytes: analyzer_pipeline::parquet_source::DEFAULT_SEGMENT_CACHE_BYTES,
    }
}

fn min_at_ms_of_jsonl(bytes: &[u8]) -> Option<f64> {
    let text = String::from_utf8_lossy(bytes);
    let min = text
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|r| r.get("at_ms").and_then(Value::as_f64))
        .fold(f64::INFINITY, f64::min);
    min.is_finite().then_some(min)
}

use analyzer_pipeline::parquet_source::{
    FsStore, HorizonHours, ParquetSource, SegmentTransport, SourceConfig,
};

/// Seed a fixture-authored record stream: its JSONL segment twins planted under a dated tree
/// the returned `ParquetSource` derives from, and the same bytes ingested into the input log,
/// so a tick's deliveries match the log byte for byte.
///
/// **The segment twins are written in the WIRE model.** Run-constant facts — `incarnation`,
/// `workload_class`, `key_epoch` — ride a segment's header and are stamped onto every record
/// behind it, so the records partition by that triple and each partition gets a file. A real
/// `segment_open` record is a header in its own right and takes its own file. A partition with
/// no open of its own gets a SYNTHETIC one carrying only the declarations, stamped `at_ms: 0`
/// and no `segment_seq`: epoch sits outside every wall clip and a seq-less open enters no
/// sequence accounting, so the synthetic header declares and does nothing else.
pub fn seed_source(
    engine: &Engine,
    fleet: &str,
    records: &[Value],
    offset: f64,
    label: &str,
) -> ParquetSource<FsStore> {
    seed_ingested_source(engine, fleet, records, offset, label)
}

/// Seed `records` as one tap run delivers them: a single sequenced segment of run `run-1`
/// ([`run_segment`]) with heartbeats at the first instant and a heartbeat interval after the
/// last, planted for the returned source and the same bytes ingested.
///
/// A keyed tick classifies against the arrivals the heartbeats state, and its edge is the run's
/// last heartbeat in a sequenced segment, so a cache-only fixture needs this framing to be read
/// through its newest record.
pub fn seed_run(
    engine: &Engine,
    fleet: &str,
    records: &[Value],
    offset: f64,
    label: &str,
) -> ParquetSource<FsStore> {
    let instants = records
        .iter()
        .filter_map(|r| r.get("at_ms").and_then(Value::as_f64));
    let first = instants.clone().fold(f64::INFINITY, f64::min);
    let last = instants.fold(f64::NEG_INFINITY, f64::max);
    let root = fresh_dir(&format!("{label}-segments"));
    ship_run_segment(
        engine,
        fleet,
        &root,
        records,
        offset,
        0,
        &[first, last + 60_000.0],
    );
    source_over_tree(label, root, first + offset)
}

/// Seed `records`, rebased by `offset`, as ONE segment in their own order: a fixture that
/// authors its own header, so the header's declarations reach exactly the records behind it.
/// Planted for the returned source and the same bytes ingested.
pub fn seed_segment(
    engine: &Engine,
    fleet: &str,
    records: &[Value],
    offset: f64,
    label: &str,
) -> ParquetSource<FsStore> {
    let bytes: String = records
        .iter()
        .map(|r| rebase_record(r, offset).to_string() + "\n")
        .collect();
    ingest_bytes(engine, fleet, &format!("{label}.jsonl"), bytes.as_bytes())
        .expect("ingest the segment");
    source_over_bytes(label, bytes.as_bytes())
}

/// Plant segment `seq` of run `run-1` ([`run_segment`]) under `root` and ingest the same bytes.
pub fn ship_run_segment(
    engine: &Engine,
    fleet: &str,
    root: &std::path::Path,
    records: &[Value],
    offset: f64,
    seq: u32,
    heartbeats: &[f64],
) {
    let (first, bytes) = run_segment(records, offset, seq, heartbeats);
    let name = format!("run-1-{seq}.jsonl");
    plant_dated(root, &dated_hour_of(first), &name, bytes.as_bytes());
    ingest_bytes(engine, fleet, &name, bytes.as_bytes()).expect("ingest the segment");
}

/// Segment `seq` of run `run-1` carrying `records`, rebased by `offset`: its open, a run start
/// when it is the first, a heartbeat at each of `heartbeats` (fixture instants) attesting every
/// instance the records name, and the records. Returns its first instant and its bytes.
pub fn run_segment(records: &[Value], offset: f64, seq: u32, heartbeats: &[f64]) -> (f64, String) {
    let rebased: Vec<Value> = records.iter().map(|r| rebase_record(r, offset)).collect();
    let first = rebased
        .iter()
        .filter_map(|r| r.get("at_ms").and_then(Value::as_f64))
        .chain(heartbeats.iter().map(|at| at + offset))
        .fold(f64::INFINITY, f64::min);
    assert!(first.is_finite(), "a segment carries at least one instant");
    let instances: std::collections::BTreeSet<&str> = rebased
        .iter()
        .filter_map(|r| r.get("instance_id").and_then(Value::as_str))
        .collect();
    let endpoints: Vec<Value> = instances
        .iter()
        .map(|i| {
            serde_json::json!({"source": i, "endpoint": format!("tcp://{i}"),
                               "msgs_seen": 1, "dropped": 0})
        })
        .collect();
    let mut open = serde_json::json!({"kind": "segment_open", "at_ms": first,
        "contract_version": "1.2", "incarnation": "run-1", "segment_seq": seq});
    if let Some((hb, seg)) = declared_bounds(records).values().next() {
        open["heartbeat_secs"] = serde_json::json!(hb);
        open["max_segment_secs"] = serde_json::json!(seg);
    }
    let mut lines = vec![open];
    if seq == 0 {
        lines.push(
            serde_json::json!({"kind": "agent_start", "incarnation": "run-1",
            "at_ms": first, "heartbeat_secs": 60, "max_segment_secs": 300}),
        );
    }
    lines.extend(heartbeats.iter().map(|at| {
        serde_json::json!({"kind": "heartbeat", "incarnation": "run-1", "at_ms": at + offset,
                           "msgs_seen": 1, "dropped": 0, "oversized": 0,
                           "endpoints": endpoints})
    }));
    lines.extend(rebased);
    (first, lines.iter().map(|l| l.to_string() + "\n").collect())
}

/// [`seed_source`]'s work: plant the stream, then ingest every planted file, so the input log
/// holds the batches a tick's deliveries name by content.
pub fn seed_ingested_source(
    engine: &Engine,
    fleet: &str,
    records: &[Value],
    offset: f64,
    label: &str,
) -> ParquetSource<FsStore> {
    let (root, min_at) = plant_tree(records, offset, label);
    for (key, bytes) in planted_files(&root) {
        ingest_bytes(engine, fleet, &key, &bytes).expect("ingest source fixture");
    }
    source_over_tree(label, root, min_at)
}

/// The segment-twin half of [`seed_source`], for streams that never touch the store.
///
/// **One file per (partition, UTC day), at the day's own dated hour.** Dated discovery reads
/// a window's hour range plus the listing margin, which prices ship-hour-vs-record-clock
/// skew — never a multi-day archive in one object, which no conforming shipper produces. A
/// fixture spanning days planted as one file would sit outside most windows' discovery range
/// and read as loss.
pub fn source_for_records(records: &[Value], offset: f64, label: &str) -> ParquetSource<FsStore> {
    let (root, min_at) = plant_tree(records, offset, label);
    source_over_tree(label, root, min_at)
}

/// The planting half of [`source_for_records`]: the dated segment tree and its earliest
/// stamp, returned so a test can assemble COMBINED trees from byte-identical wave files
/// (the slice-gait acceptance drives ingest and the source from one set of bytes).
pub fn plant_tree(records: &[Value], offset: f64, label: &str) -> (PathBuf, f64) {
    let root = fresh_dir(&format!("{label}-segments"));
    let mut files: Vec<(f64, String)> = Vec::new();

    // Real headers first, one file each: a batch refuses two opens, and an open's own
    // declarations must stamp exactly its own row.
    let mut partitions: std::collections::BTreeMap<(String, String, String, i64), Vec<String>> =
        std::collections::BTreeMap::new();
    let mut min_at = f64::INFINITY;
    for r in records {
        let rebased = rebase_record(r, offset);
        let at = rebased.get("at_ms").and_then(Value::as_f64);
        if let Some(at) = at {
            min_at = min_at.min(at);
        }
        if r.get("kind").and_then(Value::as_str) == Some("segment_open") {
            files.push((
                at.expect("a segment_open carries at_ms"),
                format!("{rebased}\n"),
            ));
            continue;
        }
        #[allow(clippy::cast_possible_truncation)] // epoch days fit i64
        let day = (at.expect("a record carries at_ms") / DAY_MS).floor() as i64;
        let key = (
            field_str(r, "incarnation"),
            field_str(r, "workload_class"),
            r.get("key_epoch")
                .and_then(Value::as_u64)
                .map(|e| e.to_string())
                .unwrap_or_default(),
            day,
        );
        partitions.entry(key).or_default().push(rebased.to_string());
    }
    assert!(min_at.is_finite(), "a fixture carries at least one at_ms");
    let bounds = declared_bounds(records);

    for ((incarnation, class, epoch, day), lines) in partitions {
        let mut body = String::new();
        if !(incarnation.is_empty() && class.is_empty() && epoch.is_empty()) {
            let mut header = serde_json::Map::new();
            header.insert("kind".into(), "segment_open".into());
            header.insert("at_ms".into(), serde_json::json!(0.0));
            header.insert("incarnation".into(), incarnation.clone().into());
            if !class.is_empty() {
                header.insert("workload_class".into(), class.into());
            }
            if !epoch.is_empty() {
                let e: u64 = epoch.parse().expect("epoch renders back");
                header.insert("key_epoch".into(), serde_json::json!(e));
            }
            if let Some((hb, seg)) = bounds.get(&incarnation) {
                header.insert("heartbeat_secs".into(), serde_json::json!(hb));
                header.insert("max_segment_secs".into(), serde_json::json!(seg));
            }
            body.push_str(&Value::Object(header).to_string());
            body.push('\n');
        }
        for line in lines {
            body.push_str(&line);
            body.push('\n');
        }
        #[allow(clippy::cast_precision_loss)] // epoch days are far below 2^53
        let day_start = day as f64 * DAY_MS;
        files.push((day_start.max(min_at), body));
    }

    for (i, (at, body)) in files.iter().enumerate() {
        plant_dated(
            &root,
            &dated_hour_of(*at),
            &format!("{i:04}.jsonl"),
            body.as_bytes(),
        );
    }
    (root, min_at)
}

/// Each run's liveness bounds as its records declare them, the larger where several do: run
/// declarations the wire carries on `segment_open` (spec 5.8), which a synthetic header states.
fn declared_bounds(records: &[Value]) -> std::collections::BTreeMap<String, (u64, u64)> {
    let mut bounds = std::collections::BTreeMap::new();
    for r in records {
        if let (Some(hb), Some(seg)) = (
            r.get("heartbeat_secs").and_then(Value::as_u64),
            r.get("max_segment_secs").and_then(Value::as_u64),
        ) {
            let b: &mut (u64, u64) = bounds.entry(field_str(r, "incarnation")).or_default();
            *b = (b.0.max(hb), b.1.max(seg));
        }
    }
    bounds
}

fn field_str(r: &Value, key: &str) -> String {
    r.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// The wire record with every engine/wall stamp shifted by the rebase.
fn rebase_record(r: &Value, offset: f64) -> Value {
    let mut o = r.as_object().expect("record object").clone();
    for key in [
        "at_ms",
        "window_start_ms",
        "window_end_ms",
        // holder_reset's engine-clock ordering instant — the same stamp family as
        // at_ms, and a fixture reset left unrebased would sort into 1970 and fall
        // outside every window
        "boundary_ms",
    ] {
        if let Some(t) = o.get(key).and_then(Value::as_f64) {
            o.insert(key.to_string(), serde_json::json!(t + offset));
        }
    }
    if let Some(endpoints) = o.get_mut("endpoints").and_then(Value::as_array_mut) {
        for e in endpoints {
            if let Some(eo) = e.as_object_mut() {
                if let Some(t) = eo.get("last_msg_at_ms").and_then(Value::as_f64) {
                    eo.insert("last_msg_at_ms".to_string(), serde_json::json!(t + offset));
                }
            }
        }
    }
    Value::Object(o)
}

/// The full engine-domain span of the fleet's cache facts, padded by 1 ms each side.
pub fn cache_window(engine: &Engine, fleet: &str) -> analyzer_analysis::Window {
    let committed = lake(engine, fleet)
        .ledger(analyzer_pipeline::ledger::SHARD)
        .expect("input log")
        .committed_since(0)
        .expect("committed batches");
    let min = committed
        .iter()
        .filter_map(|b| b.first_at_ms)
        .fold(f64::INFINITY, f64::min);
    let max = committed
        .iter()
        .filter_map(|b| b.last_at_ms)
        .fold(f64::NEG_INFINITY, f64::max);
    assert!(
        min.is_finite() && max.is_finite(),
        "the fleet has cache facts"
    );
    analyzer_analysis::Window {
        start_ms: min - 1.0,
        end_ms: max + 1.0,
    }
}

/// Every planted segment file under a source tree, path-sorted so two databases ingest
/// the same bytes in the same order — the gait suites' staging half.
pub fn planted_files(root: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "jsonl") {
                out.push((
                    path.to_string_lossy().to_string(),
                    std::fs::read(&path).expect("read file"),
                ));
            }
        }
    }
    out.sort();
    out
}

/// Copy one planted tree's files into a combined root, preserving the dated layout.
/// `prefix` disambiguates filenames across waves — two waves in one hour would otherwise
/// plant colliding `0000.jsonl` names and the copy would silently drop a wave.
pub fn copy_planted(
    files: &[(String, Vec<u8>)],
    src_root: &std::path::Path,
    dest_root: &std::path::Path,
    prefix: &str,
) -> Vec<(String, Vec<u8>)> {
    let mut copied = Vec::new();
    for (path, bytes) in files {
        let rel = path
            .strip_prefix(&src_root.to_string_lossy().to_string())
            .expect("relative")
            .trim_start_matches('/')
            .to_string();
        let renamed = match rel.rsplit_once('/') {
            Some((dir, name)) => format!("{dir}/{prefix}-{name}"),
            None => format!("{prefix}-{rel}"),
        };
        let dest = dest_root.join(&renamed);
        std::fs::create_dir_all(dest.parent().expect("parent")).expect("mkdirs");
        std::fs::write(&dest, bytes).expect("copy");
        copied.push((renamed, bytes.clone()));
    }
    copied
}

/// The product tables in the chaos drill's own projections. The gait suites' equivalence
/// surface.
#[allow(clippy::too_many_lines)] // one query per served table: the inventory IS the function
pub fn product_dump(engine: &Engine) -> String {
    // The settled tables: a dump taken while a whole-day write's retire is still pending
    // holds a departed key one side of an equivalence and not the other.
    settled(engine);
    let mut out = String::new();
    for (name, sql) in [
        // Every derived table is keyed replace, written by the fold: read latest per key and
        // not gone, the version left out (each run's own attempt clock).
        (
            "class_rollup_daily",
            "SELECT fleet_id, semantics_version, discriminator, class, workload_class, model, instance_id, dp_rank, group_idx, month, bucket, toInt64(events), toInt64(tokens) FROM class_rollup_daily FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "class_rollup_hourly",
            "SELECT fleet_id, semantics_version, discriminator, class, workload_class, model, instance_id, dp_rank, group_idx, bucket, toInt64(events), toInt64(tokens) FROM class_rollup_hourly FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "recompute_minute",
            "SELECT fleet_id, semantics_version, discriminator, class, workload_class, model, bucket, toInt64(events), toInt64(tokens) FROM recompute_minute FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "band_minute",
            "SELECT fleet_id, semantics_version, discriminator, class, band, workload_class, model, bucket, toInt64(events), toInt64(tokens) FROM band_minute FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "band_rollup_daily",
            "SELECT fleet_id, semantics_version, discriminator, class, band, workload_class, model, instance_id, dp_rank, group_idx, month, bucket, toInt64(events), toInt64(tokens) FROM band_rollup_daily FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "recompute_rank_minute",
            "SELECT fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, workload_class, model, toInt64(events), toInt64(tokens) FROM recompute_rank_minute FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "band_rank_minute",
            "SELECT fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, band, workload_class, model, toInt64(events), toInt64(tokens) FROM band_rank_minute FINAL WHERE gone = 0 ORDER BY ALL",
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
            "band_rollup_hourly",
            "SELECT fleet_id, semantics_version, discriminator, class, band, workload_class, model, instance_id, dp_rank, group_idx, bucket, toInt64(events), toInt64(tokens) FROM band_rollup_hourly FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "coverage_daily",
            "SELECT * EXCEPT (version, gone) FROM coverage_daily FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "replication_minute",
            "SELECT * EXCEPT (version, gone) FROM replication_minute FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "replication_hourly",
            "SELECT * EXCEPT (version, gone) FROM replication_hourly FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "replication_daily",
            "SELECT * EXCEPT (version, gone) FROM replication_daily FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "replication_pairs_day",
            "SELECT * EXCEPT (version, gone) FROM replication_pairs_day FINAL WHERE gone = 0 ORDER BY ALL",
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
            "replication_instance_spans_day",
            "SELECT * EXCEPT (version, gone) FROM replication_instance_spans_day FINAL WHERE gone = 0 ORDER BY ALL",
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
            "coverage_minute",
            "SELECT * EXCEPT (version, gone) FROM coverage_minute FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "tag_minute",
            "SELECT * EXCEPT (version, gone) FROM tag_minute FINAL WHERE gone = 0 ORDER BY ALL",
        ),
        (
            "markers",
            "SELECT * EXCEPT (version, gone) FROM markers FINAL WHERE gone = 0 ORDER BY ALL",
        ),
    ] {
        out.push_str(name);
        out.push('\n');
        out.push_str(&engine.run(sql).expect(name));
        out.push('\n');
    }
    out
}

/// One keyed tick over everything the fleet's log committed, through the executor: what the
/// daemon runs on each cycle.
///
/// # Errors
/// The tick failed.
pub fn advance(
    engine: &Engine,
    source: &dyn analyzer_pipeline::source::FillSource,
    fleet: &str,
    params: &analyzer_analysis::Params,
) -> Result<analyzer_pipeline::executor::Advance, analyzer_pipeline::Error> {
    advance_on(engine, spine(engine, fleet), source, params)
}

/// [`advance`] over a spine the caller built rather than the database's own. Every tick's
/// sampled audit must agree: a disagreement is a defect in the keyed path, whatever the test
/// was holding.
///
/// # Errors
/// The tick failed.
pub fn advance_on(
    engine: &Engine,
    spine: std::sync::Arc<analyzer_pipeline::Spine>,
    source: &dyn analyzer_pipeline::source::FillSource,
    params: &analyzer_analysis::Params,
) -> Result<analyzer_pipeline::executor::Advance, analyzer_pipeline::Error> {
    let advance =
        analyzer_pipeline::Executor::open(spine, params, serving(engine))?.advance(source)?;
    if let Some(d) = advance.audit.disagreements.first() {
        panic!("the sampled audit disagreed: {d}");
    }
    assert_eq!(
        advance.audit_edge.disagreed_at_ms, None,
        "the audit edge is held"
    );
    Ok(advance)
}

/// A spine over `store` for the fleet bound on `engine`, for tests that need the executor's
/// journal in a store they control.
pub fn spine_over(
    engine: &Engine,
    store: std::sync::Arc<dyn analyzer_objectstore::ObjectStore + Send + Sync>,
    fleet: &str,
) -> std::sync::Arc<analyzer_pipeline::Spine> {
    std::sync::Arc::new(
        analyzer_pipeline::Spine::new(
            store,
            "",
            &namespace(engine, fleet),
            analyzer_pipeline::lineage::Lineage::of_data_dir(engine.data_dir()).expect("lineage"),
        )
        .with_working_state(
            &engine
                .data_dir()
                .join(analyzer_pipeline::spine::WORKING_STATE),
            analyzer_lake::working::DEFAULT_MEMORY_BYTES,
        )
        .expect("working state"),
    )
}
