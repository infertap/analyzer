// SPDX-License-Identifier: AGPL-3.0-only

//! Golden responses per endpoint family, against data a tick actually wrote.
//!
//! **This suite is what takes the no-naked-numbers law off review** (`docs/read-api.md` section 2.2):
//! [`the_figure_walk`] runs over every response this file exercises.
//!
//! **Not a mock.** A tick runs, writes the rollups and the cells, and the API
//! serves what the DAO reads back. A golden over hand-built JSON proves only that `json!` works.

#![allow(clippy::float_cmp)] // token counts are integers riding as f64; exact is the right test

use harness::journal_faults;

use analyzer_analysis::{Params, Window};
use analyzer_api::{ApiConfig, ApiState};
use serde_json::Value;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

/// Every `$ref` in the generated document resolves to a registered component. The contract job
/// diffs committed-vs-emitted — a FRESHNESS check, satisfied by two identically broken
/// documents — so it never asks "does the spec resolve?". This asks exactly that.
#[test]
fn every_schema_ref_in_the_contract_resolves() {
    let doc = serde_json::to_value(analyzer_api::openapi::document()).expect("document");
    let mut refs: Vec<String> = Vec::new();
    collect_refs(&doc, &mut refs);
    assert!(
        !refs.is_empty(),
        "a contract with no $refs is a parse bug here"
    );
    let schemas = doc
        .pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("components.schemas");
    for r in refs {
        let Some(name) = r.strip_prefix("#/components/schemas/") else {
            panic!("a $ref outside components.schemas: {r}");
        };
        assert!(
            schemas.contains_key(name),
            "dangling $ref: {r} — register the wire type in openapi.rs's schema list"
        );
    }
}

fn collect_refs(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            for (k, val) in map {
                if k == "$ref" {
                    if let Some(s) = val.as_str() {
                        out.push(s.to_string());
                    }
                }
                collect_refs(val, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_refs(item, out);
            }
        }
        _ => {}
    }
}

/// A fixed instant, so a lag figure is asserted rather than raced. The injected clock and the
/// batch's insert stamp derive from the same recent base: a fixture timestamp is a delta, never
/// an absolute (`testing::recent_day_base_ms`).
fn now_ms() -> f64 {
    analyzer_serving_clickhouse::engine::testing::recent_day_base_ms() + 3.0 * 3_600_000.0
}

/// When the seeded batch was ingested — two hours before the injected now, so `lag_ms` has a
/// known value that does not depend on when the suite runs.
///
/// Whole seconds by construction: the column is a `DateTime`, and deriving the millisecond form
/// FROM the second keeps the seeded row and the expected lag exact rather than agreeing to
/// within a rounding.
fn batch_ingested_secs() -> i64 {
    #[allow(clippy::cast_possible_truncation)] // a day-aligned base over a f64 mantissa
    let base = (analyzer_serving_clickhouse::engine::testing::recent_day_base_ms() / 1000.0) as i64;
    base + 3_600
}

struct Fixture {
    sources: analyzer_pipeline::SourceConfig,
    state: ApiState,
    engine: Arc<Mutex<analyzer_serving_clickhouse::engine::Engine>>,
    _db: analyzer_serving_clickhouse::engine::testing::TestDatabase,
    window: Window,
}

impl Fixture {
    /// What a test body needs, without the ownership that must not cross into async.
    fn handle(&self) -> Handle {
        Handle {
            sources: self.sources.clone(),
            state: self.state.clone(),
            engine: Arc::clone(&self.engine),
            window: self.window,
        }
    }
}

/// The cheap half of a fixture: clones of the shared state, no `TestDatabase` and no sole
/// ownership of the engine.
#[derive(Clone)]
struct Handle {
    sources: analyzer_pipeline::SourceConfig,
    state: ApiState,
    /// The same engine the state reads, for a test that seeds rows beside the reads.
    engine: Arc<Mutex<analyzer_serving_clickhouse::engine::Engine>>,
    window: Window,
}

fn fixture(name: &str) -> Fixture {
    let fixture = classify_fixture(name, |_| {});
    {
        let engine = fixture.engine.lock().expect("engine");
        wait_for_retirement(&engine);
        assert!(
            journal_faults::executor(&engine, name, &Params::default())
                .unwrap()
                .revision()
                .unwrap()
                .is_some(),
            "ordinary fixtures require settled, committed output"
        );
    }
    fixture
}

// Classification acknowledges replacement before obsolete-row retirement finishes.
// Ordinary API fixtures promise settled output; fault tests can use classify_fixture
// directly to keep that distinct state observable. Never sleep a guessed fixed delay.
fn wait_for_retirement(engine: &analyzer_serving_clickhouse::engine::Engine) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let pending = engine.run("SELECT count() FROM system.mutations WHERE database = currentDatabase() AND is_done = 0").expect("retirement standing");
        if pending.trim() == "0" {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "retirement did not finish in {}: {}", engine.identity(),
            engine.run("SELECT table, mutation_id, command, parts_to_do, latest_fail_reason FROM system.mutations WHERE database = currentDatabase() AND is_done = 0 FORMAT JSONEachRow").expect("pending mutation diagnostics"));
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Ship one more segment of the fixture's run: `records` after a sequenced open at `seq`
/// declaring `epoch` when given, written into the source's tree and ingested, as a shipper
/// delivers it.
fn ship(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    sources: &analyzer_pipeline::SourceConfig,
    fleet: &str,
    (seq, at, epoch): (i64, f64, Option<u32>),
    records: &[Value],
) {
    let analyzer_pipeline::parquet_source::SegmentTransport::Directory { root } =
        &sources.transport
    else {
        panic!("the fixture's source is a directory");
    };
    let mut open = serde_json::json!({"kind": "segment_open", "at_ms": at - 1000.0,
        "contract_version": "1.10", "incarnation": "run-1", "segment_seq": seq,
        "heartbeat_secs": 60, "max_segment_secs": 300});
    if let Some(epoch) = epoch {
        open["key_epoch"] = epoch.into();
    }
    let mut body = format!("{open}\n");
    for r in records {
        body.push_str(&r.to_string());
        body.push('\n');
    }
    let name = format!("{seq:04}.jsonl");
    harness::plant_segment(root, "tap/", at, &name, body.as_bytes());
    analyzer_ingest::ingest_bytes(
        &journal_faults::spine(engine, fleet).lake(),
        fleet,
        &name,
        body.as_bytes(),
    )
    .expect("ingest");
    journal_faults::executor(engine, fleet, &Params::default())
        .and_then(|executor| executor.publish_inputs())
        .expect("publish the input tables");
}

/// A heartbeat of the fixture's run naming both instances.
fn heartbeat(at: f64) -> Value {
    serde_json::json!({"kind": "heartbeat", "at_ms": at, "incarnation": "run-1", "canary": "k1",
        "msgs_seen": 3, "dropped": 0, "oversized": 0, "heartbeat_secs": 60,
        "max_segment_secs": 300, "endpoints": [
            {"source": "i0", "endpoint": "tcp://e:1", "msgs_seen": 2, "dropped": 0,
             "last_msg_at_ms": at},
            {"source": "i1", "endpoint": "tcp://e:2", "msgs_seen": 1, "dropped": 0,
             "last_msg_at_ms": at}]})
}

// The held-retirement regression needs a classified fixture before it is readable.
fn classify_fixture(
    name: &str,
    before_classify: impl FnOnce(&analyzer_serving_clickhouse::engine::Engine),
) -> Fixture {
    let (engine, db) = analyzer_serving_clickhouse::engine::testing::database(name);
    analyzer_serving_clickhouse::engine::migrate(
        &engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig::default(),
    )
    .expect("migrate");

    // Day-aligned and inside the event-grain TTL. A fixed absolute date here is a fuse: the TTL
    // is `at_ms + ledger_retention_days`, so seeded facts start disappearing on a wall-clock
    // date and the test reports success right up until then — and since ClickHouse applies TTL
    // on merge, racily rather than at once.
    let day = analyzer_serving_clickhouse::engine::testing::recent_day_base_ms();
    let window = Window {
        start_ms: day,
        end_ms: day + 86_400_000.0,
    };

    let dir = harness::fresh_dir(&format!("api-golden-{name}"));
    // The derivation source: the seeded stores as one JSONL segment twin under a dated tree,
    // read back through `ParquetSource` with the filesystem fetch.
    // `key_epoch` rides the segment header and stamps every record behind it, which is what
    // lets the wire tests below assert the epoch SURVIVES to a drill response.
    // A tap's segment: a sequenced open, the agent's start and its heartbeats, which state the
    // arrivals the classification's completeness model is built from.
    let mut body = format!(
        "{{\"kind\":\"segment_open\",\"at_ms\":{open},\"contract_version\":\"1.10\",\
         \"incarnation\":\"run-1\",\"segment_seq\":0,\"heartbeat_secs\":60,\
         \"max_segment_secs\":300,\"key_epoch\":7}}\n\
         {{\"kind\":\"agent_start\",\"at_ms\":{day},\"incarnation\":\"run-1\",\
         \"canary\":\"k1\",\"heartbeat_secs\":60,\"max_segment_secs\":300}}\n",
        open = day - 1000.0,
    );
    for hb in [day, day + 600_000.0] {
        let _ = writeln!(
            body,
            "{{\"kind\":\"heartbeat\",\"at_ms\":{hb},\"incarnation\":\"run-1\",\
             \"canary\":\"k1\",\"msgs_seen\":3,\"dropped\":0,\"oversized\":0,\
             \"heartbeat_secs\":60,\"max_segment_secs\":300,\"endpoints\":[\
             {{\"source\":\"i0\",\"endpoint\":\"tcp://e:1\",\"msgs_seen\":2,\
             \"dropped\":0,\"last_msg_at_ms\":{hb}}},\
             {{\"source\":\"i1\",\"endpoint\":\"tcp://e:2\",\"msgs_seen\":1,\
             \"dropped\":0,\"last_msg_at_ms\":{hb}}}]}}"
        );
    }
    for (inst, block, at, tokens) in [
        ("i0", "p", day + 1000.0, 16),
        ("i1", "p", day + 2000.0, 8),
        ("i0", "q", day + 3000.0, 32),
    ] {
        let _ = writeln!(
            body,
            "{{\"kind\":\"store\",\"instance_id\":\"{inst}\",\"block_id\":\"{block}\",\
             \"content_id\":\"{block}\",\"n_tokens\":{tokens},\"at_ms\":{at}}}"
        );
    }
    harness::plant_segment(
        &dir.join("segments"),
        "tap/",
        day,
        "0000.jsonl",
        body.as_bytes(),
    );
    let sources = harness::directory_sources(dir.join("segments"), dir.join("segment-cache"), day);
    journal_faults::serving(&engine)
        .bind(&analyzer_pipeline::execution::Namespace::new("api-fixture-source", name).unwrap())
        .unwrap();
    // The same bytes ingested, as a shipper delivers them: the input log names the batch by
    // its content, which is how the source names it too.
    analyzer_ingest::ingest_bytes(
        &journal_faults::spine(&engine, name).lake(),
        name,
        "segment-0442.jsonl",
        body.as_bytes(),
    )
    .expect("ingest the segment");
    journal_faults::executor(&engine, name, &Params::default())
        .and_then(|executor| executor.publish_inputs())
        .expect("publish the input tables");
    before_classify(&engine);
    journal_faults::executor(&engine, name, &Params::default())
        .and_then(|executor| executor.advance(&sources.source()))
        .expect("tick");

    let spine = journal_faults::spine(&engine, name);
    let serving = journal_faults::serving(&engine);
    let engine = Arc::new(Mutex::new(engine));
    let state = ApiState::new(
        Arc::clone(&engine),
        (spine, serving),
        sources.clone(),
        &Params::default(),
        analyzer_serving_clickhouse::engine::RetentionConfig::default(),
        ApiConfig::new(dir.join("tokens")),
    )
    .expect("state")
    .with_clock(now_ms());

    Fixture {
        sources,
        state,
        engine,
        _db: db,
        window,
    }
}

/// A session cookie for an issued token, so a golden exercises the shipped auth path rather than
/// bypassing it.
fn session(state: &ApiState) -> String {
    let (plaintext, _) = state.tokens_for_test().issue("golden", 1).expect("issue");
    let token_id = state
        .tokens_for_test()
        .verify(&plaintext)
        .expect("the issued token verifies");
    // Bound to its token, the way `session_create` binds it — so a golden runs through the same
    // pairing `require_session` checks rather than a session no token owns.
    state
        .sessions_for_test()
        .create(&token_id)
        .expect("session")
}

async fn get(state: &ApiState, path: &str, cookie: &str) -> (u16, Value) {
    use tower::ServiceExt as _;
    let app = analyzer_api::router(state.clone());
    let request = axum::http::Request::builder()
        .uri(path)
        .header(
            axum::http::header::COOKIE,
            format!("infertap_session={cookie}"),
        )
        .body(axum::body::Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    let status = response.status().as_u16();
    let bytes = axum::body::to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .expect("body");
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// Drive an async body from a synchronous test.
///
/// **The fixture is built OUTSIDE the runtime and the requests inside it, and that ordering is
/// the crate's own constraint rather than test scaffolding.** `Engine` drives its own runtime, so
/// `migrate`/`run_pass`/`engine.run` cannot be called from a task on this one — the handlers
/// obey that by routing every read through `spawn_blocking`, whose threads are not runtime
/// workers. A `#[tokio::test]` here would put the SETUP on a worker and panic with
/// "Cannot start a runtime from within a runtime", which is exactly the failure the crate header
/// documents and exactly the shape a future test would hit again.
fn drive<F: std::future::Future<Output = ()>>(
    setup: impl FnOnce() -> Fixture,
    body: impl FnOnce(Handle) -> F,
) {
    // Setup first, on this thread, with no runtime in scope.
    let owned = setup();
    // The body gets a CLONE of the cheap handle. The `Fixture` — and therefore the `Engine` and
    // its own runtime — stays owned out here, because **an `Engine` may not be dropped inside an
    // async context**: its runtime's shutdown blocks, and tokio refuses that from a task. Moving
    // the fixture into the future made every test panic at teardown with "Cannot drop a runtime
    // in a context where blocking is not allowed", which is the same nesting constraint the
    // crate header describes, arriving from the other direction.
    let handle = owned.handle();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(body(handle));
    // Explicit and ordered: the runtime first, then the engine, both on this thread.
    drop(runtime);
    drop(owned);
}

// ===========================================================================================
// THE FIGURE WALK — the whole of the no-naked-numbers enforcement until typed wire structs land.
// ===========================================================================================

/// Field names that ARE figures: a measured token mass, a fraction of stored mass, or a coverage
/// fraction. Each must be an object carrying all four `Covered` fields.
const FIGURE_FIELDS: &[&str] = &["tokens", "share_of_fleet_total", "fleet_total"];

/// Walk `value` and assert every in-band refusal has its payload ABSENT, never empty.
///
/// The payloads are nullable in the TYPES and the schema ratchet holds that — and a schema
/// ratchet passes on `Some([])`, because nullable-in-schema says nothing about what a handler
/// emits. One walk over every response the goldens exercise catches the class. It is the same
/// move as [`the_figure_walk`]: the law is checked at the boundary the type stops travelling,
/// because that is where a correct type stops proving anything.
fn the_refusal_walk(value: &Value, path: &str) {
    // A refusal names its scope: `refused` guards the shape's primary collection,
    // `<thing>_refused` guards `<thing>` — the rule `openapi::refusal_scope_violations` enforces
    // over the schema, applied here to the bytes.
    const PRIMARY: &[&str] = &["rows", "records", "figures"];
    if let Value::Object(map) = value {
        for (key, refusal) in map {
            let Some(scope) = key.strip_suffix("refused") else {
                continue;
            };
            if refusal.is_null() {
                continue;
            }
            let scope = scope.trim_end_matches('_');
            let guarded: Vec<&str> = if scope.is_empty() {
                PRIMARY
                    .iter()
                    .copied()
                    .filter(|p| map.contains_key(*p))
                    .collect()
            } else {
                vec![scope]
            };
            for g in guarded {
                let payload = &map[g];
                assert!(
                    payload.is_null(),
                    "{path}.{key} is present and {path}.{g} is {payload} — a refusal must make \
                     its payload ABSENT. An empty array here is the 'looked and found nothing' \
                     misread a consumer that skipped {key:?} will render as a real answer."
                );
            }
        }
        for (key, v) in map {
            the_refusal_walk(v, &format!("{path}.{key}"));
        }
    } else if let Value::Array(items) = value {
        for (i, v) in items.iter().enumerate() {
            the_refusal_walk(v, &format!("{path}[{i}]"));
        }
    }
}

/// Walk `value` and assert every figure-vocabulary field is a full `Covered` object.
///
/// **A naked number is unrepresentable on the wire, same as in the types.** This is that law
/// checked at the boundary the type stops travelling. It recurses, so a figure nested three
/// levels inside a segment is covered too.
fn the_figure_walk(value: &Value, path: &str) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                let here = format!("{path}.{key}");
                if FIGURE_FIELDS.contains(&key.as_str()) && !v.is_null() {
                    let obj = v.as_object().unwrap_or_else(|| {
                        panic!("{here} is a figure field but serialized as {v}, not a Covered")
                    });
                    for required in ["value", "register", "coverage", "coverage_kind"] {
                        assert!(
                            obj.contains_key(required),
                            "{here} is a figure missing {required:?}: {v}"
                        );
                    }
                }
                the_figure_walk(v, &here);
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                the_figure_walk(v, &format!("{path}[{i}]"));
            }
        }
        _ => {}
    }
}

/// Every 200 carries a stamp with its meaning pair. Checked on every golden below; `where_` is
/// the request path, whose `window=` decides whether the stamp states one.
fn the_envelope_holds(body: &Value, where_: &str) {
    let stamp = &body["stamp"];
    assert!(!stamp.is_null(), "{where_}: no stamp");
    for key in [
        "fleet_id",
        "semantics_version",
        "discriminator",
        "raw_horizon",
    ] {
        assert!(!stamp[key].is_null(), "{where_}: stamp missing {key}");
    }
    // The stamp's window is the read's: present exactly when the request carried one. A read
    // with no window parameter (`/v1/stamp`, since-install delivery) reports none, never an
    // invented epoch day.
    assert_eq!(
        stamp["window"].is_null(),
        !where_.contains("window="),
        "{where_}: the stamp's window is the request's"
    );
    assert!(body.get("data").is_some(), "{where_}: no data key");
    assert_eq!(
        body.as_object().map(serde_json::Map::len),
        Some(2),
        "{where_}: the envelope is exactly two keys"
    );
}

// ===========================================================================================
// The goldens.
// ===========================================================================================

/// An unknown `/v1` path answers a JSON 404, and **the CONTENT TYPE is the assertion**.
///
/// The status alone proves nothing here, which is the whole reason this test exists. Before the
/// API namespace claimed its own misses, an unmatched `/v1` path fell through to the console
/// mount: with no bundle it answered `503 console_not_configured`, and with one — every release
/// build — it served `index.html` at **200**. A JSON client asking for an endpoint the binary
/// lacks received a successful HTML page, and no status-code check anywhere could see it.
///
/// So this asserts the shape a consumer actually depends on: a JSON body, the closed cause set,
/// and a token naming VERSION SKEW rather than a console.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn an_unknown_v1_path_answers_json_not_the_console() {
    drive(
        || fixture("api_unknown_route"),
        |f| async move {
            use tower::ServiceExt as _;
            let cookie = session(&f.state);
            let app = analyzer_api::router(f.state.clone());
            let request = axum::http::Request::builder()
                .uri("/v1/no-such-endpoint")
                .header(
                    axum::http::header::COOKIE,
                    format!("infertap_session={cookie}"),
                )
                .body(axum::body::Body::empty())
                .expect("request");
            let response = app.oneshot(request).await.expect("response");

            assert_eq!(response.status().as_u16(), 404, "route absent is NOT FOUND");
            let content_type = response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            assert!(
                content_type.starts_with("application/json"),
                "a JSON client must not receive markup: content-type was {content_type:?}"
            );

            let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .expect("body");
            let body: Value = serde_json::from_slice(&bytes).expect("json body");
            assert_eq!(
                body["error"]["cause"], "unknown_route",
                "the cause names the route, not the console: {body}"
            );
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn every_endpoint_family_carries_its_envelope_and_no_naked_figures() {
    drive(
        || fixture("api_goldens"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);

            let paths = [
                "/v1/stamp".to_string(),
                format!("/v1/figures?window={w}&dims=class"),
                format!("/v1/holders?window={w}"),
                format!("/v1/coverage?window={w}"),
                format!("/v1/roster?window={w}"),
                format!("/v1/residual/placement?window={w}"),
                // The copies read at its three stops: no API test had reached
                // this endpoint before, and it now serves each grain from its own table.
                format!("/v1/residual/replication?window={w}"),
                "/v1/residual/replication?window=48h".to_string(),
                "/v1/residual/replication?window=3h".to_string(),
                format!("/v1/markers?window={w}"),
                format!("/v1/health?window={w}"),
                format!("/v1/evidence?window={w}&block=p"),
                // **The entity-page reads, over HTTP**. The DAO pins prove the
                // scoped SQL; these prove the HANDLER wiring — that `instance=` is parsed,
                // reaches the read, and comes back inside the same envelope every other
                // surface carries. Without them the parameter could be dropped silently
                // between `Q` and the DAO and every existing test would still pass.
                format!("/v1/figures?window={w}&dims=class&instance=i0"),
                // **A short instance-scoped window with the grain left to the server**.
                // The chooser must never pick the fleet minute tier under a holder-shaped
                // scope: those cells carry no instance column, so a scope predicate naming
                // one would reach the caller as the engine's "no such column", a 503. Three
                // hours, so the span alone would qualify for minute; only the scope rules it
                // out.
                format!(
                    "/v1/figures?window={},{}&dims=class&instance=i0",
                    f.window.end_ms - 3.0 * 3_600_000.0,
                    f.window.end_ms
                ),
                format!("/v1/figures?window={w}&dims=instance"),
                // The rank-grain minute tier answers an instance-scoped explicit minute and the
                // instance cut at minute.
                format!(
                    "/v1/figures?window={},{}&dims=class&instance=i0&grain=minute",
                    f.window.end_ms - 3.0 * 3_600_000.0,
                    f.window.end_ms
                ),
                format!(
                    "/v1/figures?window={},{}&dims=instance&grain=minute",
                    f.window.end_ms - 3.0 * 3_600_000.0,
                    f.window.end_ms
                ),
                format!("/v1/figures/bands?window={w}&instance=i0"),
                format!("/v1/instances/i0/duplication?window={w}"),
            ];

            for path in &paths {
                let (status, body) = get(&f.state, path, &cookie).await;
                assert_eq!(status, 200, "{path} answered {status}: {body}");
                the_envelope_holds(&body, path);
                the_refusal_walk(&body, "");
                the_figure_walk(&body, path);

                // The state is a VALUE from the contract-closed pair, not a header that
                // exists: a console once synthesized this column from coverage and its gate
                // asserted two headers instead of two facts. Assert the fact — and the pairing
                // law with it: `reason` present exactly when the state is `inactive`.
                if path.starts_with("/v1/holders?") {
                    let rows = body["data"]["rows"].as_array().expect("holders rows");
                    for row in rows {
                        let state = row["state"].as_str().expect("state");
                        assert!(
                            ["active", "inactive"].contains(&state),
                            "state outside the closed pair: {state}"
                        );
                        match row.get("reason").and_then(|r| r.as_str()) {
                            Some(reason) => {
                                assert_eq!(state, "inactive", "a reason on an active row");
                                assert!(
                                    ["stopped", "silent", "ungraded"].contains(&reason),
                                    "reason outside the closed set: {reason}"
                                );
                            }
                            None => assert_eq!(state, "active", "inactive without a reason"),
                        }
                    }
                }
            }
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn a_figure_response_states_its_register_and_never_totals_across_segments() {
    drive(
        || fixture("api_figures"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);

            let (status, body) = get(
                &f.state,
                &format!("/v1/figures?window={w}&dims=class"),
                &cookie,
            )
            .await;
            assert_eq!(status, 200);
            let data = &body["data"];

            assert_eq!(data["register"], "recompute");
            assert_eq!(data["never_summed_with"], "capacity");
            // Never-sum expressed as an ABSENT field: there is no total spanning segments, and no
            // parameter that asks for one.
            assert!(
                data.get("total").is_none(),
                "a total across segments would be the never-sum rule undone at the wire"
            );
            let segments = data["segments"].as_array().expect("segments");
            assert!(!segments.is_empty(), "segmented always, even when clean");
            assert!(
                segments[0]["figures"].is_array(),
                "the current generation carries figures"
            );
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn the_health_ingest_block_hydrates_the_strip_before_any_event_arrives() {
    // The t₀ case the block exists for: SSE carries deltas, so a console loading before anything
    // happens to arrive has nothing to render the dark strip from.
    drive(
        || fixture("api_health"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);

            let (status, body) = get(&f.state, &format!("/v1/health?window={w}"), &cookie).await;
            assert_eq!(status, 200, "{body}");
            let ingest = &body["data"]["ingest"];

            assert_eq!(ingest["last_segment"], "segment-0442.jsonl");
            // Every record the segment carried: its open, the agent's start, two heartbeats and
            // three stores.
            assert_eq!(ingest["events_since_install"], 7, "the odometer");
            assert_eq!(ingest["batches_total"], 1);
            // Wall domain on both sides: the stamp ingest recorded against our injected clock.
            let ingested = mutate(&f, |engine, _, fleet| {
                journal_faults::spine(engine, fleet)
                    .lake()
                    .ledger(analyzer_pipeline::ledger::SHARD)
                    .unwrap()
                    .committed_since(0)
                    .unwrap()[0]
                    .ingested_at_ms
            })
            .await;
            // Two hours after the stamp, on a clock the test sets.
            #[allow(clippy::cast_precision_loss)] // an epoch millisecond fits the mantissa
            let later = f.state.clone().with_clock(ingested as f64 + 7_200_000.0);
            let (_, body) = get(&later, &format!("/v1/health?window={w}"), &cookie).await;
            assert_eq!(body["data"]["ingest"]["lag_ms"].as_f64(), Some(7_200_000.0));
            // Daemon-native fields are ABSENT rather than faked when the loop has published nothing.
            assert!(ingest["next_tick_at_ms"].is_null());
            assert!(ingest["backlog_files"].is_null());

            // Populations total to the RUN count, and the models split is
            // observed-vs-undeclared: declared at the deployment boundary, so absence is
            // `undeclared`, never `unobserved`.
            assert!(body["data"]["producer_runs"].is_number());
            assert!(body["data"]["models"]["undeclared"].is_number());

            // Every installed public module is listed with its standing, so a figure a disabled
            // module feeds is never read as a zero. The core registers are not modules:
            // they cannot be disabled, and their standing is the stamp's edges. This build
            // installs no public module.
            let modules = body["data"]["modules"].as_array().expect("modules");
            assert!(modules.is_empty(), "{modules:?}");
            assert!(
                body["stamp"]["tick"]["classified_to_ms"].is_number(),
                "{body}"
            );
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn no_session_is_401_on_every_read_route_and_the_realm_is_the_only_way_in() {
    drive(
        || fixture("api_auth"),
        |f| async move {
            let app = analyzer_api::router(f.state.clone());

            for path in ["/v1/stamp", "/v1/figures", "/v1/health", "/v1/holders"] {
                use tower::ServiceExt as _;
                let response = app
                    .clone()
                    .oneshot(
                        axum::http::Request::builder()
                            .uri(path)
                            .body(axum::body::Body::empty())
                            .expect("request"),
                    )
                    .await
                    .expect("response");
                assert_eq!(
                    response.status().as_u16(),
                    401,
                    "{path} must refuse without a session"
                );
            }
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn a_drill_past_the_raw_horizon_refuses_with_its_bound_rather_than_returning_nothing() {
    // section 7.2: a bounded refusal is a RESULT. An empty array here would read as "looked and found
    // nothing", which is the opposite claim from "there is nothing left to look at".
    drive(
        || fixture("api_horizon"),
        |f| async move {
            let cookie = session(&f.state);
            let path = "/v1/evidence?window=1599955200000,1600041600000&holder=i0/0/0".to_string();

            let (status, body) = get(&f.state, &path, &cookie).await;
            assert_eq!(status, 200, "a bounded refusal is a result, not an error");
            let data = &body["data"];
            assert_eq!(
                data["refused"]["cause"], "outside_raw_horizon",
                "full response: {body}"
            );
            assert_eq!(data["reach"]["covers"], false);
            assert!(
                !data["reach"]["reason"].is_null(),
                "the refusal states which of the two readings it is"
            );
            assert!(
                data["records"].is_null(),
                "records are ABSENT on a refusal, never `[]` — an empty array reads as 'looked \
                 and found nothing', which is the opposite claim: {data}"
            );
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn the_evidence_drill_serves_the_gap_and_the_epoch_on_the_wire() {
    // The mock's gap-since-close and key epoch columns, served.
    // The epoch is declared once on the segment
    // header (7, above) and must survive decode → classify → store → DAO → JSON; the gap is
    // null here because every seeded store is first-sight, and a null that arrived as 0 would
    // read as an instant re-fetch.
    drive(
        || fixture("api_evidence_cols"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);

            let (status, body) = get(
                &f.state,
                &format!("/v1/evidence?window={w}&block=p"),
                &cookie,
            )
            .await;
            assert_eq!(status, 200, "{body}");
            let records = body["data"]["records"].as_array().expect("records");
            assert!(!records.is_empty(), "block p was stored twice: {body}");
            for r in records {
                assert_eq!(
                    r["key_epoch"], 7,
                    "the header's epoch, stamped onto the record and surviving to the wire: {r}"
                );
                let obj = r.as_object().expect("a record is an object");
                assert!(
                    obj.contains_key("gap_ms"),
                    "the gap column is PRESENT on every record, null included: {r}"
                );
                assert!(
                    r["gap_ms"].is_null(),
                    "a first-sight store has no observed prior close — null, never zero: {r}"
                );
            }

            // The timeline serves the same two columns.
            let (status, body) = get(
                &f.state,
                &format!("/v1/holders/i0/0/0/timeline?window={w}"),
                &cookie,
            )
            .await;
            assert_eq!(status, 200, "{body}");
            let rows = body["data"]["rows"].as_array().expect("rows");
            assert!(!rows.is_empty(), "i0 stored p and q: {body}");
            for r in rows {
                assert_eq!(r["key_epoch"], 7, "one column, both drills: {r}");
                assert!(r["gap_ms"].is_null(), "first sight on this fixture: {r}");
            }
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn a_paged_surface_walks_to_the_end_and_repeats_no_row() {
    // **`limit = 1` deliberately.** At the v1 envelope one page holds the whole holders table, so
    // `next_cursor` is always null and the paging path is one nobody exercises -- then the
    // envelope lifts and every consumer that never ran that code truncates silently. Paging at
    // one row forces the path here, where getting it wrong is cheap.
    drive(
        || fixture("api_paging"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);

            let mut seen: Vec<String> = Vec::new();
            let mut cursor: Option<String> = None;
            for _ in 0..10 {
                let path = match &cursor {
                    None => format!("/v1/holders?window={w}&limit=1"),
                    Some(c) => format!("/v1/holders?window={w}&limit=1&cursor={c}"),
                };
                let (status, body) = get(&f.state, &path, &cookie).await;
                assert_eq!(status, 200, "{path} answered {status}: {body}");
                the_refusal_walk(&body, "");
                the_figure_walk(&body, "holders page");

                let rows = body["data"]["rows"].as_array().expect("rows");
                for r in rows {
                    let id = format!(
                        "{}/{}/{}",
                        r["holder"]["instance"], r["holder"]["dp_rank"], r["holder"]["group_idx"]
                    );
                    assert!(!seen.contains(&id), "row {id} appeared on two pages");
                    seen.push(id);
                }
                match body["data"]["page"]["next_cursor"].as_str() {
                    Some(c) => cursor = Some(c.to_string()),
                    None => break,
                }
            }

            let total = body_total(&f, &cookie, &w).await;
            assert_eq!(
                seen.len(),
                total,
                "walking the cursor must reach every row exactly once"
            );
            assert!(seen.len() >= 2, "the fixture must be multi-page at limit=1");
        },
    );
}

/// Two runs for the roster to page over: the shipped ingest writes the arrivals the roster
/// reads, one incarnation per sealed segment. On a blocking thread: the engine drives its
/// own runtime, and a read taken on an async worker would nest one runtime inside another.
async fn seed_two_runs(f: &Handle, start_ms: f64) {
    let engine = Arc::clone(&f.engine);
    let fleet = f.state.fleet_id().to_string();
    tokio::task::spawn_blocking(move || {
        let engine = engine.lock().expect("engine");
        for (run, inst, at) in [
            ("run-a", "i0", start_ms + 5_000.0),
            ("run-b", "i1", start_ms + 6_000.0),
        ] {
            // A run is its liveness records: the roster's clock comes from the heartbeat, and
            // the store gives the run an instance to fan out to.
            let body = format!(
                "{{\"kind\":\"segment_open\",\"at_ms\":{at},\"incarnation\":\"{run}\",\
                 \"segment_seq\":0}}\n\
                 {{\"kind\":\"heartbeat\",\"at_ms\":{at},\"msgs_seen\":1,\"dropped\":0,\
                 \"oversized\":0,\"endpoints\":[]}}\n\
                 {{\"kind\":\"store\",\"instance_id\":\"{inst}\",\"block_id\":\"r\",\
                 \"n_tokens\":4,\"at_ms\":{at}}}\n"
            );
            analyzer_ingest::ingest_bytes(
                &journal_faults::spine(&engine, &fleet).lake(),
                &fleet,
                &format!("seg-{run}-0.jsonl"),
                body.as_bytes(),
            )
            .expect("ingest a run");
            journal_faults::executor(&engine, &fleet, &analyzer_analysis::Params::default())
                .and_then(|executor| executor.publish_inputs())
                .expect("publish the input tables");
        }
    })
    .await
    .expect("the seeding thread");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn every_paged_drill_walks_to_its_end() {
    // Every surface that returns a `Page` and cuts its rows at `limit` accepts a cursor, so a
    // caller told rows remain has a way to ask for them.
    //
    // Walked at `limit = 1` for the same reason the holders walk is: the fixture fits one page
    // otherwise, and a paging path nobody runs is one that breaks when the envelope lifts.
    drive(
        || fixture("api_paging_drills"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);
            let (_, any) = get(&f.state, &format!("/v1/figures?window={w}"), &cookie).await;
            let day = any["stamp"]["window"]["start_day"]
                .as_str()
                .expect("the stamp names its own days")
                .to_string();

            seed_two_runs(&f, f.window.start_ms).await;

            let surfaces: Vec<Surface> = vec![
                (
                    format!("/v1/blocks/p/holders?window={w}"),
                    "page",
                    "rows",
                    (|r| {
                        format!(
                            "{}/{}/{}@{}",
                            r["holder"]["instance"],
                            r["holder"]["dp_rank"],
                            r["holder"]["group_idx"],
                            r["at_ms"]
                        )
                    }) as RowId,
                ),
                (
                    format!("/v1/holders/i0/0/0/timeline?window={w}"),
                    "page",
                    "rows",
                    |r| format!("{}@{}", r["block_id"], r["at_ms"]),
                ),
                (format!("/v1/days/{day}?"), "holders_page", "holders", |r| {
                    format!(
                        "{}/{}/{}",
                        r["holder"]["instance"], r["holder"]["dp_rank"], r["holder"]["group_idx"]
                    )
                }),
                // The two surfaces whose positions carry the cursor payload's own separator: a
                // decoder that split the payload on it and kept one part would serve page one
                // forever.
                (format!("/v1/roster?window={w}"), "page", "rows", |r| {
                    format!("{}@{}", r["instance"], r["incarnation"])
                }),
                (
                    format!("/v1/evidence?window={w}&block=p"),
                    "page",
                    "records",
                    |r| format!("{}@{}", r["block_id"], r["at_ms"]),
                ),
            ];

            for (base, page_key, rows_key, id_of) in surfaces {
                let mut seen: Vec<String> = Vec::new();
                let mut cursor: Option<String> = None;
                let mut pages = 0;
                for _ in 0..10 {
                    let path = match &cursor {
                        None => format!("{base}&limit=1"),
                        Some(c) => format!("{base}&limit=1&cursor={c}"),
                    };
                    let (status, body) = get(&f.state, &path, &cookie).await;
                    assert_eq!(status, 200, "{path} answered {status}: {body}");
                    pages += 1;

                    let rows = body["data"][rows_key].as_array().expect("rows").clone();
                    assert!(
                        rows.len() <= 1,
                        "{base} returned {} rows at limit=1",
                        rows.len()
                    );
                    for r in &rows {
                        let id = id_of(r);
                        assert!(
                            !seen.contains(&id),
                            "{base}: row {id} appeared on two pages"
                        );
                        seen.push(id);
                    }
                    match body["data"][page_key]["next_cursor"].as_str() {
                        Some(c) => cursor = Some(c.to_string()),
                        None => break,
                    }
                }
                assert!(
                    seen.len() >= 2,
                    "{base} must be multi-page at limit=1 or this proves nothing: {seen:?}"
                );
                assert!(
                    pages > 1,
                    "{base} ended after one page, so the cursor was never exercised"
                );
            }
        },
    );
}

/// How a row names itself, so "did this row appear twice" is decidable per surface.
type RowId = fn(&serde_json::Value) -> String;

/// One walkable surface: its URL, where its page block and its rows live, and how to identify
/// a row.
type Surface = (String, &'static str, &'static str, RowId);

/// `total_rows` from a single unpaged read, as the walk's oracle.
async fn body_total(f: &Handle, cookie: &str, w: &str) -> usize {
    let (_, body) = get(&f.state, &format!("/v1/holders?window={w}"), cookie).await;
    usize::try_from(
        body["data"]["page"]["total_rows"]
            .as_u64()
            .expect("total_rows"),
    )
    .expect("a row count fits a usize")
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn a_cursor_from_a_superseded_pass_is_refused_rather_than_answered() {
    // `cursor_stale`'s producer and its golden: every cause in the closed set has both.
    drive(
        || fixture("api_cursor_stale"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);

            let (_, first) = get(
                &f.state,
                &format!("/v1/holders?window={w}&limit=1"),
                &cookie,
            )
            .await;
            let cursor = first["data"]["page"]["next_cursor"]
                .as_str()
                .expect("a first page of a multi-page table carries one")
                .to_string();

            // A cursor this API minted, but under a stamp that no longer matches: forge the same
            // shape with a different frontier by hand-rolling one through the same encoder.
            let mut stamp = analyzer_api::envelope::Stamp::read(&f.state, Some(f.window), 14)
                .await
                .expect("stamp");
            stamp.tick_at_ms = Some(stamp.tick_at_ms.unwrap_or(0.0) + 1.0);
            let question = analyzer_api::cursor::QueryIdentity::of(&[]);
            let stale = analyzer_api::cursor::Cursor::mint(&stamp, &question, "i0/0/0").encode();

            let (status, body) = get(
                &f.state,
                &format!("/v1/holders?window={w}&limit=1&cursor={stale}"),
                &cookie,
            )
            .await;
            // **200 with the refusal in the DATA position.** As a 400 the whole response would
            // be an error body, and a console would have to blank the panel to say "your page
            // expired". The token was correct when it was minted and the world moved underneath
            // it, which is a bounded refusal by section 7.2 rather than a malformed request.
            assert_eq!(
                status, 200,
                "a stale cursor is a RESULT, not a bad request: {body}"
            );
            let data = &body["data"];
            assert_eq!(data["refused"]["cause"], "cursor_stale");
            assert!(
                !data["refused"]["bound"].is_null(),
                "the refusal states what moved, so a console can say so"
            );

            // The frame survives, which is the whole reason for the shape.
            assert!(
                !body["stamp"].is_null(),
                "a refused page still carries its stamp -- the heading stays"
            );
            assert_eq!(
                data["page"]["limit"], 1,
                "the page block describes the REQUEST, so it stands through a refusal"
            );

            // And the rows are ABSENT, never `[]`. A consumer that skipped `refused` must not be
            // able to render this as "no holders".
            assert!(
                data["rows"].is_null(),
                "rows must be null on a refusal, never an empty array: {data}"
            );
            assert!(
                data["fleet_total"].is_null(),
                "the denominator was never read; a zero here is the naked number Covered forbids"
            );

            // A drill that propagated the parse as an HTTP error would be the worst shape on the
            // wire: a 200 whose body is an error object and not the envelope. The same stale
            // token must refuse in-band here too, because the binding is to the stamp and not to
            // any one surface.
            let path = format!("/v1/evidence?window={w}&block=p&cursor={stale}");
            let (status, body) = get(&f.state, &path, &cookie).await;
            assert_eq!(status, 200, "evidence with a stale cursor: {body}");
            the_envelope_holds(&body, &path);
            assert_eq!(body["data"]["refused"]["cause"], "cursor_stale");
            assert!(
                body["data"]["records"].is_null(),
                "records must be absent on a refusal: {body}"
            );

            // And the live one still works, so the refusal is about staleness rather than cursors.
            let (ok, _) = get(
                &f.state,
                &format!("/v1/holders?window={w}&limit=1&cursor={cursor}"),
                &cookie,
            )
            .await;
            assert_eq!(ok, 200);
        },
    );
}

/// Paged reads use the same live-writer and durable completion boundary as standalone use.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn a_paged_read_is_withheld_while_a_writer_publishes_or_stands_unfinished() {
    drive(
        || fixture("api_publication_standing"),
        |f| async move {
            let cookie = session(&f.state);
            let w = format!("{},{}", f.window.start_ms, f.window.end_ms);
            let holders = format!("/v1/holders?window={w}&limit=1");
            let (_, before) = get(&f.state, &holders, &cookie).await;
            assert!(before["data"]["refused"].is_null(), "{before}");
            let cursor = before["data"]["page"]["next_cursor"]
                .as_str()
                .expect("cursor")
                .to_string();
            assert_live_writer_withheld(&f, &holders, &cookie, &w).await;
            // A tick whose apply is lost leaves its intent unapplied: the batch is published and
            // readers are not yet told so.
            mutate(&f, |engine, sources, fleet| {
                let (journal, spine) = journal_faults::faulty_default_spine(engine, fleet);
                journal_faults::lose_apply(&journal, "lose_api_apply");
                let failure = analyzer_pipeline::Executor::open(
                    spine,
                    &Params::default(),
                    journal_faults::serving(engine),
                )
                .and_then(|executor| executor.advance(&sources.source()))
                .expect_err("lost acknowledgment");
                assert!(failure.to_string().contains("lose_api_apply"), "{failure}");
                journal_faults::heal(&journal);
            })
            .await;
            assert_withheld(&f.state, &holders, &cookie).await;
            // The next tick publishes the unapplied batch again and applies it.
            let recovered = mutate(&f, |engine, sources, fleet| {
                journal_faults::executor(engine, fleet, &Params::default())
                    .and_then(|executor| executor.advance(&sources.source()))
                    .expect("recovery")
            })
            .await;
            assert!(recovered.republished, "{recovered:?}");
            assert_served(&f, &holders, &cookie, "after recovery").await;
            let (_, paged) = get(&f.state, &format!("{holders}&cursor={cursor}"), &cookie).await;
            assert_eq!(paged["data"]["refused"]["cause"], "cursor_stale", "{paged}");
        },
    );
}

async fn mutate<T: Send + 'static>(
    f: &Handle,
    work: impl FnOnce(
            &analyzer_serving_clickhouse::engine::Engine,
            &analyzer_pipeline::SourceConfig,
            &str,
        ) -> T
        + Send
        + 'static,
) -> T {
    let engine = Arc::clone(&f.engine);
    let sources = f.sources.clone();
    let fleet = f.state.fleet_id().to_string();
    tokio::task::spawn_blocking(move || work(&engine.lock().expect("engine"), &sources, &fleet))
        .await
        .expect("writer task")
}

async fn assert_withheld(state: &ApiState, route: &str, cookie: &str) {
    let (status, response) = get(state, route, cookie).await;
    assert_eq!(status, 200, "{response}");
    assert_eq!(
        response["data"]["refused"]["cause"], "cursor_stale",
        "{response}"
    );
}

async fn assert_served(f: &Handle, route: &str, cookie: &str, label: &str) {
    // Replacement acknowledges its statements before asynchronous row retirement ends.
    // Pages deliberately remain withheld until that retirement has actually finished.
    mutate(f, |engine, _, _| wait_for_retirement(engine)).await;
    let (status, response) = get(&f.state, route, cookie).await;
    if !response["data"]["refused"].is_null() {
        let standing = mutate(f, |engine, _, fleet| journal_faults::default_journal(engine, fleet).to_string()  + &engine.run("SELECT command FROM system.mutations WHERE database = currentDatabase() AND is_done = 0 FORMAT JSONEachRow").expect("mutations")).await;
        panic!("{label}: {response}; standing: {standing}");
    }
    assert_eq!(status, 200, "{response}");
    assert!(response["data"]["refused"].is_null(), "{response}");
    assert_eq!(response["data"]["rows"].as_array().expect("rows").len(), 1);
}

/// A real tick holds the writer while its source is paused. No daemon handle is attached.
async fn assert_live_writer_withheld(f: &Handle, route: &str, cookie: &str, window: &str) {
    // New input, so the tick reads the source.
    let at = f.window.start_ms + 700_000.0;
    mutate(f, move |engine, sources, fleet| {
        ship(
            engine,
            sources,
            fleet,
            (1, at, None),
            &[
                serde_json::json!({"kind": "store", "instance_id": "i0", "dp_rank": 0,
                    "group_idx": 0, "block_id": "w", "content_id": "w", "n_tokens": 4,
                    "at_ms": at, "incarnation": "run-1"}),
                heartbeat(at + 60_000.0),
            ],
        );
    })
    .await;
    let (entered, waiting) = std::sync::mpsc::channel();
    let (resume, resumed) = std::sync::mpsc::channel();
    let engine = Arc::clone(&f.engine);
    let sources = f.sources.clone();
    let fleet = f.state.fleet_id().to_string();
    let writer = tokio::task::spawn_blocking(move || {
        let sibling = engine
            .lock()
            .expect("engine")
            .sibling()
            .expect("writer connection");
        let source = PausedSource {
            inner: Box::new(sources.source()),
            entered,
            resumed: Mutex::new(resumed),
        };
        journal_faults::executor(&sibling, &fleet, &Params::default())
            .and_then(|executor| executor.advance(&source))
    });
    tokio::task::spawn_blocking(move || waiting.recv_timeout(std::time::Duration::from_secs(30)))
        .await
        .expect("wait task")
        .expect("writer entered source");
    assert_withheld(&f.state, route, cookie).await;
    let (status, figures) = get(&f.state, &format!("/v1/figures?window={window}"), cookie).await;
    assert_eq!(status, 200, "unpaged figures remain available: {figures}");
    resume.send(()).expect("resume source");
    writer.await.expect("tick task").expect("tick");
    assert_served(f, route, cookie, "after the tick").await;
}

struct PausedSource {
    inner: Box<dyn analyzer_pipeline::source::FillSource>,
    entered: std::sync::mpsc::Sender<()>,
    resumed: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl analyzer_analysis::events::EventSource for PausedSource {
    fn cache_events(
        &self,
        fleet: &str,
        window: &Window,
    ) -> Result<Arc<Vec<analyzer_analysis::events::Event>>, analyzer_analysis::Error> {
        self.inner.cache_events(fleet, window)
    }
    fn lifecycle_events(
        &self,
        fleet: &str,
        lo: f64,
        hi: f64,
    ) -> Result<Vec<analyzer_analysis::events::LifecycleEvent>, analyzer_analysis::Error> {
        self.inner.lifecycle_events(fleet, lo, hi)
    }
    fn segment_opens_at_retention_horizon(
        &self,
        fleet: &str,
    ) -> Result<Vec<analyzer_analysis::events::LifecycleEvent>, analyzer_analysis::Error> {
        self.inner.segment_opens_at_retention_horizon(fleet)
    }
}

impl analyzer_pipeline::source::FillSource for PausedSource {
    fn stream_cache_events_into(
        &self,
        fleet: &str,
        selection: analyzer_pipeline::source::InputSelection,
        exclude: &analyzer_pipeline::source::FillExclusion,
        sink: &mut analyzer_pipeline::source::SegmentSink<'_>,
    ) -> Result<Vec<(String, String)>, analyzer_analysis::Error> {
        self.entered.send(()).expect("entered");
        self.resumed
            .lock()
            .expect("gate")
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("resume");
        self.inner
            .stream_cache_events_into(fleet, selection, exclude, sink)
    }
}

/// A serve-only reader withholds while the durable journal stands unapplied, and an applied
/// tick under a new revision makes earlier cursors stale.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn a_reader_without_a_daemon_withholds_an_unapplied_tick() {
    drive(
        || fixture("api_durable_tick"),
        |f| async move {
            let cookie = session(&f.state);
            let route = format!(
                "/v1/holders?window={},{}&limit=1",
                f.window.start_ms, f.window.end_ms
            );
            let (_, before) = get(&f.state, &route, &cookie).await;
            let cursor = before["data"]["page"]["next_cursor"]
                .as_str()
                .expect("cursor")
                .to_string();
            for applied in [false, true] {
                let engine = Arc::clone(&f.engine);
                tokio::task::spawn_blocking(move || {
                    journal_faults::edit_default_journal(
                        &engine.lock().expect("engine"),
                        "api_durable_tick",
                        |head| {
                            head["keyed"]["applied"] = applied.into();
                            if applied {
                                // The applied tick is a new revision.
                                let clock = &mut head["clocks"][0][1];
                                *clock = (clock.as_u64().expect("revision") + 1).into();
                            }
                        },
                    );
                })
                .await
                .expect("write task");
                let (_, page) = get(&f.state, &route, &cookie).await;
                if applied {
                    assert!(page["data"]["refused"].is_null(), "{page}");
                } else {
                    assert_eq!(page["data"]["refused"]["cause"], "cursor_stale", "{page}");
                    assert!(page["data"]["rows"].is_null(), "{page}");
                }
            }
            let (_, stale) = get(&f.state, &format!("{route}&cursor={cursor}"), &cookie).await;
            assert_eq!(stale["data"]["refused"]["cause"], "cursor_stale", "{stale}");
        },
    );
}

/// An applied tick does not imply asynchronous row retirement has finished.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn held_retirement_explains_horizon_refusal_without_an_active_writer_or_pending_attempt() {
    drive(
        || {
            classify_fixture("api_retirement_horizon", |engine| {
                engine
                    .run("SYSTEM STOP MERGES recompute_minute")
                    .expect("hold retirement");
            })
        },
        |f| async move {
            let cookie = session(&f.state);
            let clock = mutate(&f, |engine, _, fleet| {
                // A retirement in flight: the replace path's own statement, held by the stopped
                // merges. A settled day's module output retires its older rows this way.
                engine
                    .run(&format!(
                        "DELETE FROM recompute_minute WHERE fleet_id = {} AND version < 1 \
                         SETTINGS lightweight_deletes_sync = 0",
                        analyzer_serving_clickhouse::engine::quote_literal(fleet)
                    ))
                    .expect("retire");
                assert!(!analyzer_pipeline::execution::is_writing(&journal_faults::spine(engine, fleet).lake()));
                assert_eq!(journal_faults::default_journal(engine, fleet)["keyed"]["applied"], true);
                let mutations = engine.run("SELECT table, command, parts_to_do FROM system.mutations WHERE database = currentDatabase() AND is_done = 0 FORMAT JSONEachRow").unwrap();
                assert!(mutations.lines().any(|line| {
                    let row: Value = serde_json::from_str(line).unwrap();
                    row["table"] == "recompute_minute" && row["command"].as_str().unwrap().contains("version")
                }), "held cell retirement: {mutations}");
                assert!(journal_faults::executor(engine, fleet, &Params::default()).unwrap().revision().unwrap().is_none());
                journal_faults::clock(&journal_faults::default_journal(engine, fleet))
            }).await;
            let path = "/v1/evidence?window=1599955200000,1600041600000&holder=i0/0/0".to_string();
            let (status, pending) = get(&f.state, &path, &cookie).await;
            // Release before response assertions so a failure cannot leave retirement held.
            mutate(&f, |engine, _, _| {
                engine
                    .run("SYSTEM START MERGES recompute_minute")
                    .expect("release retirement")
            })
            .await;
            assert_eq!(status, 200, "{pending}");
            assert_eq!(
                pending["data"]["refused"]["cause"], "cursor_stale",
                "{pending}"
            );
            assert!(pending["data"]["records"].is_null());
            assert_eq!(pending["data"]["reach"]["covers"], false);
            mutate(&f, |engine, _, _| wait_for_retirement(engine)).await;
            mutate(&f, move |engine, _, fleet| {
                assert_eq!(
                    journal_faults::clock(&journal_faults::default_journal(engine, fleet)),
                    clock,
                    "retirement does not require a new arrival or completion"
                );
                assert!(journal_faults::executor(engine, fleet, &Params::default())
                    .unwrap()
                    .revision()
                    .unwrap()
                    .is_some());
            })
            .await;
            let (status, served) = get(&f.state, &path, &cookie).await;
            assert_eq!(status, 200, "{served}");
            assert_eq!(
                served["data"]["refused"]["cause"], "outside_raw_horizon",
                "{served}"
            );
            assert!(served["data"]["records"].is_null());
            assert_eq!(served["data"]["reach"]["covers"], false);
            let live = format!(
                "/v1/evidence?window={},{}&block=p",
                f.window.start_ms, f.window.end_ms
            );
            let (_, actual) = get(&f.state, &live, &cookie).await;
            assert!(actual["data"]["refused"].is_null(), "{actual}");
            assert!(!actual["data"]["records"].as_array().unwrap().is_empty());
        },
    );
}

/// Invalid transport input is never a successful bounded refusal.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn malformed_paging_and_conflicting_selectors_are_client_errors() {
    drive(
        || fixture("api_invalid_pages"),
        |f| async move {
            let cookie = session(&f.state);
            for route in [
                "/v1/holders?cursor=not-a-cursor",
                "/v1/roster?cursor=not-a-cursor",
                "/v1/days/2026-09-17?cursor=not-a-cursor",
                "/v1/evidence?block=p&cursor=not-a-cursor",
                "/v1/blocks/p/holders?cursor=not-a-cursor",
                "/v1/holders/a/0/0/timeline?cursor=not-a-cursor",
                "/v1/evidence?block=p&holder=not-a-holder",
                "/v1/evidence?day=2026-09-17&block=p",
            ] {
                let (status, body) = get(&f.state, route, &cookie).await;
                assert_eq!(status, 400, "{route}: {body}");
                assert_eq!(body["error"]["cause"], "request_invalid", "{route}: {body}");
                assert!(body.get("data").is_none(), "{body}");
            }
        },
    );
}

/// A cursor must distinguish every stored key, including epochs at the same instant.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn tied_store_keys_survive_every_event_page_boundary() {
    drive(
        || fixture("api_tied_keys"),
        |f| async move {
            // Four stores of one block at one instant, three on one holder: tied keys, after the
            // fixture's last record so they arrive in order.
            let at = f.window.start_ms + 700_000.0;
            mutate(&f, move |engine, sources, fleet| {
                let store = |instance: &str, tokens: i64| {
                    serde_json::json!({"kind": "store", "instance_id": instance, "dp_rank": 0,
                        "group_idx": 0, "block_id": "tied", "content_id": "tied",
                        "n_tokens": tokens, "at_ms": at, "incarnation": "run-1"})
                };
                // The epoch rides each segment's header, so three epochs are three segments, each
                // with the heartbeat a tap writes into every segment.
                ship(
                    engine,
                    sources,
                    fleet,
                    (1, at, None),
                    &[store("i0", 11), heartbeat(at + 20_000.0)],
                );
                ship(
                    engine,
                    sources,
                    fleet,
                    (2, at, Some(0)),
                    &[store("i0", 12), store("i1", 14), heartbeat(at + 40_000.0)],
                );
                ship(
                    engine,
                    sources,
                    fleet,
                    (3, at, Some(1)),
                    &[store("i0", 13), heartbeat(at + 60_000.0)],
                );
                journal_faults::executor(engine, fleet, &Params::default())
                    .and_then(|executor| executor.advance(&sources.source()))
                    .expect("tick");
            })
            .await;
            let cookie = session(&f.state);
            let window = format!("{},{}", f.window.start_ms, f.window.end_ms);
            let mut failures = Vec::new();
            for (base, key) in [
                (
                    format!("/v1/evidence?window={window}&block=tied"),
                    "records",
                ),
                (format!("/v1/blocks/tied/holders?window={window}"), "rows"),
                (
                    format!("/v1/holders/i0/0/0/timeline?window={window}"),
                    "rows",
                ),
            ] {
                let (_, whole) = get(&f.state, &format!("{base}&limit=200"), &cookie).await;
                let mut expected: Vec<String> = whole["data"][key]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(Value::to_string)
                    .collect();
                expected.sort();
                assert!(
                    expected.len() >= 3,
                    "{base}: the fixture must contain tied keys: {whole}"
                );
                for limit in [1, 2, 3, 4] {
                    let mut found = Vec::new();
                    let mut cursor = None;
                    for _ in 0..20 {
                        let suffix = cursor
                            .as_ref()
                            .map_or_else(String::new, |c| format!("&cursor={c}"));
                        let (status, body) =
                            get(&f.state, &format!("{base}&limit={limit}{suffix}"), &cookie).await;
                        assert_eq!(status, 200, "{body}");
                        found.extend(
                            body["data"][key]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(Value::to_string),
                        );
                        cursor = body["data"]["page"]["next_cursor"]
                            .as_str()
                            .map(str::to_string);
                        if cursor.is_none() {
                            break;
                        }
                    }
                    assert!(cursor.is_none(), "walk must terminate");
                    found.sort();
                    if found != expected {
                        failures.push(format!(
                            "{base}, limit={limit}: {} of {} rows",
                            found.len(),
                            expected.len()
                        ));
                    }
                }
            }
            assert!(failures.is_empty(), "{}", failures.join("\n"));
        },
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn queryless_operations_and_api_root_use_the_json_contract() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt as _;
    drive(
        || fixture("api_empty_query"),
        |f| async move {
            let cookie = session(&f.state);
            for (method, route, body) in [
                ("GET", "/v1/stamp?window=14d", ""),
                ("GET", "/v1/events?window=14d", ""),
                ("GET", "/v1/delivery?window=14d", ""),
                ("POST", "/v1/analyze?window=14d", ""),
                ("POST", "/v1/session?unknown=1", "{\"token\":\"invalid\"}"),
                ("DELETE", "/v1/session?unknown=1", ""),
                (
                    "POST",
                    "/v1/annotations?unknown=1",
                    "{\"at_ms\":1,\"label\":\"deploy\"}",
                ),
            ] {
                let request = Request::builder()
                    .method(method)
                    .uri(route)
                    .header("cookie", format!("infertap_session={cookie}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap();
                let response = analyzer_api::router(f.state.clone())
                    .oneshot(request)
                    .await
                    .unwrap();
                assert_eq!(response.status(), 400, "{method} {route}");
                let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
                    .await
                    .unwrap();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body["error"]["cause"], "request_invalid");
            }
            for path in ["/v1", "/v1/", "/v1/unknown"] {
                let (status, body) = get(&f.state, path, &cookie).await;
                assert_eq!(status, 404, "{path}: {body}");
                assert_eq!(body["error"]["cause"], "unknown_route");
            }
            let request = Request::builder()
                .method("HEAD")
                .uri("/v1/coverage?window=14d")
                .header("cookie", format!("infertap_session={cookie}"))
                .body(Body::empty())
                .unwrap();
            let response = analyzer_api::router(f.state.clone())
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
            assert!(axum::body::to_bytes(response.into_body(), 1 << 20)
                .await
                .unwrap()
                .is_empty());
        },
    );
}

/// One sealed segment's arrival, with no cache facts, as ingest records it.
fn delivery_batch(position: u64, at: f64) -> analyzer_pipeline::ledger::BatchRecord {
    analyzer_pipeline::ledger::BatchRecord {
        batch_id: "delivery-batch".into(),
        carrier: "segment".into(),
        source: "delivery".into(),
        object_key: String::new(),
        etag: String::new(),
        input_position: position,
        ingested_at_ms: u64::try_from(batch_ingested_secs() * 1000).unwrap(),
        record_count: 1,
        unknown_kinds: 0,
        first_at_ms: None,
        last_at_ms: None,
        days: Vec::new(),
        segments: vec![analyzer_pipeline::ledger::SegmentArrival {
            incarnation: "delivery-run".into(),
            segment_seq: 0,
            instance: None,
            first_record_at_ms: at,
            last_record_at_ms: at,
            heartbeat_secs: 0,
            max_segment_secs: 0,
            hb_count: 0,
            hb_first_at_ms: None,
            hb_last_at_ms: None,
            stop_at_ms: None,
            msgs_seen: 7,
            dropped: 0,
            oversized: 0,
            dropped_at_hb_first: 0,
            oversized_at_hb_first: 0,
            publisher_restarts: 0,
            agent_version: None,
            topic_model: None,
            topic_pod: None,
            declared_model: None,
            declared_pod: None,
            engine_version: None,
            attributed: true,
            canary: None,
        }],
        drops: Vec::new(),
        lifecycle: Vec::new(),
        stored: Vec::new(),
        annotations: Vec::new(),
        tags: Vec::new(),
    }
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine api"]
fn since_install_delivery_has_no_window_and_keeps_its_totals() {
    drive(
        || fixture("api_delivery_stamp"),
        |f| async move {
            let cookie = session(&f.state);
            let (code, before) = get(&f.state, "/v1/delivery", &cookie).await;
            assert_eq!(code, 200, "{before}");
            assert!(before["stamp"].as_object().unwrap().contains_key("window"));
            assert!(before["stamp"]["window"].is_null(), "{before}");
            // The fixture's own run is the one row so far.
            let rows = before["data"]["rows"].as_array().unwrap();
            assert_eq!(rows.len(), 1, "{before}");
            let fleet = before["data"]["fleet"].clone();
            let count = |v: &Value, key: &str| v[key].as_u64().unwrap_or_else(|| panic!("{key}"));
            let at = f.window.start_ms - 30.0 * analyzer_analysis::DAY_MS;
            mutate(&f, move |engine, _, fleet| {
                // One sealed segment's arrival, as ingest records it and the core publishes it.
                let log = journal_faults::spine(engine, fleet)
                    .lake()
                    .ledger(analyzer_pipeline::ledger::SHARD)
                    .unwrap();
                let position = log.next_position().unwrap();
                log.commit_cycle(position, vec![delivery_batch(position, at)])
                    .unwrap();
                journal_faults::executor(engine, fleet, &Params::default())
                    .and_then(|executor| executor.publish_inputs())
                    .unwrap();
            })
            .await;
            let (code, before) = get(&f.state, "/v1/delivery", &cookie).await;
            assert_eq!(code, 200, "{before}");
            assert!(before["stamp"]["window"].is_null());
            // One more run, one segment, seven messages seen.
            let now = &before["data"]["fleet"];
            for (key, more) in [
                ("segments_expected", 1),
                ("segments_captured", 1),
                ("records_seen", 7),
            ] {
                assert_eq!(
                    count(now, key),
                    count(&fleet, key) + more,
                    "{key}: {before}"
                );
            }
            assert_eq!(before["data"]["rows"].as_array().unwrap().len(), 2);
            let (_, stamp) = get(&f.state, "/v1/stamp", &cookie).await;
            assert_eq!(before["stamp"], stamp["stamp"]);
            let later = f
                .state
                .clone()
                .with_clock(now_ms() + 30.0 * analyzer_analysis::DAY_MS);
            let (code, after) = get(&later, "/v1/delivery", &cookie).await;
            assert_eq!(code, 200, "{after}");
            assert_eq!(
                after, before,
                "since-install totals and freshness do not follow the request clock"
            );
            let (code, error) = get(&f.state, "/v1/delivery?window=14d", &cookie).await;
            assert_eq!(code, 400, "{error}");
            assert_eq!(error["error"]["cause"], "request_invalid");
        },
    );
}
