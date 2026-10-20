// SPDX-License-Identifier: AGPL-3.0-only

//! The listener, end to end: a real socket, a real request, and a stop that actually stops.
//!
//! **What `goldens.rs` deliberately does not cover.** That suite drives the router through
//! `ServiceExt::oneshot` because a test that binds a port is a test the session sandbox kills.
//! These bind one, and are `#[ignore]`d beside the rest of the engine-backed battery for it.
//!
//! The property worth a socket is the SHUTDOWN. `analyzer_api::serve::serve` waits on a polled
//! flag rather than a signal, because the daemon owns SIGTERM and a process with two opinions
//! about how to stop has no ordered stop at all. A poll loop that never breaks is a daemon that
//! hangs on every `systemctl stop` — which turns crash recovery into the steady state, the exact
//! shape the daemon's signal handling exists to avoid.

use analyzer_analysis::Params;
use analyzer_api::{ApiConfig, ApiState};
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The cheapest state that is still the real one: a migrated database, a derivation source over
/// an empty tree, and no seeded facts. Nothing here asks the API for a figure — the subject is
/// the listener.
fn state(
    name: &str,
) -> (
    ApiState,
    analyzer_serving_clickhouse::engine::testing::TestDatabase,
) {
    let (engine, db) = analyzer_serving_clickhouse::engine::testing::database(name);
    analyzer_serving_clickhouse::engine::migrate(
        &engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig::default(),
    )
    .expect("migrate");

    let dir = std::env::temp_dir().join(format!("api-serving-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    let sources = analyzer_pipeline::parquet_source::SourceConfig {
        transport: analyzer_pipeline::parquet_source::SegmentTransport::Directory {
            root: dir.join("segments"),
        },
        key_prefix: String::new(),
        cache_dir: dir.join("segment-cache"),
        horizon_hours: analyzer_pipeline::parquet_source::HorizonHours::fixed(24),
        admit_corpus_forms: true,
        cache_budget_bytes: analyzer_pipeline::parquet_source::DEFAULT_SEGMENT_CACHE_BYTES,
    };
    // A serving store bound to its own namespace, over a spine of its own: the read API's
    // consistency boundary reads that spine's journal.
    let namespace = analyzer_pipeline::execution::Namespace::new("api-serving", name).expect("ns");
    let serving: Arc<dyn analyzer_pipeline::publish::Serving> =
        Arc::new(analyzer_serving_clickhouse::serving::ClickHouse::new(&engine).expect("serving"));
    serving.bind(&namespace).expect("bind");
    let spine =
        Arc::new(analyzer_pipeline::spine::local(&dir.join("spine"), &namespace).expect("spine"));
    let state = ApiState::new(
        Arc::new(Mutex::new(engine)),
        (spine, serving),
        sources,
        &Params::default(),
        analyzer_serving_clickhouse::engine::RetentionConfig::default(),
        ApiConfig::new(dir.join("tokens")),
    )
    .expect("state");
    (state, db)
}

/// One HTTP/1.1 request over a bare socket. No client crate: this asks whether bytes cross a
/// real listener, and a dependency would only get in the way of that question.
fn request(addr: SocketAddr, line: &str) -> String {
    let mut socket = TcpStream::connect(addr).expect("connect to the read API");
    socket
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("read timeout");
    write!(
        socket,
        "{line} HTTP/1.1\r\nHost: analyzer\r\nConnection: close\r\n\r\n"
    )
    .expect("write the request");
    let mut response = String::new();
    socket
        .read_to_string(&mut response)
        .expect("read the reply");
    response
}

#[test]
#[ignore = "integration: binds a socket and needs the pinned engine. Run: scripts/test.sh engine api"]
fn the_api_answers_on_a_real_socket_and_stops_when_the_daemon_does() {
    let (state, _db) = state("serving");
    let listener = analyzer_api::serve::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind");
    let addr = listener.local_addr().expect("the bound address");

    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let serving = std::thread::Builder::new()
        .name("api-test".to_string())
        .spawn(move || {
            analyzer_api::serve::serve(listener, state, move || flag.load(Ordering::Relaxed))
        })
        .expect("spawn");

    // **Unauthenticated, and 401 is the pass condition.** Every endpoint sits behind a session
    // including `/v1/stamp`, so a reachable listener answering 401 proves the whole path: the
    // socket accepted, axum routed, and the auth middleware ran. A 200 here would be the defect.
    let answer = request(addr, "GET /v1/stamp");
    assert!(
        answer.starts_with("HTTP/1.1 401"),
        "a reachable API refuses an anonymous read; got: {}",
        answer.lines().next().unwrap_or("<nothing>")
    );
    assert!(
        answer.contains("unauthenticated"),
        "and it names the cause a console switches on: {answer}"
    );

    // The subject of the test.
    stop.store(true, Ordering::Relaxed);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !serving.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        serving.is_finished(),
        "the graceful-shutdown poll must end when the daemon's flag is set. It did not, which \
         means `systemctl stop` hangs and every ordinary stop becomes crash recovery"
    );
    serving
        .join()
        .expect("the serving thread did not panic")
        .expect("serving ended cleanly");
}

#[test]
#[ignore = "integration: binds a socket and needs the pinned engine. Run: scripts/test.sh engine api"]
fn the_embedded_console_serves_its_document_and_deep_links_reach_it() {
    // Only meaningful in a build that staged one. A clean checkout has no console and asserts the
    // refusal instead, in the test below.
    if !analyzer_api::console::present() {
        return;
    }
    let (state, _db) = state("serving-embedded");
    let listener = analyzer_api::serve::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind");
    let addr = listener.local_addr().expect("the bound address");

    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let serving = std::thread::Builder::new()
        .name("api-embedded".to_string())
        .spawn(move || {
            analyzer_api::serve::serve(listener, state, move || flag.load(Ordering::Relaxed))
        })
        .expect("spawn");

    let root = request(addr, "GET /");
    assert!(
        root.starts_with("HTTP/1.1 200"),
        "the document serves without a session — the sign-in screen IS the console, so requiring \
         one to fetch it would make authentication unreachable. Got: {}",
        root.lines().next().unwrap_or("<nothing>")
    );
    assert!(
        root.contains("text/html"),
        "and it is HTML rather than an octet-stream download prompt: {root:.200}"
    );

    // **The SPA fallback, which the three-level addressing depends on.** A reader pastes a holder
    // address; no such file exists, and a 404 would break the console's own grammar.
    let deep = request(addr, "GET /holder/i-004/0/1");
    assert!(
        deep.starts_with("HTTP/1.1 200") && deep.contains("text/html"),
        "a deep link must reach index.html so its router can resolve the address: {}",
        deep.lines().next().unwrap_or("<nothing>")
    );

    // **But a missing ASSET is a 404, not the document.** Answering a script request with HTML
    // produces a syntax error in a file that looks fine, which is a genuinely hard afternoon.
    let missing = request(addr, "GET /assets/never-built-this.js");
    assert!(
        missing.starts_with("HTTP/1.1 404"),
        "a missing asset must not fall back to the document: {}",
        missing.lines().next().unwrap_or("<nothing>")
    );

    stop.store(true, Ordering::Relaxed);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !serving.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(serving.is_finished(), "the listener stops on the flag");
    let _ = serving.join().expect("no panic");
}

#[test]
#[ignore = "integration: binds a socket and needs the pinned engine. Run: scripts/test.sh engine api"]
fn a_console_path_is_refused_with_a_named_cause_when_no_bundle_is_built_in() {
    // Only meaningful in a build that staged nothing — the state a clean checkout produces.
    if analyzer_api::console::present() {
        return;
    }
    // A build with no console staged must read as a deployment fact and never as a 404 — the difference
    // between "this build serves no console" and "that URL is wrong" is the whole answer.
    let (state, _db) = state("serving-console");
    let listener = analyzer_api::serve::bind(SocketAddr::from(([127, 0, 0, 1], 0))).expect("bind");
    let addr = listener.local_addr().expect("the bound address");

    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let serving = std::thread::Builder::new()
        .name("api-test-console".to_string())
        .spawn(move || {
            analyzer_api::serve::serve(listener, state, move || flag.load(Ordering::Relaxed))
        })
        .expect("spawn");

    let answer = request(addr, "GET /holder/i-004/0/1");
    assert!(
        answer.starts_with("HTTP/1.1 503"),
        "a missing bundle is a deployment fact, not a wrong URL; got: {}",
        answer.lines().next().unwrap_or("<nothing>")
    );
    assert!(
        answer.contains("console_not_configured"),
        "and it says so by name: {answer}"
    );

    stop.store(true, Ordering::Relaxed);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !serving.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(serving.is_finished(), "the listener stops on the flag");
    let _ = serving.join().expect("no panic");
}
