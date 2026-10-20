// SPDX-License-Identifier: AGPL-3.0-only

//! Structural hygiene for the workspace, test-enforced (the tap's static-audit pattern,
//! analyzer edition). Differences from the tap are deliberate and stated: this product
//! MUST spawn a process (the pinned `ClickHouse` engine), so `process::Command` is allowed
//! in exactly one crate — `analyzer-serving-clickhouse`, the engine boundary — and forbidden
//! everywhere else. The dependency allowlist starts EMPTY: every future crate dependency
//! is a deliberate, justified addition to this test, per boundary crate.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    // crates/cli -> workspace root
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Walk a directory for `.rs` files, tolerating one that does not exist.
///
/// **The absence is legitimate.** A crate may have no integration tests, and git does not track
/// empty directories, so a `tests/` directory can exist locally and not in a fresh clone. A
/// missing directory contributes no sources; that is the answer, not a panic.
fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn crate_dirs() -> Vec<PathBuf> {
    let crates = workspace_root().join("crates");
    let mut dirs: Vec<PathBuf> = fs::read_dir(crates)
        .expect("crates dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.is_dir())
        .collect();
    dirs.sort();
    dirs
}

#[test]
fn unsafe_is_forbidden_workspace_wide() {
    let ws = fs::read_to_string(workspace_root().join("Cargo.toml")).expect("workspace toml");
    assert!(
        ws.contains("unsafe_code = \"forbid\""),
        "workspace must keep [workspace.lints.rust] unsafe_code = \"forbid\""
    );
    for dir in crate_dirs() {
        let toml = fs::read_to_string(dir.join("Cargo.toml")).expect("crate toml");
        assert!(
            toml.contains("[lints]") && toml.contains("workspace = true"),
            "{} does not inherit workspace lints",
            dir.display()
        );
        let mut sources = Vec::new();
        rust_sources(&dir.join("src"), &mut sources);
        for path in &sources {
            let text = fs::read_to_string(path).expect("read source");
            assert!(
                !text.contains("allow(unsafe_code)"),
                "{} re-allows `unsafe_code`; `unsafe` is forbidden workspace-wide \
                 (Cargo.toml, [workspace.lints.rust]) and no crate may opt out. Remove the \
                 allow and write the code without `unsafe`.",
                path.display()
            );
        }
    }
}

/// Execution mutation entry points are private to pipeline; the compiler requires the shared
/// executor. This audit keeps daemon configuration in one constructor, while the real
/// lost-acknowledgment tests pin recovery and exclusion through that executor.
#[test]
fn daemon_execution_configuration_has_one_owner() {
    let daemon = workspace_root().join("crates/daemon/src");
    let mut sources = Vec::new();
    rust_sources(&daemon, &mut sources);
    let mut constructors = 0;
    for path in &sources {
        let text = fs::read_to_string(path).expect("read source");
        let calls = text.matches("Executor::open(").count();
        if path.file_name().is_some_and(|n| n == "execution.rs") {
            constructors += calls;
        } else {
            assert_eq!(calls, 0,
                "{} constructs an executor outside daemon/src/execution.rs; use Daemon::executor so namespace, options and caches have one owner",
                path.display());
        }
    }
    assert_eq!(
        constructors, 1,
        "daemon/src/execution.rs must contain the one configured Executor::open call"
    );
}

#[test]
fn process_spawn_is_confined_to_the_engine_boundary() {
    // The engine boundary (the serving crate's `engine` module) is the ONE place allowed to
    // reach process::Command — it owns the pinned ClickHouse invocation. Everywhere else,
    // spawning is forbidden; libc is forbidden everywhere.
    for dir in crate_dirs() {
        let engine = workspace_root().join("crates/serving-clickhouse/src/engine");
        let mut sources = Vec::new();
        rust_sources(&dir.join("src"), &mut sources);
        for path in &sources {
            let text = fs::read_to_string(path).expect("read source");
            assert!(
                !text.contains("libc::"),
                "{} uses `libc::`. Raw system calls are confined to the engine boundary \
                 (crates/serving-clickhouse/src/engine/server.rs, where the engine is started and supervised); \
                 everything else goes through the standard library or the storage crate's API.",
                path.display()
            );
            if !path.starts_with(&engine) {
                assert!(
                    !text.contains("process::Command"),
                    "{} spawns a process. `process::Command` is allowed only in \
                     crates/serving-clickhouse/src/engine/server.rs, the one place that starts the engine; \
                     nothing else in the workspace may spawn.",
                    path.display()
                );
            }
        }
    }
}

/// The workspace crates some crate's `[dependencies]` names: the ones a shipped binary can
/// contain. A crate named only under `[dev-dependencies]` ships in nothing.
fn crates_named_in_dependencies() -> BTreeSet<String> {
    let mut named = BTreeSet::new();
    for dir in crate_dirs() {
        let manifest = fs::read_to_string(dir.join("Cargo.toml")).expect("read manifest");
        let mut section = "";
        for line in manifest.lines() {
            if line.starts_with('[') {
                section = line.trim();
            } else if section == "[dependencies]" && line.starts_with("analyzer-") {
                named.insert(line.split(['=', ' ']).next().unwrap_or("").to_string());
            }
        }
    }
    named
}

#[test]
fn network_use_is_confined_to_our_io_layer_and_the_engine_reaches_nothing() {
    // The network fence, and the posture it states:
    //
    // - **Outbound, one crate.** `crates/objectstore` is the only crate that egresses: the
    //   transport to the customer's bucket is ours, and the engine reaches nothing. A
    //   `SELECT … FROM s3(…)` is a string, not an API name, so the checks below look for the
    //   strings: no SQL this workspace builds may name the bucket, and the generated engine
    //   configuration may not carry a collection to name.
    // - **The engine, loopback only.** The serving crate talks to the `clickhouse-server` it
    //   owns over loopback HTTP. That limit is enforced by construction in
    //   `Engine::connect_server`, which refuses any endpoint that is not literal loopback
    //   (`the_engine_endpoint_can_only_ever_be_loopback`); a grep could not see a
    //   dependency's sockets.
    // - **Inbound, two listeners.** `daemon/src/metrics.rs` serves one read-only document
    //   (the latest self-health sample) and discards the request unread. `crates/api` serves
    //   the read API: a route table, authentication, and the one listener a party off the
    //   host is expected to reach; no endpoint alters a measurement. Both bind loopback unless
    //   the operator names another address, and `off` turns either off. No other crate
    //   listens.
    for dir in crate_dirs() {
        let mut sources = Vec::new();
        rust_sources(&dir.join("src"), &mut sources);
        for path in &sources {
            let text = fs::read_to_string(path).expect("read source");
            for line in text.lines() {
                let code = line.trim_start();
                if code.starts_with("//") || code.starts_with("///") || code.starts_with("//!") {
                    continue; // history is written down on purpose; only live code is fenced
                }
                for forbidden in ["FROM s3(", "INTO FUNCTION s3(", "TO S3(", "records_bucket"] {
                    assert!(
                        !code.contains(forbidden),
                        "{}: the engine is handed object storage again ({forbidden:?}). Object \
                         I/O belongs to crates/objectstore; the engine gets rows and questions.",
                        path.display()
                    );
                }
            }
        }
    }

    // The engine's own configuration cannot name a bucket: no named collection is rendered, so
    // there is no credential at rest in it and nothing for a statement to reference.
    let rendered = analyzer_serving_clickhouse::engine::server::ServerConfig::new(
        std::path::Path::new("/tmp/fence"),
    )
    .render_config_xml();
    for forbidden in ["named_collections", "records_bucket", "access_key_id"] {
        assert!(
            !rendered.contains(forbidden),
            "the generated engine config names {forbidden:?}; the engine must not be able to \
             reach object storage at all"
        );
    }

    let listening = ["TcpListener", "UdpSocket"];
    let outbound = ["TcpStream", "std::net::"];
    // The test-support module probes loopback ports before a test's engine binds them
    // (`port_block`). It is compiled only under `analyzer-serving-clickhouse`'s `testing`
    // feature, which only dev-dependencies switch on, or a crate that is itself only ever a
    // dev-dependency (the harness), so no shipped binary contains it; that premise is checked
    // here rather than trusted.
    let shipped = crates_named_in_dependencies();
    for dir in crate_dirs() {
        let manifest = fs::read_to_string(dir.join("Cargo.toml")).expect("read manifest");
        let crate_name = format!(
            "analyzer-{}",
            dir.file_name()
                .expect("a crate directory")
                .to_string_lossy()
        );
        let mut section = "";
        for line in manifest.lines() {
            if line.starts_with('[') {
                section = line.trim();
            } else if line.contains("analyzer-serving-clickhouse") && line.contains("\"testing\"") {
                assert!(
                    section == "[dev-dependencies]" || !shipped.contains(&crate_name),
                    "{} enables analyzer-serving-clickhouse's `testing` feature in its {section}, \
                     and a shipped crate depends on it, which would ship the port probe",
                    dir.display()
                );
            }
        }
    }
    for dir in crate_dirs() {
        let engine = workspace_root().join("crates/serving-clickhouse/src/engine");
        let mut sources = Vec::new();
        rust_sources(&dir.join("src"), &mut sources);
        for path in &sources {
            let is_metrics_listener = path.ends_with("daemon/src/metrics.rs");
            let is_test_support = path.ends_with("serving-clickhouse/src/engine/testing.rs");
            // The read API's listening is inside `axum`, so no API name appears in our sources
            // and this grep would pass either way. The exemption is written anyway, because the
            // fence is a STATEMENT of posture as much as a check: a reader auditing what may
            // listen must find every exception here rather than discovering one in a Cargo.toml.
            let is_read_api = path.components().any(|c| c.as_os_str() == "api");
            let text = fs::read_to_string(path).expect("read source");
            for forbidden in listening {
                assert!(
                    is_metrics_listener
                        || is_read_api
                        || is_test_support
                        || !text.contains(forbidden),
                    "{} listens; only daemon/src/metrics.rs, crates/api and the test-support \
                     port probe may ({forbidden:?})",
                    path.display()
                );
            }
            if is_metrics_listener || is_read_api {
                continue; // the accepted connection each of them serves
            }
            if !path.starts_with(&engine) {
                for forbidden in outbound {
                    assert!(
                        !text.contains(forbidden),
                        "{} reaches the network outside the engine boundary ({forbidden:?})",
                        path.display()
                    );
                }
            }
        }
    }
}

/// The allowlist itself — two columns per crate: third-party crates, then workspace edges.
///
/// Lifted out of the test body because the table is the DOCUMENT and the loop below is four
/// lines of comparison. A reviewer reads this; nothing here executes.
type Allowlist = &'static [(
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
)];
#[allow(clippy::too_many_lines)] // the workspace's dependency budget, one reviewed entry per crate
fn expected_dependencies() -> Allowlist {
    &[
        // **Empty, and the emptiness is the point: the bright line.** This crate is the fold:
        // typed events in, figures out. It reads nothing, writes nothing and spawns nothing,
        // so a partner can run it without our appliance. A dependency here is a claim that
        // the derivation needs the world.
        ("analysis", &[], &[]),
        // The read API: the HTTP server and its runtime, the wire format, the contract's
        // schemas (`utoipa`) and the constant-time credential comparison (`subtle`). Each
        // entry's reason is in the crate's Cargo.toml.
        (
            "api",
            &[
                "log",
                "thiserror",
                "axum",
                "tokio",
                "tower-http",
                "serde",
                "serde_json",
                "utoipa",
                "subtle",
                "tokio-stream",
            ],
            &[
                "analyzer-serving-clickhouse",
                "analyzer-analysis",
                "analyzer-pipeline",
            ],
        ),
        // The binary: the --config file (`toml`), the shell's error enum, the selftest's
        // corpus (`serde_json`), and jemalloc as the allocator, so the resident size a box or
        // a container meters is what the daemon holds.
        (
            "cli",
            &[
                "log",
                "toml",
                "thiserror",
                "serde_json",
                "tikv-jemallocator",
            ],
            &[
                "analyzer-lake",
                "analyzer-daemon",
                "analyzer-ingest",
                "analyzer-serving-clickhouse",
                "analyzer-analysis",
                "analyzer-pipeline",
                "analyzer-api",
            ],
        ),
        // The loop is cadence and policy over operations that belong to the crates beneath it.
        // Its one third-party need is SIGTERM: `std` has no signal API and `libc` is forbidden
        // in our sources, so an ordered stop needs `signal-hook`'s `flag` module (a handler
        // that only sets an atomic), default features off. Preflight probes the bucket through
        // the object-storage port, the transport the product uses, so a deployment whose engine
        // will not start still learns whether its bucket works.
        (
            "daemon",
            &["log", "thiserror", "signal-hook"],
            &[
                "analyzer-lake",
                "analyzer-objectstore",
                "analyzer-serving-clickhouse",
                "analyzer-ingest",
                "analyzer-analysis",
                "analyzer-pipeline",
            ],
        ),
        // The suites' shared harness: a dev-dependency of every suite and of no shipped
        // binary. It writes Parquet fixtures with the reader's own crate, and reaches every
        // layer a suite drives, the serving store's test support included.
        (
            "harness",
            &["serde_json", "parquet"],
            &[
                "analyzer-analysis",
                "analyzer-lake",
                "analyzer-pipeline",
                "analyzer-ingest",
                "analyzer-segments",
                "analyzer-objectstore",
                "analyzer-serving-clickhouse",
            ],
        ),
        // Segment to input log. The tap's write-ahead form is JSON. Ingest lists and fetches
        // segment bytes through the object-storage port and writes the input log; no serving
        // store is on its path.
        (
            "ingest",
            &["log", "thiserror", "serde_json"],
            &["analyzer-objectstore", "analyzer-lake", "analyzer-segments"],
        ),
        // The bucket's layout and the lake's commit primitives: serialized documents, hashed
        // lineage ids, one error enum. `rocksdb` holds the working state between checkpoints;
        // `self_cell` lets a stream own the database handle its iterator reads with no unsafe
        // code of ours; `log` says what a restore, a rollback and a checkpoint did.
        (
            "lake",
            &[
                "thiserror",
                "serde",
                "serde_json",
                "sha2",
                "log",
                "rocksdb",
                "self_cell",
            ],
            &["analyzer-objectstore"],
        ),
        // A separate-crate module: the pure fold and the public registration only. The
        // framework owns a module's tables.
        (
            "module-example",
            &[],
            &["analyzer-analysis", "analyzer-pipeline"],
        ),
        // The workspace's one outbound network client. `object_store` is bought rather than
        // written because the decisive part is the credential chain (IMDSv2's token TTL, IRSA's
        // rotated token file, expiry under clock skew), which fails weeks in on an unattended
        // box where nothing may report back to us. `tokio` drives it on a current-thread
        // runtime, `bytes` is its payload type, and `futures-core` brings the `Stream` trait
        // alone. No workspace edges, deliberately: a transport that knew about records would be
        // a second place the record model lives.
        (
            "objectstore",
            &[
                "log",
                "thiserror",
                "object_store",
                "tokio",
                "bytes",
                "futures-core",
            ],
            &[],
        ),
        // The engine-free core: the keyed driver and its executor, the journal, the serving
        // port. `snap` compresses the log partitioned by block; `sha2` names a namespace's
        // objects. No serving store is in its closure (`the_core_depends_on_no_serving_store`).
        (
            "pipeline",
            &["log", "thiserror", "snap", "serde", "serde_json", "sha2"],
            &[
                "analyzer-objectstore",
                "analyzer-lake",
                "analyzer-analysis",
                "analyzer-segments",
            ],
        ),
        // **The consumer's reader, and its emptiness of workspace edges is the assertion.** It
        // holds the record model's decode and nothing that talks to a store, so a partner links
        // it beside `analysis` and gets a reader and the folds with no database in the closure.
        // `parquet` because Parquet is the shipped form and this crate is its decode; `snap`
        // is in parquet's closure as the contract's codec floor; `bytes` is parquet's input
        // type; `arrow-array` and `arrow-schema` are the columnar read's types, already in
        // parquet's tree and direct only because parquet does not re-export them.
        (
            "segments",
            &[
                "inference-events", // shared wire validation; no store or network dependency
                "thiserror",
                "serde_json",
                "sha2",
                "hex",
                "parquet",
                "bytes",
                "arrow-array",
                "arrow-schema",
            ],
            &[],
        ),
        // The ClickHouse serving store: the engine it owns, its migrations, the serving port's
        // implementation and every read the product performs. The official `clickhouse` client
        // (default features off, so no compression codec and no TLS stack) over loopback, on
        // `tokio`; `sha2` for the engine credential, of which only the digest reaches disk;
        // `serde_json` for the namespace binding and the self-health artifacts. The two
        // supply-chain consequences of `clickhouse` are recorded in deny.toml.
        (
            "serving-clickhouse",
            &[
                "log",
                "thiserror",
                "sha2",
                "clickhouse",
                "tokio",
                "serde_json",
            ],
            &[
                "analyzer-analysis",
                "analyzer-lake",
                "analyzer-pipeline",
                "analyzer-ingest",
            ],
        ),
    ]
}

/// **The core depends on no serving store** (`docs/lake.md`): the
/// pipeline's closure of normal dependencies, followed through every workspace crate's own
/// manifest, reaches neither the `ClickHouse` serving crate nor the `ClickHouse` client. The core
/// states its output through the serving port and never reads it back; a store is the
/// deployment's choice, bound at the edge. The allowlist pins each crate's direct edges; this
/// pins the property the edges exist to keep, however a later edge would reintroduce it.
#[test]
fn the_core_depends_on_no_serving_store() {
    fn normal_dependencies(krate: &str) -> Vec<String> {
        let dir = workspace_root()
            .join("crates")
            .join(krate.trim_start_matches("analyzer-"));
        let toml = fs::read_to_string(dir.join("Cargo.toml")).expect("crate toml");
        toml.lines()
            .skip_while(|l| l.trim() != "[dependencies]")
            .skip(1)
            .take_while(|l| !l.trim().starts_with('['))
            .filter_map(|l| l.split('=').next())
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.starts_with('#'))
            .map(str::to_string)
            .collect()
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut queue = vec!["analyzer-pipeline".to_string()];
    let mut third_party = std::collections::BTreeSet::new();
    while let Some(krate) = queue.pop() {
        if !seen.insert(krate.clone()) {
            continue;
        }
        for dep in normal_dependencies(&krate) {
            if dep.starts_with("analyzer-") {
                queue.push(dep);
            } else {
                third_party.insert(dep);
            }
        }
    }
    assert!(
        !seen.contains("analyzer-serving-clickhouse"),
        "the core's closure reaches the ClickHouse serving store: {seen:?}"
    );
    assert!(
        !third_party.contains("clickhouse"),
        "the core's closure reaches the ClickHouse client: {third_party:?}"
    );
    assert!(
        seen.contains("analyzer-lake") && seen.contains("analyzer-analysis"),
        "the walk found the core's own edges, or it proves nothing: {seen:?}"
    );
}

#[test]
fn dependency_allowlist_per_crate() {
    let expected = expected_dependencies();
    for (dir, (name, allowed, workspace)) in crate_dirs().iter().zip(expected) {
        assert_eq!(
            dir.file_name().unwrap().to_str().unwrap(),
            *name,
            "the workspace's crate list changed: crates/{} sits where the allowlist expected \
             `{name}`. Add a new crate to `expected_dependencies()` in \
             crates/cli/tests/static_audit.rs with its two columns (third-party crates, then \
             workspace edges), or remove the entry of a crate that left; keep the order of the \
             crates/ directory.",
            dir.file_name().unwrap().to_str().unwrap()
        );
        let toml = fs::read_to_string(dir.join("Cargo.toml")).expect("crate toml");
        let declared: Vec<String> = toml
            .lines()
            .skip_while(|l| l.trim() != "[dependencies]")
            .skip(1)
            .take_while(|l| !l.trim().starts_with('['))
            .filter_map(|l| l.split('=').next())
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.starts_with('#'))
            .map(str::to_string)
            .collect();
        // `partition` preserves order within each side, so both columns stay in the order the
        // crate's own toml declares them — a reordering is a diff to review, not a silent pass.
        let (ws, third): (Vec<String>, Vec<String>) = declared
            .into_iter()
            .partition(|s| s.starts_with("analyzer-"));
        assert_eq!(
            third, *allowed,
            "{name}: the third-party dependency set in crates/{name}/Cargo.toml (left) differs \
             from the allowlist (right). A new crate is allowed by adding it to `{name}`'s first \
             column in `expected_dependencies()` (crates/cli/tests/static_audit.rs) with a \
             comment saying why it is needed, and to deny.toml if its licence or source is new \
             there (CLAUDE.md, \"Adding a dependency\"); a crate that left is removed the same way."
        );
        assert_eq!(
            ws, *workspace,
            "{name}: WORKSPACE dependency set changed. This is an ARCHITECTURE change — a new \
             edge between crates — not a supply-chain one. For `analysis` specifically, any \
             entry at all retires the store-free bright line."
        );
    }
}

#[test]
fn the_server_backend_prepares_every_query() {
    // The transport's version of the quoting law, as a ratchet rather than a habit.
    //
    // The client reads `?` in raw SQL as a bind placeholder, even inside a string
    // literal. This codebase never binds — every value reaches SQL through
    // `quote_literal` — so a surviving `?` is always a bug, and its failure mode is the
    // worst kind: passes every harness, fails on the first customer whose fleet id or
    // workload pattern contains a question mark. The defense is structural: `prepare` is
    // the only path to the client, and this counts the call sites so a new one cannot
    // quietly skip it.
    let storage_src =
        fs::read_to_string(workspace_root().join("crates/serving-clickhouse/src/engine/mod.rs"))
            .expect("storage lib");
    let sites: Vec<usize> = storage_src
        .match_indices(".query(")
        .map(|(i, _)| i)
        .collect();
    assert!(
        sites.len() == 2,
        "crates/serving-clickhouse/src/engine/mod.rs has {} `.query(` call sites, the ratchet expects 2.\n\
         Every statement reaches the client through `statements::prepare`, which is the one \
         place placeholders are escaped; a new transport path must take its SQL from there. \
         If the change is intended, update the expected count in \
         `the_server_backend_prepares_every_query` (crates/cli/tests/static_audit.rs) and say \
         in its comment what the new site is.",
        sites.len()
    );
    for i in sites {
        let window = &storage_src[i..storage_src.len().min(i + 40)];
        assert!(
            window.contains("&prepared"),
            "a client query is built from something other than a prepared statement:\n{window}"
        );
    }
    // and `prepare` is the only place the escape exists, so it cannot be reimplemented
    // per-site with a different idea of the rules
    let statements_src = fs::read_to_string(
        workspace_root().join("crates/serving-clickhouse/src/engine/statements.rs"),
    )
    .expect("statements");
    assert_eq!(
        statements_src
            .matches("fn escape_client_placeholders")
            .count(),
        1,
        "the escape must have exactly one definition"
    );
    assert!(
        !storage_src.contains("escape_client_placeholders"),
        "the escape is applied inside `prepare`, never at a call site"
    );
}

#[test]
fn sql_interpolation_routes_through_the_quoting_helpers() {
    // The boundary rule ("no raw string reaches SQL unquoted") as a ratchet, not a
    // comment: every `.run(&format!` call site in the storage crate must visibly route
    // its interpolations through quote_ident/quote_literal (or interpolate only numerics,
    // reviewed here). A new interpolating call site fails this count until reviewed in --
    // the dependency-allowlist shape applied to SQL construction.
    let storage_src =
        fs::read_to_string(workspace_root().join("crates/serving-clickhouse/src/engine/mod.rs"))
            .expect("lib");
    let sites: Vec<usize> = storage_src
        .match_indices(".run(&format!")
        .map(|(i, _)| i)
        .collect();
    assert!(
        sites.len() == 4,
        "crates/serving-clickhouse/src/engine/mod.rs has {} `.run(&format!` call sites, the ratchet expects 4.\n\
         Each one interpolates into SQL, so every value it splices must route through \
         `quote_literal` (a string) or `quote_ident` (a table or column name) within the \
         statement's own lines. If the change is intended, update the expected count in \
         `sql_interpolation_routes_through_the_quoting_helpers` (crates/cli/tests/static_audit.rs) \
         and add the new site to the reviewed list in its comment.",
        sites.len()
    );
    // The sites: `apply_retention` (the table through quote_ident, a validated integer
    // interval), `truncate_all_but_migrations` (the table through quote_ident), and the rest
    // in the reviewed list.
    for i in sites {
        let window = &storage_src[i..storage_src.len().min(i + 300)];
        assert!(
            window.contains("quote_literal") || window.contains("quote_ident"),
            "an interpolating run() site does not visibly route through the quoting \
             helpers:\n{window}"
        );
    }
}

// Every statement verb this workspace could build, DDL and backup included: a ratchet that knew
// only the read and write verbs could not see a `DROP PARTITION`.
const SQL_VERBS: &[&str] = &[
    "SELECT ",
    "INSERT INTO",
    "DELETE FROM",
    "ALTER TABLE",
    "CREATE DATABASE",
    "CREATE TABLE",
    "DROP DATABASE",
    "DROP TABLE",
    "BACKUP DATABASE",
    "RESTORE DATABASE",
    "OPTIMIZE TABLE",
    "TRUNCATE TABLE",
];

/// Offsets of the `format!`/`write!` sites in `src` that build SQL.
///
/// **The verb must be in the site's OWN string literal**, not merely near it: a window scan would
/// count `format!("backup dir: {e}")` as a query because a `SELECT` sat nearby, and then fail it
/// for not quoting what it had no reason to quote.
fn sql_sites(src: &str) -> Vec<usize> {
    let bytes = src.as_bytes();
    let mut sites = Vec::new();
    for (i, _) in src
        .match_indices("format!(")
        .chain(src.match_indices("write!("))
    {
        let Some(paren) = src[i..].find('(') else {
            continue;
        };
        let after = i + paren + 1;
        // The literal opens the arguments, allowing for a line break after the paren.
        //
        // Scanned by CHAR rather than sliced at `after + 60`: a byte offset 60 ahead can land
        // inside a multibyte character, and `&str[..]` panics there. This file's own subject
        // matter guarantees the case — the sources it audits are dense with em dashes — and the
        // audit crashed the first time one landed within 60 bytes of a `format!(`. A test that
        // panics rather than fails is a test whose verdict nobody gets.
        let Some(quote) = src[after..]
            .char_indices()
            .take_while(|(offset, _)| *offset < 60)
            .find(|(_, ch)| *ch == '"')
            .map(|(offset, _)| offset)
        else {
            continue;
        };
        let start = after + quote + 1;
        let mut end = start;
        while end < src.len() {
            match bytes[end] {
                b'\\' => end += 2, // an escape, including the `\`-newline continuations
                b'"' => break,
                _ => end += 1,
            }
        }
        if src
            .get(start..src.len().min(end))
            .is_some_and(|lit| SQL_VERBS.iter().any(|verb| lit.contains(verb)))
        {
            sites.push(i);
        }
    }
    sites
}

/// Every file outside `storage`'s own two ratchets that builds SQL, with its reviewed site count.
///
/// Ingest carries the first CALLER-SUPPLIED strings through the quoting boundary (fleet ids,
/// batch keys, file paths) and analysis builds every windowed query — the surfaces the storage
/// ratchet was built for, extended here: an SQL-building site must visibly route through
/// `quote_literal` or `quote_ident` (the window looks 300 chars back for bindings and 700 forward
/// for inline args). The per-file count is the ratchet: a new SQL-building site fails until
/// reviewed here.
const RATCHETED_SQL_FILES: &[(&str, usize)] = &[
    // Zero, locked: these files build no SQL, and a first site in any of them is a review.
    // Ingest writes the input log and nothing else; preflight probes the bucket through the
    // object-storage port; the derivation reads segments through that port; the execution
    // namespace's claims read the lake.
    ("crates/pipeline/src/execution.rs", 0),
    ("crates/ingest/src/lib.rs", 0),
    // The harness resets a suite's database by truncating each table, its name through
    // `quote_ident`.
    ("crates/harness/src/lib.rs", 1),
    ("crates/ingest/src/bucket.rs", 0),
    ("crates/daemon/src/preflight.rs", 0),
    ("crates/pipeline/src/parquet_source/mod.rs", 0),
    // The framework's writer, read-back and retire over the `Persisted` contract, and the
    // installer of a module's declared tables. The insert: the table through quote_ident,
    // every column name through `Writer::columns`, the rows a RowBinary stream. The served
    // read: the table through quote_ident, fleet and meaning pair through quote_literal, the
    // day ranges through sql_i64. The retire: the same, the version through sql_i64. The
    // installer: every identifier through quote_ident, the types and engine family this
    // crate's own literals.
    ("crates/serving-clickhouse/src/persist.rs", 4),
    // One module per read shape, so a new query fails the line that names its shape. Every
    // read binds the fleet id and, where the table carries it, the meaning pair through
    // quote_literal immediately above its statement; ranks and window bounds interpolate as
    // rendered numerics. `day_literal` builds a `toDate(...)` fragment with no verb, so it is
    // not a site.
    ("crates/serving-clickhouse/src/shape1.rs", 1),
    // The blocks by stored mass: a day's summary and its ranked blocks; days, day index and
    // limit through sql_i64.
    ("crates/serving-clickhouse/src/events.rs", 2),
    // The run-level verdict facts and the per-instance display rows over the served segment
    // arrivals.
    ("crates/serving-clickhouse/src/roster.rs", 2),
    // The raw span and the days not yet classified, over the served input tables; the input
    // format and the meaning pair through quote_literal.
    ("crates/serving-clickhouse/src/raw.rs", 2),
    // The coverage read and its token-weighted basis; day bounds through `day_literal`.
    ("crates/serving-clickhouse/src/coverage.rs", 2),
    // The delivery ledger since install: the per-run arrivals aggregate and the declared-drops
    // rollup; the sequence arithmetic lives in Rust.
    ("crates/serving-clickhouse/src/delivery.rs", 2),
    // The copies histogram: the reach probe and the bucket, members, scope-total,
    // fleet-scalars and fleet-profile reads, each taking its table and bucket column from
    // `copies_tier` (the table through quote_ident, the column one of two literals); buckets
    // and window bounds through sql_i64.
    ("crates/serving-clickhouse/src/replication_read.rs", 8),
    // The whale ranking and its totals; day bounds through sql_i64.
    ("crates/serving-clickhouse/src/whales.rs", 2),
    // The crate root: the minute tier's measured reach, a standing fact of
    // `derivation_status` (fact name through quote_literal), and the published segment rows
    // the arrivals read.
    ("crates/serving-clickhouse/src/lib.rs", 4),
    // Co-residency token-time per workload and holder pair, and each instance's spans; day
    // bounds through sql_i64.
    ("crates/serving-clickhouse/src/placement.rs", 2),
    // The thrash lens: the minute read over `band_minute` or `band_rank_minute`, the day and
    // hour reads over the band rollups, and the banked reach over the grain's table; table
    // names are literals.
    ("crates/serving-clickhouse/src/bands.rs", 4),
    // The stored-mass denominator and the zero basis over `facts_rollup_daily` and
    // `input_key_spaces`; the fleet and meaning through quote_literal, day bounds as epoch
    // seconds.
    ("crates/serving-clickhouse/src/stored.rs", 2),
    // The markers read.
    ("crates/serving-clickhouse/src/markers.rs", 1),
    // The input log's newest batch and its totals: the status strip's hydration source.
    ("crates/serving-clickhouse/src/ingest.rs", 2),
    // The test harness: creates and drops a database per test, both names through quote_ident.
    ("crates/serving-clickhouse/src/engine/testing.rs", 2),
    // The escalated reclamation: the heaviest table's name through quote_ident.
    ("crates/daemon/src/reclaim.rs", 1),
    // The tag grouped read: window bounds through sql_i64.
    ("crates/serving-clickhouse/src/tags.rs", 2),
];

#[test]
fn sql_construction_outside_storage_routes_through_the_quoting_helpers() {
    for (file, count) in RATCHETED_SQL_FILES {
        let src = fs::read_to_string(workspace_root().join(file)).expect("source");
        let sites = sql_sites(&src);
        assert!(
            sites.len() == *count,
            "{file}: {found} SQL-building site(s), the ratchet expects {count}.\n\
             A site is a `format!` or `write!` whose own string literal carries an SQL verb. \
             Every value spliced into one must visibly route through `quote_literal` (a \
             string), `quote_ident` (a table or column name) or `sql_i64` (an integer) near \
             the statement — the check reads 300 bytes back and 700 forward from each site.\n\
             If the change is intended (a site added, removed, or moved between files), \
             update this file's entry in RATCHETED_SQL_FILES, crates/cli/tests/static_audit.rs:\n\
                 (\"{file}\", {count}) -> {edit}\n\
             and say in the comment above it what the new site interpolates and how it is quoted.",
            found = sites.len(),
            edit = if sites.is_empty() {
                "remove the entry; the file builds no SQL now".to_string()
            } else {
                format!("(\"{file}\", {})", sites.len())
            }
        );
        for i in sites {
            // Snapped to char boundaries: byte offsets land inside em dashes in exactly
            // the sources this audits — the same crash `sql_sites`' own scanner already
            // documents and guards, caught here the first time a site's 300-byte lookback
            // edge fell mid-dash.
            let mut lo = i.saturating_sub(300);
            while !src.is_char_boundary(lo) {
                lo -= 1;
            }
            let mut hi = src.len().min(i + 700);
            while !src.is_char_boundary(hi) {
                hi += 1;
            }
            let window = &src[lo..hi];
            assert!(
                // Either helper: identifiers take `quote_ident`, literals `quote_literal`, and a
                // site interpolating a table name is doing the first. The storage-crate ratchet
                // has always accepted both; this one accepting only literals is why the two
                // unquoted table interpolations in `backup.rs` had to be fixed before it could
                // join the list at all.
                window.contains("quote_literal")
                    || window.contains("quote_ident")
                    // The typed integer helper: i64's Display cannot break out of SQL,
                    // so `sql_i64` is the visible routing for numeric interpolations —
                    // added when the day-partitioned scratch made whole
                    // files of integer-only SQL and the two string helpers had nothing
                    // to say about them.
                    || window.contains("sql_i64"),
                "{file}: an SQL-building site does not visibly route through the quoting \
                 helpers:\n{window}"
            );
        }
    }
}

/// **The half that was missing: the file list is now DERIVED rather than remembered.**
///
/// Every entry in `RATCHETED_SQL_FILES` was hand-added, so a file nobody thought to list could
/// build SQL forever without failing anything — "a list that omits a file cannot fail for it", as
/// the `selfhealth.rs` note records after that happened once. It had happened four more times by:
/// `residual.rs` since v1, `differential.rs`, the whole `dao` read layer, and
/// `backup.rs` — where two sites interpolated a table name with no quoting at all, and one of
/// those names was parsed off disk rather than taken from the compile-time list.
///
/// So the check runs the other direction: walk every crate source, and any file that builds SQL
/// must be on the list. Forgetting is a test failure now rather than something review has to
/// catch, which is the same argument `every_analytic_constant_is_on_the_re_derivation_list`
/// makes one ratchet over.
#[test]
fn every_sql_building_file_is_on_the_ratchet() {
    const NOT_ON_THIS_LIST: &[&str] = &[
        // These two files have ratchets of their own that are STRICTER than this one:
        // `the_server_backend_prepares_every_query` (every client query comes from a prepared
        // statement) and `sql_interpolation_routes_through_the_quoting_helpers` (a pinned count
        // of `.run(&format!` sites, each reviewed). A third, weaker rule over the same code would
        // add nothing. The exclusion names the FILES rather than the crate, because naming the
        // crate is what let `testing.rs` sit uncovered by all three.
        "crates/serving-clickhouse/src/engine/mod.rs",
        "crates/serving-clickhouse/src/engine/statements.rs",
    ];
    let listed: BTreeSet<&str> = RATCHETED_SQL_FILES.iter().map(|(f, _)| *f).collect();
    let mut unlisted = Vec::new();
    for dir in crate_dirs() {
        let mut sources = Vec::new();
        rust_sources(&dir.join("src"), &mut sources);
        for path in &sources {
            let rel = path
                .strip_prefix(workspace_root())
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            if NOT_ON_THIS_LIST.iter().any(|skip| rel.starts_with(skip)) {
                continue;
            }
            let src = fs::read_to_string(path).expect("read source");
            let sites = sql_sites(&src).len();
            if sites > 0 && !listed.contains(rel.as_str()) {
                unlisted.push(format!("(\"{rel}\", {sites}),"));
            }
        }
    }
    unlisted.sort();
    assert!(
        unlisted.is_empty(),
        "these files build SQL (a `format!`/`write!` whose literal carries an SQL verb) and are \
         on neither RATCHETED_SQL_FILES nor its exclusions in crates/cli/tests/static_audit.rs. \
         Check that every value each site splices routes through `quote_literal`, `quote_ident` \
         or `sql_i64` near the statement, then add the entry below to RATCHETED_SQL_FILES with a \
         comment saying what the site interpolates and how it is quoted:\n  {}",
        unlisted.join("\n  ")
    );
}

/// Every `Persisted` module's table spec names real columns of real tables with the store's
/// own types — the spec is a statement about the schema, and this is what makes it one. Read
/// off the migration rather than asked of an engine, so it runs with the other static checks:
/// each key and measure column exists with a type its `Kind` encodes, the framework's head and
/// tail columns exist, and the day column is one of the key columns in the kind its unit says.
#[test]
#[allow(clippy::too_many_lines)] // one pass over every declared table, each rule beside the others
fn every_persisted_table_spec_matches_the_schema() {
    use analyzer_analysis::module::{DayOf, Kind, Persisted, Table};
    let sql = fs::read_to_string(
        workspace_root().join("crates/serving-clickhouse/migrations/0001_schema.sql"),
    )
    .expect("the schema");
    let columns_of = |table: &str| -> std::collections::BTreeMap<String, String> {
        let start = sql
            .find(&format!("CREATE TABLE IF NOT EXISTS {table}\n("))
            .unwrap_or_else(|| panic!("{table}: not in the schema"));
        let body = &sql[start..];
        let open = body.find('(').expect("the column list opens");
        let close = body.find("\n)").expect("the column list closes");
        body[open + 1..close]
            .lines()
            .filter_map(|l| {
                let l = l.trim();
                if l.is_empty() || l.starts_with("--") {
                    return None;
                }
                let (name, ty) = l.trim_end_matches(',').split_once(' ')?;
                let ty = ty.split(" DEFAULT").next().unwrap_or(ty).trim();
                Some((name.to_string(), ty.to_string()))
            })
            .collect()
    };
    let fits = |kind: Kind, ty: &str| -> bool {
        match kind {
            Kind::Str => matches!(ty, "String" | "LowCardinality(String)"),
            Kind::NullableStr => {
                matches!(ty, "Nullable(String)" | "LowCardinality(Nullable(String))")
            }
            Kind::U32 => ty == "UInt32",
            Kind::NullableU32 => ty == "Nullable(UInt32)",
            Kind::I64 => ty == "Int64",
            Kind::F32 => ty == "Float32",
            Kind::F64 => ty == "Float64",
            Kind::NullableF64 => ty == "Nullable(Float64)",
            Kind::Date => ty == "Date",
            Kind::DateTime => ty == "DateTime",
        }
    };
    let specs: Vec<&Table> = <analyzer_analysis::emitted::Cells as Persisted>::TABLES
        .iter()
        .chain(<analyzer_analysis::dup_cells::DupCells as Persisted>::TABLES)
        .chain([
            &analyzer_analysis::emitted::COVERAGE_DAILY,
            &analyzer_analysis::coverage_sums::COVERAGE_MINUTE,
            &analyzer_analysis::block_mass::BLOCK_MASS_DAILY,
            &analyzer_analysis::block_mass::BLOCK_MASS_DAY,
            &analyzer_analysis::whale_days::WHALE_DAY,
            &analyzer_analysis::whale_days::WHALE_DAY_TOTAL,
            &analyzer_analysis::tags::TAG_MINUTE,
            &analyzer_analysis::tags::TAG_INTERVALS,
            &analyzer_analysis::restated::RESTATED_HOURLY,
            &analyzer_analysis::dup_cells::INSTANCE_SPANS_DAY,
            &analyzer_pipeline::publish::DERIVATION_STATUS,
            &analyzer_analysis::emitted::MARKERS,
            &analyzer_analysis::roster::FLEET_INVENTORY,
        ])
        .chain(analyzer_pipeline::inputs::INPUT_TABLES.iter())
        .collect();
    assert_eq!(
        specs.len(),
        32,
        "the two keyed modules declare fourteen tables; coverage two; the blocks by stored mass \
         two; the whale register two; the declared tags two; the restatements, the pair basis, the derivation's standing \
         facts, the markers and the roster one each; the served input facts five"
    );
    for t in specs {
        let cols = columns_of(t.name);
        for framework in [
            "fleet_id",
            "semantics_version",
            "discriminator",
            "version",
            "gone",
        ] {
            assert!(
                cols.contains_key(framework),
                "{}: no {framework} column",
                t.name
            );
        }
        for c in t.key.iter().chain(t.measures) {
            let ty = cols
                .get(c.name)
                .unwrap_or_else(|| panic!("{}.{}: not in the schema", t.name, c.name));
            assert!(
                fits(c.kind, ty),
                "{}.{}: the spec says {:?}, the schema {ty}",
                t.name,
                c.name,
                c.kind
            );
        }
        let (day_col, want) = match t.day {
            DayOf::Date(c) => (c, Kind::Date),
            DayOf::DateTime(c) => (c, Kind::DateTime),
            DayOf::Millis(c) | DayOf::Day(c) => (c, Kind::I64),
            DayOf::Undated => continue,
        };
        let key =
            t.key.iter().find(|c| c.name == day_col).unwrap_or_else(|| {
                panic!("{}: the day column {day_col} is not a key column", t.name)
            });
        // Epoch milliseconds are an integer or a float: the fact ledger stamps its store
        // instant as the tap does, a `Float64`.
        let fits_day =
            key.kind == want || (matches!(t.day, DayOf::Millis(_)) && key.kind == Kind::F64);
        assert!(fits_day, "{}: the day column's kind {:?}", t.name, key.kind);
    }
}

// **A ratchet for `docs/conformance.md`.** Every comparable rule in this workspace has a
// test — metric names, SQL construction, the dependency allowlist — and this one is the
// constants list's. Review is not a control.
//
// The shape is the dependency allowlist's: a new `pub const` fails this test until
// somebody decides which it is — an analytic constant that joins the list with its
// derivation, or a non-analytic one that joins `NON_ANALYTIC` with its reason. Neither can be
// skipped by accident.
//
// **THE TEST FOR WHICH LIST A CONSTANT BELONGS ON, promoted here so the next one
// is classified by the sentence rather than by matching against precedent:**
//
//     THE RE-DERIVATION LIST IS FOR NUMBERS THE WORLD COULD TELL US.
//
// If a real deployment, a real fleet or a real measurement could in principle say what the
// value should be, it is analytic and it owes `docs/conformance.md` a derivation and a
// re-derivation target — even when the current value is judgment (`STALENESS_GRACE_MS`,
// `ACTION_THRESHOLD_DEFAULT`) rather than measurement, because judgment is what a
// measurement would eventually replace.
//
// If nothing observable bears on it — because it is not a quantity, or because what would
// move it is a decision rather than an observation — it belongs here with the reason
// written out. `SESSION_TTL` is the clean case: no fleet has an opinion about how long a
// browser session should last, and what changes it is a deployment stating its own security
// policy. That is a configuration decision, not something the world reports.
//
// The distinction matters because the failure is silent in both directions. A judgment
// constant hidden here never gets its measurement. A policy constant on the list acquires a
// fake re-derivation target that nobody can ever satisfy, and the list stops meaning
// anything.
/// Constants that are not analytic. Each is here because it is not a number derived
/// from a measurement or a judgment about the world:
const NON_ANALYTIC: &[&str] = &[
    // Names, not numbers: the daemon's cadence selectors (`Due`), the metric inventory and
    // two of its gauges, the core registers' names, and the working state's directory and
    // the derived ones `reset-derived` removes.
    "INGEST",
    "BOTH",
    "NONE",
    "MAINTAIN",
    "METRIC_NAMES",
    "DISK_FREE_BYTES",
    "DISK_TOTAL_BYTES",
    "CORE",
    "WORKING_STATE",
    "LOCAL_DERIVED",
    // Schemas: the served tables' specs and their lists, the migrations and the knob map
    // (held to `0001_schema.sql` by
    // `every_knob_horizon_in_the_migration_is_in_the_retention_map`). What moves one is a
    // column or a table, caught by the schema suite, never by a measurement.
    "CELL_TABLES",
    "TABLES",
    "GAP_SOURCES",
    "GAP_TOTAL",
    "COVERAGE_DAILY",
    "COVERAGE_MINUTE",
    "BLOCK_MASS_DAILY",
    "BLOCK_MASS_DAY",
    "WHALE_DAY",
    "WHALE_DAY_TOTAL",
    "TAG_MINUTE",
    "TAG_INTERVALS",
    "RESTATED_HOURLY",
    "INSTANCE_SPANS_DAY",
    "DERIVATION_STATUS",
    "INPUT_BATCHES",
    "INPUT_SEGMENTS",
    "INPUT_DROPS",
    "FACTS_ROLLUP_DAILY",
    "INPUT_KEY_SPACES",
    "INPUT_TABLES",
    "MARKERS",
    "FLEET_INVENTORY",
    "MIGRATIONS",
    "RETENTION_TTLS",
    // Version strings and identities: the meanings rows are stated under, the pinned engine,
    // and the input log's one shard.
    "INPUT_FORMAT",
    "GENERATIONS",
    "SEMANTICS_VERSION",
    "PINNED_ENGINE_VERSION",
    "SHARD",
    // Contract vocabulary and layout tokens: kinds, words and names the spec or the bucket
    // layout fixes. What moves one is a contract or layout revision, caught by the contract
    // pin and the conformance corpora.
    "DECLARED_KINDS",
    "LIFECYCLE_KINDS",
    "UNDECLARED",
    "SHIPPED_SUFFIX",
    "SEALED_PREFIX",
    "ANNOTATIONS_PREFIX",
    "ANALYZER_ROOT",
    "PREFLIGHT_PREFIX",
    // A bound from the annotation format's own caps (label 256, scope 128, fixed fields), not
    // from an observation: the largest legal object is a few hundred bytes.
    "ANNOTATION_MAX_BYTES",
    // Closed sets the semantics define: the six classes and the recompute register's
    // members. A change is a `SEMANTICS_VERSION` change, never a re-derivation.
    "ALL",
    "RECOMPUTE",
    // Ruled product constants, fixed fleet to fleet so figures compare: the band edges
    // (`Band::of` walks them; `the_two_band_sources_state_one_lens` pins the served bands to
    // them) and the stamped window.
    "UPPER_EDGES_MS",
    "STAMPED_WINDOW_DAYS",
    // A shape: the field names a coverage-carrying figure carries, held to the serializer by
    // `figure_schema_matches_what_covered_json_emits`.
    "FIGURE_FIELDS",
    // Units: a day, an hour and a minute in milliseconds, by definition.
    "DAY_MS",
    "HOUR_MS",
    "MINUTE_MS",
    // The retention floors and default rollup horizon, each derived at its definition from
    // the stop or the history it must hold; the knob, not a measurement, moves them.
    "MINUTE_RETENTION_FLOOR_DAYS",
    "HOUR_RETENTION_FLOOR_DAYS",
    "ROLLUP_RETENTION_FLOOR_YEARS",
    "INSTANCE_DETAIL_RETENTION_FLOOR_DAYS",
    "DEFAULT_ROLLUP_RETENTION_YEARS",
    // A fixture's retention: wide enough that a corpus's fixed stamps never age out, narrow
    // enough that every TTL stays representable.
    "FIXTURE_WIDE",
    // Budgets in the resource's own unit: the segment cache's default and the smallest
    // budget either cache accepts (one busy instance-hour). A deployment sets its own.
    "DEFAULT_SEGMENT_CACHE_BYTES",
    "MIN_CACHE_BYTES",
    // Boundedness, not calibration: how long a writer's lease stands, how many stores a
    // holder's ring keeps, the read path's admission bound and deadline, and the process
    // lifecycle waits. Their job is that nothing is unbounded; the rationale is at each
    // definition.
    "LEASE_TTL_MS",
    "RING_CAPACITY",
    "READ_CONCURRENCY",
    "READ_DEADLINE",
    "CLIENT_LIFECYCLE_TIMEOUT_SECS",
    "READINESS_TIMEOUT",
    "SHUTDOWN_TIMEOUT",
    "ORPHAN_STOP_TIMEOUT",
    // Postures: the default ports (chosen not to collide with an existing ClickHouse) and
    // the engine family a replicated cell substitutes at its own build.
    "DEFAULT_HTTP_PORT",
    "DEFAULT_TCP_PORT",
    "ENGINE_FAMILY",
    // A spelling: the token a store's text form renders a null as.
    "NULL_TEXT",
    // A fixture's dead pid: beyond every platform's `pid_max`, so it names no process.
    "UNUSED_PID",
    // A column list: the fields the ingest fast walk decodes, which its profiler reads too.
    "FAST_WALK_PROJECTION",
];

#[test]
fn every_analytic_constant_is_on_the_re_derivation_list() {
    let list =
        fs::read_to_string(workspace_root().join("docs/conformance.md")).expect("conformance.md");
    let mut unlisted = Vec::new();
    let mut declared = BTreeSet::new();
    for dir in crate_dirs() {
        let mut sources = Vec::new();
        rust_sources(&dir.join("src"), &mut sources);
        for path in &sources {
            let text = fs::read_to_string(path).expect("read source");
            for line in text.lines() {
                let Some(rest) = line.trim_start().strip_prefix("pub const ") else {
                    continue;
                };
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_')
                    .collect();
                declared.insert(name.clone());
                if name.is_empty() || NON_ANALYTIC.contains(&name.as_str()) || list.contains(&name)
                {
                    continue;
                }
                unlisted.push(format!("{name} ({})", path.display()));
            }
        }
    }
    unlisted.sort();
    unlisted.dedup();
    assert!(
        unlisted.is_empty(),
        "these constants are on neither docs/conformance.md section 2's re-derivation list nor \
         this file's NON_ANALYTIC list. Add each one to whichever it belongs to — with its \
         derivation, or with the reason it has none:\n  {}",
        unlisted.join("\n  ")
    );
    // The exemption list names live constants only: an entry whose constant left exempts
    // nothing and reads as a reason that still applies.
    let stale: Vec<&str> = NON_ANALYTIC
        .iter()
        .copied()
        .filter(|name| !declared.contains(*name))
        .collect();
    assert!(
        stale.is_empty(),
        "NON_ANALYTIC names constants no crate declares; remove them: {stale:?}"
    );
}

/// **The same ratchet one type over: every `Params` field is an analytic constant too, and
/// none of them is a `pub const`**.
///
/// `every_analytic_constant_is_on_the_re_derivation_list` scans for `pub const` and therefore
/// cannot see a struct field. `Params` holds the numbers that decide what every classified
/// figure MEANS — the attribution window, the skew allowance, the drop-window widening, the
/// examined history, the staleness threshold — and each is exactly the kind of labeled
/// placeholder section 2 exists to hold.
///
/// Same family as `Discriminator::of`'s exhaustive destructure: a new field is a compile error
/// there until somebody classifies it, and a test failure here until somebody documents it.
#[test]
fn every_params_field_is_on_the_re_derivation_list() {
    // A field that is genuinely not an analytic quantity would go here with its reason. None
    // is today — every one of them is a number somebody chose and somebody else must re-derive.
    const EXEMPT: &[&str] = &[];

    let src = fs::read_to_string(workspace_root().join("crates/analysis/src/lib.rs"))
        .expect("analysis lib.rs");
    let body = src
        .split_once("pub struct Params {")
        .expect("Params struct")
        .1;
    let body = body.split_once("\n}").expect("Params struct end").0;
    let fields: Vec<&str> = body
        .lines()
        .filter_map(|l| l.trim().strip_prefix("pub "))
        .filter_map(|l| l.split_once(':'))
        .map(|(name, _)| name.trim())
        .collect();
    assert!(
        // A sanity floor on the PARSE, not a claim about how many parameters there ought to
        // be: it fires when the struct moves and this ratchet starts reading the wrong text.
        fields.len() >= 2,
        "the Params parse found {} fields, which means the struct moved and this ratchet is \
         reading the wrong text: {fields:?}",
        fields.len()
    );

    let list =
        fs::read_to_string(workspace_root().join("docs/conformance.md")).expect("conformance.md");
    let unlisted: Vec<&str> = fields
        .into_iter()
        .filter(|f| !EXEMPT.contains(f) && !list.contains(f))
        .collect();
    assert!(
        unlisted.is_empty(),
        "these `analysis::Params` fields appear in neither docs/conformance.md section 2 nor this \
         test's EXEMPT list. Each decides what a classified figure MEANS, so each owes a \
         derivation and a re-derivation target — by its exact field name, so this check can \
         find it:\n  {}",
        unlisted.join("\n  ")
    );
}

/// **The meaning pair, enforced over the schema file rather than argued table by table**.
///
/// > Any table that emits keyed history carries `(semantics_version, discriminator)` in its sort
/// > key. No exceptions.
///
/// A table that left the pair out of its key would let a meaning change overwrite its rows
/// while the tables beside it segment honestly: a drill contradicting its own trend. A stated
/// rule cannot hold that across every new table; this reads the file.
#[test]
fn the_meaning_pair_is_in_every_keyed_history_sort_key() {
    // Tables carrying a `discriminator` column that are exempt from the pair. **EMPTY**: what
    // makes a table subject to the law is that its rows are meaning-dependent and kept, not
    // that they are bucketed by day. An entry here is a decision with its reason.
    const EXEMPT: &[(&str, &str)] = &[];

    let schema = fs::read_to_string(
        workspace_root().join("crates/serving-clickhouse/migrations/0001_schema.sql"),
    )
    .expect("migration 0001");

    let mut offenders = Vec::new();
    let mut checked = 0usize;
    for block in schema.split("CREATE TABLE ").skip(1) {
        let name = block
            .split_once(char::is_whitespace)
            .map_or("<unnamed>", |(n, _)| n)
            .trim_end_matches('(');
        // the statement only, so the next table's text cannot answer for this one
        let stmt = block.split_once(";\n").map_or(block, |(s, _)| s);
        if !stmt.contains("discriminator ") {
            continue;
        }
        if EXEMPT.iter().any(|(n, _)| *n == name) {
            continue;
        }
        checked += 1;
        let Some((_, keyed)) = stmt.split_once("ORDER BY ") else {
            offenders.push(format!(
                "{name}: carries a discriminator and declares no ORDER BY"
            ));
            continue;
        };
        let keyed = keyed.split_once('\n').map_or(keyed, |(head, tail)| {
            // sort keys wrap; take until the closing paren of the key list
            let joined = format!("{head}{tail}");
            joined
                .split_once(')')
                .map_or(joined.clone(), |(k, _)| k.to_string())
                .leak()
        });
        // ONE spelling, checked as one string: same order on every table, lower cardinality
        // first, so the file has a single shape a reader recognises.
        if !keyed.contains("semantics_version, discriminator") {
            offenders.push(format!(
                "{name}: sort key is `{}` — the meaning pair must appear as \
                 `semantics_version, discriminator`",
                keyed.split_whitespace().collect::<Vec<_>>().join(" ")
            ));
        }
    }
    // Not vacuous: a rename or a parse drift that matched nothing would otherwise pass this
    // silently, which is the exact shape of the failure the test exists to catch.
    assert!(
        checked >= 9,
        "the schema parse found {checked} keyed-history tables, which means it is reading the \
         wrong text — nine carry the meaning pair today"
    );
    assert!(
        offenders.is_empty(),
        "these tables emit keyed history without the full meaning pair in their sort key, so a \
         parameter or join-semantics change OVERWRITES their prior rows in place instead of \
         sitting beside them (migration 0001's header states the law):\n  {}",
        offenders.join("\n  ")
    );
}

#[test]
fn local_only_paths_are_not_tracked() {
    // **A .gitignore entry does not untrack a file that is already tracked**, and a local
    // pre-push hook does not survive a fresh clone, a new machine, or CI. This is the control
    // for working documents that stay out of the repository.
    //
    // Listed by explicit path rather than by pattern: a pattern invites the next file to be
    // assumed covered, and adding to this list is a decision somebody makes. Add the entry in
    // the commit that creates the file.
    const LOCAL_ONLY: &[&str] = &[
        "docs/scale-audit.md",
        "docs/production-audit.md",
        "docs/distribution.md",
    ];

    let Ok(output) = std::process::Command::new("git")
        .arg("-C")
        .arg(workspace_root())
        .arg("ls-files")
        .arg("--")
        .args(LOCAL_ONLY)
        .output()
    else {
        // Not a git checkout (a vendored source tree, say). Nothing to enforce, and
        // failing here would be the test complaining about its own environment.
        return;
    };
    assert!(
        output.status.success(),
        "git ls-files failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tracked = String::from_utf8_lossy(&output.stdout);
    let tracked: Vec<&str> = tracked.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        tracked.is_empty(),
        "these paths are declared local-only and MUST NOT be tracked. A .gitignore entry \
         does not untrack an already-tracked file — run `git rm --cached <path>`:\n  {}",
        tracked.join("\n  ")
    );
}

/// **section 2.2's layer two, buildable at last.** A figure field on a wire type must be typed `Figure`,
/// so a bare number cannot be serialized into a response.
///
/// This test could not exist while `crates/api`'s handlers built `serde_json::Value` directly:
/// there were no wire types to count fields on, and the no-naked-numbers law stood on the golden
/// walk alone (`docs/read-api.md` section 2.2). Typed wire structs are what made it possible, and it
/// is the second half of the same law the goldens hold at runtime — one checks what serializes,
/// this checks what CAN.
///
/// **The vocabulary is masses and shares, not `coverage` itself.** A field named `coverage` or
/// `fraction` IS the qualifier; demanding it carry one would be circular, and
/// `EvidenceRecord::coverage` is an event-grain column repeat compute stamped rather than a figure this
/// layer is reporting. What must be qualified is a measured QUANTITY.
#[test]
fn api_wire_figures_are_typed() {
    const FIGURE_FIELDS: &[&str] = &["tokens", "fleet_total", "share_of_fleet_total"];
    let wire = fs::read_to_string(workspace_root().join("crates/api/src/wire.rs"))
        .expect("the api wire module");

    // The delegation, first: `Figure` must NOT grow its own `#[derive(Serialize)]`. A second
    // serializer for the four fields is exactly the duplication section 2.2 exists to prevent, and it
    // would be an easy "simplification" for a future reader to make.
    //
    // A name check would pass on a file that defined the shape twice, so the check is
    // structural: the four fields are spelled ONCE in the whole module.
    assert!(
        wire.contains("covered_json(&self.0)"),
        "Figure must serialize THROUGH covered_json, so the shape has one definition"
    );
    assert_eq!(
        wire.matches("\"coverage_kind\":").count(),
        1,
        "the four figure fields are spelled in exactly one place; a second spelling IS a second \
         serializer, whatever it is called"
    );

    let mut offenders = Vec::new();
    for line in wire.lines() {
        let trimmed = line.trim();
        // `pub name: Type,` inside a struct body.
        let Some((name, ty)) = trimmed
            .strip_prefix("pub ")
            .and_then(|l| l.split_once(": "))
        else {
            continue;
        };
        if !FIGURE_FIELDS.contains(&name) {
            continue;
        }
        if !ty.contains("Figure") {
            offenders.push(format!("{name}: {ty}"));
        }
    }
    assert!(
        offenders.is_empty(),
        "these wire fields hold a measured quantity but are not typed `Figure`, so they would \
         serialize as naked numbers:\n  {}",
        offenders.join("\n  ")
    );

    // And the vocabulary must actually be exercised — a ratchet over zero fields is a ratchet
    // that passes because it found nothing, which is the shape of a check that quietly stopped
    // working when a file moved.
    let found = FIGURE_FIELDS
        .iter()
        .filter(|f| wire.contains(&format!("pub {f}: ")))
        .count();
    assert_eq!(
        found,
        FIGURE_FIELDS.len(),
        "only {found} figure fields found in the wire module; did it move or get renamed?"
    );
}

/// The request half's other direction: a field on `Q` that nothing documents.
///
/// **The response half cannot drift** — its schemas are derived from the structs that serialize.
/// The request half is assembled by hand, so it needs two checks facing opposite ways.
/// `openapi::every_documented_parameter_is_one_a_handler_actually_reads` catches a documented
/// parameter no handler reads; this catches a parameter the handler reads that the document never
/// mentions, which is the direction that leaves a console unable to ask for something the API
/// would happily answer.
#[test]
fn api_query_parameters_are_documented() {
    // Follow real route -> handler -> raw type. No second parameter-name inventory.
    let root = workspace_root();
    let routes = fs::read_to_string(root.join("crates/api/src/routes.rs")).unwrap();
    let requests = fs::read_to_string(root.join("crates/api/src/requests.rs")).unwrap();
    let contract: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(root.join("contract/openapi.json")).unwrap())
            .unwrap();
    let mut checked = 0;
    for route in routes.split(".route(").skip(1) {
        let Some(path) = route.split('"').nth(1) else {
            continue;
        };
        for method in ["get", "post", "delete"] {
            let Some(rest) = route.split(&format!("{method}(")).nth(1) else {
                continue;
            };
            let handler = rest.split(')').next().unwrap().trim();
            let Some((module, name)) = handler.rsplit_once("::") else {
                continue;
            };
            let file = if module == "crate::health" {
                "crates/api/src/health.rs".to_string()
            } else {
                format!("crates/api/src/resources/{module}.rs")
            };
            let source = fs::read_to_string(root.join(file)).unwrap();
            let signature = source
                .split(&format!("fn {name}("))
                .nth(1)
                .unwrap()
                .split("->")
                .next()
                .unwrap();
            let raw = signature
                .split("Params<")
                .nth(1)
                .map(|r| r.split('>').next().unwrap().rsplit("::").next().unwrap());
            assert!(
                raw.is_some(),
                "{method} {path}: extract NoQuery even when no parameters are accepted"
            );
            let fields: BTreeSet<String> = raw
                .map(|raw| {
                    requests
                        .split(&format!("struct {raw} {{"))
                        .nth(1)
                        .unwrap()
                        .split('}')
                        .next()
                        .unwrap()
                        .lines()
                        .filter_map(|l| l.trim().split_once(':').map(|(f, _)| f.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let declared: BTreeSet<String> = contract["paths"][path][method]["parameters"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|p| p["in"] == "query")
                .map(|p| p["name"].as_str().unwrap().to_string())
                .collect();
            assert_eq!(fields, declared, "{method} {path}: raw request and OpenAPI differ; update the operation type and its contract together");
            checked += 1;
        }
    }
    assert_eq!(
        checked,
        contract["paths"]
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.as_object().unwrap().len())
            .sum::<usize>(),
        "every documented operation must be checked"
    );
}

/// **Every cause has a producer.** Ruled in `docs/read-api.md` section 7.3 and held here,
/// because a law stated and unenforced is a law that holds until somebody is in a hurry.
///
/// A cause with no producer is contract fiction: it teaches a console to handle a state that
/// cannot occur, and it makes the enum look considered where it is merely inherited.
#[test]
fn every_named_cause_has_a_producer() {
    let root = workspace_root();
    let error_rs = root.join("crates/api/src/error.rs");

    // The variants, from the enum itself.
    let text = fs::read_to_string(&error_rs).expect("the error module");
    let variants: Vec<String> = text
        .split("pub enum Cause {")
        .nth(1)
        .and_then(|r| r.split('}').next())
        .expect("the Cause enum")
        .lines()
        .map(|l| l.trim().trim_end_matches(',').to_string())
        .filter(|l| !l.is_empty() && !l.starts_with("//") && !l.starts_with('#'))
        .collect();
    assert!(variants.len() >= 15, "found {} variants", variants.len());

    // Every other source in the workspace.
    let mut sources = Vec::new();
    for dir in crate_dirs() {
        rust_sources(&dir.join("src"), &mut sources);
    }
    // **`error.rs` is EXCLUDED, or the check is vacuous:** its `as_str` spells every token by
    // construction, so a corpus containing it would find every cause produced.
    let corpus: String = sources
        .iter()
        .filter(|p| **p != error_rs)
        .filter_map(|p| fs::read_to_string(p).ok())
        .collect();

    let orphans: Vec<&String> = variants
        .iter()
        .filter(|v| {
            // Raised through the enum, or, for the one variant with a named constructor,
            // through that. An in-band `Refusal` carries the same enum, so there is one spelling.
            let by_enum = corpus.contains(&format!("Cause::{v}"));
            let by_ctor = *v == "EngineUnavailable" && corpus.contains("ApiError::engine");
            !(by_enum || by_ctor)
        })
        .collect();
    assert!(
        orphans.is_empty(),
        "these causes are in the closed set and nothing raises them. Either give each a producer \
         or remove it: a cause with no code path is contract fiction:\n  {orphans:?}"
    );
}

/// **The design's endpoint table IS the router, or one of them is wrong.**
///
/// `docs/read-api.md` section 3 tabulates every endpoint with the DAO method behind it, and the
/// panel grid, the DAO method list and the endpoint list are the same list. An endpoint with a
/// DAO method and tests but no route is invisible from the contract, because an unrouted
/// endpoint is not there to notice, and from the goldens, which iterate the routes that exist.
///
/// Both directions: every `GET /v1/...` in the table is routed, and every route is in the table,
/// save the recording endpoints of section 1 and the namespace's own fallbacks.
#[test]
fn every_endpoint_in_the_design_table_is_routed() {
    let doc = fs::read_to_string(workspace_root().join("docs/read-api.md")).expect("read-api");
    let routes_rs =
        fs::read_to_string(workspace_root().join("crates/api/src/routes.rs")).expect("routes");

    // Paths named in a section 3 table row.
    let mut designed: BTreeSet<String> = BTreeSet::new();
    for line in doc.lines() {
        if !line.starts_with('|') {
            continue;
        }
        for token in line.split('`') {
            if let Some(rest) = token.strip_prefix("GET /v1/") {
                designed.insert(format!(
                    "/v1/{}",
                    rest.split_whitespace().next().unwrap_or("")
                ));
            }
        }
    }
    assert!(
        designed.len() >= 10,
        "only {} endpoints parsed from the section 3 table; did its shape change?",
        designed.len()
    );

    // The router's own list, normalised: the table writes a holder as `{i}/{d}/{g}` where the
    // code writes real parameter names, so compare on the fixed prefix.
    // **Split on `.route(` and take the NEXT string literal**, rather than on `.route("`: rustfmt
    // breaks a long route call after the paren, and the stricter pattern would see no route.
    let routed: Vec<String> = routes_rs
        .split(".route(")
        .skip(1)
        .filter_map(|r| {
            let start = r.find('"')?;
            r[start + 1..].split('"').next().map(str::to_string)
        })
        .collect();
    // **Compared by SHAPE, not by prefix**: `/v1/holders/{i}/{d}/{g}/timeline` is not routed
    // because `/v1/holders` is. Segment count plus the literal segments distinguish them.
    let shape = |path: &str| -> Vec<String> {
        path.split('/')
            .filter(|s| !s.is_empty())
            .map(|s| {
                if s.starts_with('{') {
                    "*".to_string()
                } else {
                    s.to_string()
                }
            })
            .collect()
    };
    let routed_shapes: Vec<Vec<String>> = routed.iter().map(|r| shape(r)).collect();
    let is_routed = |want: &str| routed_shapes.contains(&shape(want));

    let missing: Vec<&String> = designed.iter().filter(|d| !is_routed(d)).collect();
    assert!(
        missing.is_empty(),
        "these endpoints are in `docs/read-api.md` section 3's table and have no route. Either \
         route them or take them out of the table:\n  {missing:?}"
    );

    // The recording endpoints (section 1) and the namespace's fallbacks are routes the table
    // does not list by design.
    let not_tabulated: &[&str] = &[
        "/v1",
        "/v1/",
        "/v1/{*rest}",
        "/v1/session",
        "/v1/analyze",
        "/v1/annotations",
    ];
    let designed_shapes: Vec<Vec<String>> = designed.iter().map(|d| shape(d)).collect();
    let untabulated: Vec<&String> = routed
        .iter()
        .filter(|r| !not_tabulated.contains(&r.as_str()))
        .filter(|r| !designed_shapes.contains(&shape(r)))
        .collect();
    assert!(
        untabulated.is_empty(),
        "these routes are not in `docs/read-api.md` section 3's table. A route is part of the \
         contract the document states:\n  {untabulated:?}"
    );
}

/// **No seeded timestamp is an absolute literal.** The class of bug this closes was invisible
/// until a date and permanent afterwards.
///
/// `the ingest ledger` carries `TTL toDateTime(at_ms / 1000) + INTERVAL ledger_retention_days
/// DAY DELETE` (as raw `records` did before it). Two suites seeded facts at a hard-coded,
/// fourteen days before the then-current retention floor
/// expired them, and the rows began being deleted between the seed and the
/// assertion. The symptom surfaced three layers away — a comparison refusing
/// `UnreadableGeneration` over a `pass_ledger` that was empty because the facts under it were
/// gone — so the search started at generation logic and the cause was retention.
///
/// A grep for that date would have found those two and nothing else. The rule is about the
/// SHAPE: an absolute epoch literal feeding a seed is a fuse whatever value it holds today,
/// because every fixed date eventually falls outside a rolling horizon.
///
/// **Scoped to the function that builds the `INSERT`**, which is what keeps it exclusion-free.
/// Two neighbouring shapes are legitimate and neither is excused, because neither is in scope:
/// digits inside a string are data rather than a stamp (an `incarnation` is an opaque token per
/// spec 4.2, and segment filenames carry one), and a deliberately far-past value handed to a
/// QUERY is the assertion rather than a seed. An exclusion list would have had to name both,
/// and "a list that omits a file cannot fail for it" is the failure this repo has already had
/// four times.
#[test]
fn every_seeded_timestamp_derives_from_the_clock() {
    // Epoch seconds and milliseconds over the plausible range: ~2001-2033 either way. Narrow
    // enough that token counts and byte sizes do not trip it, wide enough that no fixture date
    // slips under.
    const RANGES: [(u64, u64); 2] = [
        (1_000_000_000, 2_000_000_000),
        (1_000_000_000_000, 2_000_000_000_000),
    ];

    /// The byte range of the `fn` containing `at`, approximated by the surrounding `fn`
    /// markers of any visibility, top-level or one level in (a test module's), and a test
    /// module's start. Good enough: a seed helper is a function, and its literals sit in it.
    fn enclosing_fn(src: &str, at: usize) -> (usize, usize) {
        const STARTS: [&str; 6] = [
            "\nfn ",
            "\npub fn ",
            "\npub(crate) fn ",
            "\n    fn ",
            "\n    pub fn ",
            "\n#[cfg(test)]",
        ];
        let lo = STARTS
            .iter()
            .filter_map(|m| src[..at].rfind(m))
            .max()
            .map_or(0, |i| i + 1);
        let hi = STARTS
            .iter()
            .filter_map(|m| src[at..].find(m))
            .min()
            .map_or(src.len(), |i| at + i);
        (lo, hi)
    }

    let mut findings = Vec::new();
    for dir in crate_dirs() {
        let mut sources = Vec::new();
        rust_sources(&dir.join("src"), &mut sources);
        rust_sources(&dir.join("tests"), &mut sources);
        for path in &sources {
            let rel = path
                .strip_prefix(workspace_root())
                .unwrap_or(path)
                .to_string_lossy()
                .replace('\\', "/");
            // The checker quotes the pattern it looks for; it seeds nothing.
            if rel.ends_with("tests/static_audit.rs") {
                continue;
            }
            let src = fs::read_to_string(path).expect("read source");
            let mut spans = Vec::new();
            // The input log's commit is where a fixture's batch spans land now, as the
            // ledger INSERT once was.
            for pat in ["INSERT INTO facts_rollup_daily", ".commit_cycle("] {
                let mut from = 0;
                while let Some(i) = src[from..].find(pat) {
                    let at = from + i;
                    spans.push(enclosing_fn(&src, at));
                    from = at + pat.len();
                }
            }
            if spans.is_empty() {
                continue;
            }
            let bytes = src.as_bytes();
            let (mut i, mut in_str) = (0usize, false);
            while i < bytes.len() {
                match bytes[i] {
                    b'\\' if in_str => i += 1,
                    b'"' => in_str = !in_str,
                    d if d.is_ascii_digit() && !in_str => {
                        let start = i;
                        let mut digits = String::new();
                        while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'_') {
                            if bytes[i].is_ascii_digit() {
                                digits.push(bytes[i] as char);
                            }
                            i += 1;
                        }
                        let ident = start > 0
                            && (bytes[start - 1].is_ascii_alphabetic() || bytes[start - 1] == b'_');
                        let seeded = spans.iter().any(|(lo, hi)| start >= *lo && start < *hi);
                        if !ident && seeded {
                            if let Ok(v) = digits.parse::<u64>() {
                                if RANGES.iter().any(|(lo, hi)| (*lo..=*hi).contains(&v)) {
                                    let line = src[..start].matches('\n').count() + 1;
                                    findings.push(format!("  {rel}:{line}: {digits}"));
                                }
                            }
                        }
                        continue;
                    }
                    _ => {}
                }
                i += 1;
            }
        }
    }
    findings.sort();
    assert!(
        findings.is_empty(),
        "these seeds carry an absolute epoch timestamp. A fixed date outlives no rolling \
         retention horizon: it passes until the wall clock crosses it and fails permanently \
         after, with a symptom nowhere near the cause. Derive from \
         `analyzer_serving_clickhouse::engine::testing::recent_day_base_ms()` and keep every fixture stamp a delta \
         from it.\n{}",
        findings.join("\n")
    );
}

/// **A test daemon never takes the host's one-analyzer claim, and the construction sites are
/// the only place that can get that wrong.**
///
/// The failure this closes was live and it read as a product bug. `DaemonConfig`'s default is
/// `host_claim: true`, which is correct for the deployment and wrong for a rig that runs many
/// daemons on one host by design. The suite's `config()` helper turned it off; the horizon
/// RESTART case could not use that helper — it has to come back to the same ports and data
/// directory — so it built its config by hand and silently inherited the production default.
/// Serialized it passed. Run beside any other daemon test it lost the race and refused at the
/// door with the one-per-VM sentence, so a scheduling collision presented as the daemon
/// rejecting a legitimate restart.
///
/// **A comment on the helper would not have caught it, because the bug was written by someone
/// not looking at the helper.** So the check is structural: inside the daemon's test binaries,
/// `DaemonConfig::new` may only be called from a named rig constructor. Anything else is a
/// config nobody applied the rig defaults to.
#[test]
fn daemon_test_configs_come_from_the_rig_constructors() {
    // Each is a helper whose body applies the rig defaults, and each is small enough to read
    // in one screen. A new one joins this list deliberately, which is the moment to ask
    // whether it turned the host claim off.
    // `live_config` (ttl_gate.rs): claim off, corpus forms admitted, bucket from env.
    const RIG_CONSTRUCTORS: &[&str] = &["config", "restart_config", "daemon_config", "live_config"];

    let dir = workspace_root().join("crates/daemon/tests");
    let mut findings = Vec::new();
    for entry in fs::read_dir(&dir).expect("daemon tests dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let src = fs::read_to_string(&path).expect("read daemon test");
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        let mut current = "<file scope>".to_string();
        for (i, line) in src.lines().enumerate() {
            if let Some(rest) = line.trim_start().strip_prefix("fn ") {
                current = rest
                    .split(['(', '<'])
                    .next()
                    .unwrap_or("?")
                    .trim()
                    .to_string();
            }
            if line.contains("DaemonConfig::new") && !RIG_CONSTRUCTORS.contains(&current.as_str()) {
                findings.push(format!("{name}:{} in fn {current}", i + 1));
            }
        }
    }
    assert!(
        findings.is_empty(),
        "these build a `DaemonConfig` outside the rig constructors, so they carry the \
         DEPLOYMENT default `host_claim: true` and will take the host's one-analyzer claim for \
         real -- refusing whichever daemon test happens to be running beside them. Route them \
         through a rig constructor, or add one to RIG_CONSTRUCTORS once its body turns the \
         claim off:\n  {}",
        findings.join("\n  ")
    );
}

/// Every field the decoder TYPES is a field the contract names, for a kind that carries it.
///
/// **One direction only, and the asymmetry is the design.** The decoder types a SUBSET of what
/// the contract declares and bags the rest in `extra` — `agent_version` is the proof: it is not
/// in `LIFECYCLE_FIELDS`, and `read::agent_versions` folds it back out of the shaped `extra`. So a
/// contract field absent from a list is a deliberate choice, and demanding it be typed would be
/// wrong. The other direction is not a choice: a field the decoder types and the contract does
/// not name can never be populated by a conforming producer, so it reads `\N` forever while
/// looking like a column somebody relies on.
///
/// The schema this reads is `conformance/schema/records.json`, derived from the specification's
/// own field tables. Before it existed these lists were transcribed by hand, which is how
/// `heartbeat_secs` stayed in `LIFECYCLE_FIELDS` after contract 1.3 moved it to the header —
/// drift introduced and unnoticed inside one afternoon.
#[test]
fn every_typed_field_is_a_contract_field_for_that_kind() {
    let schema: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            workspace_root().join("contract/inference-events/conformance/schema/records.json"),
        )
        .expect("the vendored record schema"),
    )
    .expect("schema parses");
    let src = fs::read_to_string(workspace_root().join("crates/segments/src/lib.rs"))
        .expect("the decoder");

    // The families the decoder splits on, and the kinds each covers.
    let families: &[(&str, &[&str])] = &[
        ("CACHE_FIELDS", &["store", "evict", "clear", "holder_reset"]),
        (
            "LIFECYCLE_FIELDS",
            &["agent_start", "heartbeat", "agent_stop", "identity_refused"],
        ),
        (
            "DELIVERY_FIELDS",
            &["segment_open", "segment_recovered", "segments_dropped"],
        ),
        ("CAPACITY_FIELDS", &["identity_refused"]),
        ("HEADER_FIELDS", &["segment_open", "segment_recovered"]),
    ];

    let mut offenders = Vec::new();
    for (list, kinds) in families {
        let start = src
            .find(&format!("const {list}: &[&str] = &["))
            .unwrap_or_else(|| panic!("{list} is not in the decoder; update this ratchet"));
        let end = start + src[start..].find("];").expect("list terminates");
        // Comment lines are stripped first: these blocks carry prose, and prose carries quotes.
        let body: String = src[start..end]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let declared: Vec<&str> = body
            .split('"')
            .skip(1)
            .step_by(2)
            .filter(|s| !s.is_empty())
            .collect();

        let mut named = std::collections::BTreeSet::new();
        for kind in *kinds {
            if let Some(fields) = schema["kinds"][kind]["fields"].as_object() {
                named.extend(fields.keys().map(String::as_str));
            }
        }
        for field in declared {
            // `endpoints.*` are the flattened nested element, named in their own table.
            let base = field.split('.').next().unwrap_or(field);
            if !named.contains(base) && schema["endpoints_element"][base].is_null() {
                offenders.push(format!("{list}: {field}"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these fields are typed by the decoder and named by no kind the list covers, so no \
         conforming producer can populate them:\n  {}",
        offenders.join("\n  ")
    );
}

/// **The cosign pin is stated twice, so the two must be compared.**
///
/// `docs/quickstart.md` tells a customer how to obtain cosign, with a version and a SHA-256, and
/// `.github/workflows/release.yml` pins the same binary for the pipeline that signs their bundle.
/// The point of printing the digest in the quickstart is that a customer runs THE TOOL WE RAN; two
/// independently maintained copies of it stop being that the first time one moves.
///
/// Two copies of one pin drift the first time one moves, so this holds them together.
#[test]
fn the_quickstart_and_the_release_pin_the_same_cosign() {
    let quickstart = fs::read_to_string(workspace_root().join("docs/quickstart.md"))
        .expect("docs/quickstart.md");
    let release = fs::read_to_string(workspace_root().join(".github/workflows/release.yml"))
        .expect(".github/workflows/release.yml");

    // A 64-hex run on a line that also mentions cosign: the digest, wherever it sits in a curl
    // pipeline, a `sha256sum -c` line or a shell variable.
    let digests = |text: &str| -> Vec<String> {
        text.lines()
            .filter(|l| l.to_lowercase().contains("cosign"))
            .flat_map(|l| {
                l.split(|c: char| !c.is_ascii_hexdigit())
                    .filter(|t| t.len() == 64 && t.chars().all(|c| c.is_ascii_hexdigit()))
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
    };
    // Likewise the version, as `vN.N.N` on a cosign line.
    let versions = |text: &str| -> Vec<String> {
        text.lines()
            .filter(|l| l.to_lowercase().contains("cosign"))
            .flat_map(|l| {
                l.split(['/', ' ', '=', '"'])
                    .filter(|t| {
                        t.starts_with('v')
                            && t.len() > 3
                            && t[1..].split('.').count() == 3
                            && t[1..].chars().all(|c| c.is_ascii_digit() || c == '.')
                    })
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
    };

    let (qd, rd) = (digests(&quickstart), digests(&release));
    // An empty side would make this pass by finding nothing, which is the failure mode of every
    // grep-shaped check — so absence is a failure, not a silent skip.
    assert!(
        !qd.is_empty(),
        "docs/quickstart.md states no cosign digest, so a customer cannot verify the tool they \
         are about to verify our release with"
    );
    assert!(!rd.is_empty(), "release.yml states no cosign digest");
    for d in &qd {
        assert!(
            rd.contains(d),
            "docs/quickstart.md pins cosign {d}, which release.yml does not. A customer following \
             the quickstart would then run a different binary from the one that signed their \
             bundle. Move both or neither."
        );
    }

    let (qv, rv) = (versions(&quickstart), versions(&release));
    assert!(!qv.is_empty(), "docs/quickstart.md names no cosign version");
    for v in &qv {
        assert!(
            rv.contains(v),
            "docs/quickstart.md names cosign {v}, which release.yml does not pin."
        );
    }
}

/// **The quickstart's version literals must be THIS release's.**
///
/// `docs/quickstart.md` ships as the release bundle's `README.md`, and every command in it is
/// something a customer copy-pastes: `oras copy ...:1.0.3`, `helm install --version 1.0.3`,
/// `tar xzf infertap-analyzer-v1.0.3-x86_64-linux.tar.gz`. A stale number there is not cosmetic —
/// the tarball it names does not exist in the bundle it is sitting inside, so the first command a
/// customer runs fails.
///
/// `install.sh` solved its half by going version-agnostic (`infertap-analyzer-*-x86_64-linux`),
/// which a shell glob permits. These cannot: `--version` needs a version, and a registry tag is
/// not a pattern. So they are pinned to the workspace's own version and checked here.
#[test]
fn the_quickstart_names_this_version() {
    let quickstart =
        fs::read_to_string(workspace_root().join("docs/quickstart.md")).expect("quickstart");
    let cargo = fs::read_to_string(workspace_root().join("Cargo.toml")).expect("workspace toml");
    let version = cargo
        .lines()
        .find_map(|l| l.strip_prefix("version = "))
        .map(|v| v.trim_matches('"').to_string())
        .expect("workspace version");

    // Any `N.N.N` on a line that names an artifact, a chart or an image — the places a customer
    // copies from. Prose that merely mentions an older release is not a command and is left alone.
    let stale: Vec<&str> = quickstart
        .lines()
        .filter(|l| {
            l.contains("infertap-analyzer-v") || l.contains("oras copy") || l.contains("--version ")
        })
        .filter(|l| {
            l.split(|c: char| !(c.is_ascii_digit() || c == '.'))
                .any(|t| t.matches('.').count() == 2 && !t.is_empty() && t != version)
        })
        .collect();

    assert!(
        stale.is_empty(),
        "docs/quickstart.md ships as the bundle's README and names a version that is not {version}. \
         A customer copy-pasting these would reference an artifact the bundle does not contain:\n{}",
        stale.join("\n")
    );
}

/// **The analyzer's root in the bucket is the root the delivery contract reserves.**
/// `dated::ANALYZER_ROOT` is what the lake, the annotations and the preflight probe write under
/// and what ingest refuses to read as a segment; `reserved_consumer_prefix` is what a conforming shipper never writes under. Two
/// spellings of one fence, one on each side of the bucket, held to each other here.
#[test]
fn the_analyzer_root_is_the_contract_s_reserved_prefix() {
    let layout: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(
            workspace_root().join("contract/inference-events/conformance/delivery/layout.json"),
        )
        .expect("the vendored delivery layout"),
    )
    .expect("layout parses");
    let reserved = layout["reserved_consumer_prefix"]
        .as_str()
        .expect("the contract reserves a consumer prefix");
    assert_eq!(
        reserved,
        analyzer_lake::dated::ANALYZER_ROOT,
        "the analyzer writes under a root the delivery contract does not reserve"
    );
}

/// `contract/PIN.md` names the vendored contract hash. A PIN.md carrying a hash the submodule
/// does not carry asserts a contract the repo is not on, which is worse than no document: the
/// next reader trusts it. It reads two files and needs no engine, so it runs always.
#[test]
fn pin_md_states_the_hash_the_submodule_actually_carries() {
    let root = workspace_root();
    let vendored =
        fs::read_to_string(root.join("contract/inference-events/conformance/CONTRACT_HASH"))
            .expect("vendored CONTRACT_HASH — is the submodule checked out?");
    let vendored = vendored.trim();
    let pin = fs::read_to_string(root.join("contract/PIN.md")).expect("PIN.md");
    assert!(
        pin.contains(vendored),
        "contract/PIN.md does not name the vendored contract hash {vendored}.\n\
         Bump protocol: fetch, check out the new commit, update PIN.md's tag+SHA AND its \
         contract hash, re-run the conformance suite — one commit."
    );
}

/// Every record kind the pinned contract carries is a kind this reader models.
///
/// The direction is the point. A reader can always be asked "do you handle X" for the X it
/// already knows; what it cannot do is notice an X it has never heard of. Walking the corpora
/// rather than naming kinds inline is what makes this hold: a kind added upstream arrives here
/// at the pin bump, and fails until it is modelled. A pure file walk, so it runs always.
#[test]
fn every_kind_the_contract_pins_is_a_kind_this_reader_models() {
    use serde_json::Value;
    fn kinds(v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Object(m) => {
                if let Some(Value::String(k)) = m.get("kind") {
                    out.push(k.clone());
                }
                for child in m.values() {
                    kinds(child, out);
                }
            }
            Value::Array(a) => a.iter().for_each(|c| kinds(c, out)),
            _ => {}
        }
    }

    let mut found = Vec::new();
    let mut files = 0usize;
    let root = workspace_root().join("contract/inference-events/conformance");
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).expect("read conformance dir") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "json") {
                let text = fs::read_to_string(&path).expect("read fixture");
                if let Ok(v) = serde_json::from_str::<Value>(&text) {
                    files += 1;
                    kinds(&v, &mut found);
                }
            }
        }
    }
    assert!(
        files > 0,
        "no fixtures found under {} — is the submodule checked out?",
        root.display()
    );

    found.sort();
    found.dedup();
    // Guard against the walk silently finding nothing: a passing assertion over an empty
    // list would be the same can't-fail shape this test exists to prevent.
    assert!(
        found.len() >= 6,
        "expected the contract to pin at least the cache and lifecycle kinds, found {found:?}"
    );

    let unmodelled: Vec<&String> = found
        .iter()
        .filter(|k| !analyzer_segments::models_kind(k))
        .collect();
    assert!(
        unmodelled.is_empty(),
        "the contract pins {unmodelled:?}, which this reader does not model — they would be \
         counted as drift and their fields flattened into `extra`. Add them to a family in \
         crates/segments, or say in PIN.md why they are deliberately ignored."
    );
}

/// **The absent-workload bucket has ONE word.** Workload is a declaration (spec 2.7), so its
/// absence reads `undeclared`. The scope filter is a verbatim `workload_class = <value>`, so a
/// table defaulting to another word would hold rows no filter selects. `unattributed`, a
/// config-mapping miss's word, is the one a schema could plausibly copy, and this refuses it.
#[test]
fn the_absent_workload_bucket_is_undeclared_in_every_schema() {
    let dir = workspace_root().join("crates/serving-clickhouse/migrations");
    let mut offenders = Vec::new();
    for entry in fs::read_dir(&dir).expect("migrations dir") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let sql = fs::read_to_string(&path).expect("migration");
        for (n, line) in sql.lines().enumerate() {
            // Only a column declaration counts, not prose.
            if line.contains("workload_class") && line.contains("'unattributed'") {
                offenders.push(format!("{}:{}", path.display(), n + 1));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these declare the absent workload bucket as `unattributed`: workload is a \
         declaration, so its absence is `undeclared`. A caller filtering on the wrong word \
         selects nothing at all:\n  {}",
        offenders.join("\n  ")
    );
}

/// **Every migration spells engines through the cell's placeholder.** A migration that spells
/// `ENGINE = SummingMergeTree` directly is one a replicating cell would have to rewrite; the
/// placeholder scan in `migrate` catches a placeholder nothing substitutes, and this catches
/// a CREATE that forgot to use one.
#[test]
fn every_migration_spells_engines_through_the_placeholder() {
    let dir = workspace_root().join("crates/serving-clickhouse/migrations");
    let mut offenders = Vec::new();
    for entry in fs::read_dir(&dir).expect("migrations dir") {
        let path = entry.expect("entry").path();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let sql = fs::read_to_string(&path).expect("migration");
        for (n, line) in sql.lines().enumerate() {
            let code = line.split("--").next().unwrap_or_default();
            if code.contains("ENGINE =") && !code.contains("{engine_family}") {
                offenders.push(format!("{name}:{}: {}", n + 1, code.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these spell an engine or a dedup window a replicating cell would have to rewrite; \
         use `{{engine_family}}`:\n  {}",
        offenders.join("\n  ")
    );
}

/// **Every knob horizon in the migration is in the retention map, and the map names
/// nothing the migration does not**. `0001_schema.sql` spells each
/// horizon through a placeholder so a fresh database carries it from its first second;
/// `RETENTION_TTLS` spells the same horizons so `apply_retention` can move them at every
/// start. Two spellings of one fact, held to each other here, with no exception.
#[test]
fn every_knob_horizon_in_the_migration_is_in_the_retention_map() {
    let sql = fs::read_to_string(
        workspace_root().join("crates/serving-clickhouse/migrations/0001_schema.sql"),
    )
    .expect("the schema");
    let normalize = |s: &str| s.replace(['(', ')', ' '], "");
    let mut in_migration: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    for block in sql.split("CREATE TABLE IF NOT EXISTS ").skip(1) {
        let table = block
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_string();
        if let Some(ttl) = block.lines().find(|l| l.starts_with("TTL ")) {
            let clause = ttl.trim_start_matches("TTL ").trim_end_matches(';');
            let clause = clause.trim_end_matches(" DELETE");
            in_migration.insert(table, normalize(clause));
        }
    }
    let mut problems = Vec::new();
    for (table, expr, knob) in analyzer_serving_clickhouse::engine::RETENTION_TTLS {
        let placeholder = knob.placeholder();
        let unit = knob
            .value(&analyzer_serving_clickhouse::engine::RetentionConfig::default())
            .1;
        let want = format!("{expr} + INTERVAL {placeholder} {unit}");
        match in_migration.get(*table) {
            Some(have) if *have == normalize(&want) => {}
            Some(have) => problems.push(format!(
                "{table}: map says {want:?}, migration has {have:?}"
            )),
            None => problems.push(format!("{table}: in the map, no TTL in the migration")),
        }
    }
    for table in in_migration.keys() {
        if !analyzer_serving_clickhouse::engine::RETENTION_TTLS
            .iter()
            .any(|(t, _, _)| t == table)
        {
            problems.push(format!(
                "{table}: a TTL in the migration that no knob moves"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "the knob map and the migration disagree:\n  {}",
        problems.join("\n  ")
    );
    assert!(
        analyzer_serving_clickhouse::engine::RETENTION_TTLS.len() >= 20,
        "the map went vacuous"
    );
}

/// The files that carry `#![allow(missing_docs)]`, in one list, so an allowance is a ratchet:
/// a file added here is a decision with a reason, a file removed here has been documented, and
/// a new file cannot opt out by carrying the attribute unlisted. The list is EMPTY: every
/// public item is documented. The crate roots never carry an allowance — an inner attribute at
/// a root would silence its whole crate.
const MISSING_DOCS_DEBT: &[&str] = &[];

#[test]
fn missing_docs_debt_is_listed() {
    let root = workspace_root();
    let mut carrying: Vec<String> = Vec::new();
    for dir in crate_dirs() {
        let mut stack = vec![dir.join("src")];
        while let Some(d) = stack.pop() {
            for entry in fs::read_dir(&d).expect("read dir") {
                let path = entry.expect("entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "rs")
                    && fs::read_to_string(&path)
                        .expect("read source")
                        .contains("#![allow(missing_docs)]")
                {
                    carrying.push(
                        path.strip_prefix(&root)
                            .expect("under the repo")
                            .to_string_lossy()
                            .to_string(),
                    );
                }
            }
        }
    }
    carrying.sort();
    let mut expected: Vec<String> = MISSING_DOCS_DEBT.iter().map(|s| (*s).to_string()).collect();
    expected.sort();
    assert_eq!(
        carrying, expected,
        "the files carrying #![allow(missing_docs)] differ from MISSING_DOCS_DEBT in \
         crates/cli/tests/static_audit.rs: a file that was documented comes off the list (and \
         drops its attribute); a new file documents its public items instead of opting out"
    );
    for f in MISSING_DOCS_DEBT {
        assert!(
            !f.ends_with("/lib.rs") && !f.ends_with("/main.rs"),
            "{f}: a crate root never carries the allowance — it would silence the whole crate"
        );
    }
}

#[test]
fn api_pagination_lifecycle_has_one_owner() {
    let root = workspace_root().join("crates/api/src");
    for entry in fs::read_dir(root.join("resources")).unwrap() {
        let path = entry.unwrap().path();
        let source = fs::read_to_string(&path).unwrap();
        for forbidden in ["Cursor::mint", "Cursor::parse", ".read_at("] {
            assert!(!source.contains(forbidden), "{}: use pagination::Input::execute and its Completed page result; resource handlers must not reproduce {forbidden}", path.display());
        }
    }
    let source = fs::read_to_string(root.join("pagination.rs")).unwrap();
    assert!(
        source.contains("async fn read_at") && !source.contains("pub(crate) async fn read_at"),
        "the consistency read must stay private to pagination"
    );
}

/// `docs/error-direction.md` cites conformance fixtures by name as the proof of each guard. A
/// name that no longer exists is a guard proved by nothing, so every cited name must resolve to
/// a fixture under the pinned corpus.
#[test]
fn error_direction_names_only_real_fixtures() {
    let root = workspace_root();
    let doc = fs::read_to_string(root.join("docs/error-direction.md")).expect("error-direction.md");
    let corpus = root.join("contract/inference-events/conformance");
    let mut cited = 0;
    let mut missing = Vec::new();
    for piece in doc.split('`') {
        let Some((family, name)) = piece.split_once('/') else {
            continue;
        };
        if !matches!(family, "reader" | "telemetry")
            || name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            continue;
        }
        cited += 1;
        let dir = corpus.join(family);
        let found = dir.join(format!("{name}.json")).exists() || dir.join(name).exists();
        if !found {
            missing.push(piece.to_string());
        }
    }
    assert!(cited > 0, "error-direction.md cites no fixture at all");
    assert!(
        missing.is_empty(),
        "error-direction.md cites fixtures the pinned corpus does not have: {missing:?}"
    );
}
