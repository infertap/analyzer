// SPDX-License-Identifier: AGPL-3.0-only

//! Server posture against the pinned engine: the claims a render test cannot make.
//!
//! Everything here needs a process that lives. The unit tests in `storage::server` prove
//! what we WRITE — that no `MySQL` port is declared, that every system log carries a TTL,
//! that the plaintext never reaches disk. These prove what the ENGINE DOES with it: that
//! it accepts the configuration, that the TTL clause is real rather than well-formed,
//! that exclusivity holds against the server's own pid file, that a restart rotates the
//! credential for real, and that a living process reclaims what a per-invocation one cannot.
//!
//! Engine resolution mirrors the schema suite: `CLICKHOUSE_BIN` or `.tools/clickhouse`.
//!
//! Ports are assigned per test rather than taken from the defaults, because the suite
//! runs its tests concurrently and two servers on one port is the exact collision the
//! decided bind error exists to name.

use analyzer_serving_clickhouse::engine::testing::{engine_bin, UNUSED_PID};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use analyzer_serving_clickhouse::engine::server::{
    engine_config_exhibit, owner_of, require_exclusive, Owner, ServerConfig, ServerHandle,
};

static PORT_SEQ: AtomicU16 = AtomicU16::new(0);

/// A data directory and a port pair nothing else in this run is using.
fn fresh_config(name: &str) -> ServerConfig {
    let seq = PORT_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "analyzer-server-test-{name}-{}-{seq}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    // Distinct per test AND per run, clear of both platforms' ephemeral ranges: a run
    // that was killed before its servers stopped must not make the next one read as a
    // bind conflict.
    let base = analyzer_serving_clickhouse::engine::testing::port_block(2);
    let mut config = ServerConfig::new(&dir);
    config.http_port = base;
    config.tcp_port = base + 1;
    config
}

fn start(config: &ServerConfig) -> ServerHandle {
    ServerHandle::start(engine_bin(), config).expect("the engine starts and becomes ready")
}

fn cleanup(config: &ServerConfig) {
    let _ = std::fs::remove_dir_all(&config.data_dir);
}

/// Poll `probe` until it holds or the deadline passes; the failure message is the point,
/// so a timeout says what did not happen rather than that an assert failed.
fn within<F: FnMut() -> bool>(limit: Duration, what: &str, mut probe: F) {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if probe() {
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    panic!("{what} did not happen within {}s", limit.as_secs());
}

/// Bytes beneath a directory, the way `selfhealth` measures unreclaimed space: from the
/// filesystem, never from the engine's own inactive-part view.
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&entry.path()),
            Ok(t) if t.is_file() => entry.metadata().map(|m| m.len()).unwrap_or_default(),
            _ => 0,
        })
        .sum()
}

fn scalar(handle: &ServerHandle, sql: &str) -> String {
    handle.client_query(sql).expect("query").trim().to_string()
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_engine_accepts_our_configuration_and_the_system_log_ttls_are_real() {
    // The one claim the plan could not confirm from the documentation: that a `<ttl>`
    // element inside a `<query_log>` block is honored. A render test proves we wrote it.
    // Only the engine can say it took it, and T2 — the fix for unbounded growth
    // introducing unbounded growth — rests entirely on that.
    let config = fresh_config("ttl");
    let handle = start(&config);

    assert_eq!(scalar(&handle, "SELECT 1"), "1");

    // The system logs materialize on first flush; before that there is no table to ask.
    handle.client_query("SYSTEM FLUSH LOGS").expect("flush");
    for table in ["query_log", "part_log", "trace_log"] {
        let ddl = scalar(
            &handle,
            &format!(
                "SELECT create_table_query FROM system.tables WHERE database = 'system' \
                 AND name = '{table}'"
            ),
        );
        assert!(
            !ddl.is_empty(),
            "{table} was never created, so its TTL proves nothing"
        );
        let ttl = ddl.to_uppercase();
        assert!(
            ttl.contains("TTL") && ttl.contains("14"),
            "{table} carries no 14-day TTL — the engine did not take our clause:\n{ddl}"
        );
    }

    // The hardening claims, as the running server reports them: we never declare these
    // protocols, so the engine must show them untouched from its own defaults.
    for setting in ["mysql_port", "postgresql_port", "interserver_http_port"] {
        let changed = scalar(
            &handle,
            &format!("SELECT changed FROM system.server_settings WHERE name = '{setting}'"),
        );
        assert!(
            changed.is_empty() || changed == "0",
            "{setting} is configured on a running server; it must exist only by absence"
        );
    }

    // And the exhibit an auditor is handed is the bytes this running server read.
    let exhibit = engine_config_exhibit(&config).expect("exhibit");
    assert!(exhibit.contains("RUNNING STATE"), "{exhibit}");
    assert!(!exhibit.contains(handle.credential().password()));

    handle.shutdown().expect("ordered shutdown");
    cleanup(&config);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_daemon_refuses_to_supervise_a_binary_off_the_pin() {
    // The version fence lives where the process is started, so the daemon never supervises
    // whatever `clickhouse` it was pointed at, and the pin is embedded rather than passed —
    // the same reason the migrations are embedded: a caller cannot omit what it never supplies.
    let config = fresh_config("off-pin");
    let decoy = config.data_dir.join("not-clickhouse");
    std::fs::write(
        &decoy,
        "#!/bin/sh\necho 'ClickHouse local version 1.2.3.4'\n",
    )
    .expect("write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&decoy, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }

    let error = ServerHandle::start(&decoy, &config).expect_err("an off-pin binary is refused");
    assert!(
        matches!(&error, analyzer_serving_clickhouse::engine::Error::Refused(m) if m.contains("1.2.3.4") && m.contains("refusing")),
        "the refusal names what it found: {error}"
    );
    cleanup(&config);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_live_server_owns_its_directory_and_hands_it_back_on_shutdown() {
    // Exclusivity against the pid file the SERVER writes, not one a test fabricated:
    // this is the seam where our legible layer meets the engine's own directory lock.
    let config = fresh_config("exclusive");
    let handle = start(&config);

    match owner_of(&config.data_dir) {
        Owner::Live(pid) => assert!(pid > 0, "the server's own pid file names a live process"),
        other => panic!("a running server must own its directory, got {other:?}"),
    }
    let refusal = require_exclusive(&config.data_dir).expect_err("a second owner is refused");
    assert!(
        matches!(&refusal, analyzer_serving_clickhouse::engine::Error::Refused(m) if m.contains("already owns")),
        "the refusal must name the case it found: {refusal}"
    );

    handle.shutdown().expect("ordered shutdown");

    // The cutover property: after an ordered stop the directory is takeable, so a
    // restart of the analyzer is not an operator ticket.
    let again = ServerHandle::start(engine_bin(), &config)
        .expect("the directory is free for the next owner after a clean stop");
    assert_eq!(scalar(&again, "SELECT 1"), "1");
    again.shutdown().expect("ordered shutdown");
    cleanup(&config);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn stopping_the_engine_does_not_signal_the_process_that_owns_it() {
    // A defect this suite found on its first live run, pinned so it cannot return.
    // `SYSTEM SHUTDOWN` is the engine's documented way to stop itself, and it is
    // implemented as a signal to the server's PROCESS GROUP. With the server sharing our
    // group, asking it to stop killed this process too — which in production means the
    // daemon dies every time it stops or restarts its own engine, and under an init
    // system it takes the unit's other children with it. The server therefore gets a
    // process group of its own.
    let config = fresh_config("group");
    let mut handle = start(&config);

    let pgid_of = |pid: u32| -> String {
        let out = Command::new("ps")
            .arg("-o")
            .arg("pgid=")
            .arg("-p")
            .arg(pid.to_string())
            .stdin(Stdio::null())
            .output()
            .expect("ps");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    let engine_group = pgid_of(handle.pid());
    assert_eq!(
        engine_group,
        handle.pid().to_string(),
        "the engine must lead its own process group, not sit in ours"
    );
    assert_ne!(
        engine_group,
        pgid_of(std::process::id()),
        "engine and owner share a process group: a group-wide signal from the engine \
         would reach the daemon"
    );

    assert!(handle.is_alive());
    handle.shutdown().expect("ordered shutdown");
    // Reaching this line at all is the assertion: before the fix, the process was gone
    // by now. Doing real work afterwards proves it is not merely limping.
    let next = start(&config);
    assert_eq!(scalar(&next, "SELECT 1"), "1");
    next.shutdown().expect("ordered shutdown");
    cleanup(&config);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn an_ordered_stop_is_bounded_even_when_the_engine_is_already_gone() {
    // `Drop` walks the ordered-stop path, so every step of it has to terminate: the
    // graceful request carries the client's own connect/receive timeouts, the wait for
    // exit is `SHUTDOWN_TIMEOUT`, and the escalation is a kill and a reap. The realistic
    // hang is the first step — a request sent to a server that is no longer there, or is
    // there and wedged. Killing the engine underneath the handle exercises exactly that.
    let config = fresh_config("bounded-stop");
    let handle = start(&config);

    let killed = Command::new("kill")
        .arg("-9")
        .arg(handle.pid().to_string())
        .stdin(Stdio::null())
        .status()
        .expect("kill");
    assert!(
        killed.success(),
        "the engine could not be killed for the test"
    );

    let began = Instant::now();
    handle
        .shutdown()
        .expect("an ordered stop of a dead engine still succeeds");
    // A stop that waited for a dead engine to exit would take the whole SHUTDOWN_TIMEOUT.
    assert!(
        began.elapsed() < analyzer_serving_clickhouse::engine::server::SHUTDOWN_TIMEOUT,
        "the ordered stop took {:?} against a dead engine: it waited for an exit that had \
         already happened",
        began.elapsed()
    );
    cleanup(&config);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_restart_rotates_the_credential_and_the_old_one_stops_working() {
    // The ephemeral property under SUPERVISION, which is where it would quietly rot: a
    // restart that reused the password would turn a per-process secret into a persistent
    // one, and nothing downstream would notice.
    let config = fresh_config("rotate");
    let handle = start(&config);
    let old_password = handle.credential().password().to_string();
    let users_path = config.data_dir.join("engine-config/users.xml");
    let old_users = std::fs::read_to_string(&users_path).expect("users.xml");

    let handle = handle.restart().expect("restart");
    assert_eq!(scalar(&handle, "SELECT 1"), "1", "the new server answers");

    let new_users = std::fs::read_to_string(&users_path).expect("users.xml");
    assert_ne!(
        old_users, new_users,
        "the credential file was reused instead of regenerated"
    );

    // The sharp form: the old secret is not merely absent from the file, it is refused
    // by the running server.
    let rejected = Command::new(engine_bin())
        .arg("client")
        .arg("--host")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(config.tcp_port.to_string())
        .arg("--user")
        .arg(&config.user)
        .arg("--password")
        .arg(&old_password)
        .arg("--query")
        .arg("SELECT 1")
        .stdin(Stdio::null())
        .output()
        .expect("client spawn");
    assert!(
        !rejected.status.success(),
        "the credential from before the restart still opens the database"
    );

    handle.shutdown().expect("ordered shutdown");
    cleanup(&config);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_port_already_in_use_is_a_decided_error_and_does_not_wait_out_the_timeout() {
    // A target shop plausibly already runs ClickHouse on the standard ports. Ours are
    // deliberately not those, but the collision must still land as an error naming the
    // conflict rather than a minute of silence followed by a raw engine message.
    let first = fresh_config("bind-a");
    let handle = start(&first);

    let mut second = fresh_config("bind-b");
    second.http_port = first.http_port;
    second.tcp_port = first.tcp_port;

    let began = Instant::now();
    let error = ServerHandle::start(engine_bin(), &second)
        .expect_err("a second server on the same ports cannot start");
    assert!(
        matches!(&error, analyzer_serving_clickhouse::engine::Error::Server(m)
            if m.contains(&first.tcp_port.to_string()) || m.contains(&first.http_port.to_string())),
        "the server's error must name the conflicting port: {error}"
    );
    assert!(
        matches!(&error, analyzer_serving_clickhouse::engine::Error::Server(m) if m.contains("already listening")),
        "the error must say what happened: {error}"
    );
    // Waiting it out would take the whole READINESS_TIMEOUT.
    assert!(
        began.elapsed() < analyzer_serving_clickhouse::engine::server::READINESS_TIMEOUT,
        "the bind failure was waited out instead of found in the log"
    );

    handle.shutdown().expect("ordered shutdown");
    cleanup(&first);
    cleanup(&second);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_living_process_collapses_replacing_rows_and_reclaims_dead_parts() {
    // Two properties only a living process holds, closed at their mechanism. Both are
    // background work: a replacing table holds two rows per restated key until a merge
    // collapses them, and dead parts sit on disk until the cleanup task runs; a process that
    // exits before either leaves both undone.
    //
    // Nothing here issues OPTIMIZE: an explicit merge would prove the syntax, not the
    // claim. The claim is that a process which stays alive does this work itself.
    //
    // **What this pin also measured, and what it changes.** A living server does NOT
    // merge two small idle parts on its own within any bounded time: driven by hand
    // against this engine, two 50k-row parts sat unmerged past 60 s with
    // `system.merges` empty, because the merge selector treats such a merge as
    // unprofitable until pressure arrives. So "a server collapses the duplication" is
    // true only eventually and at the engine's discretion — a STATED bound, unacceptable for
    // a surface that grows with run count. The enforcement lever is
    // `min_age_to_force_merge_seconds`, set here on the table: with it, the merge lands
    // in ~10 s and the dead parts are gone ~5 s later. Carrying it into the product
    // schema for the rollups is a storage-schema change and is
    // flagged for ratification, not smuggled in here. The shortened lifetimes move the
    // clock inside a test's patience; they do not create the behavior.
    let config = fresh_config("reclaim");
    let handle = start(&config);

    handle
        .client_query(
            "CREATE TABLE demo (k UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k \
             SETTINGS min_age_to_force_merge_seconds = 5, old_parts_lifetime = 5, \
             cleanup_delay_period = 5, max_cleanup_delay_period = 10",
        )
        .expect("create");
    for _ in 0..2 {
        handle
            .client_query("INSERT INTO demo SELECT number, number FROM numbers(50000)")
            .expect("insert");
    }
    assert_eq!(
        scalar(&handle, "SELECT count() FROM demo"),
        "100000",
        "two inserts of the same keys are two parts holding 2x the rows"
    );

    // The table's own store path, asked of the engine rather than assumed from the
    // layout. Measuring the whole data directory would measure the system logs flushing
    // alongside us — real growth, but not the growth this claim is about.
    let table_dir = PathBuf::from(scalar(
        &handle,
        "SELECT arrayElement(data_paths, 1) FROM system.tables \
         WHERE database = currentDatabase() AND name = 'demo'",
    ));
    let peak_bytes = dir_bytes(&table_dir);
    assert!(
        peak_bytes > 0,
        "the table's parts are not where the engine says"
    );

    within(
        Duration::from_secs(90),
        "the background merge (no OPTIMIZE was issued)",
        || scalar(&handle, "SELECT count() FROM demo") == "50000",
    );
    within(
        Duration::from_secs(90),
        "reclamation of the merged-away parts",
        || {
            scalar(
                &handle,
                "SELECT count() FROM system.parts WHERE table = 'demo' AND active = 0",
            ) == "0"
        },
    );

    // And the claim in the form the self-health sensor makes it: unreclaimed space is
    // measured from the FILESYSTEM, because the engine's inactive-part view is
    // process-local and under-reports. Bytes must come back down, not merely be reported
    // as freed.
    let settled_bytes = dir_bytes(&table_dir);
    assert!(
        settled_bytes < peak_bytes,
        "the data directory did not shrink after reclamation: {peak_bytes} B at peak, \
         {settled_bytes} B after — dead parts are still on disk"
    );

    handle.shutdown().expect("ordered shutdown");
    cleanup(&config);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_dead_daemons_orphaned_engine_is_adopted_rather_than_left_for_an_operator() {
    // **The gap this closes is a real one in crash-anywhere recovery, and it exists
    // BECAUSE of the process-group fix.** The engine runs in its own process group so
    // that stopping it cannot kill the daemon — which means the reverse also holds:
    // killing the daemon leaves the engine running. Its credential is ephemeral and died
    // with the process that minted it, so a new daemon cannot authenticate to it, cannot
    // `SYSTEM SHUTDOWN` it, and would meet Owner::Live and refuse. That is an operator
    // turning a knob to restore health, which is a defect.
    //
    // The evidence that makes taking over safe rather than reckless is the PAIR of pid
    // files: a stale daemon file plus a live engine says a previous *us* died here.
    let config = fresh_config("orphan");
    let dir = config.data_dir.clone();

    // an engine running with no daemon alive to own it
    let mut handle = ServerHandle::start(engine_bin(), &config).expect("start the engine");
    assert!(handle.is_alive(), "the orphan is running");
    // NOTE: the pid file does NOT name our direct child. ClickHouse runs a watchdog parent
    // whose child is the actual server, and the pid file names the SERVER. Measured here,
    // not assumed — an earlier draft of this test asserted they were equal and they were
    // 23 apart. Recovery must therefore act on the pid FILE, which is what `owner_of`
    // reads, and not on `ServerHandle::pid`.
    let Owner::Live(engine_pid) = owner_of(&dir) else {
        panic!("a running server owns its directory")
    };
    assert_ne!(
        engine_pid,
        handle.pid(),
        "the watchdog is our child; the server named in the pid file is its child"
    );

    // the residue a killed daemon leaves: its pid file, naming a process that is gone
    std::fs::write(dir.join("analyzer-daemon.pid"), format!("{UNUSED_PID}\n"))
        .expect("write a stale daemon pid file");

    let guard = analyzer_serving_clickhouse::engine::server::claim_for_daemon(&dir)
        .expect("the takeover must succeed");
    assert_eq!(
        *guard.claim(),
        analyzer_serving_clickhouse::engine::server::DaemonClaim::AdoptedOrphan(engine_pid),
        "the takeover names the engine it stopped, so the recovery appears in the log \
         rather than happening silently"
    );
    assert!(
        !handle.is_alive(),
        "the orphaned engine must actually be stopped, not merely declared adopted"
    );
    assert_eq!(
        owner_of(&dir),
        Owner::Free,
        "and its pid file is cleared, so the next start meets a clean directory"
    );

    // while the guard lives, a second daemon is refused by pid
    let refusal = analyzer_serving_clickhouse::engine::server::claim_for_daemon(&dir)
        .expect_err("a live daemon is a refusal");
    assert!(
        matches!(&refusal, analyzer_serving_clickhouse::engine::Error::Refused(m)
            if m.contains(&std::process::id().to_string()) && m.contains("already owns")),
        "the refusal names the live owner: {refusal}"
    );

    // and dropping the guard frees the directory for an ordinary next start
    drop(guard);
    let next = analyzer_serving_clickhouse::engine::server::claim_for_daemon(&dir)
        .expect("a clean directory");
    assert_eq!(
        *next.claim(),
        analyzer_serving_clickhouse::engine::server::DaemonClaim::Fresh,
        "no orphan, no takeover"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_live_engine_with_no_daemon_residue_is_still_a_refusal() {
    // The strict half, and the reason the takeover is safe. A customer's own ClickHouse
    // pointed at this directory presents as a live engine with NO daemon pid file beside
    // it — and killing it would be our bug destroying their process. Absent evidence that
    // the engine is ours, the answer stays the decided refusal it has always been.
    let config = fresh_config("nodaemon");
    let dir = config.data_dir.clone();
    let handle = ServerHandle::start(engine_bin(), &config).expect("start");

    let refusal = require_exclusive(&dir).expect_err("a live owner is a refusal");
    assert!(
        matches!(&refusal, analyzer_serving_clickhouse::engine::Error::Refused(m) if m.contains("already owns")),
        "{refusal}"
    );
    // and the claim path agrees: no daemon residue means no adoption
    let guard =
        analyzer_serving_clickhouse::engine::server::claim_for_daemon(&dir).expect("claims");
    assert_eq!(
        *guard.claim(),
        analyzer_serving_clickhouse::engine::server::DaemonClaim::Fresh,
        "with no stale daemon file there is no evidence the engine is ours, so it is left \
         running and the SERVER start is what refuses"
    );

    handle.shutdown().expect("ordered shutdown");
    let _ = std::fs::remove_dir_all(&dir);
}
