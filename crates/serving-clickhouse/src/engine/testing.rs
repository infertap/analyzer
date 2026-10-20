// SPDX-License-Identifier: AGPL-3.0-only

//! The suite's fixture: ONE server per test binary, ONE database per test.
//!
//! Behind the `testing` feature and pulled in as a dev-dependency, so it never reaches a
//! shipped binary. It lives in the storage crate because it is engine-boundary machinery
//! and because every crate above needs it — duplicating it per crate is how three
//! slightly different fixtures appear and one of them starts leaking servers.
//!
//! **Why one server rather than one per test.** A server start is ~1 s; the suite has
//! roughly sixty engine-backed tests. Per-test servers would trade the local backend's
//! honesty problem for a wall-clock problem, and a suite people avoid running is a suite
//! that stops catching things. Databases are the isolation unit instead: cheap to create,
//! invisible to each other, and dropped when their test ends.
//!
//! **Lifetime, and the one case it does not cover.** The shared server is held by a weak
//! reference; each test takes a strong handle, and the last handle to drop stops the
//! server — including when a test panics, because unwinding runs `Drop`. What it cannot
//! cover is a process that dies without unwinding (`SIGKILL`, a `--test-threads` abort).
//! That leak is visible rather than silent: the next run's bind fails with the decided
//! port-conflict error rather than hanging, and the process-group isolation is why the
//! server outlives its parent at all — the same property that keeps the daemon alive when
//! it stops its own engine.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::engine::server::{ServerConfig, ServerHandle};
use crate::engine::Engine;

static SHARED: Mutex<Weak<TestServer>> = Mutex::new(Weak::new());
static DB_SEQ: AtomicU32 = AtomicU32::new(0);

/// A block of `width` consecutive loopback ports, each free when probed: a random base below
/// both platforms' ephemeral ranges (Linux's from 32768, macOS's from 49152), probed port by
/// port and drawn again until every one binds. A base derived from the pid would collide whenever
/// two test processes' pids matched modulo the span. A block, not single
/// ports, because an engine takes two and a daemon's restore check two more above them.
///
/// # Panics
/// No free block after many draws: the host's loopback is exhausted.
#[must_use]
pub fn port_block(width: u16) -> u16 {
    use std::hash::{BuildHasher, Hasher};
    static DRAWS: AtomicU32 = AtomicU32::new(0);
    const LOW: u16 = 20_000;
    const HIGH: u16 = 32_000;
    for _ in 0..256 {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u32(std::process::id());
        hasher.write_u32(DRAWS.fetch_add(1, Ordering::Relaxed));
        #[allow(clippy::cast_possible_truncation)] // reduced below HIGH - LOW
        let base = LOW + (hasher.finish() % u64::from(HIGH - LOW - width)) as u16;
        let free = (base..base + width)
            .map(|port| std::net::TcpListener::bind(("127.0.0.1", port)))
            .collect::<Result<Vec<_>, _>>()
            .is_ok();
        if free {
            return base;
        }
    }
    panic!("no block of {width} free loopback ports between {LOW} and {HIGH}");
}

/// The workspace root, from this crate's manifest directory: the same path whichever crate's
/// tests are calling.
///
/// # Panics
/// If this crate does not sit two directories below the root, which the workspace layout fixes.
#[must_use]
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("crates/<crate> sits two below the workspace root")
        .to_path_buf()
}

/// A pid beyond any platform's `pid_max` (macOS 99 998, Linux's default 4 194 304), so it
/// names no living process on any machine a suite runs on — a dead pid without the reuse
/// window a spawn-and-reap would leave open.
pub const UNUSED_PID: u32 = 2_147_483_647;

/// The engine binary the suite runs against: `CLICKHOUSE_BIN`, or the pinned tarball's
/// binary under the workspace root.
#[must_use]
pub fn engine_bin() -> PathBuf {
    std::env::var_os("CLICKHOUSE_BIN")
        .map_or_else(|| repo_root().join(".tools/clickhouse"), PathBuf::from)
}

/// A running server shared by every test in one binary.
pub(crate) struct TestServer {
    /// Dropping this stops the server: `ServerHandle`'s own `Drop` performs the ordered
    /// stop, so there is no second shutdown path here to get wrong.
    _handle: ServerHandle,
    admin: Engine,
    data_dir: PathBuf,
}

impl TestServer {
    fn start() -> Self {
        let data_dir = std::env::temp_dir().join(format!("analyzer-suite-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("suite scratch directory");

        let mut config = ServerConfig::new(&data_dir);
        // Free ports drawn, so concurrently-running test BINARIES do not collide; within a
        // binary there is only ever this one server.
        let base = port_block(2);
        config.http_port = base;
        config.tcp_port = base + 1;
        // **No bucket is injected, because a server cannot be given one.** Object storage is the
        // analyzer's: a test that wants a bucket opens an `analyzer-objectstore` store exactly
        // as production does.

        let bin = engine_bin();
        assert!(
            bin.is_file(),
            "no engine at {}: the engine-backed tests need the pinned ClickHouse. Run \
             scripts/fetch-engine.sh once, or point CLICKHOUSE_BIN at a build of the pinned \
             version. Plain `cargo test` needs neither: every test that reaches here is \
             `#[ignore]`d.",
            bin.display()
        );
        let handle = ServerHandle::start(bin, &config).expect("the suite's server starts");
        let admin = Engine::for_handle(&handle).expect("admin connection");
        Self {
            _handle: handle,
            admin,
            data_dir,
        }
    }

    /// The server's data directory — for the few tests that measure the filesystem
    /// rather than the database.
    #[must_use]
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

/// A test's claim on the shared server: it stays up while any guard lives, and the
/// guard's database goes away with it.
///
/// Handed out beside the engine rather than owning it, so a converted test keeps the
/// shape it already had — `let (engine, db) = fresh();` — and conversion is deleting a
/// cleanup line rather than restructuring a test.
pub struct TestDatabase {
    server: Arc<TestServer>,
    name: String,
}

impl TestDatabase {
    /// The shared server's data directory, for the few tests that measure the filesystem.
    #[must_use]
    pub fn data_dir(&self) -> &std::path::Path {
        self.server.data_dir()
    }

    /// This guard's database name — for the tests whose statements must name it
    /// explicitly (BACKUP/RESTORE take database identifiers, not the connection's
    /// default).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // Best effort: the server is going away with the binary anyway, but a suite that
        // leaves sixty databases behind makes the next failure harder to read.
        let _ = self.server.admin.run(&format!(
            "DROP DATABASE IF EXISTS {}",
            crate::engine::quote_ident(&self.name)
        ));
    }
}

/// A fresh database on the suite's shared server, starting that server if this is the
/// first caller.
///
/// A recent, day-aligned base for seeded facts. **Every fixture timestamp must derive from
/// this, and none may be an absolute literal.**
///
/// the ingest ledger carried `TTL toDateTime(at_ms / 1000) + INTERVAL ledger_retention_days DAY DELETE`,
/// so a seed written as a fixed date has an expiry: it passes until the wall clock crosses the
/// horizon and fails permanently after, with the visible symptom three layers away — a comparison refusing `UnreadableGeneration` over a pass ledger
/// that was empty because the facts beneath it had been deleted between the seed and the
/// assertion.
///
/// The comments beside those seeds read "a fixed UTC instant; nothing here reads the wall
/// clock". True, and exactly the defect: determinism bought with a fuse. Reading the clock
/// costs nothing that was being protected, because every relationship in a fixture is a DELTA
/// from its base — a constant shift moves no semantics, only the origin.
///
/// Three days back, day-aligned, matching `analyzer_harness::rebase_offset`. The margin is
/// for a suite that runs near midnight, not for retention: the floor is fourteen days.
///
/// `every_seeded_timestamp_derives_from_the_clock` in the CLI static audit is what keeps this
/// from being advice.
#[must_use]
pub fn recent_day_base_ms() -> f64 {
    use analyzer_analysis::DAY_MS;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    (now / DAY_MS).floor() * DAY_MS - 3.0 * DAY_MS
}

/// # Panics
/// If the server cannot start or the database cannot be created — a fixture that limps
/// on produces failures that look like product bugs.
#[must_use]
pub fn database(label: &str) -> (Engine, TestDatabase) {
    let server = shared();
    let name = format!(
        "t_{}_{}_{}",
        label.replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
        std::process::id(),
        DB_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    server
        .admin
        .run(&format!(
            "CREATE DATABASE {}",
            crate::engine::quote_ident(&name)
        ))
        .expect("create the test database");
    let engine = server
        .admin
        .in_database(&name)
        .expect("bind the engine to the test database");
    (engine, TestDatabase { server, name })
}

fn shared() -> Arc<TestServer> {
    let mut slot = SHARED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(running) = slot.upgrade() {
        return running;
    }
    let server = Arc::new(TestServer::start());
    *slot = Arc::downgrade(&server);
    server
}
