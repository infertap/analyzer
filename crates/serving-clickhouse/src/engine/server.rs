// SPDX-License-Identifier: AGPL-3.0-only

//! Server posture: the analyzer owns a long-lived `clickhouse-server`. It generates the
//! server's configuration, starts it, proves it ready, supervises it, and shuts it down.
//!
//! **Why a server at all**, since the answer is measured rather than preferred: merges,
//! dead-part reclamation, TTL application and mutation drain are background work, and
//! background work needs a process that lives. A short-lived process leaves every dead
//! part on disk and every replaced row uncollapsed, because `ReplacingMergeTree` collapses
//! only at merge.
//!
//! **The configuration is generated from scratch, never layered over a shipped default.**
//! The pinned tarball carries only the bare binary, so anything this module does not
//! declare does not exist — which is precisely how the `MySQL` (9004), `PostgreSQL` (9005)
//! and interserver (9009) listeners are disabled: by never being configured. Rendering is
//! a pure function ([`ServerConfig::render_config_xml`]) so every hardening claim is a
//! unit test that needs no server, and the `engine-config dump` exhibit prints those same
//! bytes for an auditor without starting anything.
//!
//! **The credential is ephemeral by construction.** A fresh password is minted at every
//! start (and at every restart); only its SHA-256 reaches disk, in the generated
//! `users.xml`; the plaintext lives in this process's memory and dies with it. There is
//! no secret at rest, nothing to rotate, and no key-management story to defend — the
//! server is our own child, so a credential that outlives the process would be a
//! liability with no compensating benefit. The `default` user is not defined at all.
//!
//! **Process boundaries this module respects.** `libc` is forbidden workspace-wide, so
//! graceful shutdown goes through `SYSTEM SHUTDOWN` (the engine's own documented
//! equivalent of `service clickhouse-server stop`) rather than a signal, and liveness
//! probing goes through `kill -0`. Readiness is proven by spawning `clickhouse client`,
//! which needs no runtime and answers before the HTTP interface is up.

use crate::engine::Error;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

/// Default HTTP port. **Deliberately not 8123**: a target deployment plausibly already
/// runs `ClickHouse` on the standard ports, and silently colliding with a customer's
/// existing database is the worst possible first impression. Overridable.
pub const DEFAULT_HTTP_PORT: u16 = 18123;
/// Default native port. Deliberately not 9000, for the same reason.
pub const DEFAULT_TCP_PORT: u16 = 19000;

/// The engine's `max_server_memory_usage`, as a fraction of the process ceiling — and
/// under the unit's shared cgroup, that is a backstop on the SUM, not a share for the
/// engine.
///
/// A labeled analytic constant (`docs/conformance.md`). The engine child lives in the
/// unit's own cgroup, and its total-memory tracker corrects against the cgroup's RSS, so
/// any value here is functionally a ceiling on both processes together: a "share" carved
/// below the cap starves whichever process the other one crowds, and deriving the value
/// from "what is left after the analyzer's budget" would make the tracker count the
/// analyzer's peak against the smaller number and refuse instantly.
///
/// 0.9 is therefore the last refusal line before the kernel's OOM killer: the engine sheds
/// and refuses only when the domain is genuinely near its cap, which is the engine's own
/// upstream default posture for exactly this role. What keeps the two processes from
/// crowding each other is the analyzer's own in-process read budget, not this number.
/// Re-derive against measured completed-pass peaks.
pub const MAX_SERVER_MEMORY_RATIO: f64 = 0.9;

/// Retention for the engine's own bookkeeping tables.
///
/// A labeled analytic constant. `query_log`, `part_log` and friends grow unbounded by
/// default — it would be an embarrassment for the change that fixes unbounded growth to
/// introduce a fresh instance of it. Re-derived when a real deployment measures how far
/// back an operator actually needs to look.
pub const SYSTEM_LOG_TTL_DAYS: u32 = 14;

/// How long readiness may take before the start is called a failure.
pub const READINESS_TIMEOUT: Duration = Duration::from_mins(1);

/// How long a graceful shutdown may take before escalating to a kill.
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// The bound on a single lifecycle client call (readiness probe, shutdown request).
///
/// Every lifecycle wait in this module is bounded, and the chain is the point: a
/// graceful request that cannot hang (this), then a wait for the process to exit
/// ([`SHUTDOWN_TIMEOUT`]), then a kill, then a reap. `Drop` walks that chain, so an
/// unbounded step anywhere in it would turn tidiness into a wedged daemon.
pub const CLIENT_LIFECYCLE_TIMEOUT_SECS: u64 = 10;

/// An ephemeral server credential: plaintext in memory, hash on disk. `Debug` prints the
/// user and the digest, never the plaintext: a handle is logged, and a logged password is
/// a leaked one.
#[derive(Clone)]
pub struct Credential {
    /// The engine user the credential authenticates — the one user `users.xml` defines.
    pub user: String,
    password: String,
    sha256_hex: String,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("user", &self.user)
            .field("sha256_hex", &self.sha256_hex)
            .finish_non_exhaustive()
    }
}

impl Credential {
    /// Mint a fresh credential from the system CSPRNG.
    ///
    /// `/dev/urandom` through `std::fs` rather than a crate: no dependency, and `libc`
    /// (which `getrandom` would reach for) is forbidden workspace-wide.
    ///
    /// # Errors
    /// If the system random source cannot be read — which is not a condition to paper
    /// over with a weaker source.
    pub fn generate(user: &str) -> Result<Self, Error> {
        let password = hex_lower(&read_random(32)?);
        let sha256_hex = hex_upper(&Sha256::digest(password.as_bytes()));
        Ok(Self {
            user: user.to_string(),
            password,
            sha256_hex,
        })
    }

    /// The plaintext, for this process's own connections only. Never written to disk.
    #[must_use]
    pub fn password(&self) -> &str {
        &self.password
    }

    /// The SHA-256 the server config carries. Not a secret.
    #[must_use]
    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }
}

/// A 32-byte random secret, hex: the one definition of "random" on the credential path.
/// Both credentials, the ephemeral engine password and an operator's viewer token, want the
/// same source and the same width, and a second call site rolling its own is how one of
/// them quietly becomes weaker than the other.
///
/// # Errors
/// The system random source is unavailable.
pub fn random_token() -> Result<String, Error> {
    Ok(hex_lower(&read_random(32)?))
}

/// Upper-case hex of the SHA-256 of `bytes` — the digest form that reaches disk.
///
/// The same rendering [`Credential`] writes into `users.xml`, promoted for the same reason as
/// [`random_token`]: one definition of what a stored digest looks like.
#[must_use]
pub fn sha256_hex_upper(bytes: &[u8]) -> String {
    hex_upper(&Sha256::digest(bytes))
}

/// Read exactly `n` bytes from the system CSPRNG. `read_exact` for a fixed count, never
/// `fs::read`: `/dev/urandom` is an endless stream with no EOF, so a whole-file read never
/// returns and allocates until the process dies.
fn read_random(n: usize) -> Result<Vec<u8>, Error> {
    use std::io::Read as _;
    let mut f = std::fs::File::open("/dev/urandom")
        .map_err(|e| Error::Io(format!("opening /dev/urandom: {e}")))?;
    let mut buf = vec![0u8; n];
    f.read_exact(&mut buf)
        .map_err(|e| Error::Io(format!("reading /dev/urandom: {e}")))?;
    Ok(buf)
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn hex_upper(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut s, b| {
        let _ = write!(s, "{b:02X}");
        s
    })
}

/// Escape a value for an XML text node. Paths are caller-supplied, so this is the
/// quoting boundary the same way `quote_literal` is SQL's.
fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Everything the generated server configuration declares.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The engine's data directory: its store and pid file, and the generated
    /// `engine-config/` beneath it.
    pub data_dir: PathBuf,
    /// The HTTP listener's port; [`DEFAULT_HTTP_PORT`] unless overridden.
    pub http_port: u16,
    /// The native-protocol listener's port; [`DEFAULT_TCP_PORT`] unless overridden.
    pub tcp_port: u16,
    /// Absolute ceiling in bytes; `None` leaves the engine's own (unbounded) default,
    /// which no deployment should choose.
    pub max_server_memory_bytes: Option<u64>,
    /// Retention, in days, for the engine's own bookkeeping tables (`query_log`,
    /// `part_log` and friends); [`SYSTEM_LOG_TTL_DAYS`] by default.
    pub system_log_ttl_days: u32,
    /// The one engine user the generated `users.xml` defines; the credential is minted for
    /// it.
    pub user: String,
}

impl ServerConfig {
    /// A configuration for `data_dir` with every hardening default applied.
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            http_port: DEFAULT_HTTP_PORT,
            tcp_port: DEFAULT_TCP_PORT,
            max_server_memory_bytes: None,
            system_log_ttl_days: SYSTEM_LOG_TTL_DAYS,
            user: "infertap".to_string(),
        }
    }

    fn config_dir(&self) -> PathBuf {
        self.data_dir.join("engine-config")
    }

    fn users_path(&self) -> PathBuf {
        self.config_dir().join("users.xml")
    }

    fn config_path(&self) -> PathBuf {
        self.config_dir().join("config.xml")
    }

    fn pid_path(&self) -> PathBuf {
        pid_file(&self.data_dir)
    }

    /// One system-log block with its TTL. Every log the server keeps about itself is a
    /// disk surface nobody watches and gets an enforced horizon here.
    fn system_log(&self, name: &str) -> String {
        format!(
            "    <{name}>\n\
             \x20       <database>system</database>\n\
             \x20       <table>{name}</table>\n\
             \x20       <flush_interval_milliseconds>7500</flush_interval_milliseconds>\n\
             \x20       <ttl>event_date + INTERVAL {} DAY DELETE</ttl>\n\
             \x20   </{name}>\n",
            self.system_log_ttl_days
        )
    }

    /// The generated server configuration. A pure function of this struct — which is the
    /// point: every claim about what this server does and does not listen on is testable
    /// without starting it, and `engine-config dump` shows an auditor the same bytes the
    /// server will read.
    ///
    /// What is deliberately ABSENT is as load-bearing as what is present: no
    /// `mysql_port`, no `postgresql_port`, no `interserver_http_port`. The binary ships
    /// without a default config, so an undeclared protocol is never started.
    ///
    /// **The server runs in UTC, whatever the host's zone.** Every day the analyzer names is
    /// a UTC day; a server in the host's zone would place `today()`, `toDate(x)` and
    /// partition dates on local days, which west of UTC is the previous day for hours at a
    /// time, so a horizon counted in days would land a day off on a host in a US zone.
    #[must_use]
    pub fn render_config_xml(&self) -> String {
        let dir = xml_escape(&self.data_dir.display().to_string());
        let cfg = xml_escape(&self.config_dir().display().to_string());
        let mut memory = String::new();
        if let Some(bytes) = self.max_server_memory_bytes {
            memory = format!("    <max_server_memory_usage>{bytes}</max_server_memory_usage>\n");
        }
        let logs: String = [
            "query_log",
            "part_log",
            "trace_log",
            "metric_log",
            "asynchronous_metric_log",
        ]
        .iter()
        .map(|n| self.system_log(n))
        .collect();

        format!(
            "<clickhouse>\n\
             \x20   <logger>\n\
             \x20       <level>warning</level>\n\
             \x20       <log>{dir}/logs/server.log</log>\n\
             \x20       <errorlog>{dir}/logs/server.err.log</errorlog>\n\
             \x20       <size>100M</size>\n\
             \x20       <count>3</count>\n\
             \x20   </logger>\n\
             \x20   <path>{dir}/</path>\n\
             \x20   <tmp_path>{dir}/tmp/</tmp_path>\n\
             \x20   <user_files_path>{dir}/user_files/</user_files_path>\n\
             \x20   <user_directories>\n\
             \x20       <users_xml><path>{cfg}/users.xml</path></users_xml>\n\
             \x20   </user_directories>\n\
             \x20   <timezone>UTC</timezone>\n\
             \x20   <listen_host>127.0.0.1</listen_host>\n\
             \x20   <http_port>{http}</http_port>\n\
             \x20   <tcp_port>{tcp}</tcp_port>\n\
             \x20   <mark_cache_size>268435456</mark_cache_size>\n\
             {memory}{logs}</clickhouse>\n",
            http = self.http_port,
            tcp = self.tcp_port,
        )
    }

    /// The generated users file. The `default` user is not defined at all — a deployment
    /// where it exists with an empty password is a line item in every security review,
    /// and there is no reason to carry one when the only client is our own process.
    #[must_use]
    pub fn render_users_xml(&self, credential: &Credential) -> String {
        format!(
            "<clickhouse>\n\
             \x20   <profiles>\n\
             \x20       <default/>\n\
             \x20   </profiles>\n\
             \x20   <users>\n\
             \x20       <{user}>\n\
             \x20           <password_sha256_hex>{hash}</password_sha256_hex>\n\
             \x20           <networks><ip>127.0.0.1</ip></networks>\n\
             \x20           <profile>default</profile>\n\
             \x20           <quota>default</quota>\n\
             \x20           <access_management>0</access_management>\n\
             \x20       </{user}>\n\
             \x20   </users>\n\
             \x20   <quotas><default/></quotas>\n\
             </clickhouse>\n",
            user = xml_escape(&credential.user),
            hash = credential.sha256_hex(),
        )
    }
}

/// A transient engine session: this process starts a server, owns it, works, and stops it.
///
/// **The shape the credential architecture forces.** The daemon's password is ephemeral,
/// in memory, hash-only at rest — so a sibling process cannot authenticate to a RUNNING
/// daemon without inventing a credential-sharing mechanism, and inventing one would repeal
/// the no-secret-at-rest property for a convenience. A one-shot command therefore does not
/// borrow the daemon's engine; it starts its own, which is possible exactly when nobody
/// else owns the directory. Exclusivity does the routing, and no plaintext ever crosses a
/// process boundary.
///
/// Against a directory a daemon already owns this refuses, naming the owner — because the
/// resident report path is the daemon's own loop, running inside the process that holds
/// the credential. A future CLI-to-daemon query belongs to the read API and its own auth
/// realm, not here.
pub struct Session {
    handle: Option<ServerHandle>,
    engine: crate::engine::Engine,
}

impl Session {
    /// Start a server this process owns and connect to it.
    ///
    /// # Errors
    /// If another process owns the directory (the decided refusal), if the binary is off
    /// the pin, or if the server cannot start or be reached.
    pub fn open(bin: impl Into<PathBuf>, data_dir: impl Into<PathBuf>) -> Result<Self, Error> {
        Self::open_with(bin, &ServerConfig::new(data_dir.into()))
    }

    /// [`Session::open`] with the caller's own configuration — ports off the defaults so a
    /// scratch session can coexist with a running daemon, so RESTORE is
    /// permitted.
    ///
    /// # Errors
    /// As [`Session::open`].
    pub fn open_with(bin: impl Into<PathBuf>, config: &ServerConfig) -> Result<Self, Error> {
        let handle = ServerHandle::start(bin, config)?;
        let engine = crate::engine::Engine::for_handle(&handle)?;
        Ok(Self {
            handle: Some(handle),
            engine,
        })
    }

    /// The engine bound to this session's server.
    #[must_use]
    pub fn engine(&self) -> &crate::engine::Engine {
        &self.engine
    }

    /// Stop the server in order, rather than leaving it to `Drop`.
    ///
    /// # Errors
    /// If the engine cannot be stopped or reaped.
    pub fn close(mut self) -> Result<(), Error> {
        match self.handle.take() {
            Some(handle) => handle.shutdown(),
            None => Ok(()),
        }
    }
}

/// The compliance exhibit for the engine's configuration, mirroring `schema_dump`
/// exactly: one artifact an auditor reads instead of taking our word for it.
///
/// **Running state when there is any.** If a configuration pair is already on disk those
/// bytes are printed, because they are what the server actually read — the same property
/// that makes the schema exhibit worth having. Only when no server has ever been started
/// here does it render what a start WOULD write, and it says which of the two it is.
///
/// The credential hash shown is genuine but not stable: a fresh password is minted at
/// every start, so what an auditor verifies here is shape — which user exists, that no
/// `default` user is defined, that access is loopback-restricted, that the protocols we
/// do not want are absent, that the system logs carry TTLs, and that no plaintext secret
/// appears anywhere in either file.
///
/// # Errors
/// If a configuration file exists but cannot be read, if a credential cannot be minted
/// for the would-write rendering, or if the assembled exhibit fails its own
/// no-plaintext check.
pub fn engine_config_exhibit(config: &ServerConfig) -> Result<String, Error> {
    let cfg_path = config.config_path();
    let users_path = config.users_path();
    let on_disk = cfg_path.exists() && users_path.exists();

    let (config_xml, users_xml, minted) = if on_disk {
        (
            std::fs::read_to_string(&cfg_path)
                .map_err(|e| Error::Io(format!("reading {}: {e}", cfg_path.display())))?,
            std::fs::read_to_string(&users_path)
                .map_err(|e| Error::Io(format!("reading {}: {e}", users_path.display())))?,
            None,
        )
    } else {
        let credential = Credential::generate(&config.user)?;
        let users = config.render_users_xml(&credential);
        (config.render_config_xml(), users, Some(credential))
    };

    let provenance = if on_disk {
        "RUNNING STATE: these are the bytes on disk, the ones the server read at its \
         last start."
    } else {
        "NOT YET STARTED: no configuration exists in this data directory, so this is \
         what a start would write. The password hash below belongs to a credential \
         minted for this exhibit and discarded."
    };

    let exhibit = format!(
        "# infertap-analyzer engine configuration exhibit\n\
         # {provenance}\n\
         # A fresh credential is minted at every start and only its SHA-256 ever reaches\n\
         # disk, so the hash below changes on every start by construction. What is stable\n\
         # — and what this exhibit is for — is the shape: one loopback listener, no\n\
         # `default` user, no MySQL/PostgreSQL/interserver protocol, a TTL on every system\n\
         # log, and no plaintext secret in either file.\n\
         \n\
         ## {}\n\
         {config_xml}\n\
         ## {}\n\
         {users_xml}",
        cfg_path.display(),
        users_path.display(),
    );

    // Defense in depth rather than trust in the renderer: the one thing this artifact
    // must never do is print the secret it exists to prove is absent.
    if let Some(credential) = minted {
        if exhibit.contains(credential.password()) {
            return Err(Error::Refused(
                "refusing to emit the engine-config exhibit: it contains a plaintext \
                 credential, which is a defect in the renderer, not in this check"
                    .to_string(),
            ));
        }
    }
    Ok(exhibit)
}

/// Who, if anyone, owns a data directory.
///
/// The distinction is the point: a pid file left behind by a dead process is a different
/// situation from a running server, and an operator who is told which one they have can
/// act. `ClickHouse`'s own directory lock is the backstop underneath this; this is the
/// legible layer above it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Owner {
    /// No pid file: the directory is free.
    Free,
    /// A pid file naming a process that is alive.
    Live(u32),
    /// A pid file naming a process that no longer exists — safe to take over.
    Stale(u32),
}

/// Is this process alive? `kill -0` through a spawn, because `libc` is forbidden
/// workspace-wide and `/proc` is not portable to the development platform.
fn pid_alive(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// The engine version this build is pinned to, embedded from the repo's pin file — the
/// same discipline as the embedded migrations: the binary carries what it needs to refuse
/// a mismatch, with no argument a caller could get wrong or omit.
pub const PINNED_ENGINE_VERSION: &str = include_str!("../../../../CLICKHOUSE_VERSION");

/// Resolve the engine path so that VERIFYING it and SPAWNING it name the same file.
///
/// **A relative `--clickhouse` would otherwise name two files.** `verify_pin` runs the binary
/// from the process's own working directory, where `./.tools/clickhouse` exists and answers
/// with the right version. `spawn_with` then sets `current_dir` to the DATA DIRECTORY —
/// deliberately, so the engine's `statvfs` reads a directory it can always read — and a relative
/// program path resolves against that new directory instead. So the pin check passed and the
/// spawn failed with `No such file or directory`, naming a path that plainly existed.
///
/// A bare name like `clickhouse` is left alone: it is resolved through `PATH`, which
/// `current_dir` does not affect. Only a path WITH a separator is ambiguous, and only that is
/// rewritten.
fn absolute_engine_bin(bin: PathBuf) -> Result<PathBuf, Error> {
    if bin.is_absolute() || bin.components().count() == 1 {
        return Ok(bin);
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(&bin))
        .map_err(|e| {
            Error::Io(format!(
                "resolving the engine path {} against the working directory: {e}",
                bin.display()
            ))
        })
}

/// Refuse a binary that is not the pinned build, at the server path's front door.
///
/// The check belongs where the process is started, which is here: a daemon must never supervise
/// whatever `clickhouse` binary it found.
///
/// # Errors
/// If the binary cannot run, or reports a version other than the pin.
fn verify_pin(bin: &Path) -> Result<(), Error> {
    let out = Command::new(bin)
        .args(["local", "--version"])
        .stdin(Stdio::null())
        .output()
        .map_err(|e| Error::Io(format!("running {}: {e}", bin.display())))?;
    let reported = String::from_utf8_lossy(&out.stdout);
    let expected = PINNED_ENGINE_VERSION.trim();
    if reported.contains(expected) {
        return Ok(());
    }
    Err(Error::Refused(format!(
        "engine reports {:?}, pin is {expected:?} — refusing to supervise the wrong build",
        reported.trim()
    )))
}

/// Where the engine's pid file lives. One definition, so the writer (`--pid-file`), the
/// ownership probe and the stale-takeover cleanup can never drift apart.
fn pid_file(data_dir: &Path) -> PathBuf {
    data_dir.join("clickhouse.pid")
}

/// Determine who owns `data_dir`.
#[must_use]
pub fn owner_of(data_dir: &Path) -> Owner {
    let Ok(text) = std::fs::read_to_string(pid_file(data_dir)) else {
        return Owner::Free;
    };
    let Ok(pid) = text.trim().parse::<u32>() else {
        return Owner::Free;
    };
    if pid_alive(pid) {
        Owner::Live(pid)
    } else {
        Owner::Stale(pid)
    }
}

/// Refuse to touch a directory a live server owns, and say which case was found.
///
/// # Errors
/// When a live server owns the directory. A stale pid file is NOT an error — it is
/// reported through the returned owner so the caller can log the takeover.
pub fn require_exclusive(data_dir: &Path) -> Result<Owner, Error> {
    match owner_of(data_dir) {
        // A stale file is residue, not an owner: `Stale` is returned so the caller can
        // clear it and say it took over, which is the difference between a legible
        // recovery and a mysterious refusal after a crash.
        Owner::Live(pid) => Err(Error::Refused(format!(
            "a live ClickHouse server (pid {pid}) already owns {} — exactly one process \
             may own a data directory, so this is a refusal rather than a race. If that \
             is the analyzer daemon, ask IT for this: the daemon runs the report and the \
             health sample inside the process holding the engine credential, which is why \
             a second process cannot borrow its engine. Otherwise stop that server, or \
             point this analyzer at its own directory.",
            data_dir.display()
        ))),
        other => Ok(other),
    }
}

/// Where the DAEMON's own pid file lives — deliberately distinct from the engine's.
///
/// Two files, because the two questions are different: "is another daemon running here"
/// and "is an engine running here" have different answers after a crash, and it is exactly
/// that difference that tells an orphan from a rival.
fn daemon_pid_file(data_dir: &Path) -> PathBuf {
    data_dir.join("analyzer-daemon.pid")
}

/// How long an orphaned engine gets to exit on SIGTERM before it is killed outright.
pub const ORPHAN_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// The daemon's claim on a data directory, and what it found there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonClaim {
    /// Nothing was running: an ordinary start.
    Fresh,
    /// A previous daemon died and left its engine running. The engine was stopped and the
    /// directory taken over, with the pid stated so the takeover appears in the log rather
    /// than happening silently.
    AdoptedOrphan(u32),
}

/// Take exclusive ownership of `data_dir` as a daemon, recovering from a previous
/// daemon's death without an operator.
///
/// **This closes a real gap in crash-anywhere recovery, and the mechanism matters.** The
/// engine runs in its own process group precisely so stopping it cannot kill the daemon —
/// which means the reverse is also true: killing the daemon leaves the engine running. Its
/// credential is ephemeral and died with the process that minted it, so the new daemon
/// cannot authenticate to it, cannot `SYSTEM SHUTDOWN` it, and would meet
/// [`Owner::Live`] and refuse. That is an operator turning a knob to restore health, which
/// an unattended appliance cannot ask for.
///
/// The distinction that makes taking over safe rather than reckless:
///
/// | daemon pid file | engine pid file | verdict |
/// |---|---|---|
/// | live | any | REFUSE — a rival daemon is running |
/// | stale | live | ORPHAN — a previous *us* died here; stop it and take over |
/// | absent | live | REFUSE — no evidence the engine is ours (today's behaviour) |
/// | stale or absent | dead or absent | fresh start |
///
/// The absent-daemon-file row is the one worth keeping strict: a customer's own
/// `ClickHouse` pointed at this directory would present exactly that way, and killing it
/// would be our bug destroying their process.
///
/// # Errors
/// If a live daemon owns the directory, if an orphaned engine cannot be stopped, or if the
/// pid file cannot be written.
pub fn claim_for_daemon(data_dir: &Path) -> Result<DaemonGuard, Error> {
    std::fs::create_dir_all(data_dir)
        .map_err(|e| Error::Io(format!("creating {}: {e}", data_dir.display())))?;
    let path = daemon_pid_file(data_dir);
    let existing = std::fs::read_to_string(&path)
        .ok()
        .and_then(|t| t.trim().parse::<u32>().ok());

    if let Some(pid) = existing {
        if pid_alive(pid) {
            return Err(Error::Refused(format!(
                "another infertap-analyzer daemon (pid {pid}) already owns {} — exactly \
                 one daemon may own a data directory. Stop it, or point this one at its \
                 own directory.",
                data_dir.display()
            )));
        }
    }

    // A stale daemon file plus a live engine is our own orphan: the daemon died, the engine
    // survived in its own process group, and its credential died with the daemon.
    let claim = match (existing, owner_of(data_dir)) {
        (Some(_), Owner::Live(engine_pid)) => {
            stop_orphaned_engine(engine_pid)?;
            if let Owner::Stale(pid) = owner_of(data_dir) {
                clear_stale_pid_file(data_dir, pid)?;
            }
            DaemonClaim::AdoptedOrphan(engine_pid)
        }
        _ => DaemonClaim::Fresh,
    };

    write_private(&path, &format!("{}\n", std::process::id()))?;
    Ok(DaemonGuard { path, claim })
}

/// Stop an engine we did not start and cannot authenticate to.
///
/// `SYSTEM SHUTDOWN` is unavailable — that path needs the credential, which died with the
/// daemon that minted it. So this is a signal, escalating only if the graceful one is
/// ignored, and it goes through `kill` rather than `libc` like every other signal in this
/// module.
///
/// **Two measured facts this depends on, neither of them obvious.** First, the pid it is
/// given comes from the pid FILE, and that is not our direct child: `ClickHouse` runs a
/// watchdog parent whose child is the actual server, and the pid file names the server.
/// An earlier draft asserted the two were equal and measured them 23 apart. Second,
/// SIGTERM to that server brings the WATCHDOG down with it rather than provoking a
/// restart — verified in
/// `a_dead_daemons_orphaned_engine_is_adopted_rather_than_left_for_an_operator`, which
/// checks the watchdog handle is dead afterwards. If a future engine version changed that,
/// this would leave a watchdog running and the pin would catch it.
fn stop_orphaned_engine(pid: u32) -> Result<(), Error> {
    let signal = |arg: Option<&str>| {
        let mut command = Command::new("kill");
        if let Some(a) = arg {
            command.arg(a);
        }
        command
            .arg(pid.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
    };
    let _ = signal(None); // SIGTERM: ClickHouse drains on it
    let deadline = Instant::now() + ORPHAN_STOP_TIMEOUT;
    while Instant::now() < deadline {
        if !pid_alive(pid) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = signal(Some("-9"));
    // A short grace for the kernel to reap; if it is still alive after this, the start
    // will meet a live owner and refuse with the existing decided error, which is the
    // right outcome rather than a silent overwrite.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !pid_alive(pid) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(Error::Server(format!(
        "an orphaned engine (pid {pid}) from a previous daemon did not stop, even on \
         SIGKILL — refusing to start a second server against the same directory"
    )))
}

/// Holds the daemon's pid file for the life of the daemon, and says what the claim found.
#[derive(Debug)]
pub struct DaemonGuard {
    path: PathBuf,
    claim: DaemonClaim,
}

impl DaemonGuard {
    /// What the claim found in the directory: nothing, or an orphaned engine it stopped.
    #[must_use]
    pub fn claim(&self) -> &DaemonClaim {
        &self.claim
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        // Best effort. A file left by a hard kill is exactly the residue `claim_for_daemon`
        // is built to recognise, so failing to remove it degrades to the recovery path
        // rather than to a wedge.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A running server owned by this process.
#[derive(Debug)]
pub struct ServerHandle {
    bin: PathBuf,
    config: ServerConfig,
    credential: Credential,
    child: Child,
    started: Instant,
}

impl ServerHandle {
    /// Write the configuration, start the server, and wait until it answers.
    ///
    /// # Errors
    /// A live owner of the data directory, a filesystem failure, a spawn failure, or a
    /// server that never becomes ready inside [`READINESS_TIMEOUT`] — the last one
    /// including the decided error for a port already in use.
    pub fn start(bin: impl Into<PathBuf>, config: &ServerConfig) -> Result<Self, Error> {
        let bin = absolute_engine_bin(bin.into())?;
        verify_pin(&bin)?;
        if let Owner::Stale(pid) = require_exclusive(&config.data_dir)? {
            clear_stale_pid_file(&config.data_dir, pid)?;
        }
        let credential = Credential::generate(&config.user)?;
        Self::spawn_with(bin, config.clone(), credential)
    }

    fn spawn_with(
        bin: PathBuf,
        config: ServerConfig,
        credential: Credential,
    ) -> Result<Self, Error> {
        write_config_pair(&config, &credential)?;

        let mut command = Command::new(&bin);
        command
            .arg("server")
            .arg(format!("--config-file={}", config.config_path().display()))
            .arg(format!("--pid-file={}", config.pid_path().display()))
            // The engine's cwd is its OWN data directory, never an ambient inheritance:
            // its async metrics statvfs the working directory, and a parent invoked from
            // a directory the service user cannot read (sudo from an operator's home —
            // the first thing the real Linux boot found) kills the server at startup
            // with an empty-path CANNOT_STATVFS. The one directory the engine may
            // always assume readable is the one it owns.
            .current_dir(&config.data_dir)
            // The engine is systemd-aware and would send its own READY to the unit's
            // notify socket; NotifyAccess=main rightly refuses it, but at a journal
            // line per engine start (the first boot logged three). The daemon is the
            // unit's only notifier; its children have no socket to find.
            .env_remove("NOTIFY_SOCKET")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // **The engine gets a process group of its own, and this is load-bearing.**
        // `SYSTEM SHUTDOWN` — the documented, signal-free way to stop a server — is
        // implemented inside the engine as a signal to its PROCESS GROUP, not to itself.
        // Measured, not inferred: with the server sharing our group, stopping it killed
        // this process too (the parent died on the shutdown call, exit 128+16, and so did
        // an enclosing shell). A daemon that cannot stop or restart its own engine
        // without dying is not a daemon, and under an init system the blast radius is
        // every sibling in the unit's group. `process_group` is `std`'s own, so isolating
        // the child costs no `libc`.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.process_group(0);
        }
        let child = command
            .spawn()
            .map_err(|e| Error::Server(format!("starting {}: {e}", bin.display())))?;

        let handle = Self {
            bin,
            config,
            credential,
            child,
            started: Instant::now(),
        };
        handle.await_ready()?;
        Ok(handle)
    }

    /// Poll until the server answers a trivial query, or fail with a decided error.
    fn await_ready(&self) -> Result<(), Error> {
        let deadline = Instant::now() + READINESS_TIMEOUT;
        let mut last = String::new();
        while Instant::now() < deadline {
            match self.client_bounded("SELECT 1") {
                Ok(_) => return Ok(()),
                Err(e) => last = e.to_string(),
            }
            if let Some(reason) = self.port_conflict_in_log() {
                return Err(Error::Server(reason));
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Err(Error::Server(format!(
            "the engine did not become ready within {}s; last probe said: {last}",
            READINESS_TIMEOUT.as_secs()
        )))
    }

    /// Translate the engine's bind failure into a decided error naming the port, rather
    /// than leaving an operator to find it in a log file.
    fn port_conflict_in_log(&self) -> Option<String> {
        let log = self.config.data_dir.join("logs/server.err.log");
        let text = std::fs::read_to_string(log).ok()?;
        if text.contains("Address already in use") || text.contains("Cannot assign requested") {
            return Some(format!(
                "the engine could not bind 127.0.0.1 ports {} (HTTP) / {} (native) — \
                 something else is already listening. These default to analyzer-specific \
                 ports precisely to avoid colliding with an existing ClickHouse install; \
                 set them explicitly if this deployment needs different ones.",
                self.config.http_port, self.config.tcp_port
            ));
        }
        None
    }

    /// Run a query through the engine's own client. The credential's plaintext travels
    /// on this process's argv only — never to disk.
    ///
    /// Deliberately unbounded: an analysis query may legitimately run for a long time,
    /// and a timeout here would be a data-loss knob wearing a safety costume. The
    /// LIFECYCLE paths bound themselves instead — see `ServerHandle::client_bounded` (private).
    ///
    /// # Errors
    /// The client's stderr on a non-zero exit, or the spawn error.
    pub fn client_query(&self, sql: &str) -> Result<String, Error> {
        self.client(sql, None)
    }

    /// A lifecycle query that cannot wait forever.
    ///
    /// Readiness probing and the ordered stop both run on paths that must terminate: the
    /// stop path is reachable from `Drop`, and a `Drop` that can block indefinitely is
    /// its own hang class — the daemon would wedge while trying to be tidy. A wedged
    /// server that accepts a connection and never answers is exactly the case a bare
    /// `output()` would wait out forever, so these calls carry the engine's own
    /// connect/receive/send timeouts.
    ///
    /// # Errors
    /// As [`ServerHandle::client_query`], including the timeout as a client error.
    fn client_bounded(&self, sql: &str) -> Result<String, Error> {
        self.client(sql, Some(CLIENT_LIFECYCLE_TIMEOUT_SECS))
    }

    fn client(&self, sql: &str, bound_secs: Option<u64>) -> Result<String, Error> {
        let mut command = Command::new(&self.bin);
        command
            .arg("client")
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(self.config.tcp_port.to_string())
            .arg("--user")
            .arg(&self.credential.user)
            .arg("--password")
            .arg(self.credential.password());
        if let Some(secs) = bound_secs {
            command
                .arg("--connect_timeout")
                .arg(secs.to_string())
                .arg("--receive_timeout")
                .arg(secs.to_string())
                .arg("--send_timeout")
                .arg(secs.to_string());
        }
        let out = command
            .arg("--query")
            .arg(sql)
            .stdin(Stdio::null())
            .output()
            .map_err(|e| Error::Server(format!("running the client: {e}")))?;
        if !out.status.success() {
            return Err(Error::Statement(
                String::from_utf8_lossy(&out.stderr).trim().to_string(),
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// Ordered stop, shared by [`ServerHandle::shutdown`] and [`ServerHandle::restart`]:
    /// `SYSTEM SHUTDOWN` (the engine's documented equivalent of
    /// `service clickhouse-server stop`), waited out, escalating to a kill only if the
    /// engine does not exit inside [`SHUTDOWN_TIMEOUT`]. A signal would need `libc`,
    /// which is forbidden workspace-wide.
    ///
    /// **Restart goes through here for the same reason a stop does.** A supervised
    /// restart that killed instead of draining would make crash recovery the steady
    /// state — unflushed inserts and mutation debt on every bounce — which is precisely
    /// the class of unattended-operation damage this daemon exists to prevent.
    fn stop_and_wait(&mut self) -> Result<(), Error> {
        let _ = self.client_bounded("SYSTEM SHUTDOWN");
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(e) => return Err(Error::Server(format!("waiting for the engine: {e}"))),
            }
        }
        let _ = self.child.kill();
        self.child
            .wait()
            .map(|_| ())
            .map_err(|e| Error::Server(format!("reaping the engine after kill: {e}")))
    }

    /// Stop the server and reap it.
    ///
    /// # Errors
    /// If the child cannot be reaped.
    pub fn shutdown(mut self) -> Result<(), Error> {
        self.stop_and_wait()
    }

    /// Whether the child is still running.
    pub fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Restart the server: ordered stop, then a fresh start that **mints a new
    /// credential and regenerates both configuration files**.
    ///
    /// The ephemeral property has to survive supervision, not merely first start: a
    /// restart that reused the old password would quietly turn a per-process secret into
    /// a persistent one, and reusing the files on disk is how that would happen without
    /// anyone deciding it.
    ///
    /// **The daemon does not call this**, and that is a decided policy rather than an
    /// oversight — see the `RestartPolicy` deletion note above. An engine death exits the
    /// process so the init system runs the full claim-and-recover path. This stays as
    /// lifecycle surface on the handle, and it is where the credential-rotation property is
    /// proven in its sharp form: `a_restart_rotates_the_credential_and_the_old_one_stops_working`
    /// checks that the running server REFUSES the previous password, which a start-only test
    /// cannot show.
    ///
    /// # Errors
    /// As [`ServerHandle::start`], plus a failure to stop the outgoing server.
    pub fn restart(mut self) -> Result<Self, Error> {
        self.stop_and_wait()?;
        // Our own outgoing server's pid file, if the kill path ran: the same residue a
        // crash leaves, cleared the same way.
        if let Owner::Stale(pid) = owner_of(&self.config.data_dir) {
            clear_stale_pid_file(&self.config.data_dir, pid)?;
        }
        let credential = Credential::generate(&self.config.user)?;
        Self::spawn_with(self.bin.clone(), self.config.clone(), credential)
    }

    /// How long this server has been running, measured from its spawn.
    #[must_use]
    pub fn uptime(&self) -> Duration {
        self.started.elapsed()
    }

    /// The credential this server is running with.
    ///
    /// The plaintext lives here and nowhere else: the client needs it to open its
    /// connection, and it is deliberately reachable only from the process that owns the
    /// server. It is never written, never logged, and dies with this process.
    #[must_use]
    pub fn credential(&self) -> &Credential {
        &self.credential
    }

    /// The engine process's pid, as this process knows it — the value the server's own
    /// pid file should agree with.
    #[must_use]
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The configuration this server was started with, and a restart regenerates from.
    #[must_use]
    pub fn config(&self) -> &ServerConfig {
        &self.config
    }
}

impl Drop for ServerHandle {
    /// A handle that goes out of scope without an explicit shutdown must not leave an
    /// engine behind.
    ///
    /// Best effort and deliberately quiet, but not optional: an orphaned server keeps
    /// owning the data directory, so the next start meets a live owner and refuses —
    /// our untidiness would become an operator's incident. Skipped when the child has
    /// already exited, so the ordinary paths ([`ServerHandle::shutdown`],
    /// [`ServerHandle::restart`]) do not stop a server twice.
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.stop_and_wait();
        }
    }
}

/// Render and write the configuration pair, replacing whatever is on disk.
///
/// Every start and every restart passes through here, so the files are **regenerated,
/// never reused** — the mechanical form of "the configuration is generated from scratch"
/// and of the credential's per-start freshness.
///
/// # Errors
/// If the configuration directory or either file cannot be written.
fn write_config_pair(config: &ServerConfig, credential: &Credential) -> Result<(), Error> {
    let cfg_dir = config.config_dir();
    std::fs::create_dir_all(&cfg_dir)
        .map_err(|e| Error::Io(format!("creating {}: {e}", cfg_dir.display())))?;
    std::fs::create_dir_all(config.data_dir.join("logs"))
        .map_err(|e| Error::Io(format!("creating the log directory: {e}")))?;
    write_private(&config.config_path(), &config.render_config_xml())?;
    write_private(&config.users_path(), &config.render_users_xml(credential))
}

/// Remove a pid file whose process is gone, so a takeover after a crash meets a clean
/// directory instead of its own residue.
///
/// Only reachable once [`owner_of`] has said [`Owner::Stale`]: a live owner is a refusal,
/// never a cleanup. A file that vanished in between is the outcome we wanted anyway.
fn clear_stale_pid_file(data_dir: &Path, pid: u32) -> Result<(), Error> {
    let path = pid_file(data_dir);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Io(format!(
            "clearing the stale pid file {} (pid {pid}, no longer running): {e}",
            path.display()
        ))),
    }
}

/// Write a file only this user can read: the users file carries a password hash, and the
/// config names paths a reviewer would rather not see world-readable. The mode is set on
/// creation, so there is no window in which the bytes exist under the ambient umask.
fn write_private(path: &Path, contents: &str) -> Result<(), Error> {
    use std::io::Write as _;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|e| Error::Io(format!("creating {}: {e}", path.display())))?;
    #[cfg(unix)]
    {
        // A file that already existed keeps its old mode through `open`; assert ours.
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| Error::Io(format!("securing {}: {e}", path.display())))?;
    }
    file.write_all(contents.as_bytes())
        .map_err(|e| Error::Io(format!("writing {}: {e}", path.display())))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        absolute_engine_bin, clear_stale_pid_file, owner_of, pid_file, require_exclusive,
        write_config_pair, Credential, Owner, ServerConfig,
    };
    use std::path::PathBuf;

    fn config() -> ServerConfig {
        ServerConfig::new("/tmp/analyzer-test")
    }

    /// A pid beyond any platform's `pid_max` (macOS 99 998, Linux's default 4 194 304),
    /// so it names no living process on any machine this suite runs on — a dead pid
    /// without the reuse window a spawn-and-reap would leave open.
    const UNUSED_PID: u32 = 2_147_483_647;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "analyzer-serving-server-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    #[test]
    fn undeclared_protocols_do_not_exist() {
        // The hardening claim that matters most, and it holds by ABSENCE: the pinned
        // tarball ships no default config, so a protocol this file never names is never
        // started. A reviewer reads the exhibit; this test reads it for them.
        let xml = config().render_config_xml();
        for forbidden in [
            "mysql_port",
            "postgresql_port",
            "interserver_http_port",
            "interserver_https_port",
        ] {
            assert!(
                !xml.contains(forbidden),
                "{forbidden} must never be configured: {xml}"
            );
        }
        assert!(xml.contains("<listen_host>127.0.0.1</listen_host>"));
        assert_eq!(
            xml.matches("<listen_host>").count(),
            1,
            "exactly one listen_host, and it is loopback"
        );
        assert!(
            xml.contains("<user_files_path>"),
            "file() must be confined — the reader depends on it"
        );
    }

    #[test]
    fn every_system_log_carries_a_ttl() {
        // The server's own bookkeeping is a disk surface nobody watches. Its verification
        // pinned binary lives in the integration suite; this holds the config side.
        let xml = config().render_config_xml();
        for log in [
            "query_log",
            "part_log",
            "trace_log",
            "metric_log",
            "asynchronous_metric_log",
        ] {
            assert!(
                xml.contains(&format!("<table>{log}</table>")),
                "{log} missing"
            );
        }
        assert_eq!(
            xml.matches("<ttl>event_date + INTERVAL").count(),
            5,
            "every declared system log carries a horizon: {xml}"
        );
    }

    #[test]
    fn the_server_runs_in_utc() {
        assert!(config()
            .render_config_xml()
            .contains("<timezone>UTC</timezone>"));
    }

    #[test]
    fn the_memory_ceiling_is_declared_when_set() {
        let mut cfg = config();
        assert!(!cfg.render_config_xml().contains("max_server_memory_usage"));
        cfg.max_server_memory_bytes = Some(1234);
        assert!(cfg
            .render_config_xml()
            .contains("<max_server_memory_usage>1234</max_server_memory_usage>"));
    }

    #[test]
    fn the_plaintext_never_reaches_the_rendered_users_file() {
        // The ephemeral-credential property, held where it is easiest to break.
        let cred = Credential::generate("infertap").expect("credential");
        let xml = config().render_users_xml(&cred);
        assert!(
            !xml.contains(cred.password()),
            "the password must never be rendered"
        );
        assert!(xml.contains(cred.sha256_hex()), "the hash must be");
        assert!(
            !xml.contains("<default>"),
            "the default user must not exist at all: {xml}"
        );
        assert_eq!(cred.password().len(), 64, "32 random bytes, hex");
        let printed = format!("{cred:?}");
        assert!(
            !printed.contains(cred.password()),
            "a logged handle must not carry the plaintext: {printed}"
        );
        assert!(printed.contains(cred.sha256_hex()), "the digest may be");
    }

    #[test]
    fn each_generated_credential_is_distinct() {
        let a = Credential::generate("infertap").expect("a");
        let b = Credential::generate("infertap").expect("b");
        assert_ne!(a.password(), b.password());
        assert_ne!(a.sha256_hex(), b.sha256_hex());
    }

    #[test]
    fn a_live_owner_refuses_and_a_stale_pid_file_is_a_takeover() {
        // The distinction the exclusivity rule turns on: a running server and a pid file
        // left by a dead one are different situations, and an operator who is told which
        // one they have can act. Without it, every crash looks like a live owner and the
        // daemon refuses to start until someone deletes a file by guesswork.
        let dir = scratch("owner");
        assert_eq!(
            owner_of(&dir),
            Owner::Free,
            "no pid file: the directory is free"
        );

        // Live: this test process is unarguably alive.
        let live = std::process::id();
        std::fs::write(pid_file(&dir), format!("{live}\n")).expect("write pid file");
        assert_eq!(owner_of(&dir), Owner::Live(live));
        let refusal = require_exclusive(&dir).expect_err("a live owner is a refusal");
        assert!(
            matches!(&refusal, crate::engine::Error::Refused(m) if m.contains(&live.to_string()) && m.contains("already owns")),
            "the decided error must name the case it found: {refusal}"
        );

        // Stale: the same file, naming a process that is gone.
        std::fs::write(pid_file(&dir), format!("{UNUSED_PID}\n")).expect("write pid file");
        assert_eq!(owner_of(&dir), Owner::Stale(UNUSED_PID));
        assert_eq!(
            require_exclusive(&dir).expect("a stale file is not a refusal"),
            Owner::Stale(UNUSED_PID),
            "the caller is told it is taking over, not stopped"
        );
        clear_stale_pid_file(&dir, UNUSED_PID).expect("clear");
        assert_eq!(
            owner_of(&dir),
            Owner::Free,
            "the takeover leaves no residue"
        );
        clear_stale_pid_file(&dir, UNUSED_PID).expect("clearing an absent file is the outcome");

        // Garbage in the file is residue too, never an owner.
        std::fs::write(pid_file(&dir), "not-a-pid\n").expect("write pid file");
        assert_eq!(owner_of(&dir), Owner::Free);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn every_start_regenerates_the_configuration_pair_with_a_fresh_credential() {
        // The ephemeral-credential property has to survive SUPERVISION, not just first
        // start: `restart()` mints a new credential and comes back through
        // `write_config_pair`, so reusing the files on disk — the way a per-process
        // secret quietly becomes a persistent one — is not a path that exists. This pins
        // the write half without a server; the live half is the server-lifecycle pin.
        let dir = scratch("configpair");
        let mut cfg = ServerConfig::new(&dir);
        cfg.user = "infertap".to_string();

        let first = Credential::generate(&cfg.user).expect("first");
        write_config_pair(&cfg, &first).expect("write first");
        let users_path = cfg.config_dir().join("users.xml");
        let before = std::fs::read_to_string(&users_path).expect("read users.xml");
        assert!(before.contains(first.sha256_hex()));

        let second = Credential::generate(&cfg.user).expect("second");
        assert_ne!(first.password(), second.password());
        write_config_pair(&cfg, &second).expect("write second");
        let after = std::fs::read_to_string(&users_path).expect("read users.xml");

        assert!(
            after.contains(second.sha256_hex()),
            "the restart's credential is what the server will read"
        );
        assert!(
            !after.contains(first.sha256_hex()),
            "the outgoing credential is gone, not appended alongside"
        );
        for secret in [first.password(), second.password()] {
            assert!(!after.contains(secret), "no plaintext ever reaches disk");
        }
        assert!(
            std::fs::read_to_string(cfg.config_dir().join("config.xml"))
                .expect("read config.xml")
                .contains("<listen_host>127.0.0.1</listen_host>"),
            "the server config is regenerated on the same path"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_exhibit_shows_what_is_on_disk_and_never_a_plaintext_secret() {
        let dir = scratch("exhibit");
        let cfg = ServerConfig::new(&dir);

        // Nothing started here yet: the exhibit says so rather than implying it is
        // showing running state.
        let before = super::engine_config_exhibit(&cfg).expect("exhibit");
        assert!(before.contains("NOT YET STARTED"), "{before}");
        assert!(before.contains("<listen_host>127.0.0.1</listen_host>"));
        assert!(before.contains("<password_sha256_hex>"));
        assert!(!before.contains("<default>"), "no default user is defined");

        // Once a pair exists, the exhibit is those bytes — the schema-dump property.
        let credential = Credential::generate(&cfg.user).expect("credential");
        write_config_pair(&cfg, &credential).expect("write");
        let after = super::engine_config_exhibit(&cfg).expect("exhibit");
        assert!(after.contains("RUNNING STATE"), "{after}");
        assert!(after.contains(credential.sha256_hex()), "the live hash");
        assert!(
            !after.contains(credential.password()),
            "the plaintext is not in the artifact an auditor is handed"
        );
        for absent in ["mysql_port", "postgresql_port", "interserver_http_port"] {
            assert!(!after.contains(absent), "{absent} must not appear");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **Verifying the engine and spawning it must name the same file.** `spawn_with` sets
    /// `current_dir` to the data directory and a relative program path follows it, so a relative
    /// `--clickhouse` must be resolved before the working directory moves, or the path that
    /// passed `verify_pin` is the one reported missing.
    #[test]
    fn a_relative_engine_path_is_resolved_before_the_working_directory_moves() {
        let cwd = std::env::current_dir().expect("a working directory");

        let relative = absolute_engine_bin(PathBuf::from("./.tools/clickhouse"))
            .expect("a relative path resolves");
        assert!(
            relative.is_absolute(),
            "a relative path must be absolutised before `current_dir` moves: {}",
            relative.display()
        );
        assert_eq!(relative, cwd.join("./.tools/clickhouse"));

        // A bare name is PATH-resolved, and PATH is unaffected by `current_dir`. Rewriting it
        // would turn a working PATH lookup into a missing file in the working directory.
        assert_eq!(
            absolute_engine_bin(PathBuf::from("clickhouse")).expect("a bare name is left alone"),
            PathBuf::from("clickhouse")
        );

        let already = PathBuf::from("/usr/local/lib/infertap-analyzer/clickhouse");
        assert_eq!(
            absolute_engine_bin(already.clone()).expect("an absolute path is unchanged"),
            already
        );
    }
}
