// SPDX-License-Identifier: AGPL-3.0-only

//! Storage: the engine boundary. The one crate allowed to spawn a process (the static
//! audit enforces it): [`server`] starts and supervises the pinned `ClickHouse`, this root
//! holds the connection, the migration ledger, the retention knobs and the quoting helpers.
//! Everything above it speaks SQL results, never the engine.
//!
//! Boundary rule: **no raw string reaches SQL unquoted** — [`quote_ident`] and
//! [`quote_literal`] are the only paths for identifiers and literals into SQL text,
//! enforced here at the boundary. Safe-looking interpolation of trusted-today strings is
//! the vulnerability pattern; the helpers make the safe path the only path.
//!
//! # Where to start
//!
//! - [`Engine`] is one connection and one current-thread runtime — a single-threaded resource.
//!   Two threads calling into one `Engine` deadlock; the daemon holds two for that reason.
//! - [`server`] starts and supervises the pinned engine; the migrations under `migrations/`
//!   are the schema, applied in order; [`RetentionConfig`] carries the grain ladder's knobs.
//! - [`quote_ident`] and [`quote_literal`] are the only two paths a string may take into SQL,
//!   and the static audit counts every site that builds one.

mod error;
pub mod server;

pub use error::Error;
mod statements;
#[cfg(feature = "testing")]
pub mod testing;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Default ledger retention: how late a segment may arrive and still have its day re-derived
/// with full context. Fourteen days, derived: a segment later than that is an incident being
/// replayed, not a lag — a week for the incident and a week to notice it — and every relation
/// on this knob (the served input facts and the identity-day relation) is sized by it. A harness run with a stalled shipper is what can
/// move it.
pub const DEFAULT_LEDGER_RETENTION_DAYS: u32 = 14;

/// The floor `ledger_retention_days` may never be silently lowered below: the correction horizon,
/// the examined history and one day of margin, because a day reopened by a late record needs its
/// whole backward context still behind it whichever table serves the read.
pub const LEDGER_RETENTION_FLOOR_DAYS: u32 = analyzer_analysis::CORRECTION_HORIZON_DAYS
    + analyzer_analysis::EXAMINED_HISTORY_DAYS
    + analyzer_analysis::CONTEXT_MARGIN_DAYS;

/// **The floors the ladder's stops derive**: a knob below its floor is a
/// window the console offers and the analyzer cannot serve, so it is refused at start rather
/// than rendered as zeros. A `<N>d` stop is N whole UTC days THROUGH today, so a bucket
/// TTL of N days holds the stop's oldest bucket until tomorrow: the hour tier and the instance
/// pages serve `14d`, floor 14; the minute tier serves `48h` with a day of margin, floor 3;
/// the day tier serves `90d`, floor one year. Derived, not measured: what moves one is a new
/// stop on the ladder.
pub const MINUTE_RETENTION_FLOOR_DAYS: u32 = 3;
/// The hour-grain tables keep at least this many days.
pub const HOUR_RETENTION_FLOOR_DAYS: u32 = 14;
/// The rollup tables keep at least this many years.
pub const ROLLUP_RETENTION_FLOOR_YEARS: u32 = 1;
/// The per-instance detail tables keep at least this many days.
pub const INSTANCE_DETAIL_RETENTION_FLOOR_DAYS: u32 = 14;

/// Default rollup retention (tier 1's trend product).
pub const DEFAULT_ROLLUP_RETENTION_YEARS: u32 = 5;

/// Default retention for the INSTANCE-KEYED hourly tables: their own knob
/// rather than a share of the fleet-level hour tier's year. `replication_instance_hourly`
/// reads it. The per-instance distribution sketch that first earned the knob was cut on;
/// the knob and its reasoning stay.
///
/// **Retention ladders by DIMENSIONALITY, not only by grain.** Fleet-level history earns
/// years — the trend, "was this always like this", the quarter-over-quarter question. A
/// single instance's history does not, because the ENTITY is impermanent: `instance_id` is a
/// slot an operator named, slots churn on deploys, and a node drained three months ago is
/// not a diagnostic subject. The per-instance cut is an INCIDENT tool, and incidents are
/// diagnosed in days. Fourteen days is affordable on any box that could hold the event grain
/// feeding it; an operator wanting more sets it, and a cloud cell that owns its own disk can.
pub const DEFAULT_INSTANCE_DETAIL_RETENTION_DAYS: u32 = 14;

/// Default retention for the MINUTE tier:
/// seven days, on every minute table, pegged to BUCKET age.
///
/// Pegged to bucket age, never to `emitted_at`: a re-pass of an old day re-emits its minute
/// cells with a fresh stamp, and an emission-time peg would leave an island of old buckets the
/// reach probe (`min(bucket)`) reads as coverage, so a short window between the island and the
/// live edge would resolve to minute grain and draw zeros. Bucket age closes
/// it: a cell older than the horizon expires whatever stamped it, and the probes below bound
/// themselves by the same horizon so a not-yet-merged straggler cannot fake reach either.
///
/// Seven days because incidents are reviewed days later, and because the cost is not the
/// argument: the minute cells carry no holder, so the tier is flat in fleet size — about
/// 25 MB for a week across all three minute tables at 3,000 holders (measured 16.7 B/row
/// for the rollup shape, 11.2 B/row for the copies shape). `replication_minute` joins this
/// tier only once the copies read has hourly and daily rollups; until then its
/// year of minute cells is what serves the 48 h and 14 d stops.
///
/// **Read by the DAO's reach probes as well as by the migration**, so the two agree by
/// construction. The day a CLI flag threads this into `RetentionConfig`, thread it into the
/// DAO in the same edit, or the probe will state a reach the table no longer has.
pub const DEFAULT_MINUTE_RETENTION_DAYS: u32 = 7;

/// Default retention for the HOUR tier's fleet-level tables:
/// `class_rollup_hourly`, `band_rollup_hourly`, by bucket
/// age. The instance-keyed hourly tables read [`DEFAULT_INSTANCE_DETAIL_RETENTION_DAYS`].
///
/// 365 days is a year of hour-grain history: a choice, not a derivation. No reach probe reads
/// it: a re-derivation reaches back only the input log's horizon, far inside a year by default,
/// so a stale row past the horizon, which the minute tier's probe bounds against, cannot form
/// on the hour tier.
pub const DEFAULT_HOUR_RETENTION_DAYS: u32 = 365;

/// Spill thresholds applied on EVERY engine invocation. Verified
/// at the pin: the engine defaults both to 0 (spill disabled, memory unbounded) - a
/// window-scale sort either fits in RAM or the OS kills the engine. 4 GiB converts
/// that cliff into disk-backed completion. Labeled analytic constants
/// (docs/conformance.md re-derivation list); deliberately NOT a total-memory cap,
/// which would refuse legitimate large queries on large hosts - spill addresses the
/// identified hazard without inventing a ceiling.
pub const EXTERNAL_SORT_SPILL_BYTES: u64 = 4 * 1024 * 1024 * 1024;
/// See [`EXTERNAL_SORT_SPILL_BYTES`].
pub const EXTERNAL_GROUP_BY_SPILL_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// How much of an insert stream is held in memory at once.
///
/// The point of the streaming carrier is that this number, not the row count, is the
/// memory cost of an insert — the lesson applied to the write direction.
const INSERT_CHUNK_BYTES: usize = 256 * 1024;

/// Quote an identifier for SQL (backticks, escaped). The only path for identifiers.
#[must_use]
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// An integer into SQL text: the TYPED sibling of `quote_literal`, for sites whose
/// interpolations are numbers (a day partition, a count bound). `i64`'s `Display` can
/// produce only digits and a sign, so the type is the quoting proof — and the static
/// audit accepts this name beside the quote helpers for exactly that reason: the rule
/// is "every interpolation visibly routes through a helper whose output cannot break
/// out of SQL", and this is that helper for integers.
#[must_use]
pub fn sql_i64(value: i64) -> String {
    value.to_string()
}

/// Quote a string literal for SQL (single quotes, backslash-escaped). The only path for
/// literals.
#[must_use]
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// Retention knobs, validated before any migration applies them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionConfig {
    /// The input horizon: [`DEFAULT_LEDGER_RETENTION_DAYS`] by default, floor
    /// [`LEDGER_RETENTION_FLOOR_DAYS`]. The one knob of its kind: it governs the served input
    /// tables and the input log.
    pub ledger_retention_days: u32,
    /// How many years the rollup tables keep, at or above the floor.
    pub rollup_retention_years: u32,
    /// See [`DEFAULT_INSTANCE_DETAIL_RETENTION_DAYS`]. Zero is legal and means the
    /// per-instance curve is not kept at all — the table still exists, so a read refuses
    /// with an empty span rather than a missing relation.
    pub instance_detail_retention_days: u32,
    /// See [`DEFAULT_MINUTE_RETENTION_DAYS`]: the minute tier's horizon, by bucket age.
    pub minute_retention_days: u32,
    /// See [`DEFAULT_HOUR_RETENTION_DAYS`]: the fleet-level hour tier's horizon, by bucket age.
    pub hour_retention_days: u32,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            ledger_retention_days: DEFAULT_LEDGER_RETENTION_DAYS,
            rollup_retention_years: DEFAULT_ROLLUP_RETENTION_YEARS,
            instance_detail_retention_days: DEFAULT_INSTANCE_DETAIL_RETENTION_DAYS,
            minute_retention_days: DEFAULT_MINUTE_RETENTION_DAYS,
            hour_retention_days: DEFAULT_HOUR_RETENTION_DAYS,
        }
    }
}

impl RetentionConfig {
    /// A retention wide enough that a corpus's fixed wall-clock stamps do not age out
    /// mid-test, and narrow enough that the TTL expression stays representable: a
    /// hundred-year horizon on a current timestamp overflows `DateTime` and silently
    /// deletes every row. The shipped selftest and the fixture suites migrate with it.
    pub const FIXTURE_WIDE: Self = Self {
        ledger_retention_days: 3_650,
        rollup_retention_years: 100,
        instance_detail_retention_days: 3_650,
        minute_retention_days: 3_650,
        hour_retention_days: 3_650,
    };

    /// The floor guard: refuses values below the derived floor, with the derivation in
    /// the error where the operator reads it.
    ///
    /// # Errors
    /// If `ledger_retention_days` is below [`LEDGER_RETENTION_FLOOR_DAYS`].
    pub fn validate(&self) -> Result<(), Error> {
        if self.ledger_retention_days < LEDGER_RETENTION_FLOOR_DAYS {
            return Err(Error::Refused(format!(
                "ledger_retention_days = {} is below the floor {} = examined history + 1d \
                 margin: a late segment can reopen a derived day, and its re-derivation needs \
                 that day's whole backward context still held",
                self.ledger_retention_days, LEDGER_RETENTION_FLOOR_DAYS
            )));
        }
        for (name, value, floor, stop) in [
            (
                "minute_retention_days",
                self.minute_retention_days,
                MINUTE_RETENTION_FLOOR_DAYS,
                "the minute tier serves the 3h and 48h stops: forty-eight whole hours and a day of margin",
            ),
            (
                "hour_retention_days",
                self.hour_retention_days,
                HOUR_RETENTION_FLOOR_DAYS,
                "the hour tier serves the 14d stop: fourteen whole UTC days through today",
            ),
            (
                "rollup_retention_years",
                self.rollup_retention_years,
                ROLLUP_RETENTION_FLOOR_YEARS,
                "the day tier serves the 90d stop",
            ),
            (
                "instance_detail_retention_days",
                self.instance_detail_retention_days,
                INSTANCE_DETAIL_RETENTION_FLOOR_DAYS,
                "the instance pages stop at 14d: fourteen whole UTC days through today",
            ),
        ] {
            if value < floor {
                return Err(Error::Refused(format!(
                    "{name} = {value} is below the floor {floor}: {stop} (the grain ladder's \
                     horizon floor) — a horizon shorter than a stop the console offers is a panel \
                     of zeros"
                )));
            }
        }
        Ok(())
    }
}

/// One schema migration: its version in the ledger, its name, and its SQL with the
/// retention and posture placeholders still in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    /// The version the ledger records once it is applied; applied in ascending order.
    pub version: u32,
    /// The file stem under `migrations/`.
    pub name: &'static str,
    /// The SQL, templated: `{ledger_retention_days}` and the other knobs and
    /// `{engine_family}` are substituted at apply time.
    pub sql: &'static str,
}

/// The embedded migration set: the binary carries its own schema, so there is never a
/// deployment without a migration path. Migration 1 is the consolidated schema; later
/// migrations upgrade that schema without discarding persisted output.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "0001_engine_foundation_input_position",
    sql: include_str!("../../migrations/0001_schema.sql"),
}];

/// The engine family prefix a migration's `ENGINE = {engine_family}…MergeTree` takes:
/// empty on the appliance, `Replicated` in a cell that replicates. A posture, not a measurement — compile-time,
/// substituted where retention is, drift caught by the same placeholder scan. Every migration spells its engines through it; the audit refuses
/// one that does not.
pub const ENGINE_FAMILY: &str = "";

/// The eight cell tables the fold writes whole values into: keyed replace, the latest
/// `version` per key is the cell, `gone = 1` a tombstone. One list, so the writer, the heal
/// and the schema suite name the same eight, and named for what they are: the engine
/// derives none of them.
pub const CELL_TABLES: [&str; 8] = [
    "class_rollup_daily",
    "class_rollup_hourly",
    "recompute_minute",
    "recompute_rank_minute",
    "band_rollup_daily",
    "band_rollup_hourly",
    "band_minute",
    "band_rank_minute",
];

/// Scan for an unsubstituted `{placeholder}` after templating: template drift fails
/// closed with the surviving placeholder named, instead of reaching the engine as a
/// confusing parse error (or worse, parsing somewhere harmless).
fn surviving_placeholder(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && (bytes[j].is_ascii_lowercase() || bytes[j] == b'_') {
                j += 1;
            }
            if j > start && j < bytes.len() && bytes[j] == b'}' {
                return Some(sql[start..j].to_string());
            }
        }
        i += 1;
    }
    None
}

/// The loopback HTTP connection to a server this process owns.
///
/// One current-thread runtime lives here and nowhere else: async stops at this boundary
/// by design, so every crate above `storage` stays synchronous and no analysis code ever
/// has to know a future exists.
///
/// **The runtime is behind a mutex, and that is a correctness fix, not tidiness.** A
/// current-thread runtime has one core, and two threads calling `block_on` on it at once
/// do not queue — the second waits for a core the first is holding while it drives its own
/// future, and the suite hung exactly there (five test threads inside `block_on`, measured
/// with `sample`). Serializing makes concurrent callers queue deterministically
/// instead. The consequence is worth stating rather than discovering: **an `Engine` is ONE
/// connection and ONE runtime, so it is a single-threaded resource.** Concurrency comes
/// from holding more than one — which is why [`Engine::in_database`] builds a fresh runtime
/// rather than sharing this one, and why the loop must not sample health through the same
/// `Engine` a pass is running on if it wants the two to overlap.
struct ServerTransport {
    client: clickhouse::Client,
    runtime: std::sync::Mutex<tokio::runtime::Runtime>,
    spec: ConnectionSpec,
}

/// What it takes to build an equivalent connection from scratch.
///
/// Kept so [`Engine::in_database`] can BUILD a client rather than clone one. The
/// distinction is not stylistic: `Client::clone` deep-copies the configuration but SHARES
/// the transport, so a cloned client's pooled connections belong to the runtime that
/// created them. Driven from a second runtime they simply stall — measured, as two hung
/// tests. A connection belongs to exactly one runtime, and this is how that
/// stays true.
#[derive(Clone)]
struct ConnectionSpec {
    endpoint: String,
    user: String,
    password: String,
    database: Option<String>,
}

/// The ceiling on ONE engine round trip through [`ServerTransport::block_on`], total —
/// and, for streaming reads, the per-CHUNK-GAP staleness bound instead (see
/// `block_on_unceilinged`): a whole-call deadline is wrong in both directions, silent on a
/// wedge outside the executor and firing on a long read that is making steady progress.
///
/// The HTTP client sets no request timeout, so without this a response that never comes holds
/// the caller in `block_on` forever: a live daemon, metrics answering, failures at zero, and a
/// frontier that has silently stopped. A hang with no check on it is worse than a crash. Thirty
/// minutes is generous against the longest legitimate call and converts a wedge into a failed
/// tick: counted, journaled, retried on the next cycle. Labeled analytic: re-derive toward an
/// idle-based per-chunk bound if a legitimate streaming read ever approaches the total.
const ENGINE_CALL_CEILING: std::time::Duration = std::time::Duration::from_mins(30);

impl ServerTransport {
    /// Run a future on this transport's runtime, serialized against other callers and
    /// bounded by [`ENGINE_CALL_CEILING`] — the outer `Err` is the ceiling firing; the
    /// future's own output rides inside `Ok`.
    ///
    /// **Poisoning is recovered here, deliberately, and this is the decision rather than
    /// the default.** A caller that panics while holding this lock poisons it. The
    /// tempting reading is that this belongs to the "wedged thread, poisoned pool" class
    /// the restart rationale names — let the lock panic, let the process die, let the init
    /// system restart it. It does not: a panic unwinds OUT of `block_on`, the task
    /// is dropped, and the runtime is left usable. What the poison flag records is that
    /// *a caller* panicked, not that *the engine* is broken, and converting one failed
    /// query into a daemon restart would turn a bad window into an outage — in an
    /// airgapped deployment, repeatedly and invisibly.
    ///
    /// The safety net is that this is not the last check: if the runtime really were
    /// unusable, the next query fails and `server_alive` reports it as CRITICAL. We
    /// recover the guard and let the engine speak for itself.
    fn block_on<F: std::future::Future>(&self, future: F) -> Result<F::Output, Error> {
        self.runtime().block_on(async {
            tokio::time::timeout(ENGINE_CALL_CEILING, future)
                .await
                .map_err(|_| {
                    Error::Connection(format!(
                        "engine call exceeded the {}s ceiling: an unbounded wait is \
                             a wedge, not patience",
                        ENGINE_CALL_CEILING.as_secs()
                    ))
                })
        })
    }

    /// `block_on` without the whole-call ceiling, for [`Engine::run_to_writer`]'s streaming
    /// reads only, which bound themselves per chunk gap instead. A wall-clock deadline on a
    /// round trip cannot tell "wedged" from "working" when the round trip legitimately spans
    /// a day's data: it stays silent on a park outside the executor, where the timer never
    /// runs, and fires on steady measured progress. The gap between chunks can tell them
    /// apart, and does.
    fn block_on_unceilinged<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime().block_on(future)
    }

    /// The runtime, its lock recovered from a poison: see [`Self::block_on`].
    fn runtime(&self) -> std::sync::MutexGuard<'_, tokio::runtime::Runtime> {
        self.runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Deliberately hand-written: the client holds the engine credential, and a derived
/// `Debug` would print it into whatever log or panic message touched an `Engine`.
impl std::fmt::Debug for ServerTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerTransport")
            .field("endpoint", &self.spec.endpoint)
            .finish_non_exhaustive()
    }
}

/// The pinned engine, opened against an on-disk database directory.
///
/// **Every call blocks, and one `Engine` serves one caller at a time.** It drives its own
/// current-thread runtime behind a mutex (see `ServerTransport`), so a call MUST NOT be made
/// from a thread running an async runtime; use `tokio::task::spawn_blocking`. Concurrency
/// comes from holding more than one `Engine` ([`Engine::sibling`]).
#[derive(Debug)]
pub struct Engine {
    transport: Box<ServerTransport>,
    data_dir: PathBuf,
    queries: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Engine {
    /// Open the engine against a running server this process owns.
    ///
    /// **The endpoint must be loopback, and that is enforced rather than documented.**
    /// The analyzer's standing rule is that the database is never exposed; with an HTTP
    /// client in the crate, "never exposed" stops being a property of the config we
    /// generate and becomes a property of the code — a misconfiguration, a copied
    /// example or a future call site cannot point this at another host. It is also the
    /// reason no TLS feature is enabled: there is no remote peer to authenticate.
    ///
    /// # Errors
    /// A non-loopback or malformed endpoint, or a runtime that cannot be built.
    pub fn connect_server(
        endpoint: &str,
        user: &str,
        password: &str,
        data_dir: impl Into<PathBuf>,
    ) -> Result<Self, Error> {
        Self::connect_with(
            &ConnectionSpec {
                endpoint: endpoint.to_string(),
                user: user.to_string(),
                password: password.to_string(),
                database: None,
            },
            data_dir,
        )
    }

    fn connect_with(spec: &ConnectionSpec, data_dir: impl Into<PathBuf>) -> Result<Self, Error> {
        require_loopback(&spec.endpoint)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Connection(format!("building the engine runtime: {e}")))?;
        // The spill thresholds ride every connection as settings, so a query spills to
        // disk under memory pressure instead of taking the engine down.
        let mut client = clickhouse::Client::default()
            .with_url(&spec.endpoint)
            .with_user(&spec.user)
            .with_password(&spec.password)
            .with_setting(
                "max_bytes_before_external_sort",
                EXTERNAL_SORT_SPILL_BYTES.to_string(),
            )
            .with_setting(
                "max_bytes_before_external_group_by",
                EXTERNAL_GROUP_BY_SPILL_BYTES.to_string(),
            );
        if let Some(database) = &spec.database {
            client = client.with_database(database.clone());
        }
        Ok(Self {
            transport: Box::new(ServerTransport {
                client,
                runtime: std::sync::Mutex::new(runtime),
                spec: spec.clone(),
            }),
            data_dir: data_dir.into(),
            queries: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    /// Open the engine against a server started by [`server::ServerHandle`] — the
    /// daemon's own path, where the endpoint and the credential come from the handle
    /// rather than from configuration a caller could get wrong.
    ///
    /// # Errors
    /// As [`Engine::connect_server`].
    pub fn for_handle(handle: &server::ServerHandle) -> Result<Self, Error> {
        let config = handle.config();
        Self::connect_server(
            &format!("http://127.0.0.1:{}", config.http_port),
            &handle.credential().user,
            handle.credential().password(),
            config.data_dir.clone(),
        )
    }

    /// What this engine connects to, endpoint and database: the key a process-wide cache
    /// keeps a per-engine figure under (a test binary opens many databases on one server).
    #[must_use]
    pub fn identity(&self) -> String {
        format!(
            "{}/{}",
            self.transport.spec.endpoint,
            self.transport.spec.database.as_deref().unwrap_or("default")
        )
    }

    /// The database directory this engine was opened against. Exposed for self-health,
    /// which measures bytes on disk directly: the engine's own `system.parts` view of
    /// inactive parts is process-local and under-reports, so the filesystem is the only
    /// trustworthy witness to unreclaimed space.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Run SQL and return the engine's output: one HTTP request per statement, the results
    /// concatenated in order (see `statements`).
    ///
    /// # Errors
    /// A statement the transport refuses, a connection that fails, or the engine's own error.
    pub fn run(&self, sql: &str) -> Result<String, Error> {
        self.count(sql);
        Self::run_server(&self.transport, sql)
    }

    /// A sibling connection: same endpoint, same credential, same database, its own
    /// transport and runtime. An `Engine` is one connection and one runtime, so parallel
    /// work takes one sibling per worker rather than sharing this one; the bucket reader's
    /// burst pool is the consumer this exists for.
    ///
    /// # Errors
    /// Connection failure.
    pub fn sibling(&self) -> Result<Self, Error> {
        Self::connect_with(&self.transport.spec, self.data_dir.clone())
    }

    /// A fresh connection to `database` on the same server: the isolation unit the suite
    /// needs, since a database is cheap and a server is not, so one server per test binary
    /// serves many tests that cannot see each other's tables.
    ///
    /// # Errors
    /// Connection failure.
    pub fn in_database(&self, database: &str) -> Result<Self, Error> {
        let mut spec = self.transport.spec.clone();
        spec.database = Some(database.to_string());
        Self::connect_with(&spec, self.data_dir.clone())
    }

    /// Statements this engine has dispatched, since it was opened.
    ///
    /// **An operational counter that a test may also read, not a test shim.** The daemon
    /// exports it (`infertap_analyzer_engine_queries_total`), which is the property
    /// that makes it worth pinning against: an assertion about engine work is written
    /// against the same number production reports, rather than against a stand-in that
    /// only exists in a harness.
    ///
    /// It counts statements, not round trips: a script is one call here but several
    /// requests on the wire. Statements are what the engine actually executes, and they
    /// are the unit that stays meaningful if the transport changes.
    #[must_use]
    pub fn queries_executed(&self) -> u64 {
        self.queries.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A live handle to the same counter, for a reader on ANOTHER thread. An `Engine` is
    /// single-threaded by construction, so the exported query total — the sum over both of
    /// the daemon's engines — needs a way to read one engine's count without touching the
    /// engine or the lock that serializes it. Reading the handle mid-statement is exact:
    /// the count moves at dispatch, not completion.
    #[must_use]
    pub fn queries_shared(&self) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
        std::sync::Arc::clone(&self.queries)
    }

    fn count(&self, sql: &str) {
        let statements = statements::split_statements(sql).len() as u64;
        self.queries
            .fetch_add(statements.max(1), std::sync::atomic::Ordering::Relaxed);
    }

    /// Run SQL and stream the result into `sink`, never holding it whole.
    ///
    /// [`Engine::run`] returns a `String`, which is right for the many small results this
    /// system asks for — a count, a ledger, a facts row — and wrong for the few large
    /// ones, where it means the whole result exists in memory at once: at multi-GB results
    /// the buffering is the failure, not the query.
    ///
    /// Same contract as `run` in every other respect, including the server backend's
    /// statement-splitting and placeholder escaping, so a caller can switch on size alone.
    /// Returns the number of bytes written.
    ///
    /// # Errors
    /// The engine's failure, or the sink's.
    pub fn run_to_writer<W: std::io::Write>(&self, sql: &str, sink: &mut W) -> Result<u64, Error> {
        self.count(sql);
        Self::stream_server(&self.transport, sql, sink)
    }

    /// The server transport: one HTTP request per statement, output collected in order.
    ///
    /// Both departures from "just send the SQL" are the pin's findings, not preferences —
    /// see `statements`: the HTTP interface refuses multi-statement bodies, and the
    /// client reads `?` as a bind placeholder even inside a string literal.
    fn run_server(transport: &ServerTransport, sql: &str) -> Result<String, Error> {
        let mut out = String::new();
        for prepared in statements::prepare(sql)? {
            let bytes = transport.block_on(async {
                transport
                    .client
                    .query(&prepared)
                    .fetch_bytes("TabSeparated")
                    .map_err(|e| Error::Statement(e.to_string()))?
                    .collect()
                    .await
                    .map_err(|e| Error::Statement(e.to_string()))
            })??;
            out.push_str(&String::from_utf8_lossy(&bytes));
        }
        Ok(out)
    }

    /// Stream rows INTO a table, without a carrier file (the R1 obligation).
    ///
    /// `sql` must be an `INSERT INTO … FORMAT …` statement; the data follows it as a
    /// stream. A connection takes the rows directly; a `file()` hand-off through `/tmp` would
    /// be refused under a server, because `file()` resolves inside `user_files_path`.
    ///
    /// Nothing is written outside the database, so there is no confinement question, no /tmp
    /// permissions question and no crash residue.
    ///
    /// # Errors
    /// The engine's failure, or the reader's.
    pub fn insert_stream<R: std::io::Read>(&self, sql: &str, data: &mut R) -> Result<u64, Error> {
        // An empty stream is a no-op, decided here: a window with nothing to write is
        // ordinary (a quiet hour, a fleet with no stores that day), not an error, and no
        // caller should have to know how the engine treats an insert with no rows.
        let mut head = vec![0u8; INSERT_CHUNK_BYTES];
        let filled = fill(data, &mut head)?;
        if filled == 0 {
            // counted only when it reaches the engine: a no-op insert is not engine work,
            // and a counter that says otherwise would make the scaling pin lie
            return Ok(0);
        }
        self.count(sql);
        let mut stream = std::io::Read::chain(&head[..filled], data);
        Self::insert_server(&self.transport, sql, &mut stream)
    }

    /// Server backend: the rows are the request body, sent in chunks.
    fn insert_server<R: std::io::Read>(
        transport: &ServerTransport,
        sql: &str,
        data: &mut R,
    ) -> Result<u64, Error> {
        let prepared = statements::prepare(sql)?;
        let [statement] = prepared.as_slice() else {
            return Err(Error::Refused(format!(
                "an insert carries exactly one statement, got {} — the rows follow the \
                 statement, so a script has no meaning here",
                prepared.len()
            )));
        };
        transport.block_on(async {
            // The buffered writer takes slices, so the rows never become `Bytes` on our
            // side and the crate's own `bytes` type stays out of our dependency list.
            let mut insert = transport
                .client
                .insert_formatted_with(statement)
                .buffered_with_capacity(INSERT_CHUNK_BYTES);
            let mut buffer = vec![0u8; INSERT_CHUNK_BYTES];
            let mut written = 0u64;
            loop {
                let read = data
                    .read(&mut buffer)
                    .map_err(|e| Error::Io(format!("reading rows: {e}")))?;
                if read == 0 {
                    break;
                }
                let mut sent = 0;
                while sent < read {
                    sent += insert
                        .write(&buffer[sent..read])
                        .await
                        .map_err(|e| Error::Statement(e.to_string()))?;
                }
                written += read as u64;
            }
            insert
                .end()
                .await
                .map_err(|e| Error::Statement(e.to_string()))?;
            Ok(written)
        })?
    }

    /// Streaming read, server backend: chunks from the response go straight to the sink.
    fn stream_server<W: std::io::Write>(
        transport: &ServerTransport,
        sql: &str,
        sink: &mut W,
    ) -> Result<u64, Error> {
        let mut written = 0u64;
        for prepared in statements::prepare(sql)? {
            written += transport.block_on_unceilinged(async {
                let mut cursor = transport
                    .client
                    .query(&prepared)
                    .fetch_bytes("TabSeparated")
                    .map_err(|e| Error::Statement(e.to_string()))?;
                let mut total = 0u64;
                // The stream's own bound: each GAP between chunks answers to the
                // ceiling — no data for that long is a wedge; steady delivery is
                // patience, however long the whole stream runs. The FIRST gap carries
                // a wider allowance: it covers the engine's external sort, whose
                // lead-in grows with the day (measured: five minutes at a 71.5M-row
                // day) — and goes away entirely when the identity-first layout lands.
                let mut allowance = ENGINE_CALL_CEILING * 4;
                loop {
                    let next = tokio::time::timeout(allowance, cursor.next())
                        .await
                        .map_err(|_| {
                            Error::Connection(format!(
                                "engine stream stalled: no chunk for {}s — a gap is a \
                                 wedge; steady delivery never trips this",
                                allowance.as_secs()
                            ))
                        })?
                        .map_err(|e| Error::Statement(e.to_string()))?;
                    let Some(chunk) = next else { break };
                    allowance = ENGINE_CALL_CEILING;
                    sink.write_all(&chunk)
                        .map_err(|e| Error::Io(format!("streaming: {e}")))?;
                    total += chunk.len() as u64;
                }
                Ok::<u64, Error>(total)
            })?;
        }
        Ok(written)
    }
}

/// Read until the buffer is full or the source ends, so "no rows" can be told apart from
/// "the first read returned nothing yet".
fn fill<R: std::io::Read>(source: &mut R, buffer: &mut [u8]) -> Result<usize, Error> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = source
            .read(&mut buffer[filled..])
            .map_err(|e| Error::Io(format!("reading rows: {e}")))?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

/// Refuse any endpoint that is not this machine's loopback interface.
///
/// Parsed rather than pattern-matched, because the near-misses are the point:
/// `http://127.0.0.1.example.com/` starts with the right characters,
/// `http://user:pass@127.0.0.1/` puts a credential somewhere it must never live, and
/// `localhost` is whatever a resolver says it is — including a name a customer's DNS or
/// hosts file points elsewhere. Only the literal loopback addresses pass.
fn require_loopback(endpoint: &str) -> Result<(), Error> {
    let rest = endpoint.strip_prefix("http://").ok_or_else(|| {
        Error::Refused(format!(
            "engine endpoint {endpoint:?} must be plain http:// on loopback — there is no \
             remote peer here to authenticate, so no TLS transport is built in"
        ))
    })?;
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.contains('@') {
        return Err(Error::Refused(format!(
            "engine endpoint {endpoint:?} carries credentials in its URL; the client is \
             given them directly and they never belong in a string that gets logged"
        )));
    }
    let host = authority.strip_prefix('[').map_or_else(
        || authority.split(':').next().unwrap_or_default(),
        |v6| v6.split(']').next().unwrap_or_default(),
    );
    if host == "127.0.0.1" || host == "::1" {
        return Ok(());
    }
    Err(Error::Refused(format!(
        "engine endpoint {endpoint:?} is not loopback: the analyzer's database is never \
         reachable from off the box, and that is enforced here rather than left to \
         configuration"
    )))
}

/// Apply unapplied migrations in order, recording each in the ledger. Returns the newly
/// applied versions. A fresh database reaches schema v1 by this path only.
///
/// # Errors
/// Config floor violations, surviving template placeholders, engine failures, or a
/// malformed ledger.
pub fn migrate(engine: &Engine, cfg: &RetentionConfig) -> Result<Vec<u32>, Error> {
    cfg.validate()?;
    engine.run(
        "CREATE TABLE IF NOT EXISTS schema_migrations \
         (version UInt32, name String, applied_at DateTime DEFAULT now()) \
         ENGINE = MergeTree ORDER BY version",
    )?;
    let applied: HashSet<u32> = engine
        .run("SELECT version, name FROM schema_migrations")?
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| {
            let (version, name) = l
                .trim()
                .split_once('\t')
                .ok_or_else(|| Error::Migration("malformed migration identity".into()))?;
            let version = version
                .parse::<u32>()
                .map_err(|e| Error::Migration(format!("ledger: {e}")))?;
            if !MIGRATIONS
                .iter()
                .any(|m| m.version == version && m.name == name)
            {
                return Err(Error::Migration(format!(
                    "unsupported schema identity {version}/{name}; initialize a fresh database"
                )));
            }
            Ok(version)
        })
        .collect::<Result<_, _>>()?;
    let mut newly = Vec::new();
    for migration in MIGRATIONS {
        let (version, name) = (migration.version, migration.name);
        if applied.contains(&version) {
            continue;
        }
        let sql = migration
            .sql
            .replace(
                "{ledger_retention_days}",
                &cfg.ledger_retention_days.to_string(),
            )
            .replace(
                "{rollup_retention_years}",
                &cfg.rollup_retention_years.to_string(),
            )
            .replace(
                "{instance_detail_retention_days}",
                &cfg.instance_detail_retention_days.to_string(),
            )
            .replace(
                "{minute_retention_days}",
                &cfg.minute_retention_days.to_string(),
            )
            .replace(
                "{hour_retention_days}",
                &cfg.hour_retention_days.to_string(),
            )
            // the cell's posture: the engine family a migration spells its tables through
            .replace("{engine_family}", ENGINE_FAMILY);
        if let Some(placeholder) = surviving_placeholder(&sql) {
            return Err(Error::Migration(format!(
                "migration {name}: unsubstituted placeholder {{{placeholder}}} — the \
                 template and the substitution map have drifted; refusing to apply"
            )));
        }
        engine.run(&sql)?;
        engine.run(&format!(
            "INSERT INTO schema_migrations (version, name) VALUES ({}, {})",
            sql_i64(i64::from(version)),
            quote_literal(name)
        ))?;
        newly.push(version);
    }
    Ok(newly)
}

/// Which knob a table's horizon reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionKnob {
    /// `ledger_retention_days`: the served input facts and the identity-day relation: the
    /// box's memory of what arrived, for as long as the input log holds it.
    LedgerDays,
    /// `hour_retention_days`.
    HourDays,
    /// `minute_retention_days`.
    MinuteDays,
    /// `instance_detail_retention_days`.
    InstanceDetailDays,
    /// `rollup_retention_years`.
    RollupYears,
}

impl RetentionKnob {
    /// The placeholder the migration spells this knob through.
    #[must_use]
    pub fn placeholder(self) -> &'static str {
        match self {
            Self::LedgerDays => "{ledger_retention_days}",
            Self::HourDays => "{hour_retention_days}",
            Self::MinuteDays => "{minute_retention_days}",
            Self::InstanceDetailDays => "{instance_detail_retention_days}",
            Self::RollupYears => "{rollup_retention_years}",
        }
    }

    /// The knob's value under `cfg`, with its unit.
    #[must_use]
    pub fn value(self, cfg: &RetentionConfig) -> (u32, &'static str) {
        match self {
            Self::LedgerDays => (cfg.ledger_retention_days, "DAY"),
            Self::HourDays => (cfg.hour_retention_days, "DAY"),
            Self::MinuteDays => (cfg.minute_retention_days, "DAY"),
            Self::InstanceDetailDays => (cfg.instance_detail_retention_days, "DAY"),
            Self::RollupYears => (cfg.rollup_retention_years, "YEAR"),
        }
    }

    /// The interval clause under `cfg`, as the migration renders it after substitution.
    #[must_use]
    pub fn interval(self, cfg: &RetentionConfig) -> String {
        let (n, unit) = self.value(cfg);
        format!("INTERVAL {n} {unit}")
    }
}

/// **Every table whose horizon reads a knob, with the expression its TTL ages by** — the
/// same map `0001_schema.sql` spells through placeholders, re-applied by [`apply_retention`]
/// at every start so a changed knob takes effect on a running box. The static audit holds
/// the two spellings to each other.
pub const RETENTION_TTLS: &[(&str, &str, RetentionKnob)] = &[
    // The served input facts age from their ingest, as the input log does.
    (
        "input_batches",
        "toDateTime(intDiv(ingested_at_ms, 1000))",
        RetentionKnob::LedgerDays,
    ),
    (
        "input_segments",
        "toDateTime(intDiv(ingested_at_ms, 1000))",
        RetentionKnob::LedgerDays,
    ),
    (
        "input_drops",
        "toDateTime(intDiv(ingested_at_ms, 1000))",
        RetentionKnob::LedgerDays,
    ),
    (
        "fleet_inventory",
        "toDateTime(last_seen_ms / 1000)",
        RetentionKnob::LedgerDays,
    ),
    ("replication_whale_day", "day", RetentionKnob::LedgerDays),
    (
        "replication_whale_day_total",
        "day",
        RetentionKnob::LedgerDays,
    ),
    ("class_rollup_hourly", "bucket", RetentionKnob::HourDays),
    ("band_rollup_hourly", "bucket", RetentionKnob::HourDays),
    (
        "replication_hourly",
        "toDateTime(intDiv(bucket_ms, 1000), 'UTC')",
        RetentionKnob::HourDays,
    ),
    ("class_rollup_daily", "bucket", RetentionKnob::RollupYears),
    ("band_rollup_daily", "bucket", RetentionKnob::RollupYears),
    ("coverage_daily", "day", RetentionKnob::RollupYears),
    ("block_mass_daily", "day", RetentionKnob::RollupYears),
    ("block_mass_day", "day", RetentionKnob::RollupYears),
    ("tag_minute", "toDate(bucket)", RetentionKnob::RollupYears),
    (
        "restated_hourly",
        "toDate(bucket)",
        RetentionKnob::RollupYears,
    ),
    (
        "replication_pairs_day",
        "toDate(toDateTime(day * 86400, 'UTC'))",
        RetentionKnob::RollupYears,
    ),
    (
        "replication_daily",
        "toDate(toDateTime(intDiv(bucket_ms, 1000), 'UTC'))",
        RetentionKnob::RollupYears,
    ),
    (
        "replication_instance_spans_day",
        "toDate(toDateTime(day * 86400, 'UTC'))",
        RetentionKnob::RollupYears,
    ),
    (
        "markers",
        "toDate(toDateTime(at_ms / 1000))",
        RetentionKnob::RollupYears,
    ),
    ("facts_rollup_daily", "day", RetentionKnob::RollupYears),
    (
        "input_key_spaces",
        "toDate(toDateTime(at_ms / 1000))",
        RetentionKnob::RollupYears,
    ),
    ("recompute_minute", "bucket", RetentionKnob::MinuteDays),
    ("coverage_minute", "bucket", RetentionKnob::MinuteDays),
    ("band_minute", "bucket", RetentionKnob::MinuteDays),
    ("recompute_rank_minute", "bucket", RetentionKnob::MinuteDays),
    ("band_rank_minute", "bucket", RetentionKnob::MinuteDays),
    (
        "replication_minute",
        "toDateTime(intDiv(minute_ms, 1000), 'UTC')",
        RetentionKnob::MinuteDays,
    ),
    (
        "replication_instance_hourly",
        "bucket",
        RetentionKnob::InstanceDetailDays,
    ),
    (
        "replication_instance_minute",
        "bucket",
        RetentionKnob::MinuteDays,
    ),
];

/// Re-apply every knob's horizon to its tables — metadata only (`materialize_ttl_after_modify
/// = 0`), rows expire at their next merge. The daemon runs it at every start, after the
/// migration, so a knob changed since the first start takes effect the way the event horizon
/// already does.
///
/// # Errors
/// A knob below its floor, or engine failure on any table.
pub fn apply_retention(engine: &Engine, cfg: &RetentionConfig) -> Result<(), Error> {
    cfg.validate()?;
    for (table, expr, knob) in RETENTION_TTLS {
        engine.run(&format!(
            "ALTER TABLE {} MODIFY TTL {expr} + {} DELETE \
             SETTINGS materialize_ttl_after_modify = 0",
            quote_ident(table),
            knob.interval(cfg)
        ))?;
    }
    Ok(())
}

/// The compliance exhibit as one artifact: every table and view definition as running
/// state (actual TTL clauses, post-substitution), plus the migration ledger.
///
/// # Errors
/// Engine failures.
pub fn schema_dump(engine: &Engine) -> Result<String, Error> {
    let names = engine
        .run("SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name")?;
    let mut out = String::from("-- infertap-analyzer schema exhibit (running state)\n\n");
    for name in names.lines().filter(|l| !l.is_empty()) {
        // quote_ident even though the names come from the engine: the database may
        // contain tables the analyzer did not create (shared-database postures), and
        // circular trust in engine-returned names is trust all the same.
        out.push_str(&engine.run(&format!(
            "SHOW CREATE TABLE {} FORMAT TSVRaw",
            quote_ident(name)
        ))?);
        out.push_str(";\n\n");
    }
    out.push_str("-- migration ledger\n");
    out.push_str(
        &engine.run("SELECT version, name, applied_at FROM schema_migrations ORDER BY version")?,
    );
    Ok(out)
}

/// Empty every table but the migration ledger: the schema stays, the rows go. What the
/// shipped selftest runs between conformance cases, so one migrated session serves them
/// all with the isolation a fresh database would give. Materialised views are skipped: they
/// hold no rows of their own, and their targets are tables in the same list.
///
/// # Errors
/// Engine failure.
pub fn truncate_all_but_migrations(engine: &Engine) -> Result<(), Error> {
    let names = engine.run(
        "SELECT name FROM system.tables WHERE database = currentDatabase() \
         AND engine != 'MaterializedView' AND name != 'schema_migrations' ORDER BY name",
    )?;
    for name in names.lines().filter(|l| !l.is_empty()) {
        engine.run(&format!("TRUNCATE TABLE {}", quote_ident(name)))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{quote_ident, quote_literal, require_loopback, surviving_placeholder, Engine};

    #[test]
    fn the_engine_endpoint_can_only_ever_be_loopback() {
        // "ClickHouse is never exposed" cannot be a property of the config we generate alone:
        // with an HTTP client in the crate it has to be a property of the code, because a
        // config is something a deployment can change and a constructor is not.
        for allowed in [
            "http://127.0.0.1:18123",
            "http://127.0.0.1:18123/",
            "http://[::1]:18123",
        ] {
            assert!(
                require_loopback(allowed).is_ok(),
                "{allowed} must be allowed"
            );
        }
        for refused in [
            "http://10.0.0.5:8123",               // another host
            "http://127.0.0.1.attacker.example/", // prefix that only looks like loopback
            "http://localhost:18123",             // whatever the resolver says it is
            "https://127.0.0.1:18123",            // no TLS transport is built in
            "http://user:secret@127.0.0.1:18123", // a credential in a loggable string
            "127.0.0.1:18123",                    // no scheme at all
        ] {
            assert!(
                require_loopback(refused).is_err(),
                "{refused} must be refused"
            );
        }

        // and the refusal reaches the constructor, not just the helper
        assert!(
            Engine::connect_server("http://10.0.0.5:8123", "u", "p", "/tmp/x").is_err(),
            "a non-loopback endpoint cannot produce an Engine at all"
        );
    }

    #[test]
    fn identifiers_and_literals_are_quoted() {
        assert_eq!(quote_ident("records"), "`records`");
        assert_eq!(quote_ident("we`ird"), "`we``ird`");
        assert_eq!(quote_literal("plain"), "'plain'");
        assert_eq!(quote_literal("o'clock \\ path"), "'o\\'clock \\\\ path'");
    }

    #[test]
    fn the_deltas_retention_floor_refuses_with_its_derivation() {
        let below = super::RetentionConfig {
            ledger_retention_days: super::LEDGER_RETENTION_FLOOR_DAYS - 1,
            ..Default::default()
        };
        let err = below.validate().expect_err("below-floor must refuse");
        assert!(
            matches!(&err, crate::engine::Error::Refused(m) if m.contains("below the floor") && m.contains("examined history")),
            "a below-floor knob is a refusal, and the derivation rides it: {err}"
        );
        let at_floor = super::RetentionConfig {
            ledger_retention_days: super::LEDGER_RETENTION_FLOOR_DAYS,
            ..Default::default()
        };
        assert!(at_floor.validate().is_ok(), "the floor itself is permitted");
    }

    #[test]
    fn surviving_placeholders_are_named() {
        assert_eq!(
            surviving_placeholder("TTL x + INTERVAL {new_knob} DAY"),
            Some("new_knob".to_string())
        );
        assert_eq!(surviving_placeholder("no placeholders {123} {X}"), None);
        assert_eq!(surviving_placeholder("map literal {'k':'v'} is fine"), None);
    }
}
