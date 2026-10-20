// SPDX-License-Identifier: AGPL-3.0-only

//! The loop: the cadence the service owns.
//!
//! The daemon is the product's only operable mode; the CLI's one-shot commands are its
//! diagnostic edge. Every lifecycle decision belongs here rather than to an operator: when
//! to ingest, when to tick, when to reclaim, and when to refuse.
//!
//! **Two `Engine`s, and that is a correctness requirement rather than a tuning choice.**
//! An `Engine` is one connection and one current-thread runtime — a single-threaded
//! resource. A health sampler sharing the tick's `Engine` would queue behind the tick
//! instead of observing it, so a sample taken during a long tick would arrive late and
//! describe a moment that had gone. The loop holds one; the sampler holds
//! its own.
//!
//! **What runs at every rung of disk pressure, without exception:** merges, TTL
//! enforcement, mutation drain, and health emission. Only intake is refusable. The
//! reasoning is in [`pressure`], and it is the difference between a full disk and a
//! permanently full disk.
//!
//! One external dependency, `signal-hook`, for SIGTERM. Everything else the loop schedules
//! already belongs to a crate below it; what is new here is only when they happen.
//!
//! # Where to start
//!
//! [`Daemon::start`] boots, [`Daemon::run`] loops, and one [`Daemon::cycle`] is the order of
//! everything the loop does. The crate root holds the daemon's contract — [`DaemonConfig`],
//! [`Shutdown`], the [`Daemon`] struct and its accessors, [`LoopSnapshot`] — and each concern
//! is its own file with its own `impl Daemon` block:
//!
//! - `boot` — the refusals that need no server, the host claim, the engines, the source wiring.
//! - `run` — the run loop and one cycle of it.
//! - `ingest` — discover, widen and ingest on the ingest cadence; the prefill.
//! - `tick` — the keyed tick on every ingest cycle, and its report.
//! - `settle` — what follows a tick and what keeps the log: the ledger, the input tables, the
//!   input log's maintenance, the working state's close at a stop.
//! - `reclaim` — the response to disk pressure, and the one horizon write path.
//! - `sample` — the sampler's side of the loop, through its own engine.
//!
//! [`pressure`] is the disk ladder, [`counters`] the loop's own accounting, [`preflight`]
//! the checks before a pilot. The memory ceiling and the `/metrics` listener are private
//! to the crate.

mod error;
pub use error::Error;
mod boot;
pub use boot::reset_derived;
pub mod counters;
mod execution;
mod ingest;
mod memory;
pub(crate) mod metrics;
pub mod preflight;
pub mod pressure;
mod reclaim;
mod run;
mod sample;
mod settle;
mod tick;

pub use analyzer_objectstore::StaticCredentials;
pub use memory::Ceiling;
pub use metrics::SocketAddr;

use analyzer_serving_clickhouse::engine::server::ServerHandle;
use analyzer_serving_clickhouse::engine::Engine;
use analyzer_serving_clickhouse::selfhealth;
use boot::HostClaim;
use counters::Counters;
use pressure::Pressure;
use sample::SamplerInputs;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

/// How often the ingest cycle lists the bucket for new segments, and with it how often the
/// keyed tick runs.
///
/// Derived from the source's own cadence: the tap rotates at minute-to-hour granularity,
/// so polling faster finds nothing new on most cycles and polling slower adds ingest
/// latency for no benefit. Re-derived against a deployment's actual rotation setting.
pub const DEFAULT_INGEST_INTERVAL: Duration = Duration::from_mins(1);

/// How often the input log is kept to its retention and compacted.
///
/// The keyed tick runs on every ingest cycle; this paces only the log's upkeep, which
/// retires whole days and so has nothing to do more often than hourly.
pub const DEFAULT_MAINTAIN_INTERVAL: Duration = Duration::from_hours(1);

/// How often self-health is sampled and its artifacts rewritten.
///
/// Derived from the consumer: a 60 s Prometheus scrape interval is the common default, so
/// the textfile never serves a sample older than one scrape. Faster would write files
/// nobody reads; slower would make the scraper's freshest reading stale by construction.
pub const DEFAULT_HEALTH_INTERVAL: Duration = Duration::from_mins(1);

/// How often the sampler thread and the run loop wake to ask whether a cadence is due.
///
/// Coarse on purpose: the cadences are minutes, so a second of granularity costs nothing
/// and keeps shutdown responsive. Named because the staleness bound is derived from it: a
/// sampler cannot publish faster than it wakes, so this is the floor under any cadence a
/// caller configures.
const TICK: Duration = Duration::from_secs(1);

/// How long an ingest walk runs before it says so in the journal, and then how often it
/// repeats itself while it keeps running.
///
/// Journal policy, not a measured quantity: its job is that no long walk is silent, and
/// the specific minute is slack. The cycle is one blocking call over the whole backlog, so
/// without a mid-walk line the journal is quiet for exactly as long as the work is largest.
/// One line per file is the opposite failure; at fleet rates it buries the lines that
/// matter. A minute matches the default ingest cadence, so a walk that speaks at this
/// period reads as "still one cycle, still moving".
const INGEST_PROGRESS_INTERVAL: Duration = Duration::from_mins(1);

/// How many health intervals of silence from the sampler mean it is dead rather than slow.
///
/// **A labeled analytic constant**(`docs/conformance.md`), and a judgment about the
/// world rather than arithmetic: it is the standard missed-N-in-a-row shape, sized so one
/// slow sample is never mistaken for a dead one. A single interval would be wrong — a
/// sample runs an engine query and can legitimately overrun its cadence under load, and a
/// daemon that exited over one slow query would turn a hiccup into a restart. Two leaves no
/// margin at all for the second sample to also be slow. Three is the first value where the
/// emitter has to miss its cadence repeatedly, which no healthy sampler does.
///
/// Re-derived against a deployment's observed sample durations — if real samples routinely
/// approach the cadence, the cadence is wrong before this number is.
pub const STALE_SAMPLE_INTERVALS: u32 = 3;

/// Minimum interval between forced-merge reclamations: `OPTIMIZE ... FINAL` rewrites whole
/// partitions and its freed bytes return over minutes, so re-issuing per tick is over-actuation
/// against a lever still in motion. Derived from the engine's own reclamation timing at the pin:
/// `old_parts_lifetime` = 480 s plus `cleanup_delay_period` = 30 s, rounded up for the merge
/// itself.
pub const RECLAIM_COOLDOWN: Duration = Duration::from_mins(10);

/// The smallest byte budget either cache accepts: one busy instance-hour. Below it the
/// cache thrashes on every use.
pub const MIN_CACHE_BYTES: u64 = 64 * 1024 * 1024;

/// Hours in a UTC day, for the horizon the daemon states in days and the source reads in hours.
const HOURS_PER_DAY: i64 = 24;

/// Where the records bucket is and how to reach it.
#[derive(Debug, Clone)]
pub struct BucketConfig {
    /// The S3-API endpoint URL, with the bucket; never a path.
    pub url: String,
    /// A static credential pair, or `None` to use the provider chain, which is how a
    /// deployment with an instance profile and no secret on disk reaches its bucket.
    pub credentials: Option<StaticCredentials>,
    /// The key prefix the taps ship to, ending in the stream component.
    pub key_prefix: String,
}

/// Declare a deploy into `bucket`: its annotation object, the durable form the running
/// daemon's next annotation walk records in the input log. What the CLI's `annotate` does,
/// with no engine and no daemon of its own. Returns the object's key.
///
/// # Errors
/// An invalid annotation, or the bucket refused the write.
pub fn declare_deploy(
    bucket: &BucketConfig,
    annotation: &analyzer_ingest::annotations::DeployAnnotation,
) -> Result<String, Error> {
    let store = analyzer_objectstore::S3Store::open(&bucket.url, bucket.credentials.as_ref())
        .map_err(analyzer_ingest::Error::from)?;
    Ok(analyzer_ingest::annotations::declare(&store, annotation)?)
}

/// Everything the daemon needs to run: the fleet, the bucket, the directories, the cadences
/// and the knobs.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// What every figure this daemon emits is keyed by.
    pub fleet_id: String,
    /// The records bucket this daemon ingests; the key prefix scopes the listing under it.
    pub bucket: BucketConfig,
    /// The engine's data directory.
    pub data_dir: PathBuf,
    /// Where the health artifacts and the bootstrap token are written.
    pub out_dir: PathBuf,
    /// The engine binary to run.
    pub clickhouse_bin: PathBuf,
    /// How often the ingest cycle runs, and with it the keyed tick.
    pub ingest_interval: Duration,
    /// How often the input log is kept to its retention.
    pub maintain_interval: Duration,
    /// How often the health sample is emitted.
    pub health_interval: Duration,
    /// The five retention horizons, validated at start and re-applied at every start.
    pub retention: analyzer_serving_clickhouse::engine::RetentionConfig,
    /// The lanes each keyed operator runs on, each a thread of its own; `None` is the
    /// executor's default.
    pub classify_workers: Option<usize>,
    /// The ingest prepare pool's width; `None` is ingest's default, sized to the box.
    pub ingest_workers: Option<usize>,
    /// The segment cache's byte budget: fetched segments are kept under it, LRU by last use.
    /// Bytes, not days, so the box's disk is fixed at any event rate.
    pub segment_cache_bytes: u64,
    /// This daemon's own identity in the health artifacts, when one is configured.
    pub analyzer_id: Option<String>,
    /// The derivation parameters every tick runs under.
    pub params: analyzer_analysis::Params,
    /// The engine's HTTP port. Analyzer-specific by default, because a target shop
    /// plausibly already runs `ClickHouse` on 8123/9000; overridable because a deployment
    /// may already be using ours too.
    pub http_port: u16,
    /// The engine's native port.
    pub tcp_port: u16,
    /// Where `/metrics` listens, or `None` for no listener (the textfile artifact always
    /// exists either way, rendering the identical sample).
    pub metrics_addr: Option<SocketAddr>,
    /// The one-analyzer-per-VM claim, on by default. `false` is for tests and drills that
    /// deliberately run several daemons on one host, which is a rig, not a deployment.
    pub host_claim: bool,
    /// Where the derivation reads sealed segments, when NOT through the records bucket:
    /// the job form and the rig read a plain directory. `None` — the appliance default —
    /// derives the engine-fenced collection transport from `bucket`.
    pub segment_transport: Option<analyzer_pipeline::parquet_source::SegmentTransport>,
    /// Whether the source admits the write-ahead JSONL form beside sealed Parquet. A test
    /// rig that plants JSONL sets it; a deployment never does, since nothing conforming
    /// puts JSONL in a bucket.
    pub admit_corpus_forms: bool,
    /// Public derivation modules switched off by name, checked at start against the registry
    /// and the modules' needs; empty runs every module this build registers. The core
    /// registers always run.
    pub disabled_modules: Vec<String>,
}

impl DaemonConfig {
    /// A configuration with every cadence at its derived default.
    #[must_use]
    pub fn new(
        fleet_id: impl Into<String>,
        bucket: BucketConfig,
        data_dir: impl Into<PathBuf>,
        clickhouse_bin: impl Into<PathBuf>,
    ) -> Self {
        let data_dir = data_dir.into();
        Self {
            fleet_id: fleet_id.into(),
            bucket,
            out_dir: data_dir.clone(),
            data_dir,
            clickhouse_bin: clickhouse_bin.into(),
            ingest_interval: DEFAULT_INGEST_INTERVAL,
            maintain_interval: DEFAULT_MAINTAIN_INTERVAL,
            health_interval: DEFAULT_HEALTH_INTERVAL,
            retention: analyzer_serving_clickhouse::engine::RetentionConfig::default(),
            classify_workers: None,
            ingest_workers: None,
            segment_cache_bytes: analyzer_pipeline::parquet_source::DEFAULT_SEGMENT_CACHE_BYTES,
            analyzer_id: None,
            params: analyzer_analysis::Params::default(),
            http_port: analyzer_serving_clickhouse::engine::server::DEFAULT_HTTP_PORT,
            tcp_port: analyzer_serving_clickhouse::engine::server::DEFAULT_TCP_PORT,
            metrics_addr: None,
            host_claim: true,
            segment_transport: None,
            admit_corpus_forms: false,
            disabled_modules: Vec::new(),
        }
    }

    /// The floors, checked before a server starts: every retention horizon, the cache budget,
    /// the lane count and the maintenance cadence. The CLI refuses on the same
    /// sentence, so an embedder and a flag see one rule.
    ///
    /// # Errors
    /// The first value below its floor, with the floor's derivation.
    pub fn validate(&self) -> Result<(), Error> {
        self.retention.validate().map_err(Error::Retention)?;
        for (flag, value) in [("--segment-cache-bytes", self.segment_cache_bytes)] {
            if value < MIN_CACHE_BYTES {
                return Err(Error::BelowFloor {
                    flag,
                    value,
                    floor: MIN_CACHE_BYTES,
                    why: "less than one busy instance-hour; the cache would thrash on every use",
                });
            }
        }
        if self.classify_workers == Some(0) {
            return Err(Error::BelowFloor {
                flag: "--classify-workers",
                value: 0,
                floor: 1,
                why: "each keyed operator needs a lane",
            });
        }
        if self.ingest_workers == Some(0) {
            return Err(Error::BelowFloor {
                flag: "--ingest-workers",
                value: 0,
                floor: 1,
                why: "a cycle prepares its first file on a worker",
            });
        }
        if self.ingest_interval.is_zero() {
            return Err(Error::BelowFloor {
                flag: "--ingest-interval",
                value: 0,
                floor: 1,
                why: "a cadence of zero seconds is a busy loop",
            });
        }
        if self.maintain_interval.is_zero() {
            return Err(Error::BelowFloor {
                flag: "--maintain-interval",
                value: 0,
                floor: 1,
                why: "a cadence of zero seconds is a busy loop",
            });
        }
        Ok(())
    }

    /// The event-grain horizon this deployment declares, in days.
    fn declared_horizon_days(&self) -> u32 {
        self.retention.ledger_retention_days
    }
}

/// The stop signal: a flag the loop checks between cycles.
///
/// **Two ways to stop, and both matter.** [`Shutdown::listen`] installs SIGTERM/SIGINT
/// handlers so `systemctl stop` produces an ORDERED engine shutdown; the orphan recovery in
/// `storage::server::claim_for_daemon` covers the case where the process dies without one.
/// The second is what makes the daemon survivable, but relying on it for ordinary stops
/// would make crash recovery the steady state.
///
/// The handler is `signal_hook::flag`, which is the minimal form: an async-signal-safe
/// handler that does nothing but set this `AtomicBool`. No thread, no runtime, no channel —
/// the loop notices on its next tick and takes its normal ordered-stop path.
#[derive(Debug, Default, Clone)]
pub struct Shutdown(Arc<AtomicBool>);

impl Shutdown {
    /// A stop request nobody has made yet.
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    /// Install SIGTERM and SIGINT handlers that request this shutdown.
    ///
    /// SIGTERM is what an init system sends; SIGINT is what a terminal sends. Both mean
    /// "stop", and a daemon that honoured only one would behave differently under systemd
    /// than under an operator's hands.
    ///
    /// # Errors
    /// If a handler cannot be registered.
    pub fn listen(&self) -> Result<(), Error> {
        for signal in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
            signal_hook::flag::register(signal, Arc::clone(&self.0))
                .map_err(|source| Error::Signal { signal, source })?;
        }
        Ok(())
    }

    /// Ask the loop to stop at its next check.
    pub fn request(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// The flag itself, for a tick to honour.
    #[must_use]
    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.0)
    }

    /// Whether a stop was requested.
    #[must_use]
    pub fn requested(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// The annotation-object writer's shape. Structurally identical to the api crate's
/// `AnnotationSink` without naming it: the two crates share no types, and a structural
/// closure type unifies at the shell's call site, the same discipline as `LoopSnapshot`.
pub(crate) type AnnotationSink =
    Arc<dyn Fn(&str, &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync>;

/// The tick's source: sealed segments through whichever store the configuration names.
pub(crate) type TickSource = analyzer_pipeline::parquet_source::ParquetSource<
    Box<dyn analyzer_pipeline::parquet_source::SegmentStore>,
>;

/// Which cadences fell due for one cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Due {
    /// The ingest cadence: discover, widen, ingest, and the keyed tick.
    pub ingest: bool,
    /// The maintenance cadence: the input log kept to its retention.
    pub maintain: bool,
}

impl Due {
    /// Neither cadence.
    pub const NONE: Self = Self {
        ingest: false,
        maintain: false,
    };
    /// The ingest cadence alone.
    pub const INGEST: Self = Self {
        ingest: true,
        maintain: false,
    };
    /// The maintenance cadence alone.
    pub const MAINTAIN: Self = Self {
        ingest: false,
        maintain: true,
    };
    /// Both cadences.
    pub const BOTH: Self = Self {
        ingest: true,
        maintain: true,
    };

    /// Whether anything is due.
    #[must_use]
    pub fn any(self) -> bool {
        self.ingest || self.maintain
    }
}

/// What one cycle of the loop did. Returned for the tests, which drive cycles directly
/// rather than waiting on wall-clock cadences.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleOutcome {
    /// The disk ladder's rung this cycle ran under.
    pub pressure: Pressure,
    /// Objects ingested this cycle.
    pub ingested_files: u64,
    /// Whether a tick was applied this cycle.
    pub ticked: bool,
    /// Whether escalated reclamation ran this cycle.
    pub reclaimed: bool,
}

/// Statements dispatched by each of the daemon's own engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineQueryCounts {
    /// The loop's engine.
    pub loop_engine: u64,
    /// The sampler's engine.
    pub sampler: u64,
}

/// The live statement counters of every engine a serving layer opened through
/// [`Daemon::api_engine`], so the exported total covers them.
#[derive(Debug, Default)]
pub(crate) struct ServingQueries(Mutex<Vec<Arc<AtomicU64>>>);

impl ServingQueries {
    fn add(&self, counter: Arc<AtomicU64>) {
        lock(&self.0).push(counter);
    }

    /// The statements every serving engine has dispatched so far.
    pub(crate) fn total(&self) -> u64 {
        lock(&self.0)
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }
}

/// The daemon: owns the server, both engines, and the cadences.
pub struct Daemon {
    namespace: analyzer_pipeline::execution::Namespace,
    config: DaemonConfig,
    /// The discriminator of `config.params`, computed once.
    discriminator: analyzer_analysis::Discriminator,
    /// The public modules this daemon runs: the registered less the configuration's
    /// `disabled_modules`, validated at start.
    modules: analyzer_pipeline::modules::ModuleSet,
    /// The memory ceiling this process runs under, read once at start; `None` where nothing
    /// could be read, in which case the engine keeps its own default.
    ceiling: Option<Ceiling>,
    handle: ServerHandle,
    engine: Engine,
    /// The daemon's lake spine, built once: its working state, execution journal and input
    /// log, under this analyzer's lineage (created once in its data directory, so a
    /// restart resumes its own journal and reclaims its own lease). One spine, so every
    /// reader shares one cache of the input log.
    spine: Arc<analyzer_pipeline::spine::Spine>,
    /// The serving store the core publishes to: this daemon's database, as the port.
    serving: Arc<dyn analyzer_pipeline::publish::Serving>,
    /// The workspace's object-storage transport, owned here because the daemon is the one
    /// long-lived process that fetches segments. Ingest lists and gets through this; the
    /// engine is handed rows and never asked to be an HTTP client.
    store: Arc<analyzer_objectstore::S3Store>,
    /// The sampler's own connection (see the module header), behind a lock so the emission
    /// thread and [`Daemon::sample`] share it without sharing the loop's.
    sampler_engine: Arc<Mutex<Engine>>,
    /// What the emission thread reads and the loop writes: the ladder state the loop owns,
    /// the server's status, and the latest tick-settled products. Published whole after
    /// every cycle, and read without ever blocking on the tick. The loop's counters are
    /// deliberately absent: the sampler reads those live from [`Daemon::counters`].
    shared: Arc<RwLock<SamplerInputs>>,
    counters: Arc<Counters>,
    /// The sampler engine's statement count, as a live handle, so the loop can sum both
    /// engines' work without taking the sampler's mutex: the loop and the sampler never
    /// wait on each other's engine, in either direction.
    sampler_queries: Arc<AtomicU64>,
    /// The statement counts of every serving engine handed out, for the exported total.
    serving_queries: Arc<ServingQueries>,
    last_pressure: Pressure,
    /// The horizon discovery holds: the declared one, widened while the driver has input to
    /// read past it.
    /// **Written ONLY through [`Daemon::set_effective_horizon_days`]**, which also publishes
    /// the shared cell below — two write paths would be two horizons.
    effective_horizon_days: u32,
    /// The SAME horizon, in hours, as the live shared cell every derivation source reads at
    /// construction. The tick rebuilds its source per cycle and would track the horizon
    /// either way; the API and the DAO hold their `SourceConfig` for their whole life, and
    /// this cell is what keeps their discovery from freezing at the horizon that was in
    /// force when they bound.
    horizon: analyzer_pipeline::parquet_source::HorizonHours,
    last_reclaim: Option<Instant>,
    /// What a serving layer is told about the loop, once per iteration. `None` for every
    /// deployment that never enables one, which is what keeps the daemon serve-agnostic.
    observer: Option<Box<dyn Fn(LoopSnapshot) + Send>>,
    /// The degradation ledger over the last days a tick settled, served from memory. `None`
    /// until a day settles.
    latest_ledger: Option<analyzer_analysis::degradation::Ledger>,
    /// The fleet's stamped figures as of the last applied tick.
    latest_figures: Option<selfhealth::FleetFigures>,
    /// The run-state counts as of the last ingest cycle.
    latest_runs: Option<selfhealth::RunStates>,
    /// The analyze-now kick, set by the API through the shell: the next cycle ticks whatever
    /// its cadence says.
    kick: Arc<AtomicBool>,
    /// The stop request the tick honours: the loop's shutdown flag once `run` has it; `None`
    /// under a test that drives cycles directly.
    cancel: Option<Arc<AtomicBool>>,
    /// The keyed driver and its store, lent to every tick: the operators' state stays in
    /// memory between ticks, and a tick that fails drops it, so the next opens it again.
    lent: Mutex<analyzer_pipeline::executor::Lent>,
    _guard: analyzer_serving_clickhouse::engine::server::DaemonGuard,
    _host_claim: Option<HostClaim>,
}

impl std::fmt::Debug for Daemon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Daemon")
            .field("config", &self.config)
            .field("modules", &self.modules)
            .field("ceiling", &self.ceiling)
            .field("last_pressure", &self.last_pressure)
            .field("effective_horizon_days", &self.effective_horizon_days)
            .finish_non_exhaustive()
    }
}

/// What the loop knows about itself that a read layer cannot derive: its own cadence and
/// whether it is currently refusing intake.
///
/// Owned here, and deliberately not the API's type. The read API needs this to answer
/// `/v1/health`, and neither crate may depend on the other: the daemon must stay
/// indifferent to whether anything is served, and the API sits beside the composition,
/// not above it. So the loop hands out its own vocabulary and the shell, which already
/// depends on both, converts (`ARCHITECTURE.md`).
///
/// Two fields, because two is what the loop knows. A backlog exists only while files are
/// deferred past the slice deadline, and that is measured where the walk tallies it and
/// exported as the backlog gauges; absent beats invented on the one surface whose job is
/// saying how fresh things are.
#[derive(Debug, Clone, Default)]
pub struct LoopSnapshot {
    /// Wall-clock ms at which the next tick becomes due — the cadence that moves the figures.
    /// Absent while a tick is overdue or has never run, both of which are "now" rather than a
    /// time in the future.
    pub next_tick_at_ms: Option<f64>,
    /// Is the disk ladder refusing intake right now?
    pub refusing: bool,
    /// The degradation ledger over the last days a tick settled; the serving layer answers
    /// health from this, window-filtered per request. Incident-shaped and small, so cloning
    /// it per snapshot is the price of the loop and the server sharing nothing else.
    pub ledger: Option<analyzer_analysis::degradation::Ledger>,
}

/// Lock a mutex, recovering the value from a poisoned one: a panic on another thread
/// leaves the guarded value consistent for everything this crate guards.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Wall clock in epoch milliseconds. Wall, not monotonic: the cadences are monotonic
/// because a schedule must not move when somebody sets the clock, but a time handed to a
/// browser or compared with an event stamp has to be a time the world can name.
pub(crate) fn wall_now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0)
}

/// Today's UTC day index.
pub(crate) fn today_index() -> i64 {
    analyzer_analysis::emitted::day_index(wall_now_ms())
}

/// How many days of horizon reach back to `day`, with one day of margin.
pub(crate) fn days_back_to(day: i64) -> u32 {
    u32::try_from((today_index() - day).max(0) + 1).unwrap_or(u32::MAX)
}

impl Daemon {
    /// The loop's live counters, shared with the sampler.
    #[must_use]
    pub fn counters(&self) -> &Arc<Counters> {
        &self.counters
    }

    /// The loop's engine: an embedder that owns a daemon can ask it what it is connected to.
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// The validated retention this daemon runs under, for the API it hosts.
    #[must_use]
    pub fn retention(&self) -> analyzer_serving_clickhouse::engine::RetentionConfig {
        self.config.retention
    }

    /// A third connection, for a serving layer. Never share [`Self::sample`]'s.
    ///
    /// The module header explains why the loop and the sampler are two engines. Serving is
    /// the same argument extended by one. `sampler_engine` is also load-bearing for
    /// liveness: [`Self::run`] exits the process when the health publication is
    /// [`STALE_SAMPLE_INTERVALS`] intervals old, so a console read holding that mutex for
    /// three minutes would be indistinguishable from a dead sampler.
    ///
    /// The connection's statement count joins the exported `engine_queries_total`, so a
    /// deployment that serves reports every statement this process dispatches.
    ///
    /// # Errors
    /// The engine cannot open a connection to the running server.
    pub fn api_engine(&self) -> Result<Arc<Mutex<Engine>>, Error> {
        let engine = Engine::for_handle(&self.handle)?;
        self.serving_queries.add(engine.queries_shared());
        Ok(Arc::new(Mutex::new(engine)))
    }

    /// The analyze-now handle: whoever holds it makes the next cycle tick now.
    ///
    /// The loop consumes it once per cycle, so a kick during a running tick runs one more
    /// tick after, which delivers whatever arrived meanwhile. Kicking also resets the
    /// cadence: the next scheduled tick is one interval away.
    #[must_use]
    pub fn tick_kick(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.kick)
    }

    /// The annotation-object writer the API state gets handed — a closure over this daemon's
    /// own store, so the api crate needs no edge to the object store and the shell passes it
    /// it through opaquely. Scoped by construction to the annotations prefix: a key outside
    /// it is refused here, before any transport, so the IAM statement and the code state
    /// the same sentence.
    #[must_use]
    pub fn annotation_sink(&self) -> AnnotationSink {
        use analyzer_objectstore::ObjectStore as _;
        let store = Arc::clone(&self.store);
        Arc::new(move |key: &str, body: &[u8]| {
            if !key.starts_with(analyzer_ingest::annotations::ANNOTATIONS_PREFIX) {
                return Err(format!(
                    "the annotation sink writes only under {:?}, got {key:?}",
                    analyzer_ingest::annotations::ANNOTATIONS_PREFIX
                )
                .into());
            }
            Ok(store.put(key, body)?)
        })
    }

    /// Publish [`LoopSnapshot`] once per loop iteration to `observer`.
    ///
    /// Set before [`Self::run`], which consumes the daemon. One observer, replacing any previous:
    /// two would be two opinions about the same loop.
    pub fn observe(&mut self, observer: impl Fn(LoopSnapshot) + Send + 'static) {
        self.observer = Some(Box::new(observer));
    }

    /// Statements dispatched by each of the daemon's own engines.
    ///
    /// Exposed because "they are two connections" is the load-bearing property of this
    /// whole design and a test must be able to STATE it rather than infer it. The exported
    /// metric is their sum, which is right for a dashboard and useless for proving they are
    /// separate.
    #[must_use]
    pub fn engine_query_counts(&self) -> EngineQueryCounts {
        EngineQueryCounts {
            loop_engine: self.engine.queries_executed(),
            sampler: self.sampler_queries.load(Ordering::Relaxed),
        }
    }

    /// The DAO over the loop's engine, at this daemon's retention: the same reads the
    /// console serves, so the glass and the alarms cannot drift.
    pub(crate) fn dao(&self) -> analyzer_serving_clickhouse::Dao<'_> {
        analyzer_serving_clickhouse::Dao::new(
            &self.engine,
            self.source_config(),
            &self.config.params,
            self.spine(),
            Arc::clone(&self.serving),
        )
        .with_retention(self.config.retention)
    }
}

#[cfg(test)]
mod tests {
    use super::run::Cadence;
    use super::{BucketConfig, DaemonConfig, Error};
    use std::time::Duration;

    /// A scratch directory removed when the test ends, however it ends.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("{label}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).expect("scratch");
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn config(dir: &std::path::Path) -> DaemonConfig {
        DaemonConfig::new(
            "fx",
            BucketConfig {
                url: String::new(),
                credentials: None,
                key_prefix: String::new(),
            },
            dir,
            "clickhouse-never-started",
        )
    }

    /// A sample reads the counters live, never a copy the loop published. Here the counters
    /// move and nothing publishes before the sample; the sample must see the movement
    /// anyway. The engine points at a port nothing listens on, so the engine-derived
    /// series go absent and the sample takes the unreachable path, which still renders
    /// every loop-supplied series.
    #[test]
    fn a_sample_sees_counter_increments_the_loop_never_published() {
        let scratch = Scratch::new("live-counters");
        let engine = analyzer_serving_clickhouse::engine::Engine::connect_server(
            "http://127.0.0.1:9",
            "u",
            "p",
            &scratch.0,
        )
        .expect("an engine over a dead port constructs without I/O");
        let config = config(&scratch.0);
        let shared = std::sync::RwLock::new(super::SamplerInputs::default());
        let counters = super::counters::Counters::new();
        counters.tick_started();
        counters.files_ingested(39);
        let lake = analyzer_pipeline::Lake::new(
            std::sync::Arc::new(analyzer_objectstore::DirStore::new(&scratch.0)),
            String::new(),
            analyzer_pipeline::lineage::Lineage::new("sample", "test").expect("lineage"),
        );
        let vitals = super::sample::sample_with(&engine, &lake, &config, &shared, &counters, 0);
        assert_eq!(
            vitals.value("infertap_analyzer_ticks_total"),
            Some(1.0),
            "the tick began and no cycle has published since — a sample that misses it \
             is serving a copy again"
        );
        assert_eq!(
            vitals.value("infertap_analyzer_files_ingested_total"),
            Some(39.0),
            "thirty-nine files committed mid-cycle must be visible mid-cycle"
        );
    }

    /// The floors an embedder meets at start, on the same sentence the CLI refuses with.
    #[test]
    fn the_configuration_floors_refuse_before_a_server_starts() {
        let scratch = Scratch::new("floors");
        let mut cfg = config(&scratch.0);
        cfg.validate().expect("the defaults are above every floor");
        cfg.segment_cache_bytes = 1;
        assert!(
            matches!(
                cfg.validate(),
                Err(Error::BelowFloor {
                    flag: "--segment-cache-bytes",
                    ..
                })
            ),
            "a cache budget under one instance-hour is refused"
        );
        cfg.segment_cache_bytes = super::MIN_CACHE_BYTES;
        cfg.retention.ledger_retention_days = 1;
        assert!(
            matches!(cfg.validate(), Err(Error::Retention(_))),
            "a retention horizon under its floor is refused with the floor's derivation"
        );
    }

    /// The input horizon's floor is derived from the examined history, and this is the
    /// consequence of that derivation rather than a tripwire over a coincidence.
    ///
    /// A store is classified over a fixed history, and a late record reopens a day up to the
    /// correction horizon back, so the input a reopened day's history is built from must still
    /// be discoverable at the smallest horizon a deployment may declare.
    #[test]
    fn the_input_floor_is_the_correction_horizon_and_the_examined_history_plus_its_margin() {
        use analyzer_serving_clickhouse::engine::LEDGER_RETENTION_FLOOR_DAYS as FLOOR;
        let horizon_days =
            analyzer_analysis::Params::default().examined_history_ms / analyzer_analysis::DAY_MS;
        assert!(
            horizon_days < f64::from(FLOOR),
            "a store is classified over {horizon_days} days of history, and a deployment may \
             declare {FLOOR} days of input: the model would then see less context than the \
             store was classified under"
        );
        assert_eq!(
            FLOOR,
            analyzer_analysis::CORRECTION_HORIZON_DAYS
                + analyzer_analysis::EXAMINED_HISTORY_DAYS
                + analyzer_analysis::CONTEXT_MARGIN_DAYS,
            "the floor is a derivation, not a number that happens to be big enough"
        );
        // The same primitive builds the coverage path's evidence clip, and the equality is the
        // property that makes an emitted day's coverage retention-independent: everything that
        // fetch can see is inside the retention this floor guarantees.
        assert!(
            (analyzer_analysis::Params::default().examined_history_ms
                + analyzer_analysis::completeness::EVIDENCE_CLIP_MARGIN_MS)
                <= f64::from(FLOOR) * analyzer_analysis::DAY_MS,
            "the evidence clip reaches further back than the daemon promises to retain"
        );
    }

    /// What a serving layer is allowed to say about the next tick.
    ///
    /// Due-now is absent, not zero. `LoopSnapshot` hands this to the console's freshness
    /// strip, and a strip that renders "next tick: now" forever, because the cadence is
    /// overdue or because the daemon has not run one yet, is stating a time it does not
    /// have. Absent beats invented.
    #[test]
    fn a_cadence_reports_time_remaining_only_when_it_has_some() {
        let hour = Duration::from_hours(1);

        assert_eq!(
            Cadence::new(hour).remaining(),
            None,
            "a cadence that has never run is due now, and now is not a future instant"
        );
        let mut ran = Cadence::new(hour);
        ran.ran();
        let waiting = ran.remaining().expect("an hour is left");
        assert!(
            waiting <= hour && waiting > Duration::from_mins(59),
            "a cadence that just ran leaves about its whole interval, not {waiting:?}"
        );
    }
}
