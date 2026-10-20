// SPDX-License-Identifier: AGPL-3.0-only

//! The boot: what `Daemon::start` does before the loop runs, and the wiring it hands out.
//!
//! The refusals that need no server, the host claim, the engine and its sibling connections,
//! the migrations and the memory ceiling said out loud — then the sources the loop builds per
//! cycle: the derivation's `SourceConfig`, the tick's source over the bucket, and the lake's
//! store.

use super::sample::SamplerInputs;
use super::{Daemon, DaemonConfig, ServingQueries, TickSource, HOURS_PER_DAY};
use crate::counters::Counters;
use crate::memory::{self, Ceiling};
use crate::preflight;
use crate::pressure::Pressure;
use crate::Error;
use analyzer_serving_clickhouse::engine::server::{self, ServerConfig, ServerHandle};
use analyzer_serving_clickhouse::engine::{apply_retention, migrate, Engine, MIGRATIONS};
use analyzer_serving_clickhouse::selfhealth::{self, ServerStatus};
use std::sync::{Arc, Mutex, RwLock};

/// The host-wide singleton claim: one analyzer per VM, held as a socket for the process
/// lifetime so it cannot leak past a crash the way a lock file can. On Linux the name is
/// abstract, immune to `PrivateTmp=` and to filesystem namespacing, gone the instant the
/// process is. Elsewhere (development hosts) it is a socket file with stale-file recovery,
/// which is racy in ways the abstract name is not and is why production is Linux.
pub(crate) struct HostClaim(#[allow(dead_code)] std::os::unix::net::UnixListener);

/// The claim's name-in-common: every analyzer on a host contends for exactly this.
const HOST_CLAIM_NAME: &str = "infertap-analyzer";

/// Take the host-wide claim, or refuse with the one-per-VM sentence.
fn claim_host() -> Result<HostClaim, Error> {
    let refusal = |e: &dyn std::fmt::Display| Error::HostClaimed {
        detail: e.to_string(),
    };
    #[cfg(target_os = "linux")]
    {
        use std::os::linux::net::SocketAddrExt;
        let addr = std::os::unix::net::SocketAddr::from_abstract_name(HOST_CLAIM_NAME)
            .map_err(|source| Error::HostClaim { source })?;
        std::os::unix::net::UnixListener::bind_addr(&addr)
            .map(HostClaim)
            .map_err(|e| refusal(&e))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let path = std::env::temp_dir().join(format!("{HOST_CLAIM_NAME}.claim"));
        if let Ok(l) = std::os::unix::net::UnixListener::bind(&path) {
            return Ok(HostClaim(l));
        }
        // Alive, or stale residue? A connect answers: refused means nobody listens, so
        // the file is a dead claim and may be cleared.
        if std::os::unix::net::UnixStream::connect(&path).is_ok() {
            return Err(refusal(&"claim socket is answering"));
        }
        std::fs::remove_file(&path).map_err(|source| Error::HostClaim { source })?;
        std::os::unix::net::UnixListener::bind(&path)
            .map(HostClaim)
            .map_err(|e| refusal(&e))
    }
}

/// Say what the engine was told it may use, and where that came from. At startup, because
/// an OOM investigation begins by asking exactly this, and the source matters as much as
/// the number: a cgroup limit means the process is in a container or a capped unit.
fn announce_memory_ceiling(ceiling: Option<Ceiling>) {
    let Some(ceiling) = ceiling else {
        return;
    };
    let gib = ceiling.gib();
    match ceiling.source {
        memory::Source::Cgroup => log::info!(
            "memory ceiling {gib:.1} GiB, from a cgroup limit — this process \
             is in a container or a capped unit"
        ),
        memory::Source::HostTotal => {
            log::info!("memory ceiling {gib:.1} GiB, the host's own");
        }
    }
}

/// The daemon states what it migrated and what already held, so the journal is the
/// confirmation surface rather than the migration table alone.
fn announce_migrations(applied: &[u32]) {
    if applied.is_empty() {
        log::info!("migrations: all {} already applied", MIGRATIONS.len());
    } else {
        log::info!(
            "migrations: applied {applied:?} ({} total registered)",
            MIGRATIONS.len()
        );
    }
}

/// The module set this configuration runs: a name no module has, or a fact switched off
/// under another that needs it, is refused here, not discovered at the first tick.
fn module_set(config: &DaemonConfig) -> Result<analyzer_pipeline::modules::ModuleSet, Error> {
    let disabled: Vec<&str> = config.disabled_modules.iter().map(String::as_str).collect();
    let modules = analyzer_pipeline::modules::ModuleSet::without(&disabled)
        .map_err(|source| Error::Modules { source })?;
    log::info!(
        "public modules: {}{}; the core registers always run",
        if modules.order().is_empty() {
            "none".to_string()
        } else {
            modules.order().join(", ")
        },
        if disabled.is_empty() {
            String::new()
        } else {
            format!(" (disabled: {})", disabled.join(", "))
        }
    );
    Ok(modules)
}

/// The environment checks shared with `preflight`, so a mount that changed or a permission
/// revoked between one start and the next is refused at the door rather than discovered
/// as a daemon that came up "running" and failed every emission.
fn refuse_bad_environment(config: &DaemonConfig) -> Result<(), Error> {
    let environment = preflight::directory_checks(&config.data_dir, &config.out_dir);
    match environment
        .into_iter()
        .find(|c| c.status == selfhealth::Status::Critical)
    {
        Some(bad) => Err(Error::Environment {
            check: bad.name,
            message: bad.message,
        }),
        None => Ok(()),
    }
}

/// Start the engine child under its memory ceiling and connect the loop's engine to it.
///
/// The engine is told nothing about the bucket: it reaches object storage on no path, so it needs no credential and there is no secret at rest to guard.
///
/// The clock check is the one check that needs the engine, and it is fatal because it
/// cannot fire for a transient reason: both readings come from the same host, so an NTP
/// outage moves them together. Day partitioning, TTL aging and staleness all read the
/// clock, so a box whose engine and host disagree misfiles rows.
fn start_engine(
    config: &DaemonConfig,
    ceiling: Option<Ceiling>,
) -> Result<(ServerHandle, Engine), Error> {
    let mut server_config = ServerConfig::new(&config.data_dir);
    server_config.http_port = config.http_port;
    server_config.tcp_port = config.tcp_port;
    server_config.max_server_memory_bytes = ceiling.map(Ceiling::engine_share);
    let handle = ServerHandle::start(&config.clickhouse_bin, &server_config)?;
    let engine = Engine::for_handle(&handle)?;
    let engine_now = engine
        .run("SELECT toFloat64(now64(3))")
        .ok()
        .and_then(|row| row.trim().parse::<f64>().ok());
    let clock = preflight::clock_check(engine_now);
    if clock.status == selfhealth::Status::Critical {
        return Err(Error::Environment {
            check: clock.name,
            message: clock.message,
        });
    }
    Ok((handle, engine))
}

/// The local disk the lake's object cache may hold: the fold's day index read by positioned
/// reads, a few of the kept days' worth on the rig's scale (about 1.3 GB a day). Losing it
/// costs a re-read from the bucket.
const LAKE_CACHE_BYTES: u64 = 8 << 30;

/// The daemon's lake spine, built once at start: the lake's store under the bucket's prefix,
/// this analyzer's lineage, and the working state's memory its ceiling allows.
fn daemon_spine(
    config: &DaemonConfig,
    lake_store: &LakeStore,
    namespace: &analyzer_pipeline::execution::Namespace,
    engine: &Engine,
    ceiling: Option<Ceiling>,
) -> Result<Arc<analyzer_pipeline::spine::Spine>, Error> {
    let spine = analyzer_pipeline::spine::Spine::new(
        Arc::clone(lake_store),
        &config.bucket.key_prefix,
        namespace,
        analyzer_pipeline::lineage::Lineage::of_data_dir(engine.data_dir())
            .map_err(analyzer_pipeline::Error::from)?,
    )
    .with_object_cache(&engine.data_dir().join("lake-cache"), LAKE_CACHE_BYTES)?
    .with_working_state(
        &engine
            .data_dir()
            .join(analyzer_pipeline::spine::WORKING_STATE),
        ceiling.map_or(
            analyzer_lake::working::DEFAULT_MEMORY_BYTES,
            Ceiling::working_state,
        ),
    )?;
    Ok(Arc::new(spine))
}

/// The namespace a configuration derives in: the bucket, its key prefix and the fleet.
fn namespace_of(config: &DaemonConfig) -> Result<analyzer_pipeline::execution::Namespace, Error> {
    Ok(analyzer_pipeline::execution::Namespace::new(
        format!(
            "{}:{}{}",
            config.bucket.url.len(),
            config.bucket.url,
            config.bucket.key_prefix
        ),
        &config.fleet_id,
    )?)
}

/// Delete the derived state of the lineage `config` runs, keeping its input log, its lease and
/// the serving store's input tables ([`analyzer_pipeline::reset`]). It holds the data directory
/// as a daemon would, so it refuses while a daemon runs there, and it starts no engine.
///
/// # Errors
/// A configuration below a floor, a daemon running on the directory, the bucket, or the reset.
pub fn reset_derived(config: &DaemonConfig) -> Result<analyzer_pipeline::reset::Reset, Error> {
    config.validate()?;
    let _guard = server::claim_for_daemon(&config.data_dir)?;
    let store = Arc::new(analyzer_objectstore::S3Store::open(
        &config.bucket.url,
        config.bucket.credentials.as_ref(),
    )?);
    let spine = analyzer_pipeline::spine::Spine::new(
        lake_store(config, &store),
        &config.bucket.key_prefix,
        &namespace_of(config)?,
        analyzer_pipeline::lineage::Lineage::of_data_dir(&config.data_dir)
            .map_err(analyzer_pipeline::Error::from)?,
    );
    Ok(analyzer_pipeline::reset::reset_derived(
        &spine,
        &config.data_dir,
    )?)
}

/// The store the lake lives in.
type LakeStore = Arc<dyn analyzer_objectstore::ObjectStore + Send + Sync>;

/// The lake's store: the records bucket through the port, or the rig's directory when the
/// segment transport is a directory.
fn lake_store(config: &DaemonConfig, store: &Arc<analyzer_objectstore::S3Store>) -> LakeStore {
    match &config.segment_transport {
        Some(analyzer_pipeline::parquet_source::SegmentTransport::Directory { root }) => {
            Arc::new(analyzer_objectstore::DirStore::new(root))
        }
        _ => Arc::clone(store) as LakeStore,
    }
}

impl Daemon {
    /// Validate the configuration and the environment, claim the host and the directory,
    /// start the engine, and connect both `Engine`s.
    ///
    /// Every refusal that needs no server happens before one is started, so a
    /// misconfigured deployment costs a message rather than a server start, a claim, and
    /// an unwind.
    ///
    /// # Errors
    /// A configuration below a floor, a module set that cannot run, a rival analyzer on
    /// the host, an environment that cannot work (unwritable directories, a box whose
    /// clocks disagree with themselves), a rival daemon on the directory, an orphan that
    /// will not stop, a server that will not start, or a migration failure.
    pub fn start(config: DaemonConfig) -> Result<Self, Error> {
        config.validate()?;
        let modules = module_set(&config)?;
        // The host claim comes before everything: a second analyzer on this VM should hear
        // the one-per-VM sentence, not a directory or port symptom.
        let host_claim = if config.host_claim {
            Some(claim_host()?)
        } else {
            None
        };
        refuse_bad_environment(&config)?;
        let guard = server::claim_for_daemon(&config.data_dir)?;
        if let server::DaemonClaim::AdoptedOrphan(pid) = guard.claim() {
            // Said out loud, never silently: a takeover is a recovery, and an operator
            // reading the log after an incident needs to see that it happened.
            log::info!(
                "a previous daemon left engine pid {pid} running; it \
                 was stopped and this daemon took over the data directory"
            );
        }

        let ceiling = memory::ceiling();
        announce_memory_ceiling(ceiling);
        let (handle, engine) = start_engine(&config, ceiling)?;

        // The sampler's own connection and runtime (see the crate header). Its statement
        // counter's handle is taken now, while the engine is unshared, so the loop never
        // touches the sampler's mutex to read a number.
        let sampler_conn = Engine::for_handle(&handle)?;
        let sampler_queries = sampler_conn.queries_shared();
        let sampler_engine = Arc::new(Mutex::new(sampler_conn));

        // A configured static pair is used; its absence asks for the provider chain, which
        // is how a deployment with an instance profile and no secret on disk reaches its
        // bucket.
        let store = Arc::new(analyzer_objectstore::S3Store::open(
            &config.bucket.url,
            config.bucket.credentials.as_ref(),
        )?);
        let lake = lake_store(&config, &store);

        announce_migrations(&migrate(&engine, &config.retention)?);
        // Re-assert retention and initialize the runtime input-retention target.
        let declared_horizon = config.declared_horizon_days();
        apply_retention(&engine, &config.retention)?;

        let uptime = handle.uptime().as_secs_f64();
        let namespace = namespace_of(&config)?;
        let spine = daemon_spine(&config, &lake, &namespace, &engine, ceiling)?;
        let serving: Arc<dyn analyzer_pipeline::publish::Serving> = Arc::new(
            analyzer_serving_clickhouse::serving::ClickHouse::new(&engine)?,
        );
        let daemon = Self {
            serving,
            namespace,
            discriminator: analyzer_analysis::Discriminator::of(&config.params),
            modules,
            ceiling,
            shared: Arc::new(RwLock::new(SamplerInputs {
                server: ServerStatus {
                    up: true,
                    uptime_seconds: Some(uptime),
                },
                effective_retention_days: f64::from(declared_horizon),
                ..SamplerInputs::default()
            })),
            config,
            handle,
            spine,
            engine,
            store,
            sampler_engine,
            counters: Arc::new(Counters::new()),
            sampler_queries,
            serving_queries: Arc::new(ServingQueries::default()),
            last_pressure: Pressure::Normal,
            effective_horizon_days: declared_horizon,
            horizon: analyzer_pipeline::parquet_source::HorizonHours::fixed(
                i64::from(declared_horizon) * HOURS_PER_DAY,
            ),
            last_reclaim: None,
            observer: None,
            latest_ledger: None,
            latest_figures: None,
            latest_runs: None,
            kick: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            cancel: None,
            lent: Mutex::new(analyzer_pipeline::executor::Lent::default()),
            _guard: guard,
            _host_claim: host_claim,
        };
        // Bind the serving database to the namespace.
        daemon
            .namespace
            .prepare(&*daemon.serving, &daemon.spine())?;
        daemon.publish_inputs();
        Ok(daemon)
    }

    /// The derivation's source: sealed segments from the records bucket through the port,
    /// cached under the data directory, horizoned at the retention currently in force.
    ///
    /// Public because a serving layer needs the same derivation source the tick reads
    /// from. The API and the DAO hold one for their whole life; the horizon inside it is
    /// the live shared cell the daemon writes, read at each `source` construction, so a
    /// long-lived holder still opens each source at the horizon in force rather than the
    /// one it bound at. The cache directory is the shared thing: any reader opening a
    /// source against this data directory hits the segments the tick already landed. A
    /// second `SourceConfig` assembled by the shell would be a second answer to "which
    /// segments exist", which is the divergence this accessor exists to prevent.
    #[must_use]
    pub fn source_config(&self) -> analyzer_pipeline::parquet_source::SourceConfig {
        analyzer_pipeline::parquet_source::SourceConfig {
            transport: self.config.segment_transport.clone().unwrap_or_else(|| {
                analyzer_pipeline::parquet_source::SegmentTransport::Collection {
                    store: self.store.clone(),
                }
            }),
            key_prefix: self.config.bucket.key_prefix.clone(),
            cache_dir: analyzer_pipeline::parquet_source::segment_cache_dir(&self.config.data_dir),
            horizon_hours: self.horizon.clone(),
            admit_corpus_forms: self.config.admit_corpus_forms,
            cache_budget_bytes: self.config.segment_cache_bytes,
        }
    }

    /// The daemon's lake spine: its working state, journal and input log. The executor,
    /// the daemon's own reads and the read API all use this one, so every reader consults the
    /// journal the writer commits to.
    #[must_use]
    pub fn spine(&self) -> Arc<analyzer_pipeline::spine::Spine> {
        Arc::clone(&self.spine)
    }

    /// The serving store this daemon publishes to: its database, as the port. The read API
    /// takes the same one, so its consistency boundary asks the store the writer writes.
    #[must_use]
    pub fn serving(&self) -> Arc<dyn analyzer_pipeline::publish::Serving> {
        Arc::clone(&self.serving)
    }

    /// The tick's source over the bucket, its lifecycle fetches scoped by the input log, so
    /// its lifecycle reads run under the log's own include set instead of fetching everything
    /// a retention-wide listing returns.
    pub(super) fn tick_source(&self) -> TickSource {
        let (scope_lake, bank_lake) = (self.spine.lake(), self.spine.lake());
        self.source_config()
            .source()
            .with_lifecycle_scope(Box::new(move |lo, hi| {
                analyzer_pipeline::arrivals::lifecycle_window_keys(&scope_lake, lo, hi)
            }))
            .with_lifecycle_bank(Box::new(move |lo, hi| {
                analyzer_pipeline::arrivals::lifecycle_bank_read(&bank_lake, lo, hi)
            }))
    }
}
