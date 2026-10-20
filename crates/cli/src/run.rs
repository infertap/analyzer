// SPDX-License-Identifier: AGPL-3.0-only

//! `run`, the deployment form: the daemon, its `/metrics` listener and the read API serving
//! the console; and `reset-derived`, which reads the same configuration to name what it resets.

use crate::error::Error;
use crate::flags::Flags;
use crate::settings::{
    self, DEFAULT_API_ADDR, DEFAULT_METRICS_ADDR, ENV_ADMIT_CORPUS_FORMS, ENV_HOST_CLAIM,
};
use std::path::Path;
use std::process::ExitCode;

/// The flags `run` takes, and, through the layers, the keys its file and environment may set.
const RUN_FLAGS: &[&str] = &[
    "--config",
    "--log-level",
    "--log-format",
    "--bucket-url",
    "--bucket-prefix",
    "--fleet-id",
    "--data-dir",
    "--out",
    "--metrics-addr",
    "--api-addr",
    "--api-dev-origin",
    "--api-cookie-secure",
    "--http-port",
    "--tcp-port",
    "--ingest-interval",
    "--maintain-interval",
    "--ledger-retention-days",
    "--classify-workers",
    "--ingest-workers",
    "--disable-module",
    "--rollup-retention-years",
    "--hour-retention-days",
    "--minute-retention-days",
    "--instance-detail-retention-days",
    "--segment-cache-bytes",
    "--clickhouse",
];

/// The daemon's configuration from `run`'s flags, every floor refused here with its
/// derivation, before any server starts.
fn daemon_config(flags: &Flags) -> Result<analyzer_daemon::DaemonConfig, Error> {
    let requires = "run";
    let fleet = flags.required("--fleet-id", requires)?;
    let data_dir = flags.required("--data-dir", requires)?;
    let mut config = analyzer_daemon::DaemonConfig::new(
        fleet,
        settings::bucket_config(flags, requires)?,
        data_dir,
        settings::resolve_bin(flags.value("--clickhouse")),
    );
    if let Some(out) = flags.value("--out") {
        config.out_dir = out.into();
    }
    config.metrics_addr = settings::listener_addr(flags, "--metrics-addr", DEFAULT_METRICS_ADDR)?;
    if let Some(n) = flags.at_least(
        "--classify-workers",
        "a whole number of threads",
        &1_usize,
        "the fold needs a thread",
    )? {
        config.classify_workers = Some(n);
    }
    if let Some(n) = flags.at_least(
        "--ingest-workers",
        "a whole number of threads",
        &1_usize,
        "a cycle prepares its first file on a worker",
    )? {
        config.ingest_workers = Some(n);
    }
    // Derivation modules switched off by name, each flag one module; the daemon checks the
    // names and the modules' needs at start and refuses a set that cannot run.
    config.disabled_modules = flags.values("--disable-module").to_vec();
    // The retention horizons: each a knob on `run` with the floor its stop derives.
    let retention = &mut config.retention;
    for (flag, slot) in [
        (
            "--ledger-retention-days",
            &mut retention.ledger_retention_days,
        ),
        (
            "--rollup-retention-years",
            &mut retention.rollup_retention_years,
        ),
        ("--hour-retention-days", &mut retention.hour_retention_days),
        (
            "--minute-retention-days",
            &mut retention.minute_retention_days,
        ),
        (
            "--instance-detail-retention-days",
            &mut retention.instance_detail_retention_days,
        ),
    ] {
        if let Some(n) = flags.parsed(flag, "a whole number")? {
            *slot = n;
        }
    }
    // The cache budget: bytes, not days, so the disk a deployment gives the box is the whole
    // sizing conversation.
    if let Some(n) = flags.parsed("--segment-cache-bytes", "a whole number of bytes")? {
        config.segment_cache_bytes = n;
    }
    // The ingest cadence, and with it the keyed tick's: each cycle commits what arrived and
    // ticks over it.
    if let Some(secs) = flags.at_least(
        "--ingest-interval",
        "a whole number of seconds",
        &1_u64,
        "a cadence of zero seconds is a busy loop",
    )? {
        config.ingest_interval = std::time::Duration::from_secs(secs);
    }
    // The input log's upkeep cadence. The keyed tick runs on every ingest cycle, so this paces
    // only the retirement and compaction of whole days.
    if let Some(secs) = flags.at_least(
        "--maintain-interval",
        "a whole number of seconds",
        &1_u64,
        "a cadence of zero seconds is a busy loop",
    )? {
        config.maintain_interval = std::time::Duration::from_secs(secs);
    }
    for (flag, slot) in [
        ("--http-port", &mut config.http_port),
        ("--tcp-port", &mut config.tcp_port),
    ] {
        if let Some(port) = flags.parsed(flag, "a port number")? {
            *slot = port;
        }
    }
    // The drills' switches, and nothing else's: multi-daemon-per-host and planted JSONL are
    // a rig, never a deployment.
    config.host_claim = std::env::var(ENV_HOST_CLAIM).as_deref() != Ok("off");
    config.admit_corpus_forms = std::env::var(ENV_ADMIT_CORPUS_FORMS).as_deref() == Ok("on");
    // The daemon refuses the same floors at start; refusing here too keeps the sentence at
    // the door an operator is standing at.
    config.validate()?;
    Ok(config)
}

/// What the read API needs from the command line: its address, and the two settings whose
/// wrong value is invisible until an operator cannot sign in.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ApiSettings {
    addr: analyzer_daemon::SocketAddr,
    /// A development origin admitted by CORS; never set in a deployment.
    dev_origin: Option<String>,
    /// Whether the session cookie carries `Secure`. Off unless a deployment says it
    /// terminates TLS: Safari discards a `Secure` cookie over `http://localhost`, so an
    /// unconditional one breaks sign-in on the demo kit and over an SSH tunnel.
    cookie_secure: bool,
}

/// The API's settings, or `None` when `--api-addr off` asked for no listener.
fn api_settings(flags: &Flags) -> Result<Option<ApiSettings>, Error> {
    let Some(addr) = settings::listener_addr(flags, "--api-addr", DEFAULT_API_ADDR)? else {
        return Ok(None);
    };
    Ok(Some(ApiSettings {
        addr,
        dev_origin: flags.value("--api-dev-origin").map(str::to_string),
        cookie_secure: flags.switch("--api-cookie-secure")?.unwrap_or(false),
    }))
}

/// Serve the read API beside the daemon: the first-run bootstrap token, the third engine,
/// the state, the observer seam, and the serving thread.
fn serve_api(
    daemon: &mut analyzer_daemon::Daemon,
    listener: analyzer_api::serve::Listener,
    settings: &ApiSettings,
    shutdown: &analyzer_daemon::Shutdown,
    data_dir: &Path,
    params: &analyzer_analysis::Params,
) -> Result<(), Error> {
    let token_store = analyzer_api::token_store_for(data_dir);
    let initial_token = analyzer_api::initial_token_for(data_dir);
    // First-run bootstrap: a deployment that has never issued a viewer token gets one here
    // rather than making an operator find a second command before the product shows
    // anything. The token goes to a 0600 file, never to the journal, which ships to a log
    // aggregator.
    let store = analyzer_api::auth::TokenStore::open(&token_store)?;
    if let Some(record) =
        analyzer_api::auth::bootstrap_token(&store, &initial_token, settings::now_ms())?
    {
        log::info!("no viewer token existed, so one was issued ({})", record.id);
        log::info!(
            "read it once from {} (mode 0600), then sign in at http://{}",
            initial_token.display(),
            settings.addr
        );
        log::info!(
            "replace it any time with `infertap-analyzer token rotate --data-dir {} --id {}`, \
             which also ends every session it opened",
            data_dir.display(),
            record.id
        );
    }

    let mut api_config = analyzer_api::ApiConfig::new(token_store);
    api_config.addr = settings.addr;
    api_config.dev_origin.clone_from(&settings.dev_origin);
    api_config.cookie_secure = settings.cookie_secure;

    // The third engine, never the sampler's: a console read holding the sampler's mutex for
    // three minutes is indistinguishable from a dead sampler, and the daemon exits on that
    // reading.
    let engine = daemon.api_engine()?;
    let state = analyzer_api::ApiState::new(
        engine,
        (daemon.spine(), daemon.serving()),
        daemon.source_config(),
        params,
        daemon.retention(),
        api_config,
    )?;

    // The shell is where the loop's vocabulary becomes the API's, in both directions, and
    // that is the whole reason neither crate depends on the other: the kick, the
    // annotation writer and the loop snapshot all cross here.
    state.set_tick_kick(daemon.tick_kick());
    state.set_annotation_sink(daemon.annotation_sink());
    let sink = state.clone();
    daemon.observe(move |snapshot| {
        sink.publish_loop_hints(analyzer_api::health::LoopHints {
            next_tick_at_ms: snapshot.next_tick_at_ms,
            // Absent rather than derived: nothing measures a backlog here, and the hints'
            // own rule is absent over invented.
            backlog_files: None,
            backlog_bytes: None,
            refusing: Some(snapshot.refusing),
        });
        sink.publish_ledger(snapshot.ledger);
    });

    log::info!("read API on http://{}", settings.addr);
    match analyzer_api::console::stamp() {
        Some((v, hash)) => log::info!("console: embedded {v} {hash}"),
        None => log::info!(
            "console: ABSENT — this build embeds no bundle, so every non-API path answers \
             console_not_configured"
        ),
    }
    // The posture, stated at every boot.
    if settings.cookie_secure {
        log::info!(
            "session cookie: Secure SET — the console must be reached over HTTPS, or no \
             browser will keep the session"
        );
    } else {
        log::info!(
            "session cookie: Secure omitted (plaintext-safe). Pass --api-cookie-secure on \
             when a TLS proxy terminates in front of this listener"
        );
    }
    if let Some(origin) = &settings.dev_origin {
        log::info!(
            "CORS DEV ORIGIN ACTIVE for {origin} — this is a development flag and must not \
             be set in a deployment"
        );
    }

    // Its own thread, because `serve` blocks. The daemon's loop keeps the main one, and the
    // API stops on the same `Shutdown` flag rather than installing a second signal handler:
    // a process with two opinions about SIGTERM has no ordered stop.
    let stop = shutdown.clone();
    std::thread::Builder::new()
        .name("api".to_string())
        .spawn(move || {
            if let Err(e) = analyzer_api::serve::serve(listener, state, move || stop.requested()) {
                // Loud, and not fatal: the channels of record are `/metrics` and the health
                // artifacts; a dark console is degraded service, not an unobserved daemon.
                log::error!("the read API stopped serving: {e}");
            }
        })
        .map_err(|e| Error::io("spawning the read API thread", e))?;
    Ok(())
}

/// `run`: the deployment form. Everything `preflight` validated, running: the daemon, its
/// `/metrics` listener, the read API, its health artifacts, until SIGTERM/SIGINT stops it
/// in order.
pub(crate) fn run(args: &[String]) -> Result<ExitCode, Error> {
    let flags = settings::layered_flags(args, RUN_FLAGS)?;
    let config = daemon_config(&flags)?;
    let api = api_settings(&flags)?;
    // Bound before the engine starts: a port conflict is then a refusal in the first second,
    // naming the address, rather than a failure on a serving thread nobody reads after a
    // successful-looking startup.
    let api_listener = api
        .as_ref()
        .map(|settings| analyzer_api::serve::bind(settings.addr))
        .transpose()?;

    let shutdown = analyzer_daemon::Shutdown::new();
    shutdown.listen()?;
    // The API needs both after `Daemon::start` consumes the config, and one definition of
    // each is the point: a fleet or a parameter set assembled twice is one that eventually
    // differs.
    let fleet = config.fleet_id.clone();
    let data_dir = config.data_dir.clone();
    let params = config.params.clone();
    let mut daemon = analyzer_daemon::Daemon::start(config)?;
    log::info!("daemon running (fleet {fleet})");

    match (api, api_listener) {
        (Some(settings), Some(listener)) => serve_api(
            &mut daemon,
            listener,
            &settings,
            &shutdown,
            &data_dir,
            &params,
        )?,
        _ => log::info!("read API disabled (--api-addr off); no console is served"),
    }
    daemon.run(&shutdown)?;
    Ok(ExitCode::SUCCESS)
}

/// `reset-derived`: drop the deployment's derived state and keep its inputs, so the next `run`
/// derives afresh from the input log. It reads the same flags and configuration file as `run`,
/// so the lineage it resets is the one `run` would derive in.
pub(crate) fn reset_derived(args: &[String]) -> Result<ExitCode, Error> {
    let flags = settings::layered_flags(args, RUN_FLAGS)?;
    let config = daemon_config(&flags)?;
    let reset = analyzer_daemon::reset_derived(&config)?;
    println!(
        "reset-derived: deleted {} derived object(s) and {} local director(ies) under {}; \
         the input log, its lease and the served input tables are kept, and the next `run` \
         derives afresh from the log",
        reset.objects,
        reset.directories.len(),
        config.data_dir.display()
    );
    Ok(ExitCode::SUCCESS)
}
