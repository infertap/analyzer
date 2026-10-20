// SPDX-License-Identifier: AGPL-3.0-only

//! The one-shot checks of a deployment: `preflight` before the pilot, `health` for its vitals
//! once, and `diag` for the support bundle.

use crate::error::Error;
use crate::flags::Flags;
use crate::settings;
use std::path::Path;
use std::process::ExitCode;
use std::str::FromStr;

/// How `health` renders the sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum HealthFormat {
    #[default]
    Text,
    Json,
    Prometheus,
}

impl FromStr for HealthFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            "prometheus" => Ok(Self::Prometheus),
            other => Err(Error::usage(format!(
                "--format takes text, json or prometheus, not {other:?}"
            ))),
        }
    }
}

/// The sensor in its standalone form: sample the analyzer's own vitals without running an
/// analysis, with the same collector and metric names as the daemon's, so a dashboard never
/// sees two versions of a number. The exit code carries the status for scripts that cannot
/// parse: 0 ok or unknown, 1 warn, 2 critical.
pub(crate) fn health(args: &[String]) -> Result<ExitCode, Error> {
    let flags = Flags::parse(
        args,
        &[
            "--data-dir",
            "--fleet-id",
            "--out",
            "--format",
            "--analyzer-id",
            "--clickhouse",
        ],
    )?;
    let data_dir = flags.required("--data-dir", "health")?;
    let fleet_id = flags.required("--fleet-id", "health")?;
    let format = flags
        .parsed::<HealthFormat>("--format", "text, json or prometheus")?
        .unwrap_or_default();
    let out_dir = flags.value("--out").unwrap_or(data_dir);
    let bin = settings::resolve_bin(flags.value("--clickhouse"));
    let session = analyzer_serving_clickhouse::engine::server::Session::open(&bin, data_dir)?;
    // The input log lives in the deployment's bucket, which this one-shot is not given: its
    // figures stay unmeasured rather than read from somewhere that is not the daemon's lake.
    let vitals = analyzer_serving_clickhouse::selfhealth::collect(
        session.engine(),
        None,
        fleet_id,
        &analyzer_serving_clickhouse::selfhealth::Paths {
            data_dir: Path::new(data_dir),
            out_dir: Path::new(out_dir),
            ledger_retention_days:
                analyzer_serving_clickhouse::engine::DEFAULT_LEDGER_RETENTION_DAYS,
            analyzer_id: flags.value("--analyzer-id"),
            // This invocation owns no server, ran no tick and derived no figures: those
            // series stay unmeasured rather than claiming a healthy loop.
            server: None,
            loop_status: None,
            fleet: None,
            runs: None,
        },
    )?;
    match format {
        HealthFormat::Prometheus => print!("{}", vitals.to_prometheus()),
        HealthFormat::Json => println!("{}", vitals.to_json_string()?),
        HealthFormat::Text => {
            println!("self-health: {}", vitals.status().as_str());
            for c in &vitals.checks {
                println!("  [{}] {}: {}", c.status.as_str(), c.name, c.message);
            }
        }
    }
    Ok(match vitals.status() {
        analyzer_serving_clickhouse::selfhealth::Status::Critical => ExitCode::from(2),
        analyzer_serving_clickhouse::selfhealth::Status::Warn => ExitCode::from(1),
        _ => ExitCode::SUCCESS,
    })
}

/// The flags `preflight` takes: the deployment's subset, layered the same way as `run`'s.
const PREFLIGHT_FLAGS: &[&str] = &[
    "--config",
    "--log-level",
    "--log-format",
    "--bucket-url",
    "--bucket-prefix",
    "--data-dir",
    "--out",
    "--clickhouse",
];

/// `preflight`: validate a deployment before the pilot. Prints every check with its
/// verdict; exit 1 on refusal, so an install script can gate on it.
pub(crate) fn preflight(args: &[String]) -> Result<ExitCode, Error> {
    let flags = settings::layered_flags(args, PREFLIGHT_FLAGS)?;
    let data_dir = flags.required("--data-dir", "preflight")?;
    let bin = settings::resolve_bin(flags.value("--clickhouse"));
    let out_dir = flags.value("--out").unwrap_or(data_dir);
    let bucket = settings::bucket_config(&flags, "preflight")?;
    let result = analyzer_daemon::preflight::run(&analyzer_daemon::preflight::Deployment {
        bucket: Some(&bucket),
        data_dir: Path::new(data_dir),
        out_dir: Path::new(out_dir),
        clickhouse_bin: &bin,
    });
    for c in &result.checks {
        println!("{:<22} {:<8} {}", c.name, c.status.as_str(), c.message);
    }
    if result.refused() {
        eprintln!("preflight: REFUSED — fix the critical checks above before the pilot");
        Ok(ExitCode::FAILURE)
    } else {
        println!("preflight: ok — this deployment is inside the envelope and can start");
        Ok(ExitCode::SUCCESS)
    }
}

/// `diag`: the support bundle — the deployment's own artifacts plus version and directory
/// facts, and NOTHING else. No record, no segment, no identifier leaves with it.
pub(crate) fn diag(args: &[String]) -> Result<ExitCode, Error> {
    use std::fmt::Write as _;
    let flags = Flags::parse(args, &["--from", "--out"])?;
    let from = Path::new(flags.required(
        "--from",
        "diag (the deployment's artifact directory, where health.json lives)",
    )?);
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let bundle = Path::new(flags.value("--out").unwrap_or(".")).join(format!("diag-{stamp}"));
    std::fs::create_dir_all(&bundle)
        .map_err(|e| Error::io(format!("creating {}", bundle.display()), e))?;
    let mut manifest = format!(
        "infertap-analyzer diag bundle\nversion: {}\nplatform: {}-{}\nconsole: {}\n\
         collected_unix: {stamp}\n\
         contains ZERO records: only the named artifacts below are copied.\n\n",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        // Which console this binary carries. A support bundle that cannot say is a support
        // conversation that starts by guessing.
        analyzer_api::console::stamp()
            .map_or_else(|| "ABSENT".to_string(), |(v, hash)| format!("{v} {hash}")),
    );
    // The artifacts the deployment already emits about itself — copied, never derived here,
    // so the bundle states what the box was actually saying at collection time.
    for name in ["health.json", "health.prom"] {
        match std::fs::copy(from.join(name), bundle.join(name)) {
            Ok(bytes) => {
                let _ = writeln!(manifest, "{name}: {bytes} bytes");
            }
            Err(_) => {
                let _ = writeln!(manifest, "{name}: ABSENT in {}", from.display());
            }
        }
    }
    std::fs::write(bundle.join("MANIFEST.txt"), manifest)
        .map_err(|e| Error::io("writing manifest", e))?;
    println!(
        "diag: wrote {} — health artifacts and versions; zero records",
        bundle.display()
    );
    Ok(ExitCode::SUCCESS)
}
