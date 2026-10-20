// SPDX-License-Identifier: AGPL-3.0-only

//! `infertap-analyzer`: the shell composing the workspace crates.
//!
//! Eleven commands. `run` is the deployment form: the daemon on its own cadences, its
//! `/metrics` listener, and the read API serving the embedded console; `reset-derived` drops
//! what a `run` derived and keeps its inputs. `preflight` validates a deployment before the
//! pilot, `selftest` runs the shipped conformance corpus through the install, `health` samples
//! the vitals once, `diag` writes a support bundle, `annotate` declares a deploy into the
//! bucket, `token` mints and lists the viewer credential, `schema dump` and
//! `engine-config dump` are the compliance exhibits, and `version` says what is in the binary.
//! This file names the commands and dispatches them; each lives in the module of its name,
//! and `settings` holds what they resolve in common.
//!
//! Every option is a `--flag value` pair. `run`, `preflight` and `annotate` layer a TOML file and
//! `INFERTAP_*` environment names beneath their arguments in the same vocabulary (`flags`);
//! flag beats environment beats file. `CLICKHOUSE_BIN` is consulted only when `--clickhouse`
//! was not given: an explicit flag never loses to ambient environment.

mod annotate;
mod checks;
mod error;
mod exhibits;
mod flags;
mod logging;
mod run;
mod selftest;
mod settings;
mod token;

use error::Error;
use std::process::ExitCode;

/// The binary's allocator is jemalloc, so resident memory means live bytes: the fold's
/// memory line and the lane cost it learns read the process's resident size, and glibc's
/// arenas keep a fold's freed memory resident. jemalloc's background thread returns dirty
/// pages on its decay, so what the cgroup sees tracks what the daemon holds, the same
/// property the cloud's containers meter on. The workspace forbids unsafe code, which rules
/// out an exported `malloc_conf`; the defaults with the background thread are the
/// configuration.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// The commands, each with its one-line synopsis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Run,
    ResetDerived,
    Preflight,
    Selftest,
    Health,
    Diag,
    Annotate,
    Token,
    SchemaDump,
    EngineConfigDump,
    Version,
}

impl Command {
    /// The command the leading arguments name, and how many of them it consumed.
    fn parse(args: &[String]) -> Option<(Self, usize)> {
        let first = args.first().map(String::as_str)?;
        let second = args.get(1).map(String::as_str);
        Some(match (first, second) {
            ("run", _) => (Self::Run, 1),
            ("reset-derived", _) => (Self::ResetDerived, 1),
            ("preflight", _) => (Self::Preflight, 1),
            ("selftest", _) => (Self::Selftest, 1),
            ("health", _) => (Self::Health, 1),
            ("diag", _) => (Self::Diag, 1),
            ("annotate", _) => (Self::Annotate, 1),
            ("token", _) => (Self::Token, 1),
            ("version" | "--version" | "-V", _) => (Self::Version, 1),
            ("schema", Some("dump")) => (Self::SchemaDump, 2),
            ("engine-config", Some("dump")) => (Self::EngineConfigDump, 2),
            _ => return None,
        })
    }

    /// The command's synopsis, for a usage error.
    fn synopsis(self) -> &'static str {
        match self {
            Self::Run => {
                "run --bucket-url <URL> --fleet-id <ID> --data-dir <DIR> \
                 [--ledger-retention-days <N, floor 4>] [--rollup-retention-years <N, floor 1>] \
                 [--hour-retention-days <N, floor 14>] [--minute-retention-days <N, floor 3>] \
                 [--instance-detail-retention-days <N, floor 14>] \
                 [--segment-cache-bytes <N, floor 64 MiB>] \
                 [--classify-workers <N>] [--ingest-workers <N>] [--disable-module <NAME>]... \
                 [--log-level error|warn|info|debug|trace] [--log-format text|json] \
                 [--config <PATH>] [--bucket-prefix <P>] [--out <DIR>] \
                 [--metrics-addr HOST:PORT|off] [--api-addr HOST:PORT|off] \
                 [--api-cookie-secure on|off] [--api-dev-origin <ORIGIN>] \
                 [--ingest-interval <SECONDS>] [--maintain-interval <SECONDS>] [--http-port N] [--tcp-port N] [--clickhouse <BIN>] \
                 (the deployment form: the daemon, its /metrics listener, the read API serving \
                 the console at --api-addr (default 127.0.0.1:8088; `off` disables it), and its \
                 health artifacts)"
            }
            Self::ResetDerived => {
                "reset-derived --bucket-url <URL> --fleet-id <ID> --data-dir <DIR> \
                 [--bucket-prefix <P>] [--config <PATH>] [run's other flags] (deletes this \
                 deployment's derived state and keeps its input log, so the next `run` \
                 derives afresh from the log. Run it with the daemon stopped.)"
            }
            Self::Preflight => {
                "preflight --data-dir <DIR> --bucket-url <URL> [--bucket-prefix <P>] [--out <DIR>] \
                 [--clickhouse <BIN>] [--config <PATH>] (validate a deployment BEFORE the pilot; \
                 exit 1 on refusal)"
            }
            Self::Selftest => {
                "selftest [--scratch <DIR>] [--out <DIR>] [--clickhouse <BIN>] (the shipped \
                 conformance corpus through THIS install; exit 1 on divergence)"
            }
            Self::Health => {
                "health --data-dir <DIR> --fleet-id <ID> [--out <DIR>] \
                 [--format text|json|prometheus] [--analyzer-id <ID>] [--clickhouse <BIN>] (the \
                 analyzer's own vitals; exit 1 on warn, 2 on critical)"
            }
            Self::Diag => {
                "diag --from <OUT_DIR> [--out <DIR>] (support bundle: health artifacts, versions, \
                 directory stats; zero records)"
            }
            Self::Annotate => {
                "annotate [--config <FILE>] --bucket-url <URL> --at <YYYY-MM-DDTHH:MM:SSZ> \
                 --label <TEXT> [--scope <TEXT>] (declare a deploy into the bucket: a label and a \
                 UTC instant, drawn on the metric charts once the daemon's next ingest cycle \
                 records it; re-running with the same instant and label replaces it)"
            }
            Self::Token => {
                "token issue --data-dir <DIR> [--label <NAME>] / token rotate --data-dir <DIR> \
                 --id <ID> / token list --data-dir <DIR> (the read API's viewer credential; the \
                 plaintext prints ONCE to stdout and only its SHA-256 reaches disk)"
            }
            Self::SchemaDump => {
                "schema dump --data-dir <DIR> [--clickhouse <BIN>] (the compliance exhibit)"
            }
            Self::EngineConfigDump => {
                "engine-config dump --data-dir <DIR> [--http-port N] [--tcp-port N] [--user NAME] \
                 (the engine's own configuration exhibit: what the server reads, or would)"
            }
            Self::Version => {
                "version (what is in this binary: the analyzer's version, the platform, and the \
                 embedded console's version and content hash)"
            }
        }
    }

    const ALL: [Self; 11] = [
        Self::Run,
        Self::ResetDerived,
        Self::Preflight,
        Self::Selftest,
        Self::Health,
        Self::Diag,
        Self::Annotate,
        Self::Token,
        Self::SchemaDump,
        Self::EngineConfigDump,
        Self::Version,
    ];

    /// Run the command over its own arguments.
    fn execute(self, args: &[String]) -> Result<ExitCode, Error> {
        match self {
            Self::Run => run::run(args),
            Self::ResetDerived => run::reset_derived(args),
            Self::Preflight => checks::preflight(args),
            Self::Selftest => selftest::command(args),
            Self::Health => checks::health(args),
            Self::Diag => checks::diag(args),
            Self::Annotate => annotate::annotate(args),
            Self::Token => token::token(args),
            Self::SchemaDump => exhibits::schema_dump(args),
            Self::EngineConfigDump => exhibits::engine_config_dump(args),
            Self::Version => Ok(version()),
        }
    }
}

/// Every command's synopsis, for an unknown command.
fn usage() -> String {
    let mut out = String::from("commands:\n");
    for command in Command::ALL {
        out.push_str("  ");
        out.push_str(command.synopsis());
        out.push('\n');
    }
    out
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // The logger first, before any command speaks.
    let options = match settings::logging_options(&args, std::env::vars()) {
        Ok(v) => v,
        Err(e) => return refuse(&e),
    };
    logging::init(options);
    let Some((command, consumed)) = Command::parse(&args) else {
        eprintln!("infertap-analyzer: {}", usage());
        return ExitCode::from(2);
    };
    match command.execute(&args[consumed..]) {
        Ok(code) => code,
        Err(e) => refuse(&e),
    }
}

/// One line on stderr and the failure's exit code. Usage errors go here rather than to the
/// log, so a wrong flag is visible whatever `--log-level` says.
fn refuse(e: &Error) -> ExitCode {
    eprintln!("infertap-analyzer: {e}");
    e.exit_code()
}

/// What is in this binary, answerable offline from the binary itself. An artifact that
/// cannot say what it contains turns every support conversation into a guess about which
/// bundle is deployed, and the console is compiled in, so "which console" has no other way
/// to be asked. The console hash is the content hash from `console/PIN.toml`, verified at
/// build time against the bytes actually embedded.
fn version() -> ExitCode {
    println!("infertap-analyzer {}", env!("CARGO_PKG_VERSION"));
    println!(
        "platform: {}-{}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    match analyzer_api::console::stamp() {
        Some((version, hash)) => println!("console: {version} {hash}"),
        // Not an error and not a warning: a build with no console is legitimate (a clean
        // checkout), and it is exactly the build whose `console_not_configured` refusal an
        // operator might be trying to explain.
        None => println!("console: ABSENT — this build embeds no console bundle"),
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::Command;

    #[test]
    fn every_command_parses_by_its_name_and_a_stranger_does_not() {
        for command in Command::ALL {
            // The synopsis opens with the words that invoke the command.
            let words: Vec<String> = command
                .synopsis()
                .split(' ')
                .take_while(|w| !w.starts_with('-') && !w.starts_with('['))
                .map(str::to_string)
                .collect();
            let consumed = if matches!(command, Command::SchemaDump | Command::EngineConfigDump) {
                2
            } else {
                1
            };
            assert_eq!(
                Command::parse(&words[..consumed]),
                Some((command, consumed)),
                "{words:?}"
            );
        }
        assert_eq!(Command::parse(&["schema".to_string()]), None);
        assert_eq!(Command::parse(&["frobnicate".to_string()]), None);
        assert_eq!(Command::parse(&[]), None);
    }
}
