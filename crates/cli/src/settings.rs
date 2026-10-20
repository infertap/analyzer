// SPDX-License-Identifier: AGPL-3.0-only

//! What a command resolves before it acts: the environment names the shell reads, the
//! listeners' defaults, the engine binary, the bucket and its credential, the logger's
//! options, and a layered command line parsed.

use crate::error::Error;
use crate::flags::{self, Flags};
use crate::logging::{self, LogFormat, LogLevel, LoggingOptions};
use std::path::PathBuf;

/// The switch a drill sets to run several daemons on one host: `off` waives the
/// one-analyzer-per-VM claim.
pub(crate) const ENV_HOST_CLAIM: &str = "INFERTAP_HOST_CLAIM";
/// The switch a rig sets to admit write-ahead JSONL segments: `on`.
pub(crate) const ENV_ADMIT_CORPUS_FORMS: &str = "INFERTAP_ADMIT_CORPUS_FORMS";
/// The bucket credential pair, from the environment and never argv: an argument list is
/// visible to every user on the box (`ps`), a process environment is not.
const ENV_S3_ACCESS_KEY_ID: &str = "INFERTAP_S3_ACCESS_KEY_ID";
/// See [`ENV_S3_ACCESS_KEY_ID`].
const ENV_S3_SECRET_ACCESS_KEY: &str = "INFERTAP_S3_SECRET_ACCESS_KEY";

/// Where `/metrics` listens unless told otherwise: loopback, on. The trust kit's claim is
/// that the analyzer is watchable out of the box; `off` is the explicit opt-out, and a
/// non-loopback bind is the operator's explicit decision.
pub(crate) const DEFAULT_METRICS_ADDR: &str = "127.0.0.1:9363";
/// Where the read API listens unless told otherwise: loopback, on, the same posture as
/// `/metrics`. Every comparable product serves its UI by default; a deployment that wants
/// no listener says so in one word.
pub(crate) const DEFAULT_API_ADDR: &str = "127.0.0.1:8088";

/// Wall clock in epoch milliseconds, for the token store's stamps. One definition, because
/// `token issue|rotate` and serve mode's first-run bootstrap stamp the same file.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        // Epoch-ms fits u64 until the year 584 million; the widening the lint warns about is
        // unreachable for any instant this product can hold.
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// CLI > env > default: the env var speaks only when the flag stayed silent.
pub(crate) fn resolve_bin(flag: Option<&str>) -> PathBuf {
    flag.map(PathBuf::from)
        .or_else(|| std::env::var_os("CLICKHOUSE_BIN").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("clickhouse"))
}

/// The deployment's bucket from `--bucket-url` and `--bucket-prefix`, with the credential the
/// environment holds; `requires` names the command for the refusal when the URL is absent.
pub(crate) fn bucket_config(
    flags: &Flags,
    requires: &str,
) -> Result<analyzer_daemon::BucketConfig, Error> {
    Ok(analyzer_daemon::BucketConfig {
        url: flags.required("--bucket-url", requires)?.to_string(),
        credentials: bucket_credentials(),
        key_prefix: flags
            .value("--bucket-prefix")
            .unwrap_or_default()
            .to_string(),
    })
}

/// The bucket credential pair, or `None` for the provider chain when no key is configured.
fn bucket_credentials() -> Option<analyzer_daemon::StaticCredentials> {
    let access_key_id = std::env::var(ENV_S3_ACCESS_KEY_ID).ok()?;
    if access_key_id.is_empty() {
        return None;
    }
    Some(analyzer_daemon::StaticCredentials {
        access_key_id,
        secret_access_key: std::env::var(ENV_S3_SECRET_ACCESS_KEY).unwrap_or_default(),
        session_token: None,
    })
}

/// The logger's level and format from the command line, else the environment, else the
/// defaults. Read before the command is parsed because the logger must exist before the
/// first line any command prints; a `--config` file's keys are re-applied by the commands
/// that layer one.
pub(crate) fn logging_options(
    args: &[String],
    env: impl Iterator<Item = (String, String)>,
) -> Result<LoggingOptions, Error> {
    let env: Vec<(String, String)> = env
        .filter(|(k, _)| k == "INFERTAP_LOG_LEVEL" || k == "INFERTAP_LOG_FORMAT")
        .collect();
    let from_argv = |flag: &str| {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1).cloned())
    };
    let from_env = |name: &str| env.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
    let level = from_argv("--log-level")
        .or_else(|| from_env("INFERTAP_LOG_LEVEL"))
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or_default();
    let format = from_argv("--log-format")
        .or_else(|| from_env("INFERTAP_LOG_FORMAT"))
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or_default();
    Ok(LoggingOptions { level, format })
}

/// The logging options a layered command line carries, when it carries any.
pub(crate) fn layered_logging(flags: &Flags) -> Result<LoggingOptions, Error> {
    Ok(LoggingOptions {
        level: flags
            .parsed::<LogLevel>("--log-level", "a level")?
            .unwrap_or_default(),
        format: flags
            .parsed::<LogFormat>("--log-format", "a format")?
            .unwrap_or_default(),
    })
}

/// A command's arguments with its file and environment layered beneath, parsed, and the
/// logger re-applied from what the layers supplied.
pub(crate) fn layered_flags(args: &[String], allowed: &[&'static str]) -> Result<Flags, Error> {
    let layered = flags::layer(args, allowed, std::env::vars(), |p| {
        std::fs::read_to_string(p)
    })?;
    let flags = Flags::parse(&layered, allowed)?;
    logging::apply(layered_logging(&flags)?);
    Ok(flags)
}

/// A listener address, or `None` for `off`.
pub(crate) fn listener_addr(
    flags: &Flags,
    flag: &str,
    default: &str,
) -> Result<Option<analyzer_daemon::SocketAddr>, Error> {
    match flags.value(flag).unwrap_or(default) {
        "off" => Ok(None),
        addr => addr
            .parse()
            .map(Some)
            .map_err(|e| Error::usage(format!("{flag} {addr:?}: {e}"))),
    }
}
