// SPDX-License-Identifier: AGPL-3.0-only

//! The analyzer's logger: every diagnostic is one line on stderr, with a level, in text or
//! JSON — the tap's logger, line for line, so the two products render one shape.
//!
//! stderr and never a file: the journal and the container runtime capture it, rotate it and
//! stamp it, and a daemon that kept its own log file would need a second disk bound. stdout
//! stays clean for data (`schema dump`, `engine-config dump`, `token` write there). The
//! library crates log through the `log` facade and print nothing themselves; this is the one
//! place a line is formatted, so a pipeline sees one shape.
//!
//! Text is `<ts> <LEVEL> <target>: <msg>`. Under systemd (the `JOURNAL_STREAM` variable is
//! set) the timestamp is dropped — the journal has its own — and the line carries the `<N>`
//! priority prefix the journal reads, so `journalctl -p warning` filters our warnings with no
//! syslog library. JSON is one object per line with `ts`, `level`, `target` and `msg`, the
//! shape a log pipeline parses without a regex.

use crate::error::Error;
use log::{Level, LevelFilter, Log, Metadata, Record};
use std::fmt::Write as _;
use std::io::Write as _;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// How a line is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LogFormat {
    /// One human-readable line: timestamp (outside the journal), level, target, message.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

impl FromStr for LogFormat {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            other => Err(Error::usage(format!(
                "log format {other:?}: expected text or json"
            ))),
        }
    }
}

impl LogFormat {
    fn as_u8(self) -> u8 {
        match self {
            Self::Text => 0,
            Self::Json => 1,
        }
    }

    fn from_u8(n: u8) -> Self {
        if n == 1 {
            Self::Json
        } else {
            Self::Text
        }
    }
}

/// The level flag's value: the `log` facade's filter, parsed the way the flags take it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogLevel(pub(crate) LevelFilter);

impl Default for LogLevel {
    fn default() -> Self {
        Self(LevelFilter::Info)
    }
}

impl FromStr for LogLevel {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "error" => Ok(Self(LevelFilter::Error)),
            "warn" | "warning" => Ok(Self(LevelFilter::Warn)),
            "info" => Ok(Self(LevelFilter::Info)),
            "debug" => Ok(Self(LevelFilter::Debug)),
            "trace" => Ok(Self(LevelFilter::Trace)),
            other => Err(Error::usage(format!(
                "log level {other:?}: expected error, warn, info, debug or trace"
            ))),
        }
    }
}

/// The level and format the logger runs with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct LoggingOptions {
    /// The most verbose level rendered.
    pub(crate) level: LogLevel,
    /// The line's shape.
    pub(crate) format: LogFormat,
}

/// The installed logger. Its settings are atomics so a configuration file's keys, layered
/// after the logger exists, can still take effect.
#[derive(Debug)]
struct Logger {
    format: AtomicU8,
    /// stderr is the journal: drop the timestamp the journal supplies and add the priority
    /// prefix it reads.
    journald: AtomicBool,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let line = format_line(
            LogFormat::from_u8(self.format.load(Ordering::Relaxed)),
            self.journald.load(Ordering::Relaxed),
            &rfc3339(SystemTime::now()),
            record.level(),
            record.target(),
            &record.args().to_string(),
        );
        // A failed write to stderr has nowhere to be reported; the line is lost, the process
        // is not.
        let stderr = std::io::stderr();
        let mut h = stderr.lock();
        let _ = h.write_all(line.as_bytes());
        let _ = h.write_all(b"\n");
    }

    fn flush(&self) {
        let _ = std::io::stderr().flush();
    }
}

static LOGGER: Logger = Logger {
    format: AtomicU8::new(0),
    journald: AtomicBool::new(false),
};

/// Install the logger. Called once, before anything else runs; a second call is a no-op.
pub(crate) fn init(options: LoggingOptions) {
    LOGGER.journald.store(
        std::env::var_os("JOURNAL_STREAM").is_some(),
        Ordering::Relaxed,
    );
    if log::set_logger(&LOGGER).is_ok() {
        apply(options);
    }
}

/// Re-apply the level and format after a configuration layer supplied them: the logger
/// was installed from the command line and the environment before the file was read.
pub(crate) fn apply(options: LoggingOptions) {
    log::set_max_level(options.level.0);
    LOGGER
        .format
        .store(options.format.as_u8(), Ordering::Relaxed);
}

/// The journal's priority for a level: syslog numbers, which is what the `<N>` prefix carries.
const fn syslog_priority(level: Level) -> u8 {
    match level {
        Level::Error => 3,
        Level::Warn => 4,
        Level::Info => 6,
        Level::Debug | Level::Trace => 7,
    }
}

/// One rendered line, without its newline. Pure, so the shape is testable.
fn format_line(
    format: LogFormat,
    journald: bool,
    ts: &str,
    level: Level,
    target: &str,
    msg: &str,
) -> String {
    match format {
        LogFormat::Json => format!(
            "{{\"ts\":{},\"level\":{},\"target\":{},\"msg\":{}}}",
            json_string(ts),
            json_string(&level.as_str().to_ascii_lowercase()),
            json_string(target),
            json_string(msg)
        ),
        LogFormat::Text => {
            let mut s = String::with_capacity(msg.len() + 64);
            if journald {
                let _ = write!(s, "<{}>", syslog_priority(level));
            } else {
                s.push_str(ts);
                s.push(' ');
            }
            let _ = write!(s, "{:<5} {target}: {msg}", level.as_str());
            s
        }
    }
}

/// A JSON string literal, escaped as the format requires. Serialising a `str` cannot fail.
fn json_string(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ`, UTC, from the epoch arithmetic alone — the civil date by the
/// pulled in for one timestamp.
fn rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = i64::try_from(d.as_secs()).unwrap_or(i64::MAX);
    let millis = d.subsec_millis();
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, dd) = analyzer_analysis::emitted::civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{dd:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

#[cfg(test)]
mod tests {
    use super::{format_line, json_string, rfc3339, LogFormat, LogLevel};
    use log::{Level, LevelFilter};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn text_carries_the_timestamp_outside_the_journal_and_the_priority_inside_it() {
        let plain = format_line(
            LogFormat::Text,
            false,
            "2026-09-16T18:00:00.000Z",
            Level::Info,
            "analyzer_daemon::run",
            "pass day 20703: finish 37.3s",
        );
        assert_eq!(
            plain,
            "2026-09-16T18:00:00.000Z INFO  analyzer_daemon::run: pass day 20703: finish 37.3s"
        );
        let journal = format_line(
            LogFormat::Text,
            true,
            "ignored",
            Level::Warn,
            "analyzer_daemon::run",
            "pass failed, will retry next cadence",
        );
        assert_eq!(
            journal, "<4>WARN  analyzer_daemon::run: pass failed, will retry next cadence",
            "the journal supplies the stamp and reads the priority"
        );
    }

    #[test]
    fn json_is_one_escaped_object_per_line() {
        let line = format_line(
            LogFormat::Json,
            true,
            "2026-09-16T18:00:00.000Z",
            Level::Error,
            "t",
            "a \"quoted\" line\nwith a break",
        );
        assert_eq!(
            line,
            "{\"ts\":\"2026-09-16T18:00:00.000Z\",\"level\":\"error\",\"target\":\"t\",\
             \"msg\":\"a \\\"quoted\\\" line\\nwith a break\"}"
        );
        assert_eq!(json_string("\u{1}"), "\"\\u0001\"");
    }

    #[test]
    fn levels_and_formats_parse_as_the_flags_take_them() {
        assert_eq!(
            "warning".parse::<LogLevel>().map(|l| l.0).ok(),
            Some(LevelFilter::Warn)
        );
        assert_eq!(
            "debug".parse::<LogLevel>().map(|l| l.0).ok(),
            Some(LevelFilter::Debug)
        );
        assert!("loud".parse::<LogLevel>().is_err());
        assert_eq!("json".parse::<LogFormat>().ok(), Some(LogFormat::Json));
        assert!("yaml".parse::<LogFormat>().is_err());
    }

    #[test]
    fn the_timestamp_is_utc_to_the_millisecond() {
        // 2026-09-07T00:00:01.250Z: day index 20 703, one second and a quarter in.
        let t = UNIX_EPOCH + Duration::from_millis(20_703 * 86_400_000 + 1_250);
        assert_eq!(rfc3339(t), "2026-09-07T00:00:01.250Z");
        assert_eq!(rfc3339(SystemTime::UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
    }
}
