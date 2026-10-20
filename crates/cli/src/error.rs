// SPDX-License-Identifier: AGPL-3.0-only

//! What a command can fail with: a usage problem the shell itself found, or any crate's
//! error passing through. The shell prints one line and exits; it matches only to choose
//! the exit code.

use std::process::ExitCode;

/// What a command can fail with.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// The command line, the environment or the configuration file: a flag missing, unknown,
    /// without a value, or with a value of the wrong shape.
    #[error("{0}")]
    Usage(String),
    /// A configuration file that does not read, parse, or name known keys.
    #[error("--config {path}: {message}")]
    Config {
        /// The file, as named.
        path: String,
        /// What was wrong with it.
        message: String,
    },
    /// The daemon refused to start, or its loop exited.
    #[error(transparent)]
    Daemon(#[from] analyzer_daemon::Error),
    /// The engine boundary failed.
    #[error(transparent)]
    Storage(#[from] analyzer_serving_clickhouse::engine::Error),
    /// A tick, a module or a health collection failed.
    #[error(transparent)]
    Pipeline(#[from] analyzer_pipeline::Error),
    /// A read or a marker write failed.
    #[error(transparent)]
    Dao(#[from] analyzer_serving_clickhouse::Error),
    /// The ingest failed.
    #[error(transparent)]
    Ingest(#[from] analyzer_ingest::Error),
    /// The read API's listener, token store or state failed.
    #[error(transparent)]
    Api(#[from] analyzer_api::Error),
    /// A file or a thread the shell itself needed.
    #[error("{what}: {source}")]
    Io {
        /// What was being done.
        what: String,
        /// The system's own error.
        #[source]
        source: std::io::Error,
    },
    /// A document the shell itself renders or parses.
    #[error("{what}: {source}")]
    Json {
        /// What was being rendered or parsed.
        what: String,
        /// The parser's own error.
        #[source]
        source: serde_json::Error,
    },
}

impl Error {
    /// A usage error, exit code 2.
    pub(crate) fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    /// An I/O failure with what it was doing.
    pub(crate) fn io(what: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            what: what.into(),
            source,
        }
    }

    /// A JSON failure with what it was rendering or parsing.
    pub(crate) fn json(what: impl Into<String>, source: serde_json::Error) -> Self {
        Self::Json {
            what: what.into(),
            source,
        }
    }

    /// The process exit code this failure maps to: 2 for a usage problem (the convention
    /// every shell tool follows), 1 for everything else.
    pub(crate) fn exit_code(&self) -> ExitCode {
        match self {
            Self::Usage(_) | Self::Config { .. } => ExitCode::from(2),
            _ => ExitCode::FAILURE,
        }
    }
}
