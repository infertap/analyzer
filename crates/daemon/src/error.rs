// SPDX-License-Identifier: AGPL-3.0-only

//! What the loop can fail with.
//!
//! The crates below fail as themselves and pass through unchanged, so the shell can still
//! tell which boundary failed. This crate's own variants are the refusals at the door (a
//! configuration below a floor, a host another analyzer holds, an environment check found
//! critical, a module set that cannot run), the wiring a start needs (a signal handler, a
//! listener, a thread), the loop's two exits of its own (a dead engine child, a sampler that
//! stopped publishing), and the two derivations this crate does itself.

use crate::metrics::SocketAddr;
use std::time::Duration;

/// What the loop can fail with.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The engine boundary failed; the error is the storage crate's.
    #[error(transparent)]
    Engine(#[from] analyzer_serving_clickhouse::engine::Error),
    /// A tick, a module or a ledger failed; the error is the pipeline's.
    #[error(transparent)]
    Pipeline(#[from] analyzer_pipeline::Error),
    /// The ingest failed; the error is the ingest crate's.
    #[error(transparent)]
    Ingest(#[from] analyzer_ingest::Error),
    /// A read failed; the error is the dao's.
    #[error(transparent)]
    Dao(#[from] analyzer_serving_clickhouse::Error),
    /// The object store failed; the error is the objectstore crate's.
    #[error(transparent)]
    Store(#[from] analyzer_objectstore::Error),
    /// A retention horizon below its floor; the message carries the floor's derivation.
    #[error(transparent)]
    Retention(analyzer_serving_clickhouse::engine::Error),
    /// A configuration value below the floor this crate sets for it.
    #[error("{flag} {value} is below the floor {floor}: {why}")]
    BelowFloor {
        /// The flag that carries the value.
        flag: &'static str,
        /// The value given.
        value: u64,
        /// The smallest value the daemon starts on.
        floor: u64,
        /// Why the floor is where it is.
        why: &'static str,
    },
    /// Another analyzer already holds this host's claim.
    #[error(
        "another analyzer already holds this host's claim ({detail}). One analyzer per VM \
         is structural (a ruling, not a limit to tune): check `systemctl status \
         infertap-analyzer` for the running one. Tests and drills that genuinely need two \
         set host_claim = false / INFERTAP_HOST_CLAIM=off."
    )]
    HostClaimed {
        /// What the claim attempt reported.
        detail: String,
    },
    /// The host claim could not be attempted: its address, or a stale claim file.
    #[error("host claim: {source}")]
    HostClaim {
        /// The system's own error.
        #[source]
        source: std::io::Error,
    },
    /// A module set that cannot run: a name no module declares, or a module switched off
    /// that an enabled one needs.
    #[error("--disable-module: {source}")]
    Modules {
        /// The registry's refusal.
        #[source]
        source: analyzer_analysis::Error,
    },
    /// An environment check the start found critical: a directory, or the clock.
    #[error("{check}: {message}")]
    Environment {
        /// The check's stable name.
        check: &'static str,
        /// What the check said.
        message: String,
    },
    /// A signal handler would not register.
    #[error("registering signal {signal}: {source}")]
    Signal {
        /// The signal number.
        signal: i32,
        /// The system's own error.
        #[source]
        source: std::io::Error,
    },
    /// The metrics listener would not bind.
    #[error("binding metrics listener {addr}: {source}")]
    Bind {
        /// The address asked for.
        addr: SocketAddr,
        /// The system's own error.
        #[source]
        source: std::io::Error,
    },
    /// A thread would not spawn.
    #[error("spawning the {name} thread: {source}")]
    Thread {
        /// The thread's name.
        name: &'static str,
        /// The system's own error.
        #[source]
        source: std::io::Error,
    },
    /// The engine child died; the loop exits so the init system restarts the unit through
    /// the ordered claim-and-recover path.
    #[error(
        "the engine child died; exiting so the init system restarts the unit through the \
         ordered claim-and-recover path"
    )]
    EngineDied,
    /// The health sampler published nothing for too long; the loop exits rather than work
    /// unobserved.
    #[error(
        "the health sampler has published nothing for {}s ({intervals} health intervals) — \
         it is dead or wedged, and a loop that keeps working while nothing can observe it is \
         the one failure an unattended deployment cannot afford. Exiting so the init system \
         restarts the unit.",
        age.as_secs()
    )]
    SamplerStale {
        /// How long since the last publication.
        age: Duration,
        /// How many health intervals that is.
        intervals: u32,
    },
    /// A count the engine returned did not parse.
    #[error("{what}: unparseable count: {source}")]
    UnparseableCount {
        /// Which count.
        what: &'static str,
        /// The parser's own error.
        #[source]
        source: std::num::ParseIntError,
    },
    /// No classified day exists to anchor the stamped window to.
    #[error("no classified day yet, so the stamped window has nothing to anchor to")]
    NoClassifiedDay,
}
