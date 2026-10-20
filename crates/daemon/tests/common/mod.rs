// SPDX-License-Identifier: AGPL-3.0-only

//! Shared by the daemon's test binaries (`loop.rs`, `ttl_gate.rs`). Two binaries exist
//! because process env is process-global: `loop.rs` deliberately poisons `AWS_ENDPOINT`
//! for its dead-bucket premise, and a live-bucket test in that process would inherit the
//! poison however it built its own config.

use std::path::{Path, PathBuf};

/// The fleet every rig daemon is keyed by.
pub const FLEET: &str = "daemon-fleet";

/// A scratch directory, fresh on creation and removed when dropped, however the test ends.
pub struct Scratch(PathBuf);

impl Scratch {
    pub fn new(prefix: &str, label: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("{prefix}-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch");
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runtime retention target, also used by source discovery. Actual retirement still
/// requires verified export evidence for each day.
pub fn horizon_days(daemon: &analyzer_daemon::Daemon) -> u32 {
    u32::try_from(daemon.source_config().horizon_hours.hours() / 24).expect("horizon days")
}
