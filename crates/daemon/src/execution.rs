// SPDX-License-Identifier: AGPL-3.0-only

//! Bind daemon configuration to the executor over the daemon's spine.

use std::sync::Arc;

impl super::Daemon {
    pub(super) fn executor(
        &self,
    ) -> Result<analyzer_pipeline::Executor<'_>, analyzer_pipeline::Error> {
        let executor = analyzer_pipeline::Executor::open(
            self.spine(),
            &self.config.params,
            Arc::clone(&self.serving),
        )?
        .with_modules(self.modules.clone());
        Ok(match self.config.classify_workers {
            Some(lanes) => executor.with_lanes(lanes),
            None => executor,
        })
    }
}
