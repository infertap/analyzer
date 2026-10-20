// SPDX-License-Identifier: AGPL-3.0-only

//! What every operation runs against: the spine, which is the lineage's lake and its
//! namespace, and the serving store the core publishes to and never reads.

use crate::publish::Serving;
use crate::Error;

/// An operation's context: the spine and the serving store. Two references, so it is copied
/// into every call rather than threaded as its parts.
#[derive(Clone, Copy)]
pub(crate) struct Core<'a> {
    /// The lineage's lake, its namespace and its chunk store.
    pub(crate) spine: &'a crate::spine::Spine,
    /// The serving store.
    pub(crate) serving: &'a std::sync::Arc<dyn Serving>,
}

impl<'a> Core<'a> {
    /// The fleet the namespace owns.
    pub(crate) fn fleet(&self) -> &'a str {
        self.spine.namespace().fleet()
    }

    /// The lineage's lake.
    pub(crate) fn lake(&self) -> analyzer_lake::Lake {
        self.spine.lake()
    }

    /// The lineage's execution journal.
    pub(crate) fn journal(&self) -> Result<crate::journal::Journal, Error> {
        crate::journal::Journal::new(self.spine.lake())
    }

    /// This process's exclusive mutation capability over the lineage.
    pub(crate) fn writer(&self) -> Result<crate::execution::Writer, Error> {
        crate::execution::Writer::acquire(&self.spine.lake())
    }

    /// Publishing to the lake, then to serving.
    pub(crate) fn published(&self) -> Result<crate::state::Published<'a>, Error> {
        crate::state::Published::to(&self.spine.lake(), &**self.serving)
    }
}
