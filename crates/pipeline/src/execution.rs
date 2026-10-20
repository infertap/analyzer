// SPDX-License-Identifier: AGPL-3.0-only

//! Execution identity and local exclusion. One namespace owns one serving database.
//! The durable binding moves with that database; a connection address is only a local
//! exclusion key. Distributed publication requires fencing at the commit authority,
//! which this local implementation does not claim to provide.

use crate::Error;

static WRITERS: std::sync::LazyLock<std::sync::Mutex<std::collections::BTreeSet<String>>> =
    std::sync::LazyLock::new(std::sync::Mutex::default);

/// Whether this process holds the mutation capability over `lake`'s lineage.
#[must_use]
pub fn is_writing(lake: &analyzer_lake::Lake) -> bool {
    WRITERS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(&lake.identity())
}

/// A non-reentrant local mutation capability over one lineage, including input commit
/// ordering: two operations of one process never mutate a lineage at once. Across processes
/// the lineage's lease fences the writer.
pub struct Writer(String);

impl Writer {
    /// Reject an overlapping mutation of `lake`'s lineage in this process.
    ///
    /// # Errors
    /// Another operation of this process holds the lineage.
    pub fn acquire(lake: &analyzer_lake::Lake) -> Result<Self, Error> {
        let key = lake.identity();
        let mut writers = WRITERS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !writers.insert(key.clone()) {
            return Err(Error::Derivation("execution writer already active".into()));
        }
        Ok(Self(key))
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        WRITERS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.0);
    }
}
use serde::{Deserialize, Serialize};

/// The logical partition currently supported by the derivation engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Partition {
    /// All identities and fleet context. Execution stripes are not movable partitions.
    WholeFleet,
}

/// A portable tenant/source namespace and fleet, supplied by the deployment owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Namespace {
    source: String,
    fleet: String,
    partition: Partition,
}

impl Namespace {
    /// Construct a whole-fleet namespace. `source` is an opaque stable tenant/source ID,
    /// not a worker, local cache path or serving endpoint.
    ///
    /// # Errors
    /// Empty source or fleet identity.
    pub fn new(source: impl Into<String>, fleet: impl Into<String>) -> Result<Self, Error> {
        let (source, fleet) = (source.into(), fleet.into());
        if source.trim().is_empty() || fleet.trim().is_empty() {
            return Err(Error::Derivation(
                "execution source and fleet must be nonempty".into(),
            ));
        }
        Ok(Self {
            source,
            fleet,
            partition: Partition::WholeFleet,
        })
    }

    /// Fleet owned by this namespace.
    #[must_use]
    pub fn fleet(&self) -> &str {
        &self.fleet
    }

    /// Collision-resistant namespace component for immutable objects and checkpoints.
    #[must_use]
    pub fn object_prefix(&self) -> String {
        use sha2::{Digest, Sha256};
        format!(
            "namespaces/{:x}/",
            Sha256::digest(self.identity().as_bytes())
        )
    }

    /// This namespace as one string: what a serving store records it is bound to.
    ///
    /// # Panics
    /// Never: a namespace is strings, and strings serialize.
    #[must_use]
    pub fn identity(&self) -> String {
        serde_json::to_string(self).expect("namespace is strings")
    }

    /// The namespace an [`Namespace::identity`] names.
    ///
    /// # Errors
    /// The string is not a namespace's identity.
    pub fn from_identity(identity: &str) -> Result<Self, Error> {
        serde_json::from_str(identity).map_err(|e| Error::Row(e.to_string()))
    }

    /// Bind a serving store to this namespace. Reopening on a different worker is valid; a
    /// store bound to another namespace is refused.
    ///
    /// # Errors
    /// A store bound to another namespace, or a store failure.
    pub fn prepare(
        &self,
        serving: &dyn crate::publish::Serving,
        spine: &crate::spine::Spine,
    ) -> Result<(), Error> {
        spine.require_namespace(self)?;
        let _writer = Writer::acquire(&spine.lake())?;
        serving.bind(self)
    }
}

/// The next revision after every clock in `head`: one clock orders every family's
/// revisions, so no two operations ever share one.
pub(crate) fn next_revision(head: &crate::journal::JournalHead) -> Option<u64> {
    head.clocks
        .iter()
        .map(|(_, revision)| *revision)
        .max()
        .unwrap_or(0)
        .checked_add(1)
}

/// Set `kind`'s clock in `head` to `revision`.
pub(crate) fn set_clock(head: &mut crate::journal::JournalHead, kind: &str, revision: u64) {
    crate::journal::upsert(&mut head.clocks, (kind.to_string(), revision), |(k, _)| {
        k.clone()
    });
}

/// The highest revision among `kinds`' clocks (every clock when `kinds` is empty), 0 before
/// any.
pub(crate) fn clock(head: &crate::journal::JournalHead, kinds: &[&str]) -> u64 {
    head.clocks
        .iter()
        .filter(|(kind, _)| kinds.is_empty() || kinds.contains(&kind.as_str()))
        .map(|(_, revision)| *revision)
        .max()
        .unwrap_or(0)
}

/// Allocate before publishing a mutation or acknowledgment. The caller holds Writer.
/// A lost clock-write acknowledgment only skips a revision; it never reuses one.
pub(crate) fn advance(journal: &crate::journal::Journal, kind: &str) -> Result<u64, Error> {
    if next_revision(&journal.read()?).is_none() {
        return Err(Error::Derivation("execution revision exhausted".into()));
    }
    let head = journal.commit(|head| {
        if let Some(next) = next_revision(head) {
            set_clock(head, kind, next);
        }
    })?;
    Ok(clock(&head, &[kind]))
}

/// The namespace's committed revision, or `None` while a keyed tick's intent is unapplied or
/// the serving store has not settled what it was told. Validate before and after a consistent
/// read.
///
/// # Errors
/// Store failure, malformed standing, or revision exhaustion.
pub fn revision(
    serving: &dyn crate::publish::Serving,
    spine: &crate::spine::Spine,
) -> Result<Option<u64>, Error> {
    if is_writing(&spine.lake()) {
        return Ok(None);
    }
    let head = crate::journal::Journal::new(spine.lake())?.read()?;
    if head.keyed.is_some_and(|tick| !tick.applied) {
        return Ok(None);
    }
    // A completed journal entry does not mean readers see the write as stated: the serving
    // store says when they do.
    if !serving.settled()? {
        return Ok(None);
    }
    // The clock comes from the same head as the standing. A writer that begins after this
    // read advances the clock at its intent; the reader validates both sides of its read.
    Ok(Some(clock(&head, &[])))
}
