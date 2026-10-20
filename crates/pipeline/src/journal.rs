// SPDX-License-Identifier: AGPL-3.0-only

//! The execution journal, kept in the object store: one head object per lineage (a
//! namespace's derivation, see [`crate::lineage`]), advanced only by a conditional write
//! (`docs/lake.md`). A commit holds the lineage's lease and stamps
//! its epoch into the head; a commit under an older one is refused.
//!
//! The head holds the namespace binding, the clocks, the keyed tick's intent and the input
//! position the served input tables are published through. A commit is one write of the whole
//! head, so changes that span records land together or not at all.
//!
//! The commit, the lease and the fence are `analyzer_lake::head`'s: a clock advance, re-applied
//! after an ambiguous commit that landed, skips a revision and never reuses one.

use crate::Error;
use serde::{Deserialize, Serialize};

/// The head's format. A head in another format is refused, never guessed at.
pub(crate) const FORMAT: u32 = 1;

/// The keyed tick's intent: the publication its batch goes out under, and whether it was
/// applied. Readers are told the tick's edges only when it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct KeyedTick {
    pub(crate) publication: u64,
    pub(crate) applied: bool,
}

/// The whole journal state of one lineage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct JournalHead {
    pub(crate) format: u32,
    /// The namespace identity this journal is bound to.
    pub(crate) namespace: Option<String>,
    /// Clock kind → revision.
    pub(crate) clocks: Vec<(String, u64)>,
    /// The last keyed tick's intent.
    pub(crate) keyed: Option<KeyedTick>,
    /// The input position through which the served input tables are published.
    pub(crate) inputs_published: u64,
    /// The lease epoch of the last writer to commit: a commit under an older one is fenced.
    pub(crate) lease_epoch: u64,
}

impl Default for JournalHead {
    fn default() -> Self {
        Self {
            format: FORMAT,
            namespace: None,
            clocks: Vec::new(),
            keyed: None,
            inputs_published: 0,
            lease_epoch: 0,
        }
    }
}

/// Insert `item`, or replace the one record with the same key: the head's form of a
/// replacing table's "latest row per key".
pub(crate) fn upsert<T, K: PartialEq>(records: &mut Vec<T>, item: T, key: impl Fn(&T) -> K) {
    let k = key(&item);
    if let Some(slot) = records.iter_mut().find(|r| key(r) == k) {
        *slot = item;
    } else {
        records.push(item);
    }
}

/// The journal of one lineage: an [`analyzer_lake::head::Head`] holding a [`JournalHead`],
/// with the journal's own key layout. `read` and `commit` are the head's. It keeps its
/// lineage's lake, where the input log it consumes lives.
pub(crate) struct Journal {
    head: analyzer_lake::head::Head<JournalHead>,
    lake: analyzer_lake::Lake,
}

impl std::ops::Deref for Journal {
    type Target = analyzer_lake::head::Head<JournalHead>;
    fn deref(&self) -> &Self::Target {
        &self.head
    }
}

impl analyzer_lake::head::Document for JournalHead {
    const NAME: &'static str = "journal head";
    const FORMAT: u32 = FORMAT;
    fn format(&self) -> u32 {
        self.format
    }
    fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }
    fn set_lease_epoch(&mut self, epoch: u64) {
        self.lease_epoch = epoch;
    }
}

impl Journal {
    /// The journal of `lake`'s lineage (`<root>journal/head`, `<root>journal/lease`).
    /// Nothing is read until first use.
    ///
    /// # Errors
    /// The store cannot open a sibling connection.
    pub(crate) fn new(lake: analyzer_lake::Lake) -> Result<Self, Error> {
        Ok(Self {
            head: analyzer_lake::head::Head::at(
                lake.store().sibling()?,
                &lake.journal_dir(),
                lake.lineage().clone(),
            ),
            lake,
        })
    }

    /// The head as committed.
    ///
    /// # Errors
    /// The store failed, the head does not decode, or it is of another format.
    pub(crate) fn read(&self) -> Result<JournalHead, Error> {
        Ok(self.head.read()?)
    }

    /// Commit `change` to the head ([`analyzer_lake::head::Head::commit`]).
    ///
    /// # Errors
    /// As [`Self::read`], or the commit was fenced.
    pub(crate) fn commit(&self, change: impl Fn(&mut JournalHead)) -> Result<JournalHead, Error> {
        Ok(self.head.commit(change)?)
    }

    /// The lineage's lake: the input log this journal's ticks consume.
    pub(crate) fn lake(&self) -> &analyzer_lake::Lake {
        &self.lake
    }
}

#[cfg(test)]
mod tests {
    use super::{upsert, Journal, JournalHead};
    use analyzer_objectstore::DirStore;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "infertap-journal-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch");
        dir
    }

    fn journal(root: &std::path::Path) -> Journal {
        journal_as(root, "writer")
    }

    fn journal_as(root: &std::path::Path, instance: &str) -> Journal {
        let lineage = crate::lineage::Lineage::new("test", instance).expect("lineage");
        Journal::new(analyzer_lake::Lake::new(
            std::sync::Arc::new(DirStore::new(root)),
            format!("namespaces/test/{}", lineage.object_prefix()),
            lineage,
        ))
        .expect("journal")
    }

    /// A clock record, keyed by its kind and day.
    fn clock(kind: &str, day: i64, revision: u64) -> (String, u64) {
        (format!("{kind}-{day}"), revision)
    }

    fn record(c: (String, u64)) -> impl Fn(&mut JournalHead) {
        move |h: &mut JournalHead| {
            upsert(&mut h.clocks, c.clone(), |r| r.0.clone());
        }
    }

    #[test]
    fn a_committed_head_reads_back_from_a_fresh_journal() {
        let root = scratch("readback");
        journal(&root)
            .commit(record(clock("coverage", 1, 1)))
            .expect("commit");
        let read = journal(&root).read().expect("read");
        assert_eq!(read.clocks, vec![clock("coverage", 1, 1)]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn an_upsert_replaces_the_record_with_the_same_key() {
        let root = scratch("upsert");
        let j = journal(&root);
        j.commit(record(clock("coverage", 1, 1))).expect("start");
        j.commit(record(clock("coverage", 1, 2))).expect("commit");
        assert_eq!(
            j.read().expect("read").clocks,
            vec![clock("coverage", 1, 2)]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_superseded_writer_is_fenced_after_the_new_holder_commits() {
        let root = scratch("fenced");
        let primary = journal_as(&root, "primary");
        primary
            .commit(record(clock("coverage", 1, 1)))
            .expect("primary holds epoch 1");
        // The primary's lease expires; a standby of the same lineage takes it over.
        let lease_key = "namespaces/test/lineages/test/journal/lease";
        let store = DirStore::new(&root);
        let mut lease: serde_json::Value = serde_json::from_slice(
            &analyzer_objectstore::ObjectStore::get(&store, lease_key).expect("lease"),
        )
        .expect("decodes");
        lease["expires_ms"] = 0.into();
        analyzer_objectstore::ObjectStore::put(
            &store,
            lease_key,
            &serde_json::to_vec(&lease).unwrap(),
        )
        .unwrap();
        let standby = journal_as(&root, "standby");
        let head = standby
            .commit(record(clock("coverage", 1, 2)))
            .expect("standby takes over");
        assert_eq!(head.lease_epoch, 2);
        // The primary still believes its lease stands; its commit is refused, not merged.
        let refused = primary
            .commit(record(clock("roster", 1, 3)))
            .expect_err("fenced");
        // Refused by the head's epoch, not by the lease: the primary never re-read the lease.
        assert!(
            matches!(&refused, crate::Error::Fenced { reason, .. } if reason.starts_with("superseded")),
            "{refused}"
        );
        assert_eq!(
            journal_as(&root, "reader")
                .read()
                .expect("read")
                .clocks
                .len(),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_read_sees_a_commit_made_through_another_journal() {
        let root = scratch("fresh-read");
        let (a, b) = (journal(&root), journal(&root));
        a.commit(record(clock("coverage", 1, 1))).expect("a first");
        assert_eq!(b.read().expect("b reads").clocks.len(), 1);
        a.commit(record(clock("coverage", 1, 2)))
            .expect("a acknowledges");
        // b read before a's second commit; its next read must not be that earlier head.
        assert_eq!(
            b.read().expect("b reads again").clocks,
            vec![clock("coverage", 1, 2)]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_stale_writer_re_reads_and_keeps_both_changes() {
        let root = scratch("stale");
        let (a, b) = (journal(&root), journal(&root));
        a.commit(record(clock("coverage", 1, 1))).expect("a first");
        b.read().expect("b caches");
        a.commit(record(clock("roster", 1, 2))).expect("a second");
        // b's cached version is superseded: its write is refused, it re-reads, re-applies.
        let head = b.commit(record(clock("coverage", 2, 3))).expect("b");
        assert_eq!(head.clocks.len(), 3);
        assert_eq!(journal(&root).read().expect("read").clocks.len(), 3);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_change_already_in_the_head_is_an_ambiguous_commit_that_landed() {
        let root = scratch("ambiguous");
        let (a, b) = (journal(&root), journal(&root));
        b.read().expect("b caches the empty head");
        a.commit(record(clock("coverage", 1, 1)))
            .expect("a lands the change");
        // b holds a stale version, so its write is refused; the change is already there.
        let head = b.commit(record(clock("coverage", 1, 1))).expect("b");
        assert_eq!(head.clocks, vec![clock("coverage", 1, 1)]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_change_that_changes_nothing_writes_nothing() {
        let root = scratch("noop");
        let j = journal(&root);
        j.commit(record(clock("coverage", 1, 1))).expect("first");
        let before = std::fs::metadata(root.join("namespaces/test/lineages/test/journal/head"))
            .expect("head")
            .modified()
            .expect("mtime");
        j.commit(record(clock("coverage", 1, 1))).expect("again");
        let after = std::fs::metadata(root.join("namespaces/test/lineages/test/journal/head"))
            .expect("head")
            .modified()
            .expect("mtime");
        assert_eq!(before, after);
        std::fs::remove_dir_all(root).ok();
    }
}
