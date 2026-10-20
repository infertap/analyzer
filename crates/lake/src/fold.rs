// SPDX-License-Identifier: AGPL-3.0-only

//! The published state's records in the lake: a manifest per day and one undated head, each
//! naming the published-state logs (`crate::state`) a reader takes and reserving the revisions
//! that name their objects, and the size-tiered rule those logs compact by ([`tier`]).
//!
//! A manifest is a [`Head`] fenced by the lineage journal's lease: committed conditionally, by
//! the writer that holds the journal, after the objects it names are written. An object no
//! manifest names is an orphan, never read.
//!
//! ```text
//! <root>fold/days/<day>/head
//! <root>fold/head
//! ```

use crate::head::{Document, Head};
use crate::lineage::Lineage;
use crate::Error;
use analyzer_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The manifest's format. A manifest in another format is refused, never guessed at.
const FORMAT: u32 = 1;

/// Fewest runs a size-tiered merge takes: `SlateDB`'s `min_compaction_sources` default.
pub const TIER_MIN: usize = 4;
/// Most runs one merge takes: `SlateDB`'s `max_compaction_sources` default.
pub const TIER_MAX: usize = 8;
/// A run joins a merge while it holds under this many times the fewest records already taken:
/// `SlateDB`'s `include_size_threshold` default.
pub const TIER_RATIO: u64 = 4;

/// What size-tiered compaction owes a relation's runs ([`tier`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Nothing yet.
    Keep,
    /// Merge the newest this-many runs into one.
    Merge(usize),
    /// Fold every run into a new base.
    Fold,
}

/// The size-tiered decision over a relation's runs above its base (records per run, newest
/// last): fold into a new base once the runs hold as many records as the base, so the
/// relation never holds more than twice its live records; else merge the newest runs of like
/// size, a run joining while it holds under [`TIER_RATIO`] times the fewest records already
/// taken, [`TIER_MIN`] to [`TIER_MAX`] of them. `stop_after` ends the group at a run that may
/// only be its oldest (one that purges). `SlateDB`'s scheduler, `size_tiered_compaction.rs`.
#[must_use]
pub fn tier(runs: &[u64], base: Option<u64>, stop_after: impl Fn(usize) -> bool) -> Tier {
    let size = |r: u64| r.max(1);
    let in_runs: u64 = runs.iter().copied().map(size).sum();
    if runs.len() >= 2 && in_runs >= base.map_or(0, size) {
        return Tier::Fold;
    }
    let mut taken = 0;
    let mut fewest = u64::MAX;
    for (i, run) in runs.iter().copied().enumerate().rev() {
        if taken == TIER_MAX || (taken > 0 && size(run) >= TIER_RATIO * fewest) {
            break;
        }
        taken += 1;
        fewest = fewest.min(size(run));
        if stop_after(i) {
            break;
        }
    }
    if taken >= TIER_MIN {
        Tier::Merge(taken)
    } else {
        Tier::Keep
    }
}

/// One day's manifest: the day's published-state logs, and the next revision its objects are
/// written under.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DayManifest {
    format: u32,
    /// The day.
    pub day: i64,
    /// The next revision the day's objects are written under.
    pub next_revision: u64,
    /// The day's published-state logs, by name (`crate::state`): the objects a reader takes.
    pub published: BTreeMap<String, PublishedLog>,
    lease_epoch: u64,
}

/// One published-state log's objects, as its manifest names them: a base, and the deltas
/// above it in publication order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedLog {
    /// The base, if the log has one.
    pub base: Option<PublishedObject>,
    /// The deltas above it, oldest first.
    pub deltas: Vec<PublishedObject>,
}

/// One object of a published-state log: the publication it states, the revision reserved for
/// its name, and its size, as the compaction's tiers measure it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedObject {
    /// The publication.
    pub publication: u64,
    /// The revision its name carries.
    pub revision: u64,
    /// Its bytes.
    pub bytes: u64,
}

impl Default for DayManifest {
    fn default() -> Self {
        Self {
            format: FORMAT,
            day: 0,
            next_revision: 1,
            published: BTreeMap::new(),
            lease_epoch: 0,
        }
    }
}

impl Document for DayManifest {
    const NAME: &'static str = "day manifest";
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

/// The day manifests of one lineage.
pub struct DayManifests {
    store: Box<dyn ObjectStore>,
    root: String,
    lease_dir: String,
    lineage: Lineage,
}

impl DayManifests {
    /// The manifests under a lineage's `root` in `store`, fenced by the lineage journal's
    /// lease: the writer that holds the journal is the one writer of its days.
    ///
    /// # Errors
    /// The store cannot open a sibling connection.
    pub fn at(store: &dyn ObjectStore, root: &str, lineage: Lineage) -> Result<Self, Error> {
        Ok(Self {
            store: store.sibling()?,
            root: format!("{root}fold/days/"),
            lease_dir: format!("{root}journal/"),
            lineage,
        })
    }

    fn head(&self, day: i64) -> Result<Head<DayManifest>, Error> {
        Ok(Head::sharing_lease(
            self.store.sibling()?,
            &format!("{}{day}/", self.root),
            &self.lease_dir,
            self.lineage.clone(),
        ))
    }

    /// The day's manifest, or an empty one for a day never filled.
    ///
    /// # Errors
    /// The store failed, or the manifest does not decode.
    pub fn read(&self, day: i64) -> Result<DayManifest, Error> {
        let mut m = self.head(day)?.read()?;
        m.day = day;
        Ok(m)
    }

    /// Commit a change to the day's manifest, under the lineage's lease.
    ///
    /// # Errors
    /// Another writer holds the lease, this one was superseded, or the store failed.
    pub fn commit(
        &self,
        day: i64,
        change: impl Fn(&mut DayManifest),
    ) -> Result<DayManifest, Error> {
        self.head(day)?.commit(|m| {
            m.day = day;
            change(m);
        })
    }

    /// Reserve a revision of the day for one writer's objects: no two writes under one
    /// revision, whatever relations they are for. A reservation lost to a crash is skipped,
    /// never reused.
    ///
    /// # Errors
    /// Another writer holds the lease, this one was superseded, or the store failed.
    pub fn reserve(&self, day: i64) -> Result<(u64, DayManifest), Error> {
        let committed = self.commit(day, |m| m.next_revision += 1)?;
        Ok((committed.next_revision - 1, committed))
    }

    /// The days that hold a manifest, oldest first.
    ///
    /// # Errors
    /// The store failed.
    pub fn days(&self) -> Result<Vec<i64>, Error> {
        let mut days: Vec<i64> = self
            .store
            .list(&self.root)?
            .into_iter()
            .filter(|o| o.key.ends_with("/head"))
            .filter_map(|o| {
                o.key
                    .strip_prefix(&self.root)?
                    .split('/')
                    .next()?
                    .parse()
                    .ok()
            })
            .collect();
        days.sort_unstable();
        days.dedup();
        Ok(days)
    }

    /// Drop the day's manifest: a retired day. The objects its logs name are the caller's.
    ///
    /// # Errors
    /// The store failed.
    pub fn drop_day(&self, day: i64) -> Result<(), Error> {
        for object in self.store.list(&format!("{}{day}/", self.root))? {
            self.store.delete(&object.key)?;
        }
        Ok(())
    }
}

/// The lineage's undated records: the published state's undated logs and the revisions that
/// name their objects, committed under the journal's lease like the day manifests. The fold's
/// keyed relations are the working state's (`crate::working`), and so is their head.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FoldHead {
    format: u32,
    /// The next revision the head's objects are written under.
    pub next_revision: u64,
    /// The undated published-state logs, by name (`crate::state`).
    pub published: BTreeMap<String, PublishedLog>,
    lease_epoch: u64,
}

impl Default for FoldHead {
    fn default() -> Self {
        Self {
            format: FORMAT,
            next_revision: 1,
            published: BTreeMap::new(),
            lease_epoch: 0,
        }
    }
}

impl Document for FoldHead {
    const NAME: &'static str = "fold head";
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

impl crate::Lake {
    pub(crate) fn fold_head(&self) -> Result<Head<FoldHead>, Error> {
        Ok(Head::sharing_lease(
            self.store().sibling()?,
            &format!("{}fold/", self.root()),
            &self.journal_dir(),
            self.lineage().clone(),
        ))
    }

    /// Reserve a revision in the document of `day`, or the fold head's for `None`, to name one
    /// writer's objects: a reserved revision is never reused, so no object is written twice
    /// under one name.
    ///
    /// # Errors
    /// Another writer holds the lease, this one was superseded, or the store failed.
    pub fn reserve_revision(&self, day: Option<i64>) -> Result<u64, Error> {
        Ok(match day {
            Some(day) => self.day_manifests()?.reserve(day)?.0,
            None => {
                self.fold_head()?
                    .commit(|h| h.next_revision += 1)?
                    .next_revision
                    - 1
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::DayManifests;
    use crate::lineage::Lineage;
    use analyzer_objectstore::DirStore;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fold-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch");
        dir
    }

    #[test]
    fn a_reservation_is_never_reused_and_a_day_drops_whole() {
        let root = scratch("manifest");
        let manifests = DayManifests::at(
            &DirStore::new(&root),
            "r/",
            Lineage::new("t", "w").expect("lineage"),
        )
        .expect("manifests");
        assert!(manifests.read(7).expect("empty").published.is_empty());
        let (first, _) = manifests.reserve(7).expect("reserve");
        let (second, _) = manifests.reserve(7).expect("reserve again");
        assert!(second > first, "a revision is reserved once");
        manifests.reserve(9).expect("another day");
        assert_eq!(manifests.days().expect("days"), vec![7, 9]);
        manifests.drop_day(7).expect("drop");
        assert_eq!(manifests.days().expect("days"), vec![9]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn the_tier_decision_is_slatedbs() {
        use super::{tier, Tier};
        let never = |_| false;
        assert_eq!(tier(&[1, 1, 1], Some(100), never), Tier::Keep);
        assert_eq!(tier(&[1, 1, 1, 1], Some(100), never), Tier::Merge(4));
        assert_eq!(tier(&[9, 1, 1, 1, 1], Some(100), never), Tier::Merge(4));
        assert_eq!(tier(&[3, 1, 1, 1, 1], Some(100), never), Tier::Merge(5));
        assert_eq!(tier(&[1; 12], Some(100), never), Tier::Merge(8));
        assert_eq!(tier(&[1, 1, 1, 1, 1], Some(100), |i| i == 2), Tier::Keep);
        assert_eq!(
            tier(&[1, 1, 1, 1, 1], Some(100), |i| i == 1),
            Tier::Merge(4)
        );
        assert_eq!(tier(&[60, 50], Some(100), never), Tier::Fold);
        assert_eq!(tier(&[1, 1], None, never), Tier::Fold);
    }
}
