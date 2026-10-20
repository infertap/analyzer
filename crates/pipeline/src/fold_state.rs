// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed operators' state in the lake (`docs/pipeline.md`): the
//! relations of the lineage's working state (`analyzer_lake::working`) the keyed store keeps,
//! declared here ([`DECLARED`]), and the one writer they are written through. A writer reserves
//! a revision, its parts stage their stripes, and its commit applies them: nothing it writes is
//! read before the commit.

use crate::Error;
use analyzer_lake::keyed::Record;
use analyzer_lake::working::{Declared, Index, Relation};

/// Every relation the keyed store keeps.
pub(crate) const DECLARED: [Declared; 11] = [
    Declared {
        name: crate::keyed::KEYED_FRONTIER,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_SEEDS,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_HEAD,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_TAGS,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_RING,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_BLOCKS,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_WHALES,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_SPINE,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_ROSTER,
        dated: false,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_CELLS,
        dated: true,
        index: Index::Plain,
    },
    Declared {
        name: crate::keyed::KEYED_OUTBOX,
        dated: false,
        index: Index::Plain,
    },
];

/// One revision of a keyed relation being written: its parts stage their stripes, from any
/// thread, and the commit applies them. Nothing it writes is read before the commit.
pub(crate) struct KeyedWriter {
    lake: analyzer_lake::Lake,
    relation: Relation<'static>,
    revision: u64,
    base: bool,
    /// The parts numbered so far per stripe, and whether anything was staged: a key's latest
    /// record is the one in its latest part.
    written: std::sync::Mutex<([u32; analyzer_lake::stripe::STRIPES], bool)>,
    /// Whether the revision was committed: one dropped without it discards what it staged.
    done: std::sync::atomic::AtomicBool,
}

impl Drop for KeyedWriter {
    fn drop(&mut self) {
        if !self.done.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(working) = self.lake.working() {
                // Best effort: what it leaves is cleared when the state next opens.
                let _ = working.abandon(self.revision);
            }
        }
    }
}

impl KeyedWriter {
    /// A revision of `relation`: a base restating it whole (`base`), or a delta.
    ///
    /// # Errors
    /// The lake has no working state.
    pub(crate) fn open(
        lake: &analyzer_lake::Lake,
        relation: Relation<'static>,
        base: bool,
    ) -> Result<Self, Error> {
        Ok(Self {
            revision: lake.working()?.reserve(),
            lake: lake.clone(),
            relation,
            base,
            written: std::sync::Mutex::new(([0; analyzer_lake::stripe::STRIPES], false)),
            done: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Put records, a part per stripe they fall in. Of one key's records in one revision, the
    /// later part's wins.
    ///
    /// # Errors
    /// The working state refused a write.
    pub(crate) fn put(&self, records: Vec<(usize, Record)>) -> Result<(), Error> {
        let mut by_stripe: std::collections::BTreeMap<usize, Vec<Record>> =
            std::collections::BTreeMap::new();
        for (stripe, record) in records {
            by_stripe.entry(stripe).or_default().push(record);
        }
        // Each stripe's part named first, under one lock; then the parts staged side by side.
        let jobs: Vec<(usize, u32, Vec<Record>)> = {
            let mut written = self
                .written
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            written.1 |= !by_stripe.is_empty();
            by_stripe
                .into_iter()
                .map(|(stripe, part)| {
                    let at = written.0[stripe];
                    written.0[stripe] += 1;
                    (stripe, at, part)
                })
                .collect()
        };
        let working = self.lake.working()?;
        analyzer_lake::in_parallel(&jobs, |(stripe, at, records)| {
            working.stage(&self.relation, self.revision, (*stripe, *at), records, None)
        })?;
        Ok(())
    }

    /// Commit the revision: what it staged is read from here on. A base that staged nothing
    /// restates the relation as empty.
    ///
    /// # Errors
    /// The working state failed.
    pub(crate) fn commit(&self) -> Result<(), Error> {
        let any = self
            .written
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1;
        if self.base || any {
            self.lake
                .working()?
                .commit(&self.relation, self.revision, self.base, None)?;
        }
        self.done.store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

/// The live records of one stripe of a relation, those of `probes` (sorted, distinct) or all.
///
/// # Errors
/// The lake failed.
pub(crate) fn read(
    lake: &analyzer_lake::Lake,
    relation: &Relation<'_>,
    stripe: usize,
    probes: Option<&[u64]>,
) -> Result<Vec<Record>, Error> {
    Ok(lake.working()?.read(relation, stripe, probes)?)
}

/// A lake in `root` with its working state, for this crate's unit tests.
#[cfg(test)]
pub(crate) fn test_lake(root: &std::path::Path) -> analyzer_lake::Lake {
    let lake = analyzer_lake::Lake::new(
        std::sync::Arc::new(analyzer_objectstore::DirStore::new(root.join("store"))),
        "r/".to_string(),
        analyzer_lake::lineage::Lineage::new("t", "w").expect("lineage"),
    );
    let working = analyzer_lake::working::WorkingState::open(
        &root.join("working"),
        &lake,
        &DECLARED,
        analyzer_lake::working::DEFAULT_MEMORY_BYTES,
    )
    .expect("working state");
    lake.with_working(std::sync::Arc::new(working))
}

#[cfg(test)]
mod tests {
    use super::{read, KeyedWriter};
    use analyzer_lake::keyed::Record;
    use analyzer_lake::working::{Relation, Scope};

    fn scratch_lake(name: &str) -> (analyzer_lake::Lake, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "fold-state-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        (super::test_lake(&root), root)
    }

    fn record(key: &str, payload: &str) -> (usize, Record) {
        (
            0,
            Record {
                hash: 7,
                key: key.as_bytes().to_vec(),
                version: 1.0,
                gone: false,
                payload: payload.as_bytes().to_vec(),
            },
        )
    }

    fn held(lake: &analyzer_lake::Lake, relation: &Relation<'_>) -> Vec<(String, String)> {
        read(lake, relation, 0, None)
            .expect("read")
            .into_iter()
            .map(|r| {
                (
                    String::from_utf8(r.key).expect("utf-8"),
                    String::from_utf8(r.payload).expect("utf-8"),
                )
            })
            .collect()
    }

    /// Nothing a revision stages is read before its commit; of one key's records the later
    /// part's wins; a base that stages nothing restates the relation empty.
    #[test]
    fn a_revision_is_read_whole_at_its_commit_and_its_later_part_wins() {
        let (lake, root) = scratch_lake("revision");
        let relation = Relation {
            name: crate::keyed::KEYED_OUTBOX,
            scope: Scope::Lineage,
        };
        let writer = KeyedWriter::open(&lake, relation, false).expect("writer");
        writer
            .put(vec![record("a", "a1"), record("b", "b1")])
            .expect("first part");
        writer.put(vec![record("a", "a2")]).expect("second part");
        assert!(held(&lake, &relation).is_empty(), "read before the commit");
        writer.commit().expect("commit");
        assert_eq!(
            held(&lake, &relation),
            vec![("a".into(), "a2".into()), ("b".into(), "b1".into())]
        );
        KeyedWriter::open(&lake, relation, true)
            .expect("base")
            .commit()
            .expect("commit");
        assert!(
            held(&lake, &relation).is_empty(),
            "an empty base restates it empty"
        );
        std::fs::remove_dir_all(root).ok();
    }
}
