// SPDX-License-Identifier: AGPL-3.0-only

//! The core's published state (`docs/lake.md`): every published table's
//! day as a keyed log of encoded row batches, so the core reads its own output back from the
//! lake and never from serving.
//!
//! A log is one (table, meaning, day). A restatement of the day writes a **base**, a keyed
//! change writes a **delta**, each at its publication; a reader takes the base and the deltas
//! above it, in publication order. The bytes are the caller's (the rows' encoding is the
//! port's); this module knows only which objects a reader takes.
//!
//! **The day's manifest names a log's objects, and nothing is found by listing.** A write puts
//! its objects, then commits their names in one conditional write of the day's manifest (the
//! fold head, for an undated table), under the journal's lease: Delta's order, the data first
//! and the log entry that makes it visible after. An object a failed publication left is named
//! nowhere, so no reader sees it, and it leaves with its log. Every object's name carries a
//! revision the manifest reserved, so no object is ever written twice under one name, a retried
//! publication included.
//!
//! ```text
//! <root>state/<table>/<semantics>/<discriminator>/<day>/base-<publication>-<revision>.bin
//! <root>state/<table>/<semantics>/<discriminator>/<day>/delta-<publication>-<revision>.bin
//! ```

use crate::fold::{PublishedLog, PublishedObject};
use crate::Error;

/// One log: a table's rows under one meaning on one UTC day (`None` for an undated table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogId<'a> {
    /// The table.
    pub table: &'a str,
    /// The meaning's semantics version.
    pub semantics_version: &'a str,
    /// The meaning's discriminator; empty for the input meaning.
    pub discriminator: &'a str,
    /// The UTC day, or `None` for an undated table.
    pub day: Option<i64>,
}

/// What a reader of a log takes: the base's bytes, if any, and the deltas above it in
/// publication order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogRead {
    /// The base: its publication and bytes.
    pub base: Option<(u64, Vec<u8>)>,
    /// The deltas above the base, oldest first.
    pub deltas: Vec<(u64, Vec<u8>)>,
}

/// One object a publication writes to a log.
#[derive(Debug, Clone, Copy)]
pub struct Write<'a> {
    /// The log.
    pub id: LogId<'a>,
    /// A base, restating the log's day whole, or a delta.
    pub base: bool,
    /// The rows, encoded.
    pub body: &'a [u8],
}

/// How a compaction replaces a log's objects, as a caller read them ([`StateLogs::named`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replace {
    /// The newest this-many deltas become one delta.
    Merge(usize),
    /// The base and every delta read become one base.
    Fold,
}

/// The published-state logs of one lineage.
pub struct StateLogs {
    lake: crate::Lake,
}

/// A path segment from an identifier: alphanumerics, dash, underscore and dot kept, the rest
/// underscored, and an empty one spelled `_`.
pub(crate) fn segment(id: &str) -> String {
    if id.is_empty() {
        return "_".to_string();
    }
    id.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl StateLogs {
    /// The logs of `lake`'s lineage.
    #[must_use]
    pub fn at(lake: &crate::Lake) -> Self {
        Self { lake: lake.clone() }
    }

    fn dir(&self, id: &LogId<'_>) -> String {
        format!(
            "{}state/{}/{}/{}/{}/",
            self.lake.root(),
            segment(id.table),
            segment(id.semantics_version),
            segment(id.discriminator),
            id.day
                .map_or_else(|| "undated".to_string(), |d| d.to_string()),
        )
    }

    /// The name a log goes by in its manifest.
    fn name(id: &LogId<'_>) -> String {
        format!(
            "{}/{}/{}",
            segment(id.table),
            segment(id.semantics_version),
            segment(id.discriminator)
        )
    }

    fn key(&self, id: &LogId<'_>, base: bool, object: &PublishedObject) -> String {
        format!(
            "{}{}-{:020}-{:020}.bin",
            self.dir(id),
            if base { "base" } else { "delta" },
            object.publication,
            object.revision
        )
    }

    /// A revision no other object of the scope is named by.
    fn reserve(&self, day: Option<i64>) -> Result<u64, Error> {
        self.lake.reserve_revision(day)
    }

    /// Commit a change to the named logs of one day, or the undated ones.
    fn commit(
        &self,
        day: Option<i64>,
        change: impl Fn(&mut std::collections::BTreeMap<String, PublishedLog>),
    ) -> Result<(), Error> {
        match day {
            Some(day) => {
                self.lake
                    .day_manifests()?
                    .commit(day, |m| change(&mut m.published))?;
            }
            None => {
                self.lake
                    .fold_head()?
                    .commit(|h| change(&mut h.published))?;
            }
        }
        Ok(())
    }

    /// The objects the manifest names for `id`.
    ///
    /// # Errors
    /// The manifest does not read.
    pub fn named(&self, id: &LogId<'_>) -> Result<PublishedLog, Error> {
        let name = Self::name(id);
        Ok(match id.day {
            Some(day) => self
                .lake
                .day_manifests()?
                .read(day)?
                .published
                .remove(&name),
            None => self.lake.fold_head()?.read()?.published.remove(&name),
        }
        .unwrap_or_default())
    }

    /// Publish `writes` at `publication`, every one of them on `day` (or undated): the objects
    /// first, side by side and kept in the cache, then one commit naming them all. A base
    /// replaces its log's objects; a delta joins its log, replacing an earlier delta of the same
    /// publication (a retry's). What a write replaced is deleted after the commit.
    ///
    /// # Errors
    /// A write is on another day, the store failed, or the lease is lost.
    pub fn publish(
        &self,
        day: Option<i64>,
        publication: u64,
        writes: &[Write<'_>],
    ) -> Result<(), Error> {
        if writes.is_empty() {
            return Ok(());
        }
        if let Some(w) = writes.iter().find(|w| w.id.day != day) {
            return Err(Error::Invalid(format!(
                "{}: a write for {:?} in a publication of {day:?}",
                w.id.table, w.id.day
            )));
        }
        let revision = self.reserve(day)?;
        let objects: Vec<PublishedObject> = crate::in_parallel(writes, |w| {
            let object = PublishedObject {
                publication,
                revision,
                bytes: w.body.len() as u64,
            };
            let key = self.key(&w.id, w.base, &object);
            self.lake.store().put(&key, w.body)?;
            self.lake.keep(&key, w.body);
            Ok::<_, Error>(object)
        })?;
        let replaced = std::sync::Mutex::new(Vec::new());
        self.commit(day, |logs| {
            let mut gone = Vec::new();
            for (w, object) in writes.iter().zip(&objects) {
                let log = logs.entry(Self::name(&w.id)).or_default();
                if log.base == Some(*object) || log.deltas.contains(object) {
                    continue;
                }
                if w.base {
                    gone.extend(log.base.take().map(|b| (w.id, true, b)));
                    gone.extend(log.deltas.drain(..).map(|d| (w.id, false, d)));
                    log.base = Some(*object);
                } else {
                    log.deltas.retain(|d| {
                        let retry = d.publication == publication;
                        if retry {
                            gone.push((w.id, false, *d));
                        }
                        !retry
                    });
                    log.deltas.push(*object);
                }
            }
            *replaced
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = gone;
        })?;
        for (id, base, object) in replaced
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            let _ = self.lake.store().delete(&self.key(&id, base, &object));
        }
        Ok(())
    }

    /// What a reader of the log takes: its base and every delta above it, as the manifest
    /// names them, fetched through the cache side by side.
    ///
    /// # Errors
    /// The manifest or the store failed.
    pub fn read(&self, id: &LogId<'_>) -> Result<LogRead, Error> {
        self.read_named(id, &self.named(id)?)
    }

    /// [`Self::read`] over the objects `named` names.
    ///
    /// # Errors
    /// The store failed.
    pub fn read_named(&self, id: &LogId<'_>, named: &PublishedLog) -> Result<LogRead, Error> {
        let mut wanted: Vec<(bool, PublishedObject)> =
            named.base.map(|b| (true, b)).into_iter().collect();
        wanted.extend(named.deltas.iter().map(|d| (false, *d)));
        let bodies = crate::in_parallel(&wanted, |(base, object)| {
            let copy = self.lake.local_copy(&self.key(id, *base, object))?;
            std::fs::read(copy.path()).map_err(|e| {
                Error::Local(format!("published state {}: {e}", copy.path().display()))
            })
        })?;
        let mut bodies = bodies.into_iter();
        let base = named
            .base
            .map(|b| (b.publication, bodies.next().unwrap_or_default()));
        let deltas = named
            .deltas
            .iter()
            .map(|d| (d.publication, bodies.next().unwrap_or_default()))
            .collect();
        Ok(LogRead { base, deltas })
    }

    /// Replace objects of `id` as `named` named them when the caller read them with one object
    /// at `publication`: the newest deltas (a merge, itself a delta) or the base and every
    /// delta read (a fold, a base). A delta published since the read stays above it; a log
    /// changed otherwise since is left as it is, and the object is dropped.
    ///
    /// # Errors
    /// The store failed, or the lease is lost.
    pub fn replace(
        &self,
        id: &LogId<'_>,
        named: &PublishedLog,
        (how, publication): (Replace, u64),
        body: &[u8],
    ) -> Result<(), Error> {
        let (base, group): (bool, Vec<PublishedObject>) = match how {
            Replace::Merge(n) => (
                false,
                named.deltas[named.deltas.len().saturating_sub(n)..].to_vec(),
            ),
            Replace::Fold => (true, named.deltas.clone()),
        };
        let object = PublishedObject {
            publication,
            revision: self.reserve(id.day)?,
            bytes: body.len() as u64,
        };
        let key = self.key(id, base, &object);
        self.lake.store().put(&key, body)?;
        self.lake.keep(&key, body);
        let name = Self::name(id);
        let applied = std::sync::atomic::AtomicBool::new(false);
        self.commit(id.day, |logs| {
            let log = logs.entry(name.clone()).or_default();
            if log.base == Some(object) || log.deltas.contains(&object) {
                applied.store(true, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            if base {
                if log.base == named.base && log.deltas.starts_with(&group) {
                    log.deltas.drain(..group.len());
                    log.base = Some(object);
                    applied.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            } else if log.deltas.ends_with(&group) {
                let at = log.deltas.len() - group.len();
                log.deltas.truncate(at);
                log.deltas.push(object);
                applied.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        })?;
        if applied.into_inner() {
            if base {
                if let Some(b) = named.base {
                    let _ = self.lake.store().delete(&self.key(id, true, &b));
                }
            }
            for d in &group {
                let _ = self.lake.store().delete(&self.key(id, false, d));
            }
        } else {
            let _ = self.lake.store().delete(&key);
        }
        Ok(())
    }

    /// Drop the log: its names, and every object under its directory, named or left by a
    /// failed publication.
    ///
    /// # Errors
    /// The store failed, or the lease is lost.
    pub fn drop_log(&self, id: &LogId<'_>) -> Result<(), Error> {
        let name = Self::name(id);
        self.commit(id.day, |logs| {
            logs.remove(&name);
        })?;
        for object in self.lake.store().list(&self.dir(id))? {
            self.lake.store().delete(&object.key)?;
        }
        Ok(())
    }

    /// Retire every day before `first_kept`: its manifest first, so no reader takes its logs,
    /// then every object of every log of the day. Undated logs stay. Returns the days whose
    /// manifest it dropped. A crash between the two leaves objects no manifest names, which no
    /// reader takes and the next call deletes.
    ///
    /// # Errors
    /// The store failed.
    pub fn retire_before(&self, first_kept: i64) -> Result<Vec<i64>, Error> {
        let manifests = self.lake.day_manifests()?;
        let days: Vec<i64> = manifests
            .days()?
            .into_iter()
            .filter(|d| *d < first_kept)
            .collect();
        for day in &days {
            manifests.drop_day(*day)?;
        }
        let root = format!("{}state/", self.lake.root());
        for object in self.lake.store().list(&root)? {
            // <table>/<semantics>/<discriminator>/<day>/<object>
            let day = object
                .key
                .strip_prefix(&root)
                .and_then(|rest| rest.split('/').nth(3))
                .and_then(|d| d.parse::<i64>().ok());
            if day.is_some_and(|d| d < first_kept) {
                self.lake.store().delete(&object.key)?;
            }
        }
        Ok(days)
    }
}

#[cfg(test)]
mod tests {
    use super::{LogId, LogRead, Replace, StateLogs, Write};
    use analyzer_objectstore::DirStore;

    fn lake(name: &str) -> (crate::Lake, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "state-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("scratch");
        (
            crate::Lake::new(
                std::sync::Arc::new(DirStore::new(&root)),
                "r/".to_string(),
                crate::lineage::Lineage::new("t", "w").expect("lineage"),
            ),
            root,
        )
    }

    const DAY: LogId<'static> = LogId {
        table: "t",
        semantics_version: "js-6",
        discriminator: "d",
        day: Some(7),
    };

    fn one(base: bool, body: &[u8]) -> Vec<Write<'_>> {
        vec![Write {
            id: DAY,
            base,
            body,
        }]
    }

    /// A reader takes what the manifest names, and nothing else under the log's directory: an
    /// object a failed publication left is not read. A retried delta replaces its first copy.
    #[test]
    fn a_reader_takes_what_the_manifest_names() {
        let (lake, root) = lake("named");
        let logs = StateLogs::at(&lake);
        assert_eq!(logs.read(&DAY).expect("empty"), LogRead::default());
        // An orphan: an object of the log's shape, named nowhere.
        analyzer_objectstore::ObjectStore::put(
            lake.store(),
            &format!("{}delta-{:020}-{:020}.bin", logs.dir(&DAY), 9, 99),
            b"orphan",
        )
        .expect("orphan");
        logs.publish(Some(7), 2, &one(true, b"base 2"))
            .expect("base");
        logs.publish(Some(7), 3, &one(false, b"delta 3"))
            .expect("delta");
        logs.publish(Some(7), 4, &one(false, b"first 4"))
            .expect("delta");
        logs.publish(Some(7), 4, &one(false, b"retry 4"))
            .expect("retry");
        let read = logs.read(&DAY).expect("read");
        assert_eq!(read.base, Some((2, b"base 2".to_vec())));
        assert_eq!(
            read.deltas,
            vec![(3, b"delta 3".to_vec()), (4, b"retry 4".to_vec())],
            "the orphan is not read, the retry replaces its first copy"
        );
        assert!(
            logs.publish(Some(8), 5, &one(false, b"x")).is_err(),
            "a write for another day"
        );
        std::fs::remove_dir_all(root).ok();
    }

    /// Retiring takes the days before the cut, their manifests and objects alike, and leaves a
    /// later day and an undated log untouched.
    #[test]
    fn retiring_drops_the_old_days_whole_and_keeps_the_rest() {
        let (lake, root) = lake("retire");
        let logs = StateLogs::at(&lake);
        let at = |day: Option<i64>| LogId { day, ..DAY };
        for day in [Some(5), Some(7), None] {
            logs.publish(
                day,
                1,
                &[Write {
                    id: at(day),
                    base: true,
                    body: b"rows",
                }],
            )
            .expect("publish");
        }
        assert_eq!(logs.retire_before(7).expect("retire"), vec![5]);
        assert_eq!(logs.read(&at(Some(5))).expect("read"), LogRead::default());
        assert!(
            analyzer_objectstore::ObjectStore::list(lake.store(), &logs.dir(&at(Some(5))))
                .expect("list")
                .is_empty(),
            "the retired day's objects leave the store"
        );
        assert_eq!(
            logs.read(&at(Some(7))).expect("read").base,
            Some((1, b"rows".to_vec()))
        );
        assert_eq!(
            logs.read(&at(None)).expect("read").base,
            Some((1, b"rows".to_vec()))
        );
        std::fs::remove_dir_all(root).ok();
    }

    /// A merge replaces the newest deltas as they were read, a fold the base and every delta
    /// read, and a delta published after the read stays above either; the replaced objects
    /// leave the store. Dropping the log takes its names and every object, the orphan's too.
    #[test]
    fn a_compaction_replaces_what_it_read_and_a_log_drops_whole() {
        let (lake, root) = lake("replace");
        let logs = StateLogs::at(&lake);
        logs.publish(Some(7), 1, &one(true, b"base")).expect("base");
        for p in 2..6 {
            logs.publish(Some(7), p, &one(false, format!("d{p}").as_bytes()))
                .expect("delta");
        }
        let read_at = logs.named(&DAY).expect("named");
        logs.publish(Some(7), 6, &one(false, b"after"))
            .expect("after the read");
        logs.replace(&DAY, &read_at, (Replace::Merge(2), 5), b"d4+d5")
            .expect("merge");
        let deltas = |logs: &StateLogs| {
            logs.read(&DAY)
                .expect("read")
                .deltas
                .into_iter()
                .map(|(p, b)| (p, String::from_utf8(b).expect("utf8")))
                .collect::<Vec<_>>()
        };
        // The merge's group was not the newest when committed: left as it was.
        assert_eq!(
            deltas(&logs),
            vec![
                (2, "d2".into()),
                (3, "d3".into()),
                (4, "d4".into()),
                (5, "d5".into()),
                (6, "after".into())
            ]
        );
        let read_at = logs.named(&DAY).expect("named");
        logs.replace(&DAY, &read_at, (Replace::Merge(3), 6), b"d4+d5+after")
            .expect("merge");
        assert_eq!(
            deltas(&logs),
            vec![
                (2, "d2".into()),
                (3, "d3".into()),
                (6, "d4+d5+after".into())
            ]
        );
        let read_at = logs.named(&DAY).expect("named");
        logs.publish(Some(7), 7, &one(false, b"later"))
            .expect("later");
        logs.replace(&DAY, &read_at, (Replace::Fold, 6), b"folded")
            .expect("fold");
        let read = logs.read(&DAY).expect("read");
        assert_eq!(read.base, Some((6, b"folded".to_vec())));
        assert_eq!(
            read.deltas,
            vec![(7, b"later".to_vec())],
            "a later delta stays"
        );
        let objects = std::fs::read_dir(root.join("r/state/t/js-6/d/7"))
            .expect("dir")
            .count();
        assert_eq!(objects, 2, "the replaced objects are gone");
        logs.drop_log(&DAY).expect("drop");
        assert_eq!(logs.read(&DAY).expect("dropped"), LogRead::default());
        assert_eq!(
            std::fs::read_dir(root.join("r/state/t/js-6/d/7")).map_or(0, Iterator::count),
            0
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn an_undated_log_and_an_empty_discriminator_have_their_own_paths() {
        let (lake, root) = lake("undated");
        let logs = StateLogs::at(&lake);
        let undated = LogId {
            table: "markers",
            semantics_version: "input-1",
            discriminator: "",
            day: None,
        };
        logs.publish(
            None,
            1,
            &[Write {
                id: undated,
                base: true,
                body: b"x",
            }],
        )
        .expect("base");
        assert!(root.join("r/state/markers/input-1/_/undated").is_dir());
        assert_eq!(
            logs.read(&undated).expect("read").base,
            Some((1, b"x".to_vec()))
        );
        std::fs::remove_dir_all(root).ok();
    }
}
