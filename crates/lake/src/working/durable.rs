// SPDX-License-Identifier: AGPL-3.0-only

//! The working state's durability (`docs/lake.md`):
//! incremental checkpoints taken in the background between units of work, each naming the
//! position (the newest committed classification revision) it holds; a restore is the newest
//! checkpoint, and the days committed past its position are replayed from the spine.
//!
//! **A checkpoint** is a hard-link snapshot taken when a unit ends, once
//! [`CHECKPOINT_BYTES`] have been committed or [`CHECKPOINT_INTERVAL_MS`] has passed since the
//! last, and uploaded by a thread of its own: SST files once, under their epoch, the manifest and
//! its pointers under the checkpoint's number. Its files, sequence number and position are named
//! in the working state's head document ([`WorkingHead`]) by conditional write under the
//! journal's lease once uploaded. One upload at a time.
//!
//! **A restore** makes the newest checkpoint the live database, from the local copy when it is
//! that checkpoint and from the bucket otherwise.
//!
//! **Garbage**: a checkpoint past the newest [`RETAINED`] is queued with the time it stopped
//! being named and its files deleted once that is [`GARBAGE_MIN_AGE_MS`] past, so a restore that
//! began before never loses a file (`SlateDB`'s collector keeps the same rule). Each checkpoint
//! also sweeps the prefix: an object nothing names nor queues (a failed upload's, a fenced
//! writer's) is queued the same way. A key the head names is never deleted.

use super::{is_sst, local, now_ms, Db, WorkingState, CHECKPOINT, COMMITTED, LIVE};
use crate::head::{Document, Head};
use crate::Error;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Checkpoints the head keeps named: a restore reads the newest; the older ones cover a restore
/// that began before the newest was named.
pub const RETAINED: usize = 3;
/// How long a file stays in the bucket after nothing retained names it.
pub const GARBAGE_MIN_AGE_MS: u64 = 10 * 60 * 1000;
/// Bytes committed since the last checkpoint past which the next is due.
pub const CHECKPOINT_BYTES: u64 = 2 << 30;
/// Time since the last checkpoint past which the next is due.
pub const CHECKPOINT_INTERVAL_MS: u64 = 30 * 60 * 1000;

/// The head document's format.
const FORMAT: u32 = 1;

/// One file of a checkpoint: its name in the database, its object in the bucket, its size.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointFile {
    /// The file's name in the database directory.
    pub name: String,
    /// The object that holds it.
    pub key: String,
    /// Its bytes, checked on restore.
    pub bytes: u64,
}

/// One checkpoint, as the head names it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StateCheckpoint {
    /// The lease epoch of the writer that took it.
    pub epoch: u64,
    /// Its number: one more than the checkpoint before it.
    pub number: u64,
    /// The database's sequence number it holds everything up to.
    pub sequence: u64,
    /// The newest committed classification revision it holds: the days committed past it are
    /// replayed after a restore.
    pub position: u64,
    /// Its files.
    pub files: Vec<CheckpointFile>,
}

impl StateCheckpoint {
    /// How a local checkpoint directory names the checkpoint it holds.
    fn marker(&self) -> String {
        format!("{} {} {}", self.epoch, self.number, self.sequence)
    }
}

/// An object nothing retained names, and since when.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Garbage {
    /// The object.
    pub key: String,
    /// When it stopped being named.
    pub since_ms: u64,
}

/// The working state's head: its retained checkpoints and the garbage awaiting its minimum age.
/// Committed under the journal's lease; written by the checkpoint alone.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WorkingHead {
    format: u32,
    /// The retained checkpoints, oldest first.
    pub checkpoints: Vec<StateCheckpoint>,
    /// Objects nothing retained names.
    pub garbage: Vec<Garbage>,
    lease_epoch: u64,
}

impl Default for WorkingHead {
    fn default() -> Self {
        Self {
            format: FORMAT,
            checkpoints: Vec::new(),
            garbage: Vec::new(),
            lease_epoch: 0,
        }
    }
}

impl Document for WorkingHead {
    const NAME: &'static str = "working state head";
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

impl WorkingHead {
    /// Name `checkpoint`, retire the checkpoints past [`RETAINED`], and queue what nothing
    /// retained names.
    fn name(&mut self, checkpoint: &StateCheckpoint, now: u64) {
        if self
            .checkpoints
            .iter()
            .any(|c| c.number == checkpoint.number)
        {
            return;
        }
        self.checkpoints.push(checkpoint.clone());
        let drop = self.checkpoints.len().saturating_sub(RETAINED);
        let dropped: Vec<StateCheckpoint> = self.checkpoints.drain(..drop).collect();
        let retained: HashSet<&String> = self
            .checkpoints
            .iter()
            .flat_map(|c| c.files.iter().map(|f| &f.key))
            .collect();
        let unnamed: Vec<Garbage> = dropped
            .iter()
            .flat_map(|c| &c.files)
            .filter(|f| !retained.contains(&f.key))
            .map(|f| Garbage {
                key: f.key.clone(),
                since_ms: now,
            })
            .collect();
        self.garbage.extend(unnamed);
    }
}

impl WorkingHead {
    /// Every object the head names: the retained checkpoints' files.
    fn named(&self) -> HashSet<&str> {
        self.checkpoints
            .iter()
            .flat_map(|c| c.files.iter().map(|f| f.key.as_str()))
            .collect()
    }

    /// Queue what lies under the working state's prefix and nothing names nor queues: an
    /// upload whose checkpoint failed, and whose best-effort delete failed too, or a
    /// fenced writer's. It waits out the minimum age like any garbage.
    fn sweep(&mut self, listed: &[String], now: u64) {
        let named = self.named();
        let queued: HashSet<&str> = self.garbage.iter().map(|g| g.key.as_str()).collect();
        let orphans: Vec<Garbage> = listed
            .iter()
            .filter(|k| !named.contains(k.as_str()) && !queued.contains(k.as_str()))
            .map(|k| Garbage {
                key: k.clone(),
                since_ms: now,
            })
            .collect();
        self.garbage.extend(orphans);
    }
}

impl crate::Lake {
    pub(crate) fn working_head(&self) -> Result<Head<WorkingHead>, Error> {
        Ok(Head::sharing_lease(
            self.store().sibling()?,
            &format!("{}state-db/", self.root()),
            &self.journal_dir(),
            self.lineage().clone(),
        ))
    }
}

/// The sizes the checkpoint and the commit go by: [`CHECKPOINT_BYTES`],
/// [`CHECKPOINT_INTERVAL_MS`] and [`super::INGEST_BYTES`], smaller in a test that exercises
/// them.
#[derive(Debug, Clone, Copy)]
pub(super) struct Limits {
    pub(super) checkpoint_bytes: u64,
    pub(super) checkpoint_interval_ms: u64,
    pub(super) ingest_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            checkpoint_bytes: CHECKPOINT_BYTES,
            checkpoint_interval_ms: CHECKPOINT_INTERVAL_MS,
            ingest_bytes: super::INGEST_BYTES,
        }
    }
}

/// A live database a restore made, the position it holds, and the checkpoint it came from.
pub(super) struct Restored {
    pub(super) db: Db,
    pub(super) position: u64,
    pub(super) from: Option<StateCheckpoint>,
}

impl StateCheckpoint {
    /// The SST files it names, by name: their objects.
    pub(super) fn uploaded(&self) -> HashMap<String, String> {
        self.files
            .iter()
            .filter(|f| is_sst(&f.name))
            .map(|f| (f.name.clone(), f.key.clone()))
            .collect()
    }

    /// Who it is, as a clean close's marker and a local copy name it.
    pub(super) fn identity(&self) -> String {
        self.marker()
    }
}

/// A hard-link snapshot awaiting its upload.
struct Snapshot {
    at: PathBuf,
    sequence: u64,
    position: u64,
}

impl WorkingState {
    /// Start a checkpoint in the background once one is due and none is running: a snapshot
    /// now, between units, its upload on a thread of its own.
    pub(super) fn checkpoint_if_due(self: &Arc<Self>, lake: &crate::Lake) -> Result<(), Error> {
        let due = self.written.load(Ordering::Relaxed) >= self.limits.checkpoint_bytes
            || now_ms().saturating_sub(self.checkpointed_ms.load(Ordering::Relaxed))
                >= self.limits.checkpoint_interval_ms;
        if !due || self.uploading.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let snapshot = match self.snapshot() {
            Ok(snapshot) => snapshot,
            Err(e) => {
                self.uploading.store(false, Ordering::Release);
                return Err(e);
            }
        };
        let state = Arc::clone(self);
        let lake = lake.clone();
        std::thread::Builder::new()
            .name("working-checkpoint".into())
            .spawn(move || {
                if let Err(e) = state.publish(&lake, snapshot) {
                    log::warn!("working state checkpoint failed, the next seal retries: {e}");
                }
                state.uploading.store(false, Ordering::Release);
            })
            .map_err(local)?;
        Ok(())
    }

    /// A checkpoint now, on this thread, once a running upload is done. Everything written so
    /// far is durable when it returns; returns the bytes it uploaded.
    ///
    /// # Errors
    /// The local disk, the store or the database failed, or the lease is lost.
    pub fn checkpoint(&self, lake: &crate::Lake) -> Result<u64, Error> {
        while self.uploading.swap(true, Ordering::AcqRel) {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        let taken = self.snapshot().and_then(|s| self.publish(lake, s));
        self.uploading.store(false, Ordering::Release);
        taken
    }

    /// A hard-link snapshot of the live database at its latest sequence number, and the
    /// position it holds: no commit between the flush and the links.
    fn snapshot(&self) -> Result<Snapshot, Error> {
        let db = self.exclusive()?;
        let sequence = db.latest_sequence_number();
        let position = self.held.load(Ordering::Acquire);
        let at = self.dir.join(format!("{CHECKPOINT}.pending-{sequence}"));
        if at.exists() {
            std::fs::remove_dir_all(&at).map_err(local)?;
        }
        rocksdb::checkpoint::Checkpoint::new(&db)
            .and_then(|c| c.create_checkpoint(&at))
            .map_err(local)?;
        // What it holds is counted from here: a commit after the links is the next one's.
        self.written.store(0, Ordering::Relaxed);
        Ok(Snapshot {
            at,
            sequence,
            position,
        })
    }

    /// Upload a snapshot and name it: the background thread's work.
    fn publish(&self, lake: &crate::Lake, snapshot: Snapshot) -> Result<u64, Error> {
        let uploaded = self.upload_snapshot(lake, &snapshot);
        let _one = self
            .head_commits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.name_snapshot(lake, snapshot, uploaded?)
    }

    /// Upload a snapshot's files the bucket lacks, side by side: an SST file once, under the
    /// epoch; the manifest and its pointers under the checkpoint's number.
    fn upload_snapshot(
        &self,
        lake: &crate::Lake,
        snapshot: &Snapshot,
    ) -> Result<(StateCheckpoint, HashSet<String>), Error> {
        let at = &snapshot.at;
        let head = lake.working_head()?;
        let epoch = head.hold_lease()?;
        let number = head.read()?.checkpoints.last().map_or(1, |c| c.number + 1);
        let mut names: Vec<String> = std::fs::read_dir(at)
            .map_err(local)?
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .collect();
        names.sort();
        let known = self
            .uploaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let files = crate::in_parallel(&names, |name| {
            let bytes = std::fs::metadata(at.join(name)).map_err(local)?.len();
            if let Some(key) = known.get(name).filter(|_| is_sst(name)) {
                let file = CheckpointFile {
                    name: name.clone(),
                    key: key.clone(),
                    bytes,
                };
                return Ok::<_, Error>((file, false));
            }
            let path = if is_sst(name) {
                format!("sst/{name}")
            } else {
                format!("checkpoint-{number:020}/{name}")
            };
            let key = Self::object(lake, epoch, &path);
            lake.store()
                .put(&key, &std::fs::read(at.join(name)).map_err(local)?)?;
            let file = CheckpointFile {
                name: name.clone(),
                key,
                bytes,
            };
            Ok((file, true))
        });
        let files = match files {
            Ok(files) => files,
            Err(e) => {
                let _ = std::fs::remove_dir_all(at);
                return Err(e);
            }
        };
        let fresh = files
            .iter()
            .filter(|(_, fresh)| *fresh)
            .map(|(f, _)| f.key.clone())
            .collect();
        Ok((
            StateCheckpoint {
                epoch,
                number,
                sequence: snapshot.sequence,
                position: snapshot.position,
                files: files.into_iter().map(|(f, _)| f).collect(),
            },
            fresh,
        ))
    }

    /// Name an uploaded snapshot in the head; make it the local copy; collect the garbage.
    fn name_snapshot(
        &self,
        lake: &crate::Lake,
        Snapshot { at, .. }: Snapshot,
        (checkpoint, fresh): (StateCheckpoint, HashSet<String>),
    ) -> Result<u64, Error> {
        let started = std::time::Instant::now();
        let now = now_ms();
        let head = lake.working_head()?;
        // What lies under the prefix, the head document aside: the sweep's candidates.
        let prefix = format!("{}state-db/", lake.root());
        let listed: Vec<String> = lake
            .store()
            .list(&prefix)?
            .into_iter()
            .map(|o| o.key)
            .filter(|k| !k.ends_with("state-db/head"))
            .collect();
        let committed = match head.commit(|h| {
            h.name(&checkpoint, now);
            h.sweep(&listed, now);
        }) {
            Ok(committed) => committed,
            Err(e) => {
                // Named nowhere: best effort, the error is the commit's.
                for key in &fresh {
                    let _ = lake.store().delete(key);
                }
                let _ = std::fs::remove_dir_all(&at);
                return Err(e);
            }
        };
        std::fs::write(at.join(COMMITTED), checkpoint.marker()).map_err(local)?;
        {
            let _copy = self
                .checkpointing
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Self::replace_copy(&self.dir, &at)?;
        }
        self.uploaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(
                checkpoint
                    .files
                    .iter()
                    .filter(|f| is_sst(&f.name))
                    .map(|f| (f.name.clone(), f.key.clone())),
            );
        // Durable now that the head names it: a rollback comes back to it.
        self.checkpointed
            .fetch_max(checkpoint.position, Ordering::AcqRel);
        *self
            .descends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(checkpoint.identity());
        self.checkpointed_ms.store(now, Ordering::Relaxed);
        let uploaded: u64 = checkpoint
            .files
            .iter()
            .filter(|f| fresh.contains(&f.key))
            .map(|f| f.bytes)
            .sum();
        log::info!(
            "working state checkpoint {} at sequence {}, position {}: {} MB uploaded, named in {:.1}s; {}",
            checkpoint.number,
            checkpoint.sequence,
            checkpoint.position,
            uploaded >> 20,
            started.elapsed().as_secs_f64(),
            self.column_sizes()
                .map_or_else(|e| format!("sizes unread: {e}"), |sizes| sizes.join(", "))
        );
        Self::collect(lake, &head, &committed.garbage, now)?;
        Ok(uploaded)
    }

    fn object(lake: &crate::Lake, epoch: u64, path: &str) -> String {
        format!("{}state-db/{epoch:020}/{path}", lake.root())
    }

    /// Delete the garbage unnamed for its minimum age, then drop it from the head.
    fn collect(
        lake: &crate::Lake,
        head: &Head<WorkingHead>,
        garbage: &[Garbage],
        now: u64,
    ) -> Result<(), Error> {
        let named: HashSet<String> = head
            .read()?
            .named()
            .into_iter()
            .map(str::to_string)
            .collect();
        // A key the head names is never deleted, whatever queued it.
        let due: Vec<String> = garbage
            .iter()
            .filter(|g| now.saturating_sub(g.since_ms) >= GARBAGE_MIN_AGE_MS)
            .filter(|g| !named.contains(&g.key))
            .map(|g| g.key.clone())
            .collect();
        if !due.is_empty() {
            crate::in_parallel(&due, |key| lake.store().delete(key).map_err(Error::from))?;
            head.commit(|h| h.garbage.retain(|g| !due.contains(&g.key)))?;
        }
        Ok(())
    }

    /// Make `dir`'s live database the newest checkpoint `head` names: from the local copy when
    /// it is that one, downloaded into a sibling directory otherwise.
    pub(super) fn restore(
        dir: &Path,
        lake: &crate::Lake,
        (declared, memory): (&[super::Declared], &super::Memory),
    ) -> Result<Restored, Error> {
        let started = std::time::Instant::now();
        let head = lake.working_head()?.read()?;
        let newest = head.checkpoints.last();
        let live = dir.join(LIVE);
        if live.exists() {
            std::fs::remove_dir_all(&live).map_err(local)?;
        }
        if let Some(newest) = newest {
            let copy = dir.join(CHECKPOINT);
            let copy_is = std::fs::read_to_string(copy.join(COMMITTED)).ok();
            if copy_is.as_deref() != Some(newest.marker().as_str()) {
                Self::download(dir, lake, newest)?;
            }
            std::fs::create_dir_all(&live).map_err(local)?;
            for entry in std::fs::read_dir(&copy).map_err(local)? {
                let name = entry.map_err(local)?.file_name();
                let Some(text) = name.to_str() else { continue };
                if text == COMMITTED {
                    continue;
                }
                if is_sst(text) {
                    std::fs::hard_link(copy.join(&name), live.join(&name)).map_err(local)?;
                } else {
                    std::fs::copy(copy.join(&name), live.join(&name)).map_err(local)?;
                }
            }
        }
        let db = Self::open_live(dir, declared, memory)?;
        let sequence = newest.map_or(0, |c| c.sequence);
        if db.latest_sequence_number() != sequence {
            return Err(Error::Corrupt(format!(
                "working state checkpoint holds sequence {}, named {sequence}",
                db.latest_sequence_number()
            )));
        }
        let position = newest.map_or(0, |c| c.position);
        if let Some(newest) = newest {
            log::info!(
                "working state restored to checkpoint {} (sequence {sequence}, position {position}) in {:.1}s",
                newest.number,
                started.elapsed().as_secs_f64()
            );
        }
        Ok(Restored {
            db,
            position,
            from: newest.cloned(),
        })
    }

    /// The newest checkpoint the working head names, if any.
    pub(super) fn newest(lake: &crate::Lake) -> Result<Option<StateCheckpoint>, Error> {
        Ok(lake.working_head()?.read()?.checkpoints.last().cloned())
    }

    /// Download `checkpoint` as the local copy: into a sibling directory, every file's size
    /// checked, then renamed into place.
    fn download(dir: &Path, lake: &crate::Lake, checkpoint: &StateCheckpoint) -> Result<(), Error> {
        let started = std::time::Instant::now();
        let staging = dir.join(format!("{CHECKPOINT}.restoring-{}", std::process::id()));
        if staging.exists() {
            std::fs::remove_dir_all(&staging).map_err(local)?;
        }
        std::fs::create_dir_all(&staging).map_err(local)?;
        let fetched = crate::in_parallel(&checkpoint.files, |file| {
            let body = lake.store().get(&file.key)?;
            if body.len() as u64 != file.bytes {
                return Err(Error::Corrupt(format!(
                    "working state checkpoint {}: {} is {} bytes, named {}",
                    checkpoint.number,
                    file.key,
                    body.len(),
                    file.bytes
                )));
            }
            std::fs::write(staging.join(&file.name), body).map_err(local)
        })
        .and_then(|_| std::fs::write(staging.join(COMMITTED), checkpoint.marker()).map_err(local));
        if let Err(e) = fetched {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(e);
        }
        Self::replace_copy(dir, &staging)?;
        log::info!(
            "working state checkpoint {} downloaded: {} file(s), {} kB in {:.1}s",
            checkpoint.number,
            checkpoint.files.len(),
            checkpoint.files.iter().map(|f| f.bytes).sum::<u64>() / 1_000,
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// Make `from` the local checkpoint copy.
    fn replace_copy(dir: &Path, from: &Path) -> Result<(), Error> {
        let copy = dir.join(CHECKPOINT);
        if copy.exists() {
            std::fs::remove_dir_all(&copy).map_err(local)?;
        }
        std::fs::rename(from, &copy).map_err(local)
    }
}
