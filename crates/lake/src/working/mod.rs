// SPDX-License-Identifier: AGPL-3.0-only

//! The fold's working state: its keyed relations in one embedded `RocksDB` per lineage and
//! process, checkpointed to the bucket (`docs/lake.md`). The bucket is the
//! record; the local database is the working copy between checkpoints.
//!
//! **Relations.** A relation is declared when the state opens ([`Declared`]): its name, whether
//! it lives per day, and its index. Each is one column family. A record's key is its day (for
//! a dated relation), its identity hash, then its key: the spine's order, the hash then the
//! identity, which leads every key. So a probe by hash is a seek; a stripe, the hash's top
//! bits, is one range; any shard of the identity space is one range; and a day is a prefix,
//! cleared by one range delete. A record's value is its version, its `gone` flag, its hash and
//! its payload.
//!
//! **Indexes**, each a column family of keys that name a record, checked against the record on
//! every use, so an entry the record has moved away from is skipped, never trusted:
//! - [`Index::Instants`] finds the records at or after an instant their writer stated.
//!
//! **Writes are merges, and they must be.** A record's version is the event-time ceiling of the
//! pass that stated it, not the order of writes: a cascade re-passing an older day states the
//! current frontier's identities at a lower version than they hold, and must lose. The merge
//! operator keeps the keyed rule, the highest version winning and a later write winning a tie;
//! plain puts, the last write winning, would let the older day overwrite the frontier. A `gone`
//! winner is no record. A tombstone is dropped only at the bottom level, where nothing older
//! lies beneath it.
//!
//! **A revision** is staged part by part into local files, so a whole-day restatement never
//! sits in memory, and nothing it writes is read before its commit. The commit moves it into the
//! relation under the state's write lock, so no reader in the process sees half of it: a base
//! clears the relation's scope first, then the staged records apply in part order. A writer that drops its revision uncommitted
//! discards what it staged ([`WorkingState::abandon`]).
//!
//! **Durability** (the `durable` module): the write-ahead log is off; checkpoints are taken in the
//! background between units of work, each naming the position it holds, the newest committed
//! classification revision. A unit of work ([`WorkingState::begin`], [`WorkingState::end`]) ends
//! at a position; one that did not end, because it failed or panicked, is rolled back to the
//! newest checkpoint before the next begins ([`WorkingState::settle`]), and the days committed
//! past that checkpoint's position ([`WorkingState::position`]) are replayed from the spine.
//!
//! **A process never trusts a live database it did not open**: opening restores the newest
//! checkpoint, the local copy where it matches.

use crate::keyed::{Changes, Record, Terms};
use crate::Error;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

type Db = rocksdb::DB;
/// A record's identity in a relation's scope: its hash and key.
type KeyId = (u64, Vec<u8>);

const MERGE: &str = "keyed_highest_version";
const DROP_GONE: &str = "drop_gone_at_bottom";
/// `RocksDB`'s level count, its default: the bottom level is the one below which nothing lies.
const LEVELS: i32 = 7;
/// The working state's memory where no ceiling sets it (a local spine, a test): one block
/// cache holding data, index and filter blocks, with the memtables charged to it.
pub const DEFAULT_MEMORY_BYTES: usize = 768 << 20;
/// The share of the working state's memory its memtables may take: past it, writers stall
/// while `RocksDB` flushes, so memory stays inside the budget.
pub const MEMTABLE_SHARE: f64 = 0.25;
/// Bits per key of the bloom filters: `RocksDB`'s documented setting for about 1% false
/// positives.
pub const BLOOM_BITS: f64 = 10.0;
/// Staged bytes past which a commit goes in as sorted files, ingested, and not as write
/// batches: the bulk path `RocksDB` keeps for loads (`TiKV`'s import, `CockroachDB`'s `AddSSTable`).
/// A tick's few thousand keys stay write batches, cheaper than a file and the memtable flush an
/// overlapping ingestion takes.
pub const INGEST_BYTES: u64 = 64 << 20;
/// Stripes a large commit builds at once: each holds its stripe's staged bytes and their sort,
/// about a sixteenth of the revision.
pub const BUILD_LANES: usize = 4;
/// The size of one partition of a file's index or filter: what a probe loads.
pub const METADATA_BLOCK_BYTES: usize = 4096;
/// Background threads for flushes and compactions.
pub const BACKGROUND_JOBS: i32 = 4;
/// Bytes a commit, a retirement or a drop writes per batch.
pub const WRITE_BATCH_BYTES: usize = 32 << 20;

/// The staged revisions' directory under the state's: each revision's parts, before its commit.
const STAGING: &str = "staging";
/// In a revision's staged directory: it staged a tombstone.
const TOMBSTONES: &str = "tombstones";
/// The live database's directory under the state's.
const LIVE: &str = "live";
/// The newest checkpoint's directory under the state's.
const CHECKPOINT: &str = "checkpoint";
/// In a checkpoint directory: the checkpoint the fold head names it as, written after the head.
const COMMITTED: &str = "COMMITTED";
/// In the state's directory: the live database was closed cleanly ([`WorkingState::close`]).
/// Its lines: the position it holds, the checkpoint it descends from, the lake it is.
const CLOSED: &str = "CLOSED";

/// How a relation is looked up besides by hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Index {
    /// By hash alone.
    Plain,
    /// Its records by the instant their writer states, for a read from an instant: a dated
    /// relation.
    Instants,
}

/// A relation the working state holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Declared {
    /// The relation's name, its column family's.
    pub name: &'static str,
    /// Whether it lives per day.
    pub dated: bool,
    /// Its index.
    pub index: Index,
}

impl Declared {
    /// Its index's column family, if it has one.
    fn aux(&self) -> Option<String> {
        match self.index {
            Index::Instants => Some(format!("{}.instant", self.name)),
            Index::Plain => None,
        }
    }

    fn scope_len(&self) -> usize {
        if self.dated {
            8
        } else {
            0
        }
    }
}

/// Where a relation's records live.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// The lineage's: one set of records.
    Lineage,
    /// One UTC day's.
    Day(i64),
}

/// A relation's records in one scope: what a reader or a writer names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Relation<'a> {
    /// The relation's name.
    pub name: &'a str,
    /// Its scope.
    pub scope: Scope,
}

mod durable;

pub use durable::{
    CheckpointFile, Garbage, StateCheckpoint, WorkingHead, CHECKPOINT_BYTES,
    CHECKPOINT_INTERVAL_MS, GARBAGE_MIN_AGE_MS, RETAINED,
};

fn local(e: impl std::fmt::Display) -> Error {
    Error::Local(format!("working state: {e}"))
}

/// Make a directory's entries durable: a rename or a removal in it.
fn sync_dir(dir: &Path) -> Result<(), Error> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(local)
}

fn now_ms() -> u64 {
    #[allow(clippy::cast_possible_truncation)] // milliseconds since the epoch fit u64
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// A value: the version, the `gone` flag, the hash, the payload.
fn encode_value(r: &Record) -> Vec<u8> {
    let mut v = Vec::with_capacity(17 + r.payload.len());
    v.extend_from_slice(&r.version.to_le_bytes());
    v.push(u8::from(r.gone));
    v.extend_from_slice(&r.hash.to_le_bytes());
    v.extend_from_slice(&r.payload);
    v
}

fn version_of(v: &[u8]) -> f64 {
    f64::from_le_bytes(v.get(..8).and_then(|b| b.try_into().ok()).unwrap_or([0; 8]))
}

fn is_gone(v: &[u8]) -> bool {
    v.get(8) == Some(&1)
}

/// The keyed rule over a key's values, oldest first: the highest version, a later value winning
/// a tie.
fn resolve(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
    let mut winner: Option<&[u8]> = existing;
    for op in operands {
        if winner.is_none_or(|w| version_of(op).total_cmp(&version_of(w)).is_ge()) {
            winner = Some(op);
        }
    }
    winner.map(<[u8]>::to_vec)
}

/// An `f64` as bytes whose order is the numbers' order.
fn ordered(x: f64) -> [u8; 8] {
    let bits = x.to_bits();
    let key = if bits >> 63 == 1 {
        !bits
    } else {
        bits | 1 << 63
    };
    key.to_be_bytes()
}

/// A day from a dated key's first eight bytes.
fn day_of(key: &[u8]) -> Option<i64> {
    #[allow(clippy::cast_possible_wrap)] // the sign bit is flipped back, not lost
    key.get(..8)
        .and_then(|b| b.try_into().ok())
        .map(|b| (u64::from_be_bytes(b) ^ (1 << 63)) as i64)
}

/// A scope's key prefix: a day as its big-endian bits with the sign flipped, so days sort.
fn scope_bytes(scope: Scope) -> Vec<u8> {
    match scope {
        Scope::Lineage => Vec::new(),
        #[allow(clippy::cast_sign_loss)] // the sign bit is flipped, not lost
        Scope::Day(day) => ((day as u64) ^ (1 << 63)).to_be_bytes().to_vec(),
    }
}

/// The least key greater than every key starting with `prefix`: the end of its range. The
/// empty prefix's range is the whole column family: every key here is shorter than 64 bytes of
/// `0xff`.
fn successor(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return end;
        }
    }
    vec![u8::MAX; 64]
}

/// Whether a database file is an SST: immutable, so uploaded once and linked, never copied.
fn is_sst(name: &str) -> bool {
    Path::new(name).extension().is_some_and(|e| e == "sst")
}

/// A stripe's tag in a staged record's key, which keeps a revision's parts in order within
/// their stripe.
fn stripe_tag(stripe: usize) -> Result<u8, Error> {
    if stripe >= crate::stripe::STRIPES {
        return Err(Error::Invalid(format!(
            "working state: stripe {stripe} is not below {}",
            crate::stripe::STRIPES
        )));
    }
    u8::try_from(stripe).map_err(local)
}

/// The keys of `stripe` in a scope: from its first hash to the next stripe's, or the scope's end.
fn stripe_range(scope: &[u8], stripe: usize) -> Result<(Vec<u8>, Vec<u8>), Error> {
    if stripe >= crate::stripe::STRIPES {
        return Err(Error::Invalid(format!(
            "working state: stripe {stripe} is not below {}",
            crate::stripe::STRIPES
        )));
    }
    let at = |hash: u64| {
        let mut k = scope.to_vec();
        k.extend_from_slice(&hash.to_be_bytes());
        k
    };
    let to = if stripe + 1 == crate::stripe::STRIPES {
        successor(scope)
    } else {
        at(crate::stripe::first_hash(stripe + 1))
    };
    Ok((at(crate::stripe::first_hash(stripe)), to))
}

/// A primary key: the scope, the hash, the key.
fn primary_key(scope: &[u8], hash: u64, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(scope.len() + 8 + key.len());
    k.extend_from_slice(scope);
    k.extend_from_slice(&hash.to_be_bytes());
    k.extend_from_slice(key);
    k
}

/// The record a primary key and its value hold, if live.
fn record_of(decl: &Declared, k: &[u8], v: &[u8]) -> Option<Record> {
    if is_gone(v) || v.len() < 17 {
        return None;
    }
    let skip = decl.scope_len() + 8;
    Some(Record {
        hash: u64::from_le_bytes(v[9..17].try_into().ok()?),
        key: k.get(skip..)?.to_vec(),
        version: version_of(v),
        gone: false,
        payload: v[17..].to_vec(),
    })
}

/// An index entry for the record at `primary`: `None` for a relation without an index.
fn aux_key(
    decl: &Declared,
    scope: &[u8],
    primary: &[u8],
    instant: Option<f64>,
) -> Result<Option<Vec<u8>>, Error> {
    Ok(match decl.index {
        Index::Instants => {
            let instant = instant.ok_or_else(|| {
                Error::Invalid(format!(
                    "working state: {} is indexed by instant, and a record came without one",
                    decl.name
                ))
            })?;
            let mut k = scope.to_vec();
            k.extend_from_slice(&ordered(instant));
            k.extend_from_slice(&primary[scope.len()..]);
            Some(k)
        }
        Index::Plain => None,
    })
}

/// A staged record's value: its index entry (empty for none), its primary key, its value.
fn staged_value(aux: Option<&[u8]>, primary: &[u8], value: &[u8]) -> Vec<u8> {
    let aux = aux.unwrap_or_default();
    let mut v = Vec::with_capacity(8 + aux.len() + primary.len() + value.len());
    v.extend_from_slice(&u32::try_from(aux.len()).unwrap_or(u32::MAX).to_le_bytes());
    v.extend_from_slice(aux);
    v.extend_from_slice(
        &u32::try_from(primary.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    v.extend_from_slice(primary);
    v.extend_from_slice(value);
    v
}

/// A staged value's parts: its index entry (empty for none), its primary key, its value.
fn staged_parts(v: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
    let take = |at: usize| -> Option<(usize, usize)> {
        let len = u32::from_le_bytes(v.get(at..at + 4)?.try_into().ok()?) as usize;
        Some((at + 4, at + 4 + len))
    };
    let (aux_from, aux_to) = take(0)?;
    let (key_from, key_to) = take(aux_to)?;
    Some((
        v.get(aux_from..aux_to)?,
        v.get(key_from..key_to)?,
        v.get(key_to..)?,
    ))
}

/// A key of the state's own, in the default column family.
fn meta(name: &str) -> Vec<u8> {
    name.as_bytes().to_vec()
}

fn stated_key(relation: &Relation<'_>) -> Vec<u8> {
    let mut k = meta("stated/");
    k.extend_from_slice(relation.name.as_bytes());
    k.push(b'/');
    k.extend_from_slice(&scope_bytes(relation.scope));
    k
}

/// No write goes through the write-ahead log: a live database is never reopened, only rebuilt
/// from a checkpoint, which flushes the memtables before it links ([`durable`]).
fn write_options() -> rocksdb::WriteOptions {
    let mut opts = rocksdb::WriteOptions::default();
    opts.disable_wal(true);
    opts
}

/// One staged record, read back for a large commit: its primary key, its index entry (empty for
/// none), its value, and its place in the order written.
type StagedEntry<'a> = (&'a [u8], &'a [u8], &'a [u8], usize);

/// One stripe's built files and what they hold.
#[derive(Default)]
struct Built {
    primary: Option<PathBuf>,
    aux: Option<PathBuf>,
    bytes: u64,
    records: usize,
}

/// A large revision's files, awaiting ingestion ([`WorkingState::build_files`]).
#[derive(Default)]
struct Ingest {
    /// The stripes' files of the relation, in stripe order: disjoint ranges.
    primary: Vec<PathBuf>,
    /// The index's files, a file per stripe: overlapping ranges.
    aux: Vec<PathBuf>,
    /// Their bytes.
    bytes: u64,
}

/// Writes that go to the database in batches of [`WRITE_BATCH_BYTES`].
struct Batches<'a> {
    db: &'a Db,
    batch: rocksdb::WriteBatch,
}

impl<'a> Batches<'a> {
    fn new(db: &'a Db) -> Self {
        Self {
            db,
            batch: rocksdb::WriteBatch::default(),
        }
    }

    fn cf(&self, name: &str) -> Result<&'a rocksdb::ColumnFamily, Error> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| local(format!("no column family {name}")))
    }

    fn merge(&mut self, cf: &str, k: &[u8], v: &[u8]) -> Result<(), Error> {
        let cf = self.cf(cf)?;
        self.batch.merge_cf(cf, k, v);
        self.spill()
    }

    fn put(&mut self, cf: &str, k: &[u8], v: &[u8]) -> Result<(), Error> {
        let cf = self.cf(cf)?;
        self.batch.put_cf(cf, k, v);
        self.spill()
    }

    fn delete(&mut self, cf: &str, k: &[u8]) -> Result<(), Error> {
        let cf = self.cf(cf)?;
        self.batch.delete_cf(cf, k);
        self.spill()
    }

    fn delete_range(&mut self, cf: &str, from: &[u8], to: &[u8]) -> Result<(), Error> {
        let cf = self.cf(cf)?;
        self.batch.delete_range_cf(cf, from, to);
        self.spill()
    }

    fn spill(&mut self) -> Result<(), Error> {
        if self.batch.size_in_bytes() >= WRITE_BATCH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), Error> {
        if !self.batch.is_empty() {
            let batch = std::mem::take(&mut self.batch);
            self.db.write_opt(batch, &write_options()).map_err(local)?;
        }
        Ok(())
    }
}

type RawIter<'a> = rocksdb::DBRawIteratorWithThreadMode<'a, Db>;

self_cell::self_cell!(
    /// An iterator that owns the database handle it reads.
    struct OwnedIter {
        owner: Arc<Db>,
        #[not_covariant]
        dependent: RawIter,
    }
);

/// The live records of one stripe of a relation, in hash order, read through one iterator: one
/// consistent view, one block at a time in memory.
pub struct Stream {
    iter: OwnedIter,
    decl: Declared,
    started: bool,
}

impl Iterator for Stream {
    type Item = Result<Record, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let decl = self.decl;
        let started = &mut self.started;
        self.iter.with_dependent_mut(|_, it| loop {
            if *started {
                it.next();
            } else {
                it.seek_to_first();
                *started = true;
            }
            match (it.key(), it.value()) {
                (Some(k), Some(v)) => {
                    if let Some(r) = record_of(&decl, k, v) {
                        return Some(Ok(r));
                    }
                }
                _ => return it.status().err().map(|e| Err(local(e))),
            }
        })
    }
}

/// The live database under the lock's read side: present, checked when taken.
struct Live<'a>(std::sync::RwLockReadGuard<'a, Option<Arc<Db>>>);

impl<'a> Live<'a> {
    fn of(guard: std::sync::RwLockReadGuard<'a, Option<Arc<Db>>>) -> Result<Self, Error> {
        if guard.is_none() {
            return Err(lost());
        }
        Ok(Self(guard))
    }

    /// The handle, for a stream that outlives the lock.
    fn shared(&self) -> Arc<Db> {
        Arc::clone(
            self.0
                .as_ref()
                .unwrap_or_else(|| unreachable!("checked when taken")),
        )
    }
}

impl std::ops::Deref for Live<'_> {
    type Target = Db;
    fn deref(&self) -> &Db {
        self.0
            .as_deref()
            .unwrap_or_else(|| unreachable!("checked when taken"))
    }
}

/// The live database under the lock's write side: present, checked when taken.
struct Exclusive<'a>(std::sync::RwLockWriteGuard<'a, Option<Arc<Db>>>);

impl<'a> Exclusive<'a> {
    fn of(guard: std::sync::RwLockWriteGuard<'a, Option<Arc<Db>>>) -> Result<Self, Error> {
        if guard.is_none() {
            return Err(lost());
        }
        Ok(Self(guard))
    }
}

impl std::ops::Deref for Exclusive<'_> {
    type Target = Db;
    fn deref(&self) -> &Db {
        self.0
            .as_deref()
            .unwrap_or_else(|| unreachable!("checked when taken"))
    }
}

fn lost() -> Error {
    local("the live database is gone after a failed rollback; the next unit rebuilds it")
}

/// The working state's memory, one bound for all of it: the block cache holds data, index and
/// filter blocks, and the memtables are charged to it.
#[derive(Clone)]
struct Memory {
    cache: rocksdb::Cache,
    memtables: rocksdb::WriteBufferManager,
}

impl Memory {
    fn of(bytes: usize) -> Self {
        let cache = rocksdb::Cache::new_lru_cache(bytes);
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )] // a share of a byte count, rounded down
        // Charged to the cache and flushed past the share, but never stalling a writer, as
        // Flink's managed memory runs it: a stall waits for a flush to free memory, and the
        // memtables' reserved arena blocks count against the share whether or not they hold
        // anything, so a small share stalled a commit for good (the probe benchmark, 64 MiB).
        let memtables = rocksdb::WriteBufferManager::new_write_buffer_manager_with_cache(
            (bytes as f64 * MEMTABLE_SHARE) as usize,
            false,
            cache.clone(),
        );
        Self { cache, memtables }
    }
}

/// The working state's database, one per lineage and process ([`WorkingState::shared`]).
pub struct WorkingState {
    dir: PathBuf,
    /// The lake it is the state of ([`crate::Lake::identity`]): a clean close names it, as two
    /// lakes with one root in two stores share a directory.
    lake: String,
    declared: Vec<Declared>,
    /// The live database: a reader holds the lock's read side while it reads, a commit and a
    /// rollback its write side. `None` only while a rollback rebuilds it, or after one failed.
    db: RwLock<Option<Arc<Db>>>,
    memory: Memory,
    /// Revisions for this state's writers: local, as nothing outside it names them.
    revisions: AtomicU64,
    /// SST files already in the bucket, by name: their objects.
    uploaded: Mutex<HashMap<String, String>>,
    /// The local checkpoint copy is replaced or read by one at a time.
    checkpointing: Mutex<()>,
    /// The head is committed by one checkpoint at a time.
    head_commits: Mutex<()>,
    /// The position the live database holds: the newest one a unit ended at.
    held: AtomicU64,
    /// The position the newest named checkpoint holds: a rollback's.
    checkpointed: AtomicU64,
    /// The checkpoint the live database descends from: the one it was restored from or the
    /// newest it named. A clean close records it, and the next open trusts the live database
    /// only if the head names no newer one.
    descends: Mutex<Option<String>>,
    /// A checkpoint's upload is running.
    uploading: AtomicBool,
    /// When the last checkpoint was named, epoch milliseconds.
    checkpointed_ms: AtomicU64,
    /// A unit of work began and has not ended.
    in_unit: AtomicBool,
    /// Bytes commits applied since the last checkpoint's snapshot: records and index entries.
    written: AtomicU64,
    /// A test's crash: dropped without the clean close.
    #[cfg(test)]
    crashed: AtomicBool,
    /// The checkpoint's sizes.
    limits: durable::Limits,
}

impl Drop for WorkingState {
    /// An orderly teardown is a clean close ([`WorkingState::close`]); a failed unit still
    /// leaves no marker.
    fn drop(&mut self) {
        #[cfg(test)]
        if self.crashed.load(Ordering::Acquire) {
            return;
        }
        let open = self
            .db
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some();
        if open {
            if let Err(e) = self.close() {
                log::warn!("working state: the close on drop failed, the next open restores: {e}");
            }
        }
    }
}

impl WorkingState {
    /// The state of `lake` under `dir`, one per lake and process: in a subdirectory named by
    /// the lake's root, so two lakes on one data directory (two namespaces, say) never share
    /// one; and every opener of one lake shares it, as `RocksDB` allows one handle a directory.
    ///
    /// # Errors
    /// See [`Self::open`].
    pub fn shared(
        dir: &Path,
        lake: &crate::Lake,
        declared: &[Declared],
        memory_bytes: usize,
    ) -> Result<Arc<Self>, Error> {
        use sha2::Digest;
        use std::fmt::Write;
        static OPEN: std::sync::LazyLock<Mutex<HashMap<PathBuf, std::sync::Weak<WorkingState>>>> =
            std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
        let digest = sha2::Sha256::digest(lake.root().as_bytes());
        let name = digest.iter().take(16).fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        });
        let dir = dir.join(name);
        let dir = &dir;
        let mut open = OPEN
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(state) = open.get(dir).and_then(std::sync::Weak::upgrade) {
            if state.declared != declared {
                return Err(Error::Invalid(format!(
                    "working state {}: opened again with other relations",
                    dir.display()
                )));
            }
            return Ok(state);
        }
        let state = Arc::new(Self::open(dir, lake, declared, memory_bytes)?);
        open.insert(dir.clone(), Arc::downgrade(&state));
        Ok(state)
    }

    /// The state at `dir` holding `declared`, rebuilt from the newest checkpoint `lake`'s fold
    /// head names: from the local copy when it is that one, from the bucket otherwise.
    ///
    /// # Errors
    /// A declaration the state refuses, the local disk, the store or the database failed, or a
    /// restored file's size is not the checkpoint's.
    pub fn open(
        dir: &Path,
        lake: &crate::Lake,
        declared: &[Declared],
        memory_bytes: usize,
    ) -> Result<Self, Error> {
        for d in declared {
            let fits = match d.index {
                Index::Plain => true,
                Index::Instants => d.dated,
            };
            if !fits || d.name.is_empty() || d.name.contains('.') {
                return Err(Error::Invalid(format!(
                    "working state: {d:?} is not a relation it holds"
                )));
            }
        }
        std::fs::create_dir_all(dir).map_err(local)?;
        // What a revision staged and never committed is no one's.
        let staging = dir.join(STAGING);
        if staging.exists() {
            std::fs::remove_dir_all(&staging).map_err(local)?;
        }
        let memory = Memory::of(memory_bytes);
        let newest = Self::newest(lake)?;
        let identity = lake.identity();
        let (db, position, from) = if let Some((db, position)) =
            Self::reopen(dir, (&identity, newest.as_ref()), declared, &memory)?
        {
            (db, position, newest)
        } else {
            let restored = Self::restore(dir, lake, (declared, &memory))?;
            (restored.db, restored.position, restored.from)
        };
        let state = Self {
            dir: dir.to_path_buf(),
            lake: identity,
            declared: declared.to_vec(),
            db: RwLock::new(Some(Arc::new(db))),
            memory,
            revisions: AtomicU64::new(1),
            uploaded: Mutex::new(
                from.as_ref()
                    .map(durable::StateCheckpoint::uploaded)
                    .unwrap_or_default(),
            ),
            checkpointing: Mutex::new(()),
            head_commits: Mutex::new(()),
            held: AtomicU64::new(position),
            checkpointed: AtomicU64::new(from.as_ref().map_or(0, |c| c.position)),
            descends: Mutex::new(from.as_ref().map(durable::StateCheckpoint::identity)),
            uploading: AtomicBool::new(false),
            checkpointed_ms: AtomicU64::new(now_ms()),
            in_unit: AtomicBool::new(false),
            written: AtomicU64::new(0),
            #[cfg(test)]
            crashed: AtomicBool::new(false),
            limits: durable::Limits::default(),
        };
        log::info!(
            "working state opened at position {position}: {}",
            state
                .column_sizes()
                .map_or_else(|e| format!("sizes unread: {e}"), |sizes| sizes.join(", "))
        );
        Ok(state)
    }

    /// The live database a clean close left, and the position it holds: only when its marker
    /// is there and names this lake and `newest`, the checkpoint the head names now. The marker
    /// goes first, durably, whatever it says: a crash after this open leaves nothing to trust.
    fn reopen(
        dir: &Path,
        (lake, newest): (&str, Option<&durable::StateCheckpoint>),
        declared: &[Declared],
        memory: &Memory,
    ) -> Result<Option<(Db, u64)>, Error> {
        let marker = dir.join(CLOSED);
        let Ok(text) = std::fs::read_to_string(&marker) else {
            return Ok(None);
        };
        std::fs::remove_file(&marker).map_err(local)?;
        sync_dir(dir)?;
        let mut lines = text.lines();
        let position = lines.next().and_then(|p| p.parse::<u64>().ok());
        let descends = lines.next();
        let closed_as = lines.next();
        let newest = newest.map_or_else(|| "none".to_string(), durable::StateCheckpoint::identity);
        let ours = descends == Some(newest.as_str()) && closed_as == Some(lake);
        let (Some(position), true) = (position, ours) else {
            log::info!(
                "working state: the clean close's marker names {descends:?} of {closed_as:?}, \
                 the head {newest} of {lake}; restoring"
            );
            return Ok(None);
        };
        if !dir.join(LIVE).exists() {
            return Ok(None);
        }
        let db = Self::open_live(dir, declared, memory)?;
        log::info!("working state reopened as closed cleanly, at position {position}");
        Ok(Some((db, position)))
    }

    /// Close the live database cleanly, for a restart on this box to trust
    /// ([`Self::open`]): its memtables flushed, the handle closed, then a marker naming the
    /// position it holds and the checkpoint it descends from. A unit that did not end, or a
    /// stream still open, leaves no marker, and the next open restores. The state refuses every
    /// use after. Dropping the state closes it the same way; a killed process runs neither.
    /// Returns whether it closed cleanly.
    ///
    /// # Errors
    /// The flush or the marker's write failed: the next open restores.
    pub fn close(&self) -> Result<bool, Error> {
        let _copy = self
            .checkpointing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut db = self
            .db
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let clean = !self.in_unit.load(Ordering::Acquire)
            && db.as_ref().is_some_and(|d| Arc::strong_count(d) == 1);
        if !clean {
            log::warn!("working state: a unit or a stream is open; the next open restores");
            return Ok(false);
        }
        if let Some(live) = db.as_ref() {
            for d in &self.declared {
                for name in std::iter::once(d.name.to_string()).chain(d.aux()) {
                    live.flush_cf(Self::cf(live, &name)?).map_err(local)?;
                }
            }
        }
        // The handle closes, and RocksDB with it, before the marker says it may be trusted.
        *db = None;
        let descends = self
            .descends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| "none".to_string());
        let position = self.held.load(Ordering::Acquire);
        let pending = self.dir.join(format!("{CLOSED}.pending"));
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&pending).map_err(local)?;
            file.write_all(format!("{position}\n{descends}\n{}\n", self.lake).as_bytes())
                .map_err(local)?;
            file.sync_all().map_err(local)?;
        }
        std::fs::rename(&pending, self.dir.join(CLOSED)).map_err(local)?;
        sync_dir(&self.dir)?;
        log::info!("working state closed cleanly at position {position}");
        Ok(true)
    }

    /// Drop without the clean close, as a killed process leaves the state.
    #[cfg(test)]
    fn crash(&self) {
        self.crashed.store(true, Ordering::Release);
    }

    fn cf_options(cache: &rocksdb::Cache, prefix: Option<usize>, merge: bool) -> rocksdb::Options {
        let mut table = rocksdb::BlockBasedOptions::default();
        table.set_block_cache(cache);
        // Index and filter blocks in the cache, so the budget bounds them too; the first
        // level's pinned, as a probe reads them first.
        table.set_cache_index_and_filter_blocks(true);
        table.set_cache_index_and_filter_blocks_with_high_priority(true);
        table.set_pin_l0_filter_and_index_blocks_in_cache(true);
        table.set_bloom_filter(BLOOM_BITS, false);
        table.set_whole_key_filtering(true);
        // Partitioned: a probe loads one small partition of a file's index and filter, not
        // the whole of either, and the top levels stay pinned. A state many times its cache
        // would otherwise re-read a whole filter block on every seek once the filters alone
        // outgrow the cache's high-priority half; TiKV partitions for the same reason.
        table.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
        table.set_partition_filters(true);
        table.set_metadata_block_size(METADATA_BLOCK_BYTES);
        table.set_pin_top_level_index_and_filter(true);
        table.set_optimize_filters_for_memory(true);
        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&table);
        opts.set_num_levels(LEVELS);
        opts.set_level_compaction_dynamic_level_bytes(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        if let Some(len) = prefix {
            opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(len));
        }
        if merge {
            opts.set_merge_operator_associative(MERGE, resolve);
            opts.set_compaction_filter(DROP_GONE, |level: u32, _key: &[u8], value: &[u8]| {
                if i32::try_from(level).is_ok_and(|l| l == LEVELS - 1) && is_gone(value) {
                    rocksdb::compaction_filter::Decision::Remove
                } else {
                    rocksdb::compaction_filter::Decision::Keep
                }
            });
        }
        opts
    }

    fn open_live(dir: &Path, declared: &[Declared], memory: &Memory) -> Result<Db, Error> {
        let cache = &memory.cache;
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_write_buffer_manager(&memory.memtables);
        opts.set_max_background_jobs(BACKGROUND_JOBS);
        opts.increase_parallelism(BACKGROUND_JOBS);
        let mut families = Vec::new();
        for d in declared {
            // A probe by hash seeks the prefix up to the hash, which the bloom filter answers.
            families.push(rocksdb::ColumnFamilyDescriptor::new(
                d.name,
                Self::cf_options(cache, Some(d.scope_len() + 8), true),
            ));
            if let Some(aux) = d.aux() {
                let prefix = None;
                families.push(rocksdb::ColumnFamilyDescriptor::new(
                    aux,
                    Self::cf_options(cache, prefix, false),
                ));
            }
        }
        Db::open_cf_descriptors(&opts, dir.join(LIVE), families).map_err(local)
    }

    fn declared(&self, relation: &Relation<'_>) -> Result<Declared, Error> {
        let d = self
            .declared
            .iter()
            .find(|d| d.name == relation.name)
            .copied()
            .ok_or_else(|| {
                Error::Invalid(format!("working state: no relation {}", relation.name))
            })?;
        if d.dated != matches!(relation.scope, Scope::Day(_)) {
            return Err(Error::Invalid(format!(
                "working state: {} is declared {d:?}, not as {relation:?}",
                relation.name
            )));
        }
        Ok(d)
    }

    /// The live database, shared: readers and stagers.
    fn live(&self) -> Result<Live<'_>, Error> {
        Live::of(
            self.db
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// The live database, alone: a commit, a drop, a retirement's writes, a checkpoint's links.
    fn exclusive(&self) -> Result<Exclusive<'_>, Error> {
        Exclusive::of(
            self.db
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    fn cf<'a>(db: &'a Db, name: &str) -> Result<&'a rocksdb::ColumnFamily, Error> {
        db.cf_handle(name)
            .ok_or_else(|| local(format!("no column family {name}")))
    }

    /// Roll back a unit that began and never ended, or a database a failed rollback lost, to the
    /// newest checkpoint: before a pass chooses its days, so the days past the checkpoint's
    /// position are among them. Returns whether it rolled back.
    ///
    /// # Errors
    /// The rollback failed, or a stream of the failed unit is still open.
    pub fn settle(&self, lake: &crate::Lake) -> Result<bool, Error> {
        let failed = self.in_unit.load(Ordering::Acquire);
        let lost = self
            .db
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none();
        if failed || lost {
            self.rollback(lake)?;
            self.in_unit.store(false, Ordering::Release);
        }
        Ok(failed || lost)
    }

    /// Begin a unit of work: the writes up to its [`Self::end`] are all or nothing. A unit that
    /// began and never ended failed, and what it wrote is rolled back to the newest checkpoint
    /// first ([`Self::settle`]). Returns whether it rolled back: the days past the checkpoint
    /// are then owed a replay the caller did not plan for.
    ///
    /// # Errors
    /// The rollback failed, or a stream of the failed unit is still open.
    pub fn begin(&self, lake: &crate::Lake) -> Result<bool, Error> {
        let rolled_back = self.settle(lake)?;
        self.in_unit.store(true, Ordering::Release);
        Ok(rolled_back)
    }

    /// End a unit of work at `position`, the classification revision it committed: the live
    /// database holds it. Takes a checkpoint in the background once one is due.
    ///
    /// # Errors
    /// The snapshot failed; the unit has ended, and the next checkpoint retries.
    pub fn end(self: &Arc<Self>, lake: &crate::Lake, position: u64) -> Result<(), Error> {
        self.held.fetch_max(position, Ordering::AcqRel);
        self.in_unit.store(false, Ordering::Release);
        self.checkpoint_if_due(lake)
    }

    /// The live database's sequence number: every write so far is at or below it.
    ///
    /// # Errors
    /// The database was lost and not yet restored.
    pub fn sequence(&self) -> Result<u64, Error> {
        Ok(self.live()?.latest_sequence_number())
    }

    /// The sequence number the newest retained checkpoint holds everything up to, `None`
    /// before the first: what a rollback or a restore returns the state to. A write at or below
    /// it can never be undone.
    ///
    /// # Errors
    /// The working state's head could not be read.
    pub fn checkpointed_sequence(&self, lake: &crate::Lake) -> Result<Option<u64>, Error> {
        Ok(lake
            .working_head()?
            .read()?
            .checkpoints
            .last()
            .map(|c| c.sequence))
    }

    /// The position the state holds for certain: the newest a unit ended at, or, while a unit
    /// is open or failed, the newest checkpoint's, which a rollback would return to. The days
    /// committed past it are owed a replay.
    #[must_use]
    pub fn position(&self) -> u64 {
        let lost = self
            .db
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none();
        if lost || self.in_unit.load(Ordering::Acquire) {
            self.checkpointed.load(Ordering::Acquire)
        } else {
            self.held.load(Ordering::Acquire)
        }
    }

    /// Roll the live database back to the newest checkpoint, the local copy where it matches.
    fn rollback(&self, lake: &crate::Lake) -> Result<(), Error> {
        let started = std::time::Instant::now();
        let _copy = self
            .checkpointing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut db = self
            .db
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if db.as_ref().is_some_and(|d| Arc::strong_count(d) > 1) {
            return Err(Error::Invalid(
                "working state: a stream of the failed unit is still open".into(),
            ));
        }
        // The handle closes before its directory is rebuilt. A failed rebuild leaves none, and
        // every use refuses until a rollback succeeds.
        *db = None;
        let staging = self.dir.join(STAGING);
        if staging.exists() {
            std::fs::remove_dir_all(&staging).map_err(local)?;
        }
        let restored = Self::restore(&self.dir, lake, (&self.declared, &self.memory))?;
        let position = restored.position;
        *db = Some(Arc::new(restored.db));
        self.held.store(position, Ordering::Release);
        self.checkpointed.store(position, Ordering::Release);
        self.written.store(0, Ordering::Relaxed);
        *self
            .uploaded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = restored
            .from
            .as_ref()
            .map(durable::StateCheckpoint::uploaded)
            .unwrap_or_default();
        *self
            .descends
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = restored
            .from
            .as_ref()
            .map(durable::StateCheckpoint::identity);
        log::warn!(
            "working state rolled back to position {position}: the unit before this one did not end ({:.1}s)",
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// A revision for a writer: nothing outside this state names one, so a local counter is
    /// enough.
    pub fn reserve(&self) -> u64 {
        self.revisions.fetch_add(1, Ordering::Relaxed)
    }

    /// Stage one part of `revision` of `relation`: one stripe's records, with each record's
    /// instant for a relation indexed by instant.
    ///
    /// # Errors
    /// The relation or stripe is not the state's, an instant is missing, or the database
    /// failed.
    pub fn stage(
        &self,
        relation: &Relation<'_>,
        revision: u64,
        (stripe, part): (usize, u32),
        records: &[Record],
        instants: Option<&[f64]>,
    ) -> Result<(), Error> {
        let decl = self.declared(relation)?;
        let tag = stripe_tag(stripe)?;
        if records.iter().any(|r| crate::stripe::of(r.hash) != stripe) {
            return Err(Error::Invalid(format!(
                "working state: {}: a record staged outside its hash's stripe {stripe}",
                relation.name
            )));
        }
        if instants.is_some_and(|i| i.len() != records.len()) {
            return Err(Error::Invalid(
                "working state: one instant per record".into(),
            ));
        }
        let scope = scope_bytes(relation.scope);
        let dir = self.staged(revision);
        std::fs::create_dir_all(&dir).map_err(local)?;
        if records.iter().any(|r| r.gone) {
            // The commit reduces a revision with tombstones to each key's winner.
            std::fs::write(dir.join(TOMBSTONES), []).map_err(local)?;
        }
        let file =
            std::fs::File::create(dir.join(format!("{tag:02}-{part:010}"))).map_err(local)?;
        let mut out = std::io::BufWriter::new(file);
        for (at, record) in records.iter().enumerate() {
            let primary = primary_key(&scope, record.hash, &record.key);
            let aux = aux_key(&decl, &scope, &primary, instants.map(|i| i[at]))?;
            let staged = staged_value(aux.as_deref(), &primary, &encode_value(record));
            let len = u32::try_from(staged.len()).map_err(local)?;
            std::io::Write::write_all(&mut out, &len.to_le_bytes()).map_err(local)?;
            std::io::Write::write_all(&mut out, &staged).map_err(local)?;
        }
        std::io::Write::flush(&mut out).map_err(local)
    }

    /// Every record of `parts`, in order: its index entry (empty for none), its primary key, its
    /// value.
    fn each_staged(
        parts: &[PathBuf],
        mut visit: impl FnMut(&[u8], &[u8], &[u8]) -> Result<(), Error>,
    ) -> Result<(), Error> {
        for part in parts {
            let body = std::fs::read(part).map_err(local)?;
            let mut at = 0usize;
            while at < body.len() {
                let len = body
                    .get(at..at + 4)
                    .and_then(|b| b.try_into().ok())
                    .map(u32::from_le_bytes)
                    .ok_or_else(|| local("a staged part is torn"))?
                    as usize;
                let staged = body
                    .get(at + 4..at + 4 + len)
                    .ok_or_else(|| local("a staged part is torn"))?;
                let (aux_key, primary, value) =
                    staged_parts(staged).ok_or_else(|| local("a staged record does not decode"))?;
                visit(aux_key, primary, value)?;
                at += 4 + len;
            }
        }
        Ok(())
    }

    /// A revision's staged parts' directory: a file each, named by its stripe and part so their
    /// names sort in the order the commit applies them.
    fn staged(&self, revision: u64) -> PathBuf {
        self.dir.join(STAGING).join(format!("{revision:020}"))
    }

    /// Discard what `revision` staged: its writer dropped it uncommitted.
    ///
    /// # Errors
    /// The local disk failed.
    pub fn abandon(&self, revision: u64) -> Result<(), Error> {
        match std::fs::remove_dir_all(self.staged(revision)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(local(e)),
            _ => Ok(()),
        }
    }

    /// Commit `revision` of `relation`: a base (`base`) clears its scope first, then the staged
    /// parts apply in order. With `track`, what the revision wrote by key, returns what it
    /// changed against the state it is committed over.
    ///
    /// # Errors
    /// The relation is not the state's, or the database failed.
    pub fn commit(
        &self,
        relation: &Relation<'_>,
        revision: u64,
        base: bool,
        track: Option<&Terms>,
    ) -> Result<Option<Changes>, Error> {
        let decl = self.declared(relation)?;
        let staged = self.staged(revision);
        let mut parts: Vec<PathBuf> = match std::fs::read_dir(&staged) {
            Ok(entries) => entries
                .map(|e| e.map(|e| e.path()))
                .filter(|p| {
                    p.as_ref()
                        .map_or(true, |p| p.file_name().is_none_or(|n| n != TOMBSTONES))
                })
                .collect::<Result<_, _>>()
                .map_err(local)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(local(e)),
        };
        parts.sort();
        // A large revision goes in as files, built before the lock is taken: readers wait for
        // the ingestion alone, not for the sort.
        let staged_bytes: u64 = parts
            .iter()
            .map(|p| std::fs::metadata(p).map_or(0, |m| m.len()))
            .sum();
        let files = if staged_bytes >= self.limits.ingest_bytes {
            Some(self.build_files(&decl, &staged, base, &parts)?)
        } else {
            None
        };
        let db = self.exclusive()?;
        let changes = track
            .map(|terms| Self::changes(&db, &decl, relation, terms, base))
            .transpose()?;
        let scope = scope_bytes(relation.scope);
        let aux = decl.aux();
        let mut batches = Batches::new(&db);
        if base {
            batches.delete_range(decl.name, &scope, &successor(&scope))?;
            if let Some(aux) = &aux {
                batches.delete_range(aux, &scope, &successor(&scope))?;
            }
        }
        if let Some(files) = files {
            // The scope cleared first: the files land over the range tombstone.
            batches.flush()?;
            let written = Self::ingest(&db, &decl, &files)?;
            self.written.fetch_add(written, Ordering::Relaxed);
            batches.put("default", &stated_key(relation), &[])?;
            batches.flush()?;
            drop(db);
            self.abandon(revision)?;
            return Ok(changes);
        }
        // A revision with tombstones applies each key's winner alone: the keyed rule is
        // associative, so the winner over the stored value is what every write in order would
        // leave, and a tombstone the revision's own later record overwrites is never written,
        // logged, sealed or compacted.
        let winners = if staged.join(TOMBSTONES).exists() {
            let mut winners: HashMap<Vec<u8>, (f64, usize)> = HashMap::new();
            let mut at = 0usize;
            Self::each_staged(&parts, |_, primary, value| {
                let version = version_of(value);
                let wins = winners
                    .get(primary)
                    .is_none_or(|(v, _)| version.total_cmp(v).is_ge());
                if wins {
                    winners.insert(primary.to_vec(), (version, at));
                }
                at += 1;
                Ok(())
            })?;
            Some(winners)
        } else {
            None
        };
        let mut written = 0u64;
        let mut at = 0usize;
        Self::each_staged(&parts, |aux_key, primary, value| {
            let this = at;
            at += 1;
            if winners
                .as_ref()
                .is_some_and(|w| w.get(primary).is_some_and(|(_, winner)| *winner != this))
            {
                return Ok(());
            }
            batches.merge(decl.name, primary, value)?;
            written += (primary.len() + value.len()) as u64;
            if let (Some(aux), false) = (&aux, aux_key.is_empty()) {
                batches.put(aux, aux_key, &[])?;
                written += aux_key.len() as u64;
            }
            Ok(())
        })?;
        self.written.fetch_add(written, Ordering::Relaxed);
        batches.put("default", &stated_key(relation), &[])?;
        batches.flush()?;
        drop(db);
        self.abandon(revision)?;
        Ok(changes)
    }

    /// A large revision's files (`docs/lake.md`): each stripe's
    /// staged records reduced to each key's winner under the keyed rule (the highest version,
    /// a later write winning a tie), sorted, and written as one file of puts for a base or of
    /// merge operands for a delta, so the keyed rule still holds against what the state
    /// stores; the index's entries a file per stripe beside it. One stripe in memory at a time.
    fn build_files(
        &self,
        decl: &Declared,
        staged: &Path,
        base: bool,
        parts: &[PathBuf],
    ) -> Result<Ingest, Error> {
        let started = std::time::Instant::now();
        let primary_opts = Self::cf_options(&self.memory.cache, Some(decl.scope_len() + 8), true);
        let aux_opts = Self::cf_options(&self.memory.cache, None, false);
        let stripes: Vec<usize> = (0..crate::stripe::STRIPES).collect();
        // BUILD_LANES stripes at a time: each holds its stripe's staged bytes and their sort.
        let mut built = Vec::new();
        for wave in stripes.chunks(BUILD_LANES) {
            let results: Vec<Result<Built, Error>> = std::thread::scope(|scope| {
                let handles: Vec<_> = wave
                    .iter()
                    .map(|&stripe| {
                        let (primary_opts, aux_opts) = (&primary_opts, &aux_opts);
                        scope.spawn(move || {
                            Self::build_stripe(
                                decl,
                                (staged, parts, stripe),
                                base,
                                (primary_opts, aux_opts),
                            )
                        })
                    })
                    .collect();
                handles
                    .into_iter()
                    .map(|h| {
                        h.join()
                            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                    })
                    .collect()
            });
            for result in results {
                built.push(result?);
            }
        }
        let mut ingest = Ingest::default();
        let mut records = 0usize;
        for b in built {
            records += b.records;
            ingest.bytes += b.bytes;
            ingest.primary.extend(b.primary);
            ingest.aux.extend(b.aux);
        }
        log::info!(
            "working state: {} {} built as {} file(s), {records} staged record(s), {} MB, in {:.1}s",
            decl.name,
            if base { "base" } else { "delta" },
            ingest.primary.len() + ingest.aux.len(),
            ingest.bytes >> 20,
            started.elapsed().as_secs_f64()
        );
        Ok(ingest)
    }

    /// One stripe's files ([`Self::build_files`]).
    fn build_stripe(
        decl: &Declared,
        (staged, parts, stripe): (&Path, &[PathBuf], usize),
        base: bool,
        (primary_opts, aux_opts): (&rocksdb::Options, &rocksdb::Options),
    ) -> Result<Built, Error> {
        let mut built = Built::default();
        let tag = format!("{:02}-", stripe_tag(stripe)?);
        let mine: Vec<&PathBuf> = parts
            .iter()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&tag))
            })
            .collect();
        if mine.is_empty() {
            return Ok(built);
        }
        let bodies: Vec<Vec<u8>> = mine
            .iter()
            .map(|p| std::fs::read(p).map_err(local))
            .collect::<Result<_, _>>()?;
        let mut entries: Vec<StagedEntry<'_>> = Vec::new();
        for body in &bodies {
            let mut at = 0usize;
            while at < body.len() {
                let len = body
                    .get(at..at + 4)
                    .and_then(|b| b.try_into().ok())
                    .map(u32::from_le_bytes)
                    .ok_or_else(|| local("a staged part is torn"))?
                    as usize;
                let staged = body
                    .get(at + 4..at + 4 + len)
                    .ok_or_else(|| local("a staged part is torn"))?;
                let (aux_key, primary, value) =
                    staged_parts(staged).ok_or_else(|| local("a staged record does not decode"))?;
                entries.push((primary, aux_key, value, entries.len()));
                at += 4 + len;
            }
        }
        built.records = entries.len();
        entries.sort_unstable_by(|a, b| {
            a.0.cmp(b.0)
                .then(version_of(a.2).total_cmp(&version_of(b.2)))
                .then(a.3.cmp(&b.3))
        });
        // Each key's winner: the last of its run.
        let winners: Vec<&StagedEntry<'_>> = entries
            .iter()
            .enumerate()
            .filter(|(i, e)| entries.get(i + 1).is_none_or(|next| next.0 != e.0))
            .map(|(_, e)| e)
            .collect();
        let path = staged.join(format!("ingest-{stripe:02}.sst"));
        let mut writer = rocksdb::SstFileWriter::create(primary_opts);
        writer.open(&path).map_err(local)?;
        for (primary, _, value, _) in &winners {
            if base {
                writer.put(primary, value).map_err(local)?;
            } else {
                writer.merge(primary, value).map_err(local)?;
            }
        }
        writer.finish().map_err(local)?;
        built.bytes += writer.file_size();
        built.primary = Some(path);
        if decl.aux().is_some() {
            let mut index: Vec<&[u8]> = winners
                .iter()
                .map(|(_, aux, _, _)| *aux)
                .filter(|aux| !aux.is_empty())
                .collect();
            index.sort_unstable();
            index.dedup();
            if !index.is_empty() {
                let path = staged.join(format!("ingest-{stripe:02}.aux.sst"));
                let mut writer = rocksdb::SstFileWriter::create(aux_opts);
                writer.open(&path).map_err(local)?;
                for key in index {
                    writer.put(key, []).map_err(local)?;
                }
                writer.finish().map_err(local)?;
                built.bytes += writer.file_size();
                built.aux = Some(path);
            }
        }
        Ok(built)
    }

    /// Move a revision's files into the database. The stripes' files hold disjoint ranges and
    /// go in together; the index's overlap one another and go in one at a time. Returns their
    /// bytes.
    fn ingest(db: &Db, decl: &Declared, files: &Ingest) -> Result<u64, Error> {
        let started = std::time::Instant::now();
        let mut opts = rocksdb::IngestExternalFileOptions::default();
        opts.set_move_files(true);
        if !files.primary.is_empty() {
            db.ingest_external_file_cf_opts(Self::cf(db, decl.name)?, &opts, files.primary.clone())
                .map_err(local)?;
        }
        if let Some(aux) = decl.aux() {
            let cf = Self::cf(db, &aux)?;
            for file in &files.aux {
                db.ingest_external_file_cf_opts(cf, &opts, vec![file.clone()])
                    .map_err(local)?;
            }
        }
        log::info!(
            "working state: {} ingested {} file(s) in {:.1}s",
            decl.name,
            files.primary.len() + files.aux.len(),
            started.elapsed().as_secs_f64()
        );
        Ok(files.bytes)
    }

    /// What writing `terms` changes against the state now: the old values of the keys written,
    /// by lookup, and for a base the whole scope, since a key it does not write is deleted.
    fn changes(
        db: &Db,
        decl: &Declared,
        relation: &Relation<'_>,
        terms: &Terms,
        base: bool,
    ) -> Result<Changes, Error> {
        let scope = scope_bytes(relation.scope);
        let mut old: HashMap<KeyId, u64> = HashMap::new();
        if base {
            let cf = Self::cf(db, decl.name)?;
            let mut opts = rocksdb::ReadOptions::default();
            opts.set_total_order_seek(true);
            opts.set_iterate_upper_bound(successor(&scope));
            opts.fill_cache(false);
            let mut it = db.raw_iterator_cf_opt(cf, opts);
            it.seek(&scope);
            while let (Some(k), Some(v)) = (it.key(), it.value()) {
                if let Some(r) = record_of(decl, k, v) {
                    old.insert((r.hash, r.key.clone()), crate::keyed::term(&r));
                }
                it.next();
            }
            it.status().map_err(local)?;
        } else {
            let keys: Vec<(&KeyId, Vec<u8>)> = terms
                .keys()
                .map(|id| {
                    let (hash, key) = id;
                    Ok((id, primary_key(&scope, *hash, key)))
                })
                .collect::<Result<_, Error>>()?;
            let cf = Self::cf(db, decl.name)?;
            let values = db.batched_multi_get_cf(cf, keys.iter().map(|(_, p)| p), false);
            for ((id, primary), value) in keys.into_iter().zip(values) {
                if let Some(r) = value
                    .map_err(local)?
                    .and_then(|v| record_of(decl, &primary, &v))
                {
                    old.insert(id.clone(), crate::keyed::term(&r));
                }
            }
        }
        let mut changes = Changes {
            written: terms.len() as u64,
            ..Changes::default()
        };
        for (key, new) in terms {
            match (old.remove(key), new) {
                (None, Some(_)) => changes.inserted += 1,
                (Some(_), None) => changes.deleted += 1,
                (Some(was), Some(now)) if was != *now => changes.updated += 1,
                _ => {}
            }
        }
        if base {
            changes.deleted += old.len() as u64;
        }
        Ok(changes)
    }

    /// The live records of one stripe of `relation` whose hash is among `probes` (sorted,
    /// distinct), or all of them, in hash order.
    ///
    /// # Errors
    /// The relation or stripe is not the state's, or the database failed.
    pub fn read(
        &self,
        relation: &Relation<'_>,
        stripe: usize,
        probes: Option<&[u64]>,
    ) -> Result<Vec<Record>, Error> {
        let Some(probes) = probes else {
            return self.stream(relation, stripe)?.collect();
        };
        let decl = self.declared(relation)?;
        let scope = scope_bytes(relation.scope);
        stripe_range(&scope, stripe)?;
        let db = self.live()?;
        let mut out = Vec::new();
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let mut it = db.raw_iterator_cf_opt(Self::cf(&db, decl.name)?, opts);
        for hash in probes {
            if crate::stripe::of(*hash) != stripe {
                continue;
            }
            let mut at = scope.clone();
            at.extend_from_slice(&hash.to_be_bytes());
            it.seek(&at);
            while let (Some(k), Some(v)) = (it.key(), it.value()) {
                if !k.starts_with(&at) {
                    break;
                }
                out.extend(record_of(&decl, k, v));
                it.next();
            }
        }
        it.status().map_err(local)?;
        Ok(out)
    }

    /// The live records of one stripe of `relation`, streamed in hash order through one
    /// iterator: one consistent view, whatever commits after it opens.
    ///
    /// # Errors
    /// The relation or stripe is not the state's.
    pub fn stream(&self, relation: &Relation<'_>, stripe: usize) -> Result<Stream, Error> {
        let decl = self.declared(relation)?;
        let (from, to) = stripe_range(&scope_bytes(relation.scope), stripe)?;
        let db = self.live()?.shared();
        let iter = OwnedIter::try_new(db, |db| {
            let mut opts = rocksdb::ReadOptions::default();
            opts.set_total_order_seek(true);
            opts.set_iterate_lower_bound(from);
            opts.set_iterate_upper_bound(to);
            // A whole stripe read once: its blocks would only evict what probes reuse.
            opts.fill_cache(false);
            Ok::<_, Error>(db.raw_iterator_cf_opt(Self::cf(db, decl.name)?, opts))
        })?;
        Ok(Stream {
            iter,
            decl,
            started: false,
        })
    }

    /// The live records of `relation` at or after `since`, by the instants their writers
    /// stated, every stripe.
    ///
    /// # Errors
    /// The relation is not indexed by instant, or the database failed.
    pub fn since(&self, relation: &Relation<'_>, since: f64) -> Result<Vec<Record>, Error> {
        let decl = self.declared(relation)?;
        let (Index::Instants, Some(aux)) = (decl.index, decl.aux()) else {
            return Err(Error::Invalid(format!(
                "working state: {} is not indexed by instant",
                relation.name
            )));
        };
        let scope = scope_bytes(relation.scope);
        let db = self.live()?;
        let mut from = scope.clone();
        from.extend_from_slice(&ordered(since));
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_upper_bound(successor(&scope));
        let mut it = db.raw_iterator_cf_opt(Self::cf(&db, &aux)?, opts);
        it.seek(&from);
        let mut primaries = Vec::new();
        while let Some(k) = it.key() {
            let mut primary = scope.clone();
            primary.extend_from_slice(&k[scope.len() + 8..]);
            primaries.push(primary);
            it.next();
        }
        it.status().map_err(local)?;
        primaries.sort();
        primaries.dedup();
        let cf = Self::cf(&db, decl.name)?;
        let mut out = Vec::new();
        for (primary, value) in primaries
            .iter()
            .zip(db.batched_multi_get_cf(cf, &primaries, true))
        {
            if let Some(v) = value.map_err(local)? {
                out.extend(record_of(&decl, primary, &v));
            }
        }
        Ok(out)
    }

    /// Whether `relation` holds a live record: the first one found answers.
    ///
    /// # Errors
    /// The relation is not the state's, or the database failed.
    pub fn holds(&self, relation: &Relation<'_>) -> Result<bool, Error> {
        for stripe in 0..crate::stripe::STRIPES {
            if self.stream(relation, stripe)?.next().transpose()?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The days the dated relation `name` has records on, live or tombstoned, oldest first:
    /// one seek a day.
    ///
    /// # Errors
    /// The relation is not a dated one of the state's, or the database failed.
    pub fn days(&self, name: &str) -> Result<Vec<i64>, Error> {
        let decl = self
            .declared
            .iter()
            .find(|d| d.name == name && d.dated)
            .copied()
            .ok_or_else(|| Error::Invalid(format!("working state: no dated relation {name}")))?;
        let db = self.live()?;
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_total_order_seek(true);
        let mut it = db.raw_iterator_cf_opt(Self::cf(&db, decl.name)?, opts);
        let mut out = Vec::new();
        it.seek_to_first();
        while let Some(k) = it.key() {
            let Some(day) = day_of(k) else { break };
            out.push(day);
            it.seek(successor(&k[..8]));
        }
        it.status().map_err(local)?;
        Ok(out)
    }

    /// Whether `relation` was committed at all, empty or not, and not dropped since.
    ///
    /// # Errors
    /// The relation is not the state's, or the database failed.
    pub fn stated(&self, relation: &Relation<'_>) -> Result<bool, Error> {
        self.declared(relation)?;
        Ok(self
            .live()?
            .get(stated_key(relation))
            .map_err(local)?
            .is_some())
    }

    /// Drop `relation`'s records and its statement.
    ///
    /// # Errors
    /// The relation is not the state's, or the database failed.
    pub fn drop_relation(&self, relation: &Relation<'_>) -> Result<(), Error> {
        let decl = self.declared(relation)?;
        let db = self.exclusive()?;
        let scope = scope_bytes(relation.scope);
        let mut batches = Batches::new(&db);
        batches.delete_range(decl.name, &scope, &successor(&scope))?;
        if let Some(aux) = decl.aux() {
            batches.delete_range(&aux, &scope, &successor(&scope))?;
        }
        batches.delete("default", &stated_key(relation))?;
        batches.flush()
    }

    /// Drop every dated relation's records of `day`: a retired day.
    ///
    /// # Errors
    /// The database failed.
    pub fn drop_day(&self, day: i64) -> Result<(), Error> {
        for d in self.declared.clone().iter().filter(|d| d.dated) {
            self.drop_relation(&Relation {
                name: d.name,
                scope: Scope::Day(day),
            })?;
        }
        Ok(())
    }

    /// Retire the records of `relation`, a lineage relation, that `keep` refuses, once per
    /// `boundary`: the boundary is kept beside the data, so a checkpoint carries both or
    /// neither. Its index is rebuilt from the survivors, so entries its records moved away from
    /// leave with it. Returns whether it retired.
    ///
    /// # Errors
    /// The relation is not a lineage relation of the state's, or the database failed.
    pub fn retire(
        &self,
        relation: &Relation<'_>,
        boundary: i64,
        keep: impl Fn(&Record) -> bool,
    ) -> Result<bool, Error> {
        let decl = self.declared(relation)?;
        if decl.dated {
            return Err(Error::Invalid(format!(
                "working state: {}: only a lineage relation retires by boundary",
                relation.name
            )));
        }
        let mark = meta(&format!("retired/{}", relation.name));
        let applied = self
            .live()?
            .get(&mark)
            .map_err(local)?
            .and_then(|v| v.try_into().ok().map(i64::from_le_bytes));
        if applied.is_some_and(|a| a >= boundary) {
            return Ok(false);
        }
        // Stripe by stripe: a stripe's deletions in memory, never the relation's. A reader
        // between stripes sees some retired and some not, which a retirement that only
        // reclaims dead records allows. A lineage relation keeps no index.
        for stripe in 0..crate::stripe::STRIPES {
            let mut refused = Vec::new();
            for r in self.stream(relation, stripe)? {
                let r = r?;
                if !keep(&r) {
                    refused.push(primary_key(&[], r.hash, &r.key));
                }
            }
            let db = self.exclusive()?;
            let mut batches = Batches::new(&db);
            for primary in &refused {
                batches.delete(decl.name, primary)?;
            }
            batches.flush()?;
        }
        let db = self.exclusive()?;
        let mut batches = Batches::new(&db);
        batches.put("default", &mark, &boundary.to_le_bytes())?;
        batches.flush()?;
        Ok(true)
    }

    /// Each column family's SST bytes on disk, `RocksDB`'s estimate of its live data, and its
    /// keys: where the state's size is, and how much of it compaction has yet to reclaim.
    fn column_sizes(&self) -> Result<Vec<String>, Error> {
        let db = self.live()?;
        let mut names = Vec::new();
        for d in &self.declared {
            names.push(d.name.to_string());
            names.extend(d.aux());
        }
        let mut out = Vec::new();
        for name in names {
            let cf = Self::cf(&db, &name)?;
            let int = |p: &rocksdb::properties::PropName| {
                db.property_int_value_cf(cf, p)
                    .map_err(local)
                    .map(Option::unwrap_or_default)
            };
            out.push(format!(
                "{name} {}/{} MB {} keys",
                int(rocksdb::properties::TOTAL_SST_FILES_SIZE)? >> 20,
                int(rocksdb::properties::ESTIMATE_LIVE_DATA_SIZE)? >> 20,
                int(rocksdb::properties::ESTIMATE_NUM_KEYS)?
            ));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests;
