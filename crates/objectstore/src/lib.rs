// SPDX-License-Identifier: AGPL-3.0-only

//! The workspace's object storage port, and its ONLY outbound network client.
//!
//! **This crate exists so `ClickHouse` performs no I/O.** Every byte in or out of the customer's
//! bucket goes through this port. Routed through the engine's own S3 client, the bytes of a
//! binary object would travel hex-armored, doubled over the wire, and the credential would sit in
//! the engine's `config.xml` in cleartext for a second process to read. With the transport ours,
//! neither happens.
//!
//! **The credential chain is bought, deliberately, and it is the reason the dependency is large.**
//! `SigV4` signing is the obvious thing to point at, but the part that decided it is refresh:
//! `IMDSv2`'s own token carries a TTL, `IRSA`'s projected service-account token is rotated by kubelet
//! (cache the contents rather than re-reading and a deployment works for an hour and then stops),
//! and expiry arithmetic meets clock skew. Those fail weeks in, unattended, on a box that sends
//! no telemetry back to us — so a bug there is invisible to us and hard for a customer to
//! attribute. `object_store` carries that chain; we do not.
//!
//! **Async stops here**, exactly as it stops at the engine seam. `object_store` is async to the
//! bone and the pipeline is not, so this crate owns the runtime and every method on the port is
//! synchronous. It is one small multi-thread runtime for the process, which any number of threads
//! may block on at once: its workers carry every handle's requests side by side, as the async
//! client is built to. A current-thread runtime per handle would need a mutex (two threads
//! blocking on one current-thread runtime deadlock), making each handle a single-threaded
//! resource, and the lake shares one handle across its writers and readers, which would
//! serialize every fetch. delta-rs's synchronous bindings run on the same shape (one `OnceLock` multi-thread runtime,
//! `block_on` from any thread). A method must still never be
//! called from a thread running an async runtime: blocking there panics. And a process must not
//! fork after its first request: the runtime's threads do not survive into the child (delta-rs
//! refuses that case outright); the daemon spawns, it never forks.
//!
//! # Where to start
//!
//! - [`ObjectStore`] is the port: list, get, put, delete, synchronous, the runtime behind it.
//! - [`S3Store`] is the production implementation over any S3-API bucket; [`DirStore`] is the
//!   same port over a directory, for tests and the job form.
//!
//! Every byte the analyzer moves in or out of a bucket goes through this crate and nothing
//! else; the static audit pins that the engine reaches no network.

use std::time::Duration;

mod s3;

pub use s3::{Requests, S3Store, StaticCredentials, RUNTIME_WORKERS};

/// One object, as a listing reports it.
///
/// `etag` and `size` are carried because ingest's batch identity is built from the object's own
/// bytes and the listing dedups on the key — the same three fields `batches` records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The key, relative to the store's root.
    pub key: String,
    /// The object's size in bytes.
    pub size: u64,
    /// The object's entity tag, when the store reports one, without the quotes S3 wraps
    /// it in: one spelling, so every ledger that keys on `key@etag` agrees.
    pub etag: Option<String>,
}

impl ObjectMeta {
    /// A listing entry as the transport reported it, the etag normalised.
    fn listed(key: String, size: u64, etag: Option<String>) -> Self {
        Self {
            key,
            size,
            etag: etag.map(|e| e.trim_matches('"').to_string()),
        }
    }
}

/// The version of one object as the store reported it: the value a conditional write is
/// checked against. Opaque to callers; only the store that issued it can interpret it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version(String);

impl Version {
    /// The version's entity tag, without surrounding quotes, for logs and comparisons.
    #[must_use]
    pub fn etag(&self) -> &str {
        self.0.trim_matches('"')
    }
}

/// What a conditional write requires of the key before it writes.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// No object exists at the key.
    Absent,
    /// The object at the key is still this version.
    Matches(Version),
}

/// What a conditional write did.
#[must_use]
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Conditional {
    /// The precondition held and the body is now the object, at this version.
    Written(Version),
    /// The precondition did not hold; the object is unchanged by this call.
    Refused,
}

/// What the store can fail with. Each variant renders exactly as the message it replaced,
/// so a journal line reads the same; what changed is that a caller can now match the kind.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The store's own runtime would not start.
    #[error("starting the object-store runtime: {0}")]
    Runtime(String),
    /// The client could not be built for the URL: an unparsable URL, or options the
    /// backend refuses.
    #[error("configuring the object store for {url}: {message}")]
    Config {
        /// The store URL as configured.
        url: String,
        /// The backend's own message.
        message: String,
    },
    /// One operation on one object failed. `op` is the verb the message always carried —
    /// `fetching`, `reading`, `writing`, `deleting` on a bucket; `get`, `put`, `delete`,
    /// `list`, `stat`, `strip` on a directory — and `key` is the object or path it named.
    #[error("{op} {key}: {message}")]
    Object {
        /// The operation, as the message spells it.
        op: &'static str,
        /// The object key or path the operation named.
        key: String,
        /// The transport's or the file system's own message.
        message: String,
    },
    /// A ranged read of one object failed.
    #[error("fetching {key} [{start}..{end}): {message}")]
    FetchRange {
        /// The object key.
        key: String,
        /// The first byte asked for.
        start: u64,
        /// One past the last byte asked for.
        end: u64,
        /// The transport's own message.
        message: String,
    },
    /// A listing stream failed part way.
    #[error("listing: {0}")]
    Listing(String),
    /// A byte range that does not fit this platform's addressing.
    #[error("{0}")]
    Range(&'static str),
    /// An implementor's own failure, in its own words — for a store outside this crate (a
    /// test double, a partner's transport) that has no verb above to spell it by.
    #[error("{0}")]
    Other(String),
}

/// The port: list, get, put. Nothing else, and nothing engine-shaped.
///
/// **`Vec<u8>` is honest for what this carries today** — sealed segments are bounded objects the
/// pipeline already holds whole, and the preflight probe is a single line. It would NOT be honest
/// for something large: an O(rows) buffer is the failure at scale. If a consumer ever needs a stream, this trait grows a streaming method rather than
/// every caller paying for one.
/// **Every method blocks.** A store drives its own runtime with `block_on`, so a method MUST
/// NOT be called from a thread running an async runtime; use `tokio::task::spawn_blocking`.
///
/// `Send + Sync` is part of the contract, not an accident of the one implementation:
/// the ingest worker pool shares one handle for discovery and takes a [`ObjectStore::sibling`]
/// per worker, and a store that could not cross a thread boundary could not be ingested
/// from concurrently at all.
pub trait ObjectStore: Send + Sync {
    /// Where this store's objects live, the same for every handle on the same objects: a
    /// bucket's URL, a directory's path. Two stores with one location share their objects,
    /// which is what a lock over a key space in them must be keyed by.
    fn location(&self) -> String;

    /// Every object under `prefix`, sorted by key.
    ///
    /// # Errors
    /// Transport failure, or a key outside the configured prefix.
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, Error>;

    /// A second handle on the SAME store: a client and a connection pool of its own. One
    /// handle already carries concurrent calls; a sibling is for a worker that should not
    /// share the first one's connections (the ingest pool's, a run's part writers).
    ///
    /// # Errors
    /// Whatever building a fresh handle costs — for S3, credential/config resolution.
    fn sibling(&self) -> Result<Box<dyn ObjectStore>, Error>;

    /// Every object under `prefix` whose key sorts strictly after `after`.
    ///
    /// **A bucket-side seek, not a filter over a full listing.** S3 has a primitive for exactly
    /// this and `object_store`'s AWS backend reaches it (`start-after` on the request), so a
    /// fleet with a year of segments does not pay to list the year on each cycle.
    ///
    /// # Errors
    /// Transport failure.
    fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>, Error>;

    /// One object's bytes.
    ///
    /// # Errors
    /// Transport failure, or an absent key.
    fn get(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// The bytes of one object in `range` (a half-open byte range): a stream's window over
    /// an object too large to hold, the lake spine's chunks. The default reads the object
    /// whole and cuts; a store that can range does.
    ///
    /// # Errors
    /// Transport failure, or a missing key.
    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
        let whole = self.get(key)?;
        let from =
            usize::try_from(range.start).map_err(|_| Error::Range("range start past usize"))?;
        let to = usize::try_from(range.end)
            .map_err(|_| Error::Range("range end past usize"))?
            .min(whole.len());
        Ok(whole.get(from.min(whole.len())..to).unwrap_or(&[]).to_vec())
    }

    /// Write one object, replacing any existing key.
    ///
    /// # Errors
    /// Transport failure, or a refused write.
    fn put(&self, key: &str, body: &[u8]) -> Result<(), Error>;

    /// One object's bytes and the version they are at, or `None` when no object exists.
    ///
    /// The default refuses: a store that cannot report versions cannot take conditional
    /// writes, and a caller that needs them must fail rather than race.
    ///
    /// # Errors
    /// Transport failure, a store without versions, or an object reported without one.
    fn get_versioned(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, Error> {
        Err(Error::Other(format!(
            "reading {key} with its version: this store has no versioned reads"
        )))
    }

    /// Write `body` only if `condition` holds at the key, atomically with the check.
    ///
    /// `Refused` is an answer, not an error: another writer changed the key first. A
    /// transport failure is an error, and after one the write may or may not have landed;
    /// the caller re-reads the key before deciding.
    ///
    /// # Errors
    /// Transport failure, or a store without conditional writes (the default).
    fn put_if(
        &self,
        key: &str,
        body: &[u8],
        condition: &Precondition,
    ) -> Result<Conditional, Error> {
        let _ = (body, condition);
        Err(Error::Other(format!(
            "writing {key} conditionally: this store has no conditional writes"
        )))
    }

    /// Remove one object; a key that is not there is not an error, so a retirement that
    /// runs twice ends the same way (a day's derived runs leave
    /// the bucket with the day).
    ///
    /// # Errors
    /// Transport failure, or a refused delete.
    fn delete(&self, key: &str) -> Result<(), Error>;
}

/// Prove that `store` fences writers at `key`, a key no one else writes: a create of an
/// absent key writes, a second create is refused, an update at the current version writes,
/// an update at a superseded version is refused, and a refused write leaves the object
/// unchanged. The lake's heads and leases rest on exactly this; a store that accepts a
/// conditional write it should refuse lets two writers both commit, and nothing downstream
/// can tell. Leaves the key behind, holding `two`; the caller removes it.
///
/// # Errors
/// The first step the store got wrong, or a transport failure, as a sentence.
pub fn check_conditional_writes(store: &dyn ObjectStore, key: &str) -> Result<(), String> {
    let step = |what: &str, e: Error| format!("{what} at {key}: {e}");
    if store
        .get_versioned(key)
        .map_err(|e| step("reading the absent probe", e))?
        .is_some()
    {
        return Err(format!("the probe key {key} already exists"));
    }
    let Conditional::Written(v1) = store
        .put_if(key, b"one", &Precondition::Absent)
        .map_err(|e| step("creating the probe", e))?
    else {
        return Err(format!("creating the absent key {key} was refused"));
    };
    if store
        .put_if(key, b"again", &Precondition::Absent)
        .map_err(|e| step("creating the probe again", e))?
        != Conditional::Refused
    {
        return Err(format!(
            "the store accepted a create of {key}, which exists: it ignores If-None-Match"
        ));
    }
    let read = store
        .get_versioned(key)
        .map_err(|e| step("reading the probe", e))?
        .ok_or_else(|| format!("{key} was written and is not there"))?;
    if read.0 != b"one" || read.1 != v1 {
        return Err(format!(
            "reading {key} did not return the body and version its write reported"
        ));
    }
    let Conditional::Written(v2) = store
        .put_if(key, b"two", &Precondition::Matches(v1.clone()))
        .map_err(|e| step("updating the probe", e))?
    else {
        return Err(format!("updating {key} at its current version was refused"));
    };
    if v1 == v2 {
        return Err(format!("a write to {key} did not move its version"));
    }
    if store
        .put_if(key, b"stale", &Precondition::Matches(v1))
        .map_err(|e| step("updating the probe at a stale version", e))?
        != Conditional::Refused
    {
        return Err(format!(
            "the store accepted an update of {key} at a superseded version: it ignores If-Match"
        ));
    }
    let after = store
        .get_versioned(key)
        .map_err(|e| step("reading the probe back", e))?
        .ok_or_else(|| format!("{key} vanished"))?;
    if after.0 != b"two" {
        return Err(format!("a refused write changed {key}"));
    }
    Ok(())
}

/// **The bounded transport.** An unbounded retry is a minutes-long hang on an unreachable
/// endpoint inside the ingest cycle, so every request is bounded twice: a request timeout per
/// attempt, and a retry budget across all of them.
///
/// The bucket is also the lake's write path, and a request that fails there fails the tick, so a
/// reader's setting (three attempts under a one-second connect timeout) is too tight for a tick
/// writing thousands of objects. The attempts, backoff and connect timeout are `object_store`'s own defaults (10 retries,
/// 100 ms doubling to 15 s, a 5 s connect), the ones `delta-rs` and `SlateDB` run on; the budget
/// keeps an outage a decided error in about a minute.
///
/// A labelled analytic constant (`docs/conformance.md`). Re-derived when the soak measures real
/// bucket latency tails; the request timeout must stay above the largest segment's GET at the
/// slowest plausible link.
pub const RETRY_ATTEMPTS: usize = 10;
/// See [`RETRY_ATTEMPTS`].
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// See [`RETRY_ATTEMPTS`].
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// The time one request may spend across all its attempts. See [`RETRY_ATTEMPTS`].
pub const RETRY_BUDGET: Duration = Duration::from_mins(1);

/// A directory as an object store: keys are paths relative to `root`, `/`-separated. The
/// rig's and the corpus's transport for the WRITE side (the derived tier of the
/// grain ladder): the derivation source already reads a directory laid out like the bucket
/// (`SegmentTransport::Directory`), and this is the same layout written. Never a deployment
/// shape; a deployment's store is [`S3Store`].
#[derive(Debug, Clone)]
pub struct DirStore {
    root: std::path::PathBuf,
}

impl DirStore {
    #[must_use]
    /// A store over a local directory, for the corpora and the rig.
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_of(&self, key: &str) -> std::path::PathBuf {
        let mut p = self.root.clone();
        for seg in key.split('/').filter(|s| !s.is_empty() && *s != "..") {
            p.push(seg);
        }
        p
    }

    /// An advisory lock on one key, held until the returned file drops: exclusive for a
    /// conditional write, shared for a versioned read. The lock file lives in the system
    /// temporary directory, named by a hash of the store's root and the key, so it never
    /// appears in a listing of the store.
    fn key_lock(&self, key: &str, exclusive: bool) -> std::io::Result<std::fs::File> {
        use std::hash::{Hash, Hasher};
        let root = std::path::absolute(&self.root)?;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        root.hash(&mut h);
        self.path_of(key).hash(&mut h);
        let dir = std::env::temp_dir().join("infertap-dirstore-locks");
        std::fs::create_dir_all(&dir)?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(format!("{:016x}.lock", h.finish())))?;
        if exclusive {
            file.lock()?;
        } else {
            file.lock_shared()?;
        }
        Ok(file)
    }

    fn walk(&self, dir: &std::path::Path, keys: &mut Vec<ObjectMeta>) -> Result<(), Error> {
        let at =
            |op: &'static str, path: &std::path::Path, e: &dyn std::fmt::Display| Error::Object {
                op,
                key: path.display().to_string(),
                message: e.to_string(),
            };
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(at("list", dir, &e)),
        };
        for entry in entries {
            let entry = entry.map_err(|e| at("list", dir, &e))?;
            let path = entry.path();
            if path.is_dir() {
                self.walk(&path, keys)?;
            } else {
                let rel = path
                    .strip_prefix(&self.root)
                    .map_err(|e| at("strip", &path, &e))?;
                let key = rel
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                // An entry gone between the directory's read and its stat was a put's stage
                // renamed into place, or an object deleted: not in this listing either way.
                let size = match entry.metadata() {
                    Ok(meta) => meta.len(),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(at("stat", &path, &e)),
                };
                keys.push(ObjectMeta {
                    key,
                    size,
                    etag: None,
                });
            }
        }
        Ok(())
    }
}

impl ObjectStore for DirStore {
    fn location(&self) -> String {
        // The canonical path, so two spellings of one directory agree, and the same before
        // the directory exists as after: the deepest existing ancestor resolves, and the rest
        // of the path is kept as given (a temporary directory behind a symlink resolved only
        // once written, and one store read as two).
        let mut existing = self.root.as_path();
        let mut rest = Vec::new();
        let base = loop {
            if let Ok(resolved) = std::fs::canonicalize(existing) {
                break resolved;
            }
            match (existing.parent(), existing.file_name()) {
                (Some(parent), Some(name)) => {
                    rest.push(name.to_os_string());
                    existing = parent;
                }
                _ => break existing.to_path_buf(),
            }
        };
        let path = rest.iter().rev().fold(base, |path, name| path.join(name));
        format!("file://{}", path.display())
    }

    fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
        use std::io::{Read, Seek};
        let path = self.path_of(key);
        let get = |message: String| Error::Object {
            op: "get",
            key: key.to_string(),
            message,
        };
        let mut f = std::fs::File::open(&path).map_err(|e| get(e.to_string()))?;
        f.seek(std::io::SeekFrom::Start(range.start))
            .map_err(|e| get(format!("seek: {e}")))?;
        let mut out = Vec::new();
        f.take(range.end.saturating_sub(range.start))
            .read_to_end(&mut out)
            .map_err(|e| get(e.to_string()))?;
        Ok(out)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, Error> {
        let mut keys = Vec::new();
        self.walk(&self.root, &mut keys)?;
        keys.retain(|m| m.key.starts_with(prefix));
        keys.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(keys)
    }

    fn sibling(&self) -> Result<Box<dyn ObjectStore>, Error> {
        Ok(Box::new(self.clone()))
    }

    fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>, Error> {
        let mut keys = self.list(prefix)?;
        keys.retain(|m| m.key.as_str() > after);
        Ok(keys)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        std::fs::read(self.path_of(key)).map_err(|e| Error::Object {
            op: "get",
            key: key.to_string(),
            message: e.to_string(),
        })
    }

    fn put(&self, key: &str, body: &[u8]) -> Result<(), Error> {
        use std::io::Write as _;
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_PUT: AtomicU64 = AtomicU64::new(0);
        let path = self.path_of(key);
        let put = |e: std::io::Error| Error::Object {
            op: "put",
            key: key.to_string(),
            message: e.to_string(),
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(put)?;
        }
        // Publish whole objects by rename. A failed/terminated write must not truncate
        // the previously committed head. Staging keys have no shipped-file suffix;
        // abandoned export stages are unreferenced objects collected by its next drain.
        let name = path
            .file_name()
            .ok_or_else(|| put(std::io::Error::other("object key has no file name")))?;
        let (stage, mut file) = loop {
            let stage = path.with_file_name(format!(
                "{}.put-{}-{}",
                name.to_string_lossy(),
                std::process::id(),
                NEXT_PUT.fetch_add(1, Ordering::Relaxed)
            ));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&stage)
            {
                Ok(file) => break (stage, file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(put(error)),
            }
        };
        let result = (|| {
            file.write_all(body)?;
            file.sync_all()?;
            std::fs::rename(&stage, &path)?;
            if let Some(parent) = path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok::<(), std::io::Error>(())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&stage);
        }
        result.map_err(put)
    }

    fn get_versioned(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, Error> {
        let fail = |e: std::io::Error| Error::Object {
            op: "get",
            key: key.to_string(),
            message: e.to_string(),
        };
        let lock = self.key_lock(key, false).map_err(fail)?;
        let read = dir_versioned(&self.path_of(key)).map_err(fail);
        drop(lock);
        read
    }

    /// Check and write under an exclusive lock on the key's lock file, so writers in other
    /// processes on the same directory serialize too. The write is [`DirStore::put`]'s
    /// publish-by-rename, so a reader never sees a partial object.
    fn put_if(
        &self,
        key: &str,
        body: &[u8],
        condition: &Precondition,
    ) -> Result<Conditional, Error> {
        let fail = |e: std::io::Error| Error::Object {
            op: "put",
            key: key.to_string(),
            message: e.to_string(),
        };
        let lock = self.key_lock(key, true).map_err(fail)?;
        let current = dir_versioned(&self.path_of(key)).map_err(fail)?;
        let holds = match (condition, &current) {
            (Precondition::Absent, None) => true,
            (Precondition::Matches(want), Some((_, have))) => want == have,
            _ => false,
        };
        if !holds {
            drop(lock);
            return Ok(Conditional::Refused);
        }
        self.put(key, body)?;
        let written = dir_versioned(&self.path_of(key)).map_err(fail)?;
        drop(lock);
        written
            .map(|(_, v)| Conditional::Written(v))
            .ok_or_else(|| fail(std::io::Error::other("the written object is missing")))
    }

    fn delete(&self, key: &str) -> Result<(), Error> {
        match std::fs::remove_file(self.path_of(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Object {
                op: "delete",
                key: key.to_string(),
                message: e.to_string(),
            }),
        }
    }
}

/// A directory object's bytes and version, or `None` when it does not exist.
///
/// The version combines the file's inode, length and modification time with a hash of its
/// bytes. Every write publishes a new file by rename, so the inode alone nearly always
/// changes; the content hash covers inode reuse within one timestamp tick. The hash is
/// only stable within one build, which is the lifetime a version is held for.
fn dir_versioned(path: &std::path::Path) -> std::io::Result<Option<(Vec<u8>, Version)>> {
    use std::hash::{Hash, Hasher};
    let body = match std::fs::read(path) {
        Ok(body) => body,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let meta = std::fs::metadata(path)?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    body.hash(&mut h);
    let modified = meta
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    #[cfg(unix)]
    let inode = std::os::unix::fs::MetadataExt::ino(&meta);
    #[cfg(not(unix))]
    let inode = 0_u64;
    let version = Version(format!(
        "{inode:x}-{:x}-{modified:x}-{:016x}",
        meta.len(),
        h.finish()
    ));
    Ok(Some((body, version)))
}

/// The live-bucket fixture the integration drills share: the bucket named by the
/// `INFERTAP_TEST_S3_*` environment, or `None` when no environment supplies one, in which
/// case a drill skips loudly rather than failing.
#[cfg(feature = "testing")]
pub mod testing {
    use super::{
        Conditional, Error, ObjectMeta, ObjectStore, Precondition, S3Store, StaticCredentials,
        Version,
    };
    use std::sync::{Arc, Mutex};

    /// Decides whether a conditional write is refused: given the key, the object's current
    /// body (if any) and the body being written, the reason to refuse, or `None`.
    pub type Refusal = Arc<dyn Fn(&str, Option<&[u8]>, &[u8]) -> Option<String> + Send + Sync>;

    /// A store that refuses chosen conditional writes, for fault injection: a durable record
    /// that fails to land. Every other
    /// call goes to the inner store. Siblings share the inner store and the refusal.
    #[derive(Clone)]
    pub struct FaultyStore {
        inner: Arc<dyn ObjectStore>,
        refusal: Arc<Mutex<Option<Refusal>>>,
    }

    impl FaultyStore {
        /// A store over `inner` that refuses nothing yet.
        #[must_use]
        pub fn over(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                refusal: Arc::default(),
            }
        }

        /// Refuse the conditional writes `refusal` names, or stop refusing (`None`).
        ///
        /// # Panics
        /// A previous refusal panicked while the lock was held.
        pub fn refuse(&self, refusal: Option<Refusal>) {
            *self.refusal.lock().expect("refusal") = refusal;
        }
    }

    impl ObjectStore for FaultyStore {
        fn location(&self) -> String {
            self.inner.location()
        }

        fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, Error> {
            self.inner.list(prefix)
        }
        fn sibling(&self) -> Result<Box<dyn ObjectStore>, Error> {
            Ok(Box::new(self.clone()))
        }
        fn list_after(&self, prefix: &str, after: &str) -> Result<Vec<ObjectMeta>, Error> {
            self.inner.list_after(prefix, after)
        }
        fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
            self.inner.get(key)
        }
        fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>, Error> {
            self.inner.get_range(key, range)
        }
        fn put(&self, key: &str, body: &[u8]) -> Result<(), Error> {
            self.inner.put(key, body)
        }
        fn delete(&self, key: &str) -> Result<(), Error> {
            self.inner.delete(key)
        }
        fn get_versioned(&self, key: &str) -> Result<Option<(Vec<u8>, Version)>, Error> {
            self.inner.get_versioned(key)
        }
        fn put_if(
            &self,
            key: &str,
            body: &[u8],
            condition: &Precondition,
        ) -> Result<Conditional, Error> {
            let refusal = self.refusal.lock().expect("refusal").clone();
            if let Some(refusal) = refusal {
                let current = self.inner.get_versioned(key)?.map(|(b, _)| b);
                if let Some(reason) = refusal(key, current.as_deref(), body) {
                    return Err(Error::Other(format!("injected fault: {reason}")));
                }
            }
            self.inner.put_if(key, body, condition)
        }
    }

    /// A live bucket from the environment, opened.
    #[derive(Debug)]
    pub struct LiveBucket {
        /// `INFERTAP_TEST_S3_URL`.
        pub url: String,
        /// The static pair from `INFERTAP_TEST_S3_KEY` and `_SECRET`, when the key is set.
        pub credentials: Option<StaticCredentials>,
        /// The store over that bucket.
        pub store: S3Store,
    }

    /// The bucket the environment names, or `None` without `INFERTAP_TEST_S3_URL`.
    ///
    /// # Panics
    /// The URL is set but the store will not open over it: the environment is broken,
    /// not absent, and a drill must not skip past that.
    #[must_use]
    pub fn env_store() -> Option<LiveBucket> {
        let url = std::env::var("INFERTAP_TEST_S3_URL").ok()?;
        let key = std::env::var("INFERTAP_TEST_S3_KEY").unwrap_or_default();
        let credentials = (!key.is_empty()).then(|| StaticCredentials {
            access_key_id: key,
            secret_access_key: std::env::var("INFERTAP_TEST_S3_SECRET").unwrap_or_default(),
            session_token: None,
        });
        let store = S3Store::open(&url, credentials.as_ref())
            .expect("INFERTAP_TEST_S3_URL is set, so a store over it must open");
        Some(LiveBucket {
            url,
            credentials,
            store,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::StaticCredentials;

    /// Every thread that blocks on the store's runtime is inside it at once: eight calls,
    /// each finishing only once all eight have begun. Behind one handle's mutex the first
    /// call held the runtime alone, and a handle's calls went one at a time. The minute is a
    /// bound on a loaded host's scheduling, not on the claim: a design that never overlaps
    /// two calls fails it however long it waits.
    #[test]
    fn eight_threads_block_on_the_runtime_at_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        const CALLERS: usize = 8;
        let runtime = crate::s3::runtime().expect("runtime");
        let inside = std::sync::Arc::new(AtomicUsize::new(0));
        // The first call to give up ends the rest: a serialized runtime fails in one bound.
        let gave_up = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let met: Vec<bool> = std::thread::scope(|scope| {
            let calls: Vec<_> = (0..CALLERS)
                .map(|_| {
                    let inside = std::sync::Arc::clone(&inside);
                    let gave_up = std::sync::Arc::clone(&gave_up);
                    scope.spawn(move || {
                        runtime.block_on(async move {
                            inside.fetch_add(1, Ordering::SeqCst);
                            let deadline =
                                std::time::Instant::now() + std::time::Duration::from_mins(1);
                            while inside.load(Ordering::SeqCst) < CALLERS {
                                if gave_up.load(Ordering::SeqCst)
                                    || std::time::Instant::now() > deadline
                                {
                                    gave_up.store(true, Ordering::SeqCst);
                                    return false;
                                }
                                std::thread::yield_now();
                            }
                            true
                        })
                    })
                })
                .collect();
            calls
                .into_iter()
                .map(|c| c.join().expect("caller"))
                .collect()
        });
        assert!(met.iter().all(|m| *m), "the calls did not overlap: {met:?}");
    }

    #[test]
    fn a_credential_never_prints_its_secret() {
        let creds = StaticCredentials {
            access_key_id: "AKIA-visible".to_string(),
            secret_access_key: "hunter2".to_string(),
            session_token: Some("token-bytes".to_string()),
        };
        let printed = format!("{creds:?}");
        assert!(printed.contains("AKIA-visible"), "{printed}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(!printed.contains("token-bytes"), "{printed}");
    }

    #[test]
    fn a_directory_store_names_one_location_before_and_after_it_exists() {
        let parent = std::env::temp_dir().join(format!(
            "objectstore-location-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let store = super::DirStore::new(parent.join("a").join("b"));
        let before = super::ObjectStore::location(&store);
        super::ObjectStore::put(&store, "k", b"v").expect("put");
        assert_eq!(super::ObjectStore::location(&store), before);
        std::fs::remove_dir_all(parent).ok();
    }
}
