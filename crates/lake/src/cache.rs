// SPDX-License-Identifier: AGPL-3.0-only

//! A local cache of immutable lake objects (`docs/lake.md`): local disk is a cache, never the
//! source. An object fetched once is read from local disk after, by
//! positioned reads. Only objects that never change under their key are cached (the fold's
//! state and index objects are named by revision), so a cached copy is never stale. Losing
//! the cache costs a re-read.

use crate::Error;
use analyzer_objectstore::ObjectStore;

/// The cache: a directory, bounded in bytes, oldest-read evicted first.
#[derive(Debug)]
pub struct ObjectCache {
    dir: std::path::PathBuf,
    budget: u64,
}

/// An object as a local file: the cache's copy, or a temporary one removed when dropped (a
/// lake without a cache).
#[derive(Debug)]
pub struct LocalCopy {
    path: std::path::PathBuf,
    temporary: bool,
}

impl LocalCopy {
    /// The file.
    #[must_use]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for LocalCopy {
    fn drop(&mut self) {
        if self.temporary {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// A file name for `key`: its SHA-256, hex. An object key runs past a file name's 255 bytes
/// (a lineage's root alone is over a hundred), and two keys never share a digest.
fn file_name(key: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(key.as_bytes()))
}

fn local(e: impl std::fmt::Display, path: &std::path::Path) -> Error {
    Error::Local(format!("object cache {}: {e}", path.display()))
}

/// Write `body` to `path` through a sibling temporary and a rename, so a reader never sees a
/// part of it.
fn write_whole(path: &std::path::Path, body: &[u8]) -> Result<(), Error> {
    static STAGED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let tmp = path.with_extension(format!(
        "part-{}-{}",
        std::process::id(),
        STAGED.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::write(&tmp, body).map_err(|e| local(e, &tmp))?;
    std::fs::rename(&tmp, path).map_err(|e| local(e, path))
}

impl ObjectCache {
    /// A cache in `dir`, holding at most `budget` bytes.
    ///
    /// # Errors
    /// The directory cannot be made.
    pub fn new(dir: impl Into<std::path::PathBuf>, budget: u64) -> Result<Self, Error> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).map_err(|e| local(e, &dir))?;
        Ok(Self { dir, budget })
    }

    /// Keep `body` as the object at `key`, which this process has just written to the store:
    /// the next read of it is local. A writer that is also the reader of what it writes would
    /// otherwise fetch every object it had just put, one GET each (16,579 on one measured tick);
    /// `SlateDB`'s cached store keeps what it puts the same way. The object is
    /// already in the store, so failing to keep it only costs that read: never an error, as
    /// `SlateDB`'s cached put ignores a cache failure. Only for an object never rewritten
    /// under its key.
    pub fn keep(&self, key: &str, body: &[u8]) {
        let path = self.dir.join(file_name(key));
        if write_whole(&path, body).is_ok() {
            let _ = self.evict();
        }
    }

    /// The object at `key` as a local file, fetched from `store` on a miss.
    ///
    /// # Errors
    /// The store failed, or the local disk did.
    pub fn local(&self, store: &dyn ObjectStore, key: &str) -> Result<LocalCopy, Error> {
        let path = self.dir.join(file_name(key));
        if path.is_file() {
            // Read now: the eviction's order is the reads'.
            if let Ok(file) = std::fs::File::options().append(true).open(&path) {
                let _ = file.set_modified(std::time::SystemTime::now());
            }
        } else {
            let body = store.get(key)?;
            write_whole(&path, &body)?;
            self.evict()?;
        }
        Ok(LocalCopy {
            path,
            temporary: false,
        })
    }

    /// Drop the oldest-read files until the cache fits its budget.
    fn evict(&self) -> Result<(), Error> {
        let mut files: Vec<(std::time::SystemTime, u64, std::path::PathBuf)> = Vec::new();
        for entry in std::fs::read_dir(&self.dir).map_err(|e| local(e, &self.dir))? {
            let Ok(entry) = entry else { continue };
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_file() {
                files.push((
                    meta.modified().unwrap_or(std::time::UNIX_EPOCH),
                    meta.len(),
                    entry.path(),
                ));
            }
        }
        let mut total: u64 = files.iter().map(|(_, len, _)| len).sum();
        files.sort();
        for (_, len, path) in files {
            if total <= self.budget {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                total -= len;
            }
        }
        Ok(())
    }
}

/// The object at `key` as a local file: through `cache` when there is one, else a temporary
/// copy under the system's temporary directory.
///
/// # Errors
/// The store failed, or the local disk did.
pub fn local_copy(
    cache: Option<&ObjectCache>,
    store: &dyn ObjectStore,
    key: &str,
) -> Result<LocalCopy, Error> {
    // A counter beside the clock: the clock's grain is a microsecond on some hosts, and two
    // readers of one key in one microsecond would share, and delete, one copy.
    static COPIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    if let Some(cache) = cache {
        return cache.local(store, key);
    }
    let path = std::env::temp_dir().join(format!(
        "lake-object-{}-{}-{}-{}",
        std::process::id(),
        COPIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
        file_name(key)
    ));
    let body = store.get(key)?;
    write_whole(&path, &body)?;
    Ok(LocalCopy {
        path,
        temporary: true,
    })
}

#[cfg(test)]
mod tests {
    use super::{file_name, local_copy, ObjectCache};
    use analyzer_objectstore::{DirStore, ObjectStore};

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cache-{name}-{}-{}",
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
    fn a_cached_object_is_read_locally_and_the_oldest_is_evicted() {
        let root = scratch("cache");
        let store = DirStore::new(root.join("store"));
        store.put("a/x", &[1; 40]).unwrap();
        store.put("a/y", &[2; 40]).unwrap();
        let cache = ObjectCache::new(root.join("cache"), 60).unwrap();
        let x = cache.local(&store, "a/x").unwrap();
        assert_eq!(std::fs::read(x.path()).unwrap(), vec![1; 40]);
        store.delete("a/x").unwrap();
        // A hit never asks the store.
        assert!(cache.local(&store, "a/x").is_ok());
        std::thread::sleep(std::time::Duration::from_millis(20));
        let y = cache.local(&store, "a/y").unwrap();
        assert!(y.path().is_file());
        assert!(!x.path().is_file(), "over budget: the oldest read goes");
        assert_ne!(file_name("a/b"), file_name("a_b"));
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_kept_object_is_read_without_the_store() {
        let root = scratch("keep");
        let store = DirStore::new(root.join("store"));
        let cache = ObjectCache::new(root.join("cache"), 1 << 20).unwrap();
        store.put("a/x", &[3; 40]).unwrap();
        cache.keep("a/x", &[3; 40]);
        // Gone from the store: only the kept copy can answer.
        store.delete("a/x").unwrap();
        let x = cache.local(&store, "a/x").unwrap();
        assert_eq!(std::fs::read(x.path()).unwrap(), vec![3; 40]);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn without_a_cache_the_copy_is_temporary() {
        let root = scratch("temp");
        let store = DirStore::new(&root);
        store.put("k", b"body").unwrap();
        let path = {
            let copy = local_copy(None, &store, "k").unwrap();
            assert_eq!(std::fs::read(copy.path()).unwrap(), b"body");
            copy.path().to_path_buf()
        };
        assert!(!path.exists());
        std::fs::remove_dir_all(root).ok();
    }
}
