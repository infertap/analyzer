// SPDX-License-Identifier: AGPL-3.0-only

//! A lineage: one derivation of one collection, and the writer that may commit it.
//!
//! The execution journal belongs to a lineage, not to a namespace. Two analyzers over the
//! same bucket are two lineages with two journals, and each converges on its own; that is
//! what lets a replay or a second deployment derive the same input without touching the
//! first. Within a lineage exactly one writer commits at a time. The writer holds a lease
//! object beside the journal head (acquired and renewed by conditional writes), and every
//! commit stamps the lease's epoch into the head. A writer whose epoch is older than the
//! head's has been superseded, and its commits are refused (fenced), whatever its clock says.
//!
//! Today each analyzer is its own lineage: the identity is created once in the analyzer's
//! data directory and read back on every start, so a restart resumes its own journal and
//! reclaims its own lease at once. A standby that shares a lineage (a distinct instance)
//! waits for the lease to expire and takes it over with the next epoch.

use crate::Error;
use analyzer_objectstore::{Conditional, ObjectStore, Precondition, Version};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// How long a lease stands without renewal. A standby may take over only after this; the
/// holder renews on commit once half of it has passed.
pub const LEASE_TTL_MS: u64 = 5 * 60 * 1000;

/// The file in an analyzer's data directory that holds its lineage identity.
const IDENTITY_FILE: &str = "lineage.json";

/// Which derivation a journal records, and which writer instance this process is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
    /// The derivation: the journal's key component. Shared by a standby of the same lineage.
    pub lineage: String,
    /// The writer: the lease holder. Distinct for every analyzer data directory.
    pub instance: String,
}

impl Lineage {
    /// A lineage with explicit ids, each 1 to 64 characters of `[a-z0-9-]`.
    ///
    /// # Errors
    /// An id is empty, too long, or carries another character.
    pub fn new(lineage: &str, instance: &str) -> Result<Self, Error> {
        for (what, id) in [("lineage", lineage), ("instance", instance)] {
            if id.is_empty()
                || id.len() > 64
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            {
                return Err(Error::Invalid(format!(
                    "{what} id {id:?} must be 1 to 64 characters of [a-z0-9-]"
                )));
            }
        }
        Ok(Self {
            lineage: lineage.to_string(),
            instance: instance.to_string(),
        })
    }

    /// The lineage recorded in `dir`, created on first use: a fresh id that is both the
    /// lineage and the instance. Concurrent first uses agree on one file.
    ///
    /// # Errors
    /// The directory cannot be read or written, or the file does not decode.
    pub fn of_data_dir(dir: &Path) -> Result<Self, Error> {
        if let Some(existing) = Self::read(dir)? {
            return Ok(existing);
        }
        let id = fresh_id(dir);
        let created = Self::new(&id, &id)?;
        let body = serde_json::to_vec(&created).map_err(|e| Error::Corrupt(e.to_string()))?;
        std::fs::create_dir_all(dir)
            .map_err(|e| Error::Local(format!("lineage: {}: {e}", dir.display())))?;
        let path = dir.join(IDENTITY_FILE);
        // Whole, then linked into place: a reader never sees the file before its content,
        // and the link fails if another first use got there first. A file created empty and
        // then written was read empty by a concurrent first use.
        let staged = dir.join(format!(
            "{IDENTITY_FILE}.{}.{}.part",
            std::process::id(),
            STAGED_IDENTITY.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let written = std::fs::write(&staged, &body)
            .and_then(|()| std::fs::File::open(&staged)?.sync_all())
            .map_err(|e| Error::Local(format!("lineage: {}: {e}", staged.display())));
        let linked = written.and_then(|()| match std::fs::hard_link(&staged, &path) {
            Ok(()) => Ok(Some(created)),
            // Another first use won the race: its identity is the one.
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
            Err(e) => Err(Error::Local(format!("lineage: {}: {e}", path.display()))),
        });
        let _ = std::fs::remove_file(&staged);
        match linked? {
            Some(created) => Ok(created),
            None => Self::read(dir)?
                .ok_or_else(|| Error::Local(format!("lineage: {} vanished", path.display()))),
        }
    }

    /// The lineage recorded in `dir`, or `None` when none was ever created there.
    ///
    /// # Errors
    /// The file exists but cannot be read or does not decode.
    pub fn read(dir: &Path) -> Result<Option<Self>, Error> {
        let path = dir.join(IDENTITY_FILE);
        match std::fs::read(&path) {
            Ok(body) => {
                let recorded: Self = serde_json::from_slice(&body)
                    .map_err(|e| Error::Corrupt(format!("lineage: {}: {e}", path.display())))?;
                Self::new(&recorded.lineage, &recorded.instance).map(Some)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Local(format!("lineage: {}: {e}", path.display()))),
        }
    }

    /// The key component under a namespace's object prefix: `lineages/<id>/`.
    #[must_use]
    pub fn object_prefix(&self) -> String {
        format!("lineages/{}/", self.lineage)
    }
}

/// A counter naming each first use's staged identity file, so two first uses in one process
/// never stage into one file.
static STAGED_IDENTITY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A fresh id: a hash of this moment, this process and the directory, as 24 hex digits.
fn fresh_id(dir: &Path) -> String {
    use sha2::{Digest, Sha256};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let digest = Sha256::digest(format!("{nanos}/{}/{}", std::process::id(), dir.display()));
    format!("{digest:x}")[..24].to_string()
}

/// The lease object: who may commit the lineage, under which epoch, and until when.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Lease {
    pub(crate) holder: String,
    pub(crate) epoch: u64,
    pub(crate) expires_ms: u64,
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Hold the lease at `key` for `lineage`'s instance, and return its epoch.
///
/// The same holder renews without changing the epoch, so a restart reclaims its own lease
/// at once. Another holder's unexpired lease refuses; an expired one is taken over with the
/// next epoch. `held` is the lease this writer last wrote, and is renewed only once half its
/// time has passed, so most commits read nothing.
///
/// # Errors
/// Another holder's lease stands, the store failed, or the lease does not decode.
pub(crate) fn hold(
    store: &dyn ObjectStore,
    key: &str,
    lineage: &Lineage,
    held: Option<&Lease>,
) -> Result<Lease, Error> {
    let now = now_ms();
    if let Some(held) = held {
        if held.holder == lineage.instance && held.expires_ms > now + LEASE_TTL_MS / 2 {
            return Ok(held.clone());
        }
    }
    let fenced = |reason: String| Error::Fenced {
        lineage: lineage.lineage.clone(),
        reason,
    };
    let current: Option<(Lease, Version)> = match store.get_versioned(key)? {
        None => None,
        Some((body, version)) => Some((
            serde_json::from_slice(&body)
                .map_err(|e| Error::Corrupt(format!("lineage lease {key}: {e}")))?,
            version,
        )),
    };
    let (next, condition) = match &current {
        None => (
            Lease {
                holder: lineage.instance.clone(),
                epoch: 1,
                expires_ms: now + LEASE_TTL_MS,
            },
            Precondition::Absent,
        ),
        Some((lease, version)) if lease.holder == lineage.instance => (
            Lease {
                expires_ms: now + LEASE_TTL_MS,
                ..lease.clone()
            },
            Precondition::Matches(version.clone()),
        ),
        Some((lease, version)) if lease.expires_ms <= now => (
            Lease {
                holder: lineage.instance.clone(),
                epoch: lease.epoch + 1,
                expires_ms: now + LEASE_TTL_MS,
            },
            Precondition::Matches(version.clone()),
        ),
        Some((lease, _)) => {
            return Err(fenced(format!(
                "held by instance {} (epoch {}) for another {} s",
                lease.holder,
                lease.epoch,
                (lease.expires_ms - now) / 1000
            )))
        }
    };
    let body = serde_json::to_vec(&next).map_err(|e| Error::Corrupt(e.to_string()))?;
    match store.put_if(key, &body, &condition)? {
        Conditional::Written(_) => Ok(next),
        _ => Err(fenced(
            "another instance wrote the lease during this acquisition".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{hold, Lease, Lineage, LEASE_TTL_MS};
    use analyzer_objectstore::{DirStore, ObjectStore};

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "lineage-{name}-{}-{}",
            std::process::id(),
            super::now_ms()
        ));
        std::fs::create_dir_all(&dir).expect("scratch");
        dir
    }

    #[test]
    fn concurrent_first_uses_agree_on_one_lineage_and_never_read_it_half_written() {
        for round in 0..20 {
            let dir = scratch(&format!("race-{round}"));
            let seen: Vec<Lineage> = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..8)
                    .map(|_| scope.spawn(|| Lineage::of_data_dir(&dir)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().expect("joined").expect("every first use succeeds"))
                    .collect()
            });
            assert!(
                seen.windows(2).all(|w| w[0] == w[1]),
                "one lineage: {seen:?}"
            );
            assert_eq!(
                std::fs::read_dir(&dir).expect("dir").count(),
                1,
                "no staged file left behind"
            );
            std::fs::remove_dir_all(dir).ok();
        }
    }

    #[test]
    fn a_data_directory_keeps_one_lineage_across_opens() {
        let dir = scratch("identity");
        let first = Lineage::of_data_dir(&dir).expect("create");
        assert_eq!(first.lineage, first.instance);
        assert_eq!(Lineage::of_data_dir(&dir).expect("reopen"), first);
        assert_ne!(
            Lineage::of_data_dir(&scratch("identity-other")).expect("other"),
            first,
            "two data directories are two lineages"
        );
    }

    #[test]
    fn an_id_outside_the_alphabet_is_refused() {
        assert!(Lineage::new("a/b", "a").is_err());
        assert!(Lineage::new("", "a").is_err());
        assert!(Lineage::new("abc-1", "abc-1").is_ok());
    }

    #[test]
    fn the_same_instance_reclaims_its_lease_and_another_waits_then_takes_over() {
        let dir = scratch("lease");
        let store = DirStore::new(&dir);
        let primary = Lineage::new("shared", "primary").unwrap();
        let standby = Lineage::new("shared", "standby").unwrap();
        let first = hold(&store, "lease", &primary, None).expect("acquire");
        assert_eq!(first.epoch, 1);
        // A restart of the same instance holds nothing in memory and reclaims at once.
        assert_eq!(
            hold(&store, "lease", &primary, None)
                .expect("reclaim")
                .epoch,
            1
        );
        let refused = hold(&store, "lease", &standby, None).expect_err("held");
        assert!(matches!(refused, crate::Error::Fenced { .. }), "{refused}");
        // Expire it by hand: the standby takes over with the next epoch.
        let expired = Lease {
            expires_ms: 0,
            ..first
        };
        store
            .put("lease", &serde_json::to_vec(&expired).unwrap())
            .unwrap();
        let taken = hold(&store, "lease", &standby, None).expect("take over");
        assert_eq!((taken.holder.as_str(), taken.epoch), ("standby", 2));
        assert!(taken.expires_ms > super::now_ms() + LEASE_TTL_MS / 2);
        assert!(hold(&store, "lease", &primary, None).is_err());
    }
}
