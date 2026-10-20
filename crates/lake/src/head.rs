// SPDX-License-Identifier: AGPL-3.0-only

//! A head: one document in the object store, advanced only by a conditional write, by the
//! one writer that holds its lease. The execution journal (`analyzer-pipeline`) and the
//! input log (`crate::ledger`) are both heads.
//!
//! A commit holds the lease and stamps its epoch into the document; a commit under an older
//! epoch than the document's is refused (`Error::Fenced`), whatever the writer's clock says.
//! After a refusal or a transport error the document is re-read: if the change is already in
//! it the commit stands (an ambiguous commit that landed); otherwise the change is applied to
//! the document read and written once more.
//!
//! **A commit's change should be idempotent**: applying it to a document that already
//! contains it should leave it unchanged. Upserts by key have this property. A counter's
//! advance does not: re-applied after an ambiguous commit that landed, it advances again,
//! which skips a value and never reuses one.

use crate::Error;
use analyzer_objectstore::{Conditional, ObjectStore, Precondition, Version};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Mutex;

/// A document a head holds.
pub trait Document: Clone + Default + PartialEq + Serialize + DeserializeOwned {
    /// What the document is, for messages.
    const NAME: &'static str;
    /// The format this build reads and writes. A document in another format is refused.
    const FORMAT: u32;
    /// The document's format, as stored.
    fn format(&self) -> u32;
    /// The lease epoch of the last writer to commit.
    fn lease_epoch(&self) -> u64;
    /// Stamp the committing writer's lease epoch.
    fn set_lease_epoch(&mut self, epoch: u64);
}

/// The leases this process holds, by lease key and instance. One operation holds several
/// heads over the same document; they share the one lease rather than each re-acquiring it.
static HELD: std::sync::LazyLock<
    Mutex<std::collections::HashMap<(String, String), crate::lineage::Lease>>,
> = std::sync::LazyLock::new(Mutex::default);

/// One head: the document's and the lease's keys, the writer's identity, and the document as
/// this writer last committed or read it.
pub struct Head<D> {
    store: Box<dyn ObjectStore>,
    key: String,
    lease_key: String,
    lineage: crate::lineage::Lineage,
    cached: Mutex<Option<(D, Option<Version>)>>,
}

impl<D: Document> Head<D> {
    /// The head whose document and lease live under `dir` (`<dir>head`, `<dir>lease`) in
    /// `store`, written by `lineage`'s instance. Nothing is read until first use.
    pub fn at(store: Box<dyn ObjectStore>, dir: &str, lineage: crate::lineage::Lineage) -> Self {
        Self {
            store,
            key: format!("{dir}head"),
            lease_key: format!("{dir}lease"),
            lineage,
            cached: Mutex::new(None),
        }
    }

    /// The head whose document lives under `dir` and whose lease is another head's, under
    /// `lease_dir`: a document the same writer commits beside that head's (the fold's day
    /// manifests, under the journal's lease), fenced by the same epoch.
    pub fn sharing_lease(
        store: Box<dyn ObjectStore>,
        dir: &str,
        lease_dir: &str,
        lineage: crate::lineage::Lineage,
    ) -> Self {
        Self {
            store,
            key: format!("{dir}head"),
            lease_key: format!("{lease_dir}lease"),
            lineage,
            cached: Mutex::new(None),
        }
    }

    /// Hold the lease, renewing it once half its time has passed; its epoch. A writer that
    /// writes objects beside the document before committing (the input log's records) holds
    /// it first, so the objects carry the epoch the commit will.
    ///
    /// # Errors
    /// Another instance holds the lease, or the store failed.
    pub fn hold_lease(&self) -> Result<u64, Error> {
        let mut held = HELD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let slot = (self.lease_key.clone(), self.lineage.instance.clone());
        let lease = crate::lineage::hold(
            self.store.as_ref(),
            &self.lease_key,
            &self.lineage,
            held.get(&slot),
        );
        match lease {
            Ok(lease) => {
                let epoch = lease.epoch;
                held.insert(slot, lease);
                Ok(epoch)
            }
            Err(e) => {
                held.remove(&slot);
                Err(e)
            }
        }
    }

    fn superseded(&self, document: &D, epoch: u64) -> Option<Error> {
        (document.lease_epoch() > epoch).then(|| Error::Fenced {
            lineage: self.lineage.lineage.clone(),
            reason: format!(
                "superseded: the {} was committed under lease epoch {}, this writer holds {epoch}",
                D::NAME,
                document.lease_epoch()
            ),
        })
    }

    fn load(&self) -> Result<(D, Option<Version>), Error> {
        match self.store.get_versioned(&self.key)? {
            None => Ok((D::default(), None)),
            Some((body, version)) => {
                // The format first: a document in another format need not decode as this
                // build's, and is refused by its format, not by whatever field it lacks.
                let corrupt =
                    |e: serde_json::Error| Error::Corrupt(format!("{} {}: {e}", D::NAME, self.key));
                let found = serde_json::from_slice::<serde_json::Value>(&body)
                    .map_err(corrupt)?
                    .get("format")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|f| u32::try_from(f).ok())
                    .ok_or_else(|| {
                        Error::Corrupt(format!("{} {}: no format", D::NAME, self.key))
                    })?;
                if found != D::FORMAT {
                    return Err(Error::OtherFormat {
                        document: format!("{} {}", D::NAME, self.key),
                        found,
                        reads: D::FORMAT,
                    });
                }
                let document: D = serde_json::from_slice(&body).map_err(corrupt)?;
                Ok((document, Some(version)))
            }
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, Option<(D, Option<Version>)>> {
        self.cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The document as the store holds it now.
    ///
    /// Always a read from the store, never the cache: one operation holds several heads over
    /// the same document, and a cached read on one would miss a commit made through another.
    /// The cache serves only the version a commit conditions on, where a stale one is
    /// refused, not believed.
    ///
    /// # Errors
    /// The store failed, or the document does not decode.
    pub fn read(&self) -> Result<D, Error> {
        let loaded = self.load()?;
        let document = loaded.0.clone();
        *self.cache() = Some(loaded);
        Ok(document)
    }

    /// Apply `change` to the document and write it conditionally. Returns the committed
    /// document. A change that leaves the document as it is writes nothing.
    ///
    /// # Errors
    /// The lease is held by another instance, this writer was superseded, the store failed
    /// twice, the document does not decode, or another writer won twice.
    pub fn commit(&self, change: impl Fn(&mut D)) -> Result<D, Error> {
        let epoch = self.hold_lease()?;
        let change = |document: &mut D| {
            change(document);
            document.set_lease_epoch(document.lease_epoch().max(epoch));
        };
        let mut cached = self.cache();
        if cached.is_none() {
            *cached = Some(self.load()?);
        }
        let (document, version) = cached.clone().unwrap_or_default();
        if let Some(fenced) = self.superseded(&document, epoch) {
            return Err(fenced);
        }
        let mut next = document.clone();
        change(&mut next);
        if next == document {
            return Ok(document);
        }
        let first = self.write(&next, version.as_ref());
        if let Ok(Some(written)) = first {
            *cached = Some((next.clone(), Some(written)));
            return Ok(next);
        }
        // Refused, or the outcome is unknown: read what the store holds and decide.
        let (current, current_version) = match self.load() {
            Ok(loaded) => loaded,
            Err(e) => return Err(first.err().unwrap_or(e)),
        };
        if let Some(fenced) = self.superseded(&current, epoch) {
            *cached = Some((current, current_version));
            return Err(fenced);
        }
        let mut again = current.clone();
        change(&mut again);
        if again == current {
            *cached = Some((current.clone(), current_version));
            return Ok(current);
        }
        if let Some(written) = self.write(&again, current_version.as_ref())? {
            *cached = Some((again.clone(), Some(written)));
            Ok(again)
        } else {
            *cached = None;
            Err(Error::Fenced {
                lineage: self.lineage.lineage.clone(),
                reason: format!(
                    "another writer committed the {} {} twice during one change",
                    D::NAME,
                    self.key
                ),
            })
        }
    }

    /// Write `document` conditionally: `Some(version)` when written, `None` when refused.
    fn write(&self, document: &D, version: Option<&Version>) -> Result<Option<Version>, Error> {
        let body = serde_json::to_vec(document)
            .map_err(|e| Error::Corrupt(format!("{} {}: {e}", D::NAME, self.key)))?;
        let condition = version.map_or(Precondition::Absent, |v| Precondition::Matches(v.clone()));
        match self.store.put_if(&self.key, &body, &condition)? {
            Conditional::Written(v) => Ok(Some(v)),
            _ => Ok(None),
        }
    }
}
