// SPDX-License-Identifier: AGPL-3.0-only

//! The lake's commit primitives and shared formats (`docs/lake.md`).
//!
//! A [`head::Head`] is one document advanced only by conditional writes, by the one writer
//! holding its lease; a [`lineage::Lineage`] names the derivation a head belongs to and the
//! writer instance. The execution journal (`analyzer-pipeline`) and the input log
//! ([`ledger`]) are heads. This crate performs no I/O of its own: every object goes through
//! `analyzer-objectstore`.

pub mod cache;
pub mod dated;
pub mod fold;
pub mod head;
pub mod keyed;
pub mod ledger;
pub mod lineage;
pub mod state;
pub mod stripe;
pub mod working;

/// Requests to the store in flight at once from one call that has several to make: a stripe's
/// objects, a revision's parts. The store's runtime carries them side by side
/// (`analyzer_objectstore::RUNTIME_WORKERS`); issued one after another, a measured tick's 16,579
/// fetches were each a round trip. A labelled analytic constant
/// (`docs/conformance.md`), pinned: bounded by the bucket's request rate and the box's memory
/// for bodies in flight, not by its cores.
pub const IO_WIDTH: usize = 8;

/// `f` over `items`, at most [`IO_WIDTH`] at once, results in the items' order; the first
/// error, if any. Each call runs on a scoped thread of its own, which blocks on the store.
///
/// # Errors
/// The first of `f`'s errors, in the items' order.
pub fn in_parallel<T: Sync, R: Send, E: Send>(
    items: &[T],
    f: impl Fn(&T) -> Result<R, E> + Sync,
) -> Result<Vec<R>, E> {
    if items.len() <= 1 {
        return items.iter().map(&f).collect();
    }
    let mut out = Vec::with_capacity(items.len());
    for wave in items.chunks(IO_WIDTH) {
        let results: Vec<Result<R, E>> = std::thread::scope(|scope| {
            let handles: Vec<_> = wave.iter().map(|item| scope.spawn(|| f(item))).collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                })
                .collect()
        });
        for result in results {
            out.push(result?);
        }
    }
    Ok(out)
}

/// What a lake operation can fail with.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Another writer holds this lineage's lease, or took it over since this writer last
    /// committed: this writer may not commit.
    #[error("lineage {lineage}: this writer may not commit: {reason}")]
    Fenced {
        /// The lineage whose document refused the commit.
        lineage: String,
        /// Who holds it, or how this writer was superseded.
        reason: String,
    },
    /// A document or object that does not decode.
    #[error("{0}")]
    Corrupt(String),
    /// A document written in a format this build does not read: refused, never guessed at.
    #[error("{document} is format {found}, this build reads format {reads}")]
    OtherFormat {
        /// The document and its key.
        document: String,
        /// The format it was written in.
        found: u32,
        /// The format this build reads.
        reads: u32,
    },
    /// An identity or argument the lake refuses.
    #[error("{0}")]
    Invalid(String),
    /// The local data directory (where a lineage's identity is kept) failed.
    #[error("{0}")]
    Local(String),
    /// The object store failed.
    #[error(transparent)]
    Store(#[from] analyzer_objectstore::Error),
}

/// Where one lineage's documents live: a store, the lineage's root in it, and the lineage.
/// Built by the caller that knows the namespace (the pipeline's spine, the daemon); handed to
/// every writer and reader of the lineage's journal and input log, so they cannot disagree
/// on the location.
#[derive(Clone)]
pub struct Lake {
    store: std::sync::Arc<dyn analyzer_objectstore::ObjectStore + Send + Sync>,
    root: String,
    lineage: lineage::Lineage,
    open_days: ledger::OpenDays,
    cache: Option<std::sync::Arc<cache::ObjectCache>>,
    working: Option<std::sync::Arc<working::WorkingState>>,
}

impl Lake {
    /// The lineage's documents under `root` in `store`.
    #[must_use]
    pub fn new(
        store: std::sync::Arc<dyn analyzer_objectstore::ObjectStore + Send + Sync>,
        root: String,
        lineage: lineage::Lineage,
    ) -> Self {
        Self {
            store,
            root,
            lineage,
            open_days: ledger::OpenDays::default(),
            cache: None,
            working: None,
        }
    }

    /// This lake keeping its fold's keyed relations in `state`
    /// (`docs/lake.md`).
    #[must_use]
    pub fn with_working(mut self, working: std::sync::Arc<working::WorkingState>) -> Self {
        self.working = Some(working);
        self
    }

    /// The fold's working state.
    ///
    /// # Errors
    /// The lake was built without one: it holds no keyed relation.
    pub fn working(&self) -> Result<&working::WorkingState, Error> {
        self.working.as_deref().ok_or_else(|| {
            Error::Invalid(format!(
                "{}: this lake was built without its working state",
                self.root
            ))
        })
    }

    /// End the working state's unit of work at `position`, the classification revision it
    /// committed (`working::WorkingState::end`): a checkpoint starts in the background once
    /// one is due.
    ///
    /// # Errors
    /// The lake has no working state, or the checkpoint's snapshot failed.
    pub fn end_unit(&self, position: u64) -> Result<(), Error> {
        let working = self.working.as_ref().ok_or_else(|| {
            Error::Invalid(format!(
                "{}: this lake was built without its working state",
                self.root
            ))
        })?;
        working.end(self, position)
    }

    /// This lake reading its immutable fold objects through `cache`.
    #[must_use]
    pub fn with_cache(mut self, cache: std::sync::Arc<cache::ObjectCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// The object at `key` as a local file: the cache's copy, or a temporary one.
    ///
    /// # Errors
    /// The store failed, or the local disk did.
    pub fn local_copy(&self, key: &str) -> Result<cache::LocalCopy, Error> {
        cache::local_copy(self.cache.as_deref(), self.store(), key)
    }

    /// Keep `body`, just written at `key`, in the object cache if there is one: for an
    /// object never rewritten under its key (see [`cache::ObjectCache::keep`]).
    pub fn keep(&self, key: &str, body: &[u8]) {
        if let Some(cache) = self.cache.as_deref() {
            cache.keep(key, body);
        }
    }

    /// The store.
    #[must_use]
    pub fn store(&self) -> &dyn analyzer_objectstore::ObjectStore {
        self.store.as_ref()
    }

    /// The lineage's root in the store: `<prefix>_analyzer/namespaces/<ns>/lineages/<id>/`.
    #[must_use]
    pub fn root(&self) -> &str {
        &self.root
    }

    /// Which lineage this is, across every handle on it: the store's location and the
    /// lineage's root there. Two lakes with one identity hold the same objects.
    #[must_use]
    pub fn identity(&self) -> String {
        format!("{}/{}", self.store.location(), self.root)
    }

    /// The lineage.
    #[must_use]
    pub fn lineage(&self) -> &lineage::Lineage {
        &self.lineage
    }

    /// Where the lineage's execution journal lives: its head and its lease, the lease every
    /// document a writer commits is fenced by.
    #[must_use]
    pub fn journal_dir(&self) -> String {
        format!("{}journal/", self.root)
    }

    /// The fold's day manifests, committed under the journal's lease.
    ///
    /// # Errors
    /// The store cannot open a sibling connection.
    pub fn day_manifests(&self) -> Result<fold::DayManifests, Error> {
        fold::DayManifests::at(self.store(), &self.root, self.lineage.clone())
    }

    /// The lineage's published-state logs.
    #[must_use]
    pub fn state(&self) -> state::StateLogs {
        state::StateLogs::at(self)
    }

    /// Shard `shard` of the lineage's input log. Every clone of this lake shares one cache
    /// of the log's open days (v1 runs one shard).
    ///
    /// # Errors
    /// The store cannot open a sibling connection.
    pub fn ledger(&self, shard: u32) -> Result<ledger::Ledger, Error> {
        ledger::Ledger::sharing(
            self.store(),
            &self.root,
            shard,
            self.lineage.clone(),
            std::sync::Arc::clone(&self.open_days),
        )
    }
}

impl std::fmt::Debug for Lake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lake")
            .field("root", &self.root)
            .field("lineage", &self.lineage)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod in_parallel_tests {
    /// Results come back in the items' order, never more than `IO_WIDTH` run at once, and the
    /// first error in order is the one returned.
    #[test]
    fn calls_run_side_by_side_bounded_in_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let running = AtomicUsize::new(0);
        let most = AtomicUsize::new(0);
        let items: Vec<usize> = (0..40).collect();
        let out = super::in_parallel(&items, |i| {
            let now = running.fetch_add(1, Ordering::SeqCst) + 1;
            most.fetch_max(now, Ordering::SeqCst);
            // Wait for company: every wave has eight calls, so a second one always comes
            // unless the calls run one at a time. The minute bounds a loaded host, not the claim.
            let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
            while most.load(Ordering::SeqCst) < 2 && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
            running.fetch_sub(1, Ordering::SeqCst);
            Ok::<_, String>(i * 2)
        })
        .expect("no error");
        assert_eq!(out, items.iter().map(|i| i * 2).collect::<Vec<_>>());
        let most = most.load(Ordering::SeqCst);
        assert!(most > 1, "the calls never overlapped");
        assert!(most <= super::IO_WIDTH, "{most} calls at once");
        let failed = super::in_parallel(
            &items,
            |i| if *i == 3 || *i == 30 { Err(*i) } else { Ok(*i) },
        );
        assert_eq!(failed, Err(3));
    }
}
