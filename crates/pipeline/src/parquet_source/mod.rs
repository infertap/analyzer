// SPDX-License-Identifier: AGPL-3.0-only

//! `ParquetSource` — the derivation's source: the Ordered Event IR built from sealed segments
//! in the bucket, through a local content cache, with no store in the derivation's input path.
//!
//! **It owns everything operational; it owns no decode.** Discovery enumerates dated SHIP-hour
//! prefixes (`analyzer_lake::dated` — the same layout arithmetic the ingest LIST bounds
//! itself with): the retention horizon's whole range where a load's bounds are in the engine
//! clock domain, a margin-widened range only where they are the producer's wall clock (spec
//! 2.6; `discover_horizon` states the law and its cost) — plus, on both paths, the UNDATED
//! keys under the fleet prefix, because ingest deliberately accepts operator archive drops and
//! a source that could not see them would derive from less than the store ingested. The
//! candidate rule is ingest's own, derived from one constant
//! (`analyzer_lake::dated::SHIPPED_SUFFIX`), so the two transports cannot drift. The cache
//! fetches each immutable segment once ever, and the bytes go to
//! `analyzer_segments::columnar` for the projected, row-group-pruned decode. The rules that
//! read the rows — own-copy, the `block_id` condition, the half-open window edge — stay with
//! the derivations, exactly as the `EventSource` seam requires.
//!
//! **One undecodable object does not fail the load** (quarantine-and-declare): the object is
//! skipped, named loudly, and counted, and the skipped segment's
//! absent `segment_seq` is what prices the loss — the EXISTING sequence-gap machinery degrades
//! the window, so no cause is invented here. List and transport failures stay hard errors:
//! quarantine is per-object, never per-infrastructure.
//!
//! **The fetch seam is a trait, not a transport.** [`SegmentStore`] is that seam. The appliance
//! reads through `BucketStore`, over the workspace's own object-storage port; the job form and
//! a test hand in a directory, which is [`FsStore`].
//!
//! The transport is ours, never the engine's: the engine is handed rows and asked questions,
//! and never asked to fetch an object, so no credential reaches it and no object content has to
//! survive a text transport.
//!
//! **The content cache is endgame, and invariant I7 classifies it *cannot diverge*:**
//! segments are immutable and the cache holds bytes, not derivations. Keyed by object name,
//! fetched once per segment ever, disposable and rebuildable — a cache loss costs a re-fetch,
//! never an answer. Eviction is by age past the horizon PLUS the listing margin, read from the
//! key's own dated hour (an undated key ages by its fetch time, the only age it has): nothing
//! any discovery can still list is evictable, or fetch-once would break at the horizon edge.
//!
//! The grouping the fold consumes — `(Key, &[Event])` — stays
//! [`analyzer_analysis::events::group_by_interval_key`]'s: this source delivers the total
//! order [`analyzer_analysis::events::sort`] defines, and nothing above it changes.

use crate::Error;
use analyzer_analysis::events::{Event, EventSource, LifecycleEvent};
use analyzer_analysis::Window;
use analyzer_lake::dated;
use analyzer_segments::columnar::Projection;
use serde_json::Value;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

mod decode;

pub use decode::{cache_event_of, lifecycle_event_of, sort_lifecycle};

/// Where sealed segments are listed and fetched from. Implementations carry the transport and
/// nothing else: no decode, no cache, no predicate.
pub trait SegmentStore: Send + Sync {
    /// Every object key under `prefix`. Keys are store-absolute (they begin with the prefix),
    /// because the cache and the decode both name segments by their full key.
    ///
    /// # Errors
    /// Transport failures. A prefix with no objects is an empty listing, not an error.
    fn list(&self, prefix: &str) -> Result<Vec<String>, Error>;

    /// One object's bytes, whole. Segments are immutable, so there is no conditional form.
    ///
    /// # Errors
    /// Transport failures, including a key that does not exist.
    fn fetch(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// A client of this store's own for another thread — a bucket store is one runtime
    /// behind a mutex, so fetches side by side need one each.
    /// `None` where the store has no such thing, and the fill's fetchers share this one.
    fn sibling(&self) -> Option<Box<dyn SegmentStore>> {
        None
    }
}

/// The plain-filesystem [`SegmentStore`]: a directory whose layout is the bucket's key layout.
/// The job form and a partner mirror read this; tests hand it a corpus.
pub struct FsStore {
    root: PathBuf,
}

impl FsStore {
    /// A store over `root`: keys are paths beneath it, with the bucket's `/` separators.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl SegmentStore for FsStore {
    fn list(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let dir = self.root.join(prefix);
        if !dir.is_dir() {
            return Ok(Vec::new());
        }
        let mut keys = Vec::new();
        walk(&dir, &mut keys)?;
        let mut out = Vec::with_capacity(keys.len());
        for path in keys {
            let rel = path.strip_prefix(&self.root).map_err(|_| {
                Error::Derivation(format!("listed path {} escapes the root", path.display()))
            })?;
            // Key separators are the bucket's, whatever the host's are.
            let key = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.push(key);
        }
        Ok(out)
    }

    fn fetch(&self, key: &str) -> Result<Vec<u8>, Error> {
        std::fs::read(self.root.join(key)).map_err(|e| Error::Derivation(format!("get {key}: {e}")))
    }

    fn sibling(&self) -> Option<Box<dyn SegmentStore>> {
        Some(Box::new(Self::new(self.root.clone())))
    }
}

/// The bucket [`SegmentStore`], over the workspace's own object-storage port.
///
/// The bytes come straight off the object-storage port: the derivation never asks the engine to
/// read a segment.
pub(crate) struct BucketStore {
    store: Arc<dyn analyzer_objectstore::ObjectStore>,
}

impl BucketStore {
    #[must_use]
    pub fn new(store: Arc<dyn analyzer_objectstore::ObjectStore + Send + Sync>) -> Self {
        Self { store }
    }
}

impl SegmentStore for BucketStore {
    fn list(&self, prefix: &str) -> Result<Vec<String>, Error> {
        Ok(self
            .store
            .list(prefix)?
            .into_iter()
            .map(|m| m.key)
            .collect())
    }

    fn fetch(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.store.get(key).map_err(Error::from)
    }

    fn sibling(&self) -> Option<Box<dyn SegmentStore>> {
        let store = self.store.sibling().ok()?;
        Some(Box::new(Self {
            store: Arc::from(store),
        }))
    }
}

/// Forwarding, so [`SourceConfig::source`] can return one `ParquetSource` type over either
/// transport.
impl SegmentStore for Box<dyn SegmentStore + '_> {
    fn list(&self, prefix: &str) -> Result<Vec<String>, Error> {
        self.as_ref().list(prefix)
    }

    fn fetch(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.as_ref().fetch(key)
    }
}

/// Which transport the segments arrive over. Both are production shapes: the appliance's
/// network fence confines S3 to the engine boundary, while the job form and a partner mirror
/// read a plain directory.
#[derive(Clone)]
pub enum SegmentTransport {
    /// The customer's bucket, through the workspace's own object-storage port.
    ///
    /// **The store rides HERE rather than being a parameter**: a store is a transport, not a
    /// connection to a database, so holding one keeps the engine out of this path.
    Collection {
        /// The bucket behind the port: listed and fetched through it, never through the engine.
        store: Arc<dyn analyzer_objectstore::ObjectStore + Send + Sync>,
    },
    /// A directory whose layout is the bucket's key layout.
    Directory {
        /// The directory that stands for the bucket: keys are paths beneath it.
        root: PathBuf,
    },
}

impl std::fmt::Debug for SegmentTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Collection { .. } => f.write_str("Collection"),
            Self::Directory { root } => write!(f, "Directory({})", root.display()),
        }
    }
}

/// The retention horizon in hours, as a LIVE value with one owner.
///
/// The daemon widens the effective horizon while the driver has input to read past it, and it
/// rebuilds its tick source every cycle — but the API and the DAO hold a [`SourceConfig`] for
/// their whole life, and a plain `i64` there would freeze the horizon at bind time. This is one
/// shared cell: the daemon writes it, and every [`SourceConfig::source`] construction reads the
/// value in force at that moment.
#[derive(Debug, Clone)]
pub struct HorizonHours(Arc<AtomicI64>);

impl HorizonHours {
    /// A horizon that nothing moves: tests, the job form, and any caller that holds no input
    /// past its declared horizon.
    #[must_use]
    pub fn fixed(hours: i64) -> Self {
        Self(Arc::new(AtomicI64::new(hours)))
    }

    /// The hours currently in force.
    #[must_use]
    pub fn hours(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Move the horizon. The daemon is the only intended caller.
    pub fn set(&self, hours: i64) {
        self.0.store(hours, Ordering::Relaxed);
    }
}

/// Everything a caller states to open the derivation's source: the transport, the fleet's key
/// prefix inside it, the content cache, and the retention horizon.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Where the segments arrive from: the bucket through the port, or a directory.
    pub transport: SegmentTransport,
    /// The fleet's key prefix inside the transport (`tap/` in a shared bucket, empty for a
    /// dedicated one); dated hour segments render directly under it.
    pub key_prefix: String,
    /// The fetch-once byte cache's directory: [`segment_cache_dir`] under the data directory.
    pub cache_dir: PathBuf,
    /// Live, shared with the daemon that widens it — see [`HorizonHours`]. Any other caller
    /// states [`HorizonHours::fixed`].
    pub horizon_hours: HorizonHours,
    /// **The corpus seam, never a production shape.** `true` admits the write-ahead `.jsonl`
    /// and archive `.ndjson` forms beside the shipped `.parquet` — what the conformance
    /// corpora, the differential's two-encoding diversity, and `selftest`'s embedded fixtures
    /// read. Production constructors state `false`: nothing conforming puts JSONL in a bucket,
    /// and discovery here must match ingest's candidate rule.
    pub admit_corpus_forms: bool,
    /// The segment cache's byte budget.
    pub cache_budget_bytes: u64,
}

impl SourceConfig {
    /// The `EventSource` this configuration names.
    ///
    /// **No engine parameter any more.** It existed so the collection transport could borrow a
    /// connection and build an `EngineStore`; the transport carries its own store now, and the
    /// derivation reads segments without the engine being involved at all. The horizon is still
    /// read HERE, at construction, so a long-lived holder opens each source at the horizon in
    /// force at that moment.
    #[must_use]
    pub fn source(&self) -> ParquetSource<Box<dyn SegmentStore>> {
        let store: Box<dyn SegmentStore> = match &self.transport {
            SegmentTransport::Collection { store } => Box::new(BucketStore::new(store.clone())),
            SegmentTransport::Directory { root } => Box::new(FsStore::new(root)),
        };
        let source = ParquetSource::new(
            store,
            self.cache_dir.clone(),
            self.key_prefix.clone(),
            self.horizon_hours.hours(),
        )
        .with_cache_budget(self.cache_budget_bytes);
        if self.admit_corpus_forms {
            source.admitting_corpus_forms()
        } else {
            source
        }
    }
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), Error> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| Error::Derivation(format!("list {}: {e}", dir.display())))?;
    for entry in entries {
        let path = entry
            .map_err(|e| Error::Derivation(format!("list {}: {e}", dir.display())))?
            .path();
        if path.is_dir() {
            walk(&path, out)?;
        } else if path.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

/// The lifecycle bank's reader: for a window, the lifecycle rows the input log holds as
/// the tap's own JSON, and the segment keys it holds them for. The daemon supplies it from
/// its input log; tests and one-shot tools leave it out and walk.
pub(crate) type LifecycleBank = Box<
    dyn Fn(f64, f64) -> Result<(Vec<String>, std::collections::HashSet<String>), Error>
        + Send
        + Sync,
>;

/// The FETCH scope for lifecycle reads: `(lo_ms, hi_ms)` in, the set of segment keys the
/// input log says can hold rows in that span out — `Ok(None)` when the log cannot answer
/// (no keyed committed batches), which the fetch announces and survives by walking the
/// listing. The provider is attached by the caller that owns the input log (the daemon)
/// — the SCOPE is a capability, not a dependency.
pub(crate) type LifecycleScope =
    Box<dyn Fn(f64, f64) -> Result<Option<std::collections::HashSet<String>>, Error> + Send + Sync>;

/// The `EventSource` over sealed segments: dated discovery, the fetch-once cache, the
/// projected decode, and the IR's own ordering.
pub struct ParquetSource<S> {
    store: S,
    /// See [`LifecycleScope`]. `None` — tests, corpus readers, one-shot tools — fetches
    /// whatever discovery lists, which is the earlier behavior at scales where
    /// it is affordable. The eighth unscoped-read family member lived here: the
    /// degradation ledger's lifecycle fetch listed the retention window and fetched
    /// ALL of it (65.5 MiB/s sustained, 20+ minutes, on an idle box) because the
    /// window was scoped and the fetch was not.
    lifecycle_scope: Option<LifecycleScope>,
    /// See [`LifecycleBank`]: the banked lifecycle rows for a window and the
    /// segments the bank holds, so the walk covers only what the bank never saw.
    lifecycle_bank: Option<LifecycleBank>,
    cache_dir: PathBuf,
    /// The byte cache's budget: the daemon's knob, the default a
    /// labeled analytic.
    cache_budget_bytes: u64,
    /// The fleet's key prefix inside the store (`tap/` in a shared bucket, empty for a
    /// dedicated one). Dated hour segments render directly under it.
    key_prefix: String,
    /// The retention horizon in hours: how far back `segment_opens_at_retention_horizon`
    /// reaches, and (plus the listing margin) the age past which cached segments evict.
    /// The examined history plus margin in the deployment; a caller states it because
    /// retention is the caller's policy, read once at construction — liveness is
    /// [`SourceConfig`]'s job.
    horizon_hours: i64,
    /// The corpus seam — see [`SourceConfig::admit_corpus_forms`].
    corpus_forms: bool,
    /// Objects skipped as undecodable under quarantine-and-declare.
    /// A counter, never a record kind: the structural pricing is the skipped segment's absent
    /// `segment_seq`, and this is the operator's alarm surface beside the per-object log line.
    quarantined: AtomicU64,
}

/// The columns the cache-fact IR reads — the projection that makes the columnar decode skip
/// the rest. Everything else `Event` carries is stamped from the header (incarnation,
/// `key_epoch`, `workload_class`) or is a function of the bytes (`delivery_id`).
const CACHE_PROJECTION: &[&str] = &[
    "kind",
    "instance_id",
    "dp_rank",
    "group_idx",
    "block_id",
    "content_id",
    "tier",
    "backend_id",
    "at_ms",
    "locality",
    "n_tokens",
    "parent_id",
    "parent_unknown",
    "seq",
    "reused",
    // holder_reset's engine-clock ordering instant (spec 2.3): the loader fails closed on a
    // reset without it, so a projection that skipped it would refuse every reset segment.
    "boundary_ms",
];
const CACHE_KINDS: &[&str] = &["store", "evict", "clear", "holder_reset"];

/// A fill's decode job: the segment's sequence in the keys' order, its key, its batch id and
/// its bytes.
type Job = (u64, String, String, Vec<u8>);
/// A decoded segment: the job's sequence and key, its batch id, its cache events inside the
/// window, and the decode's error if it had one.
type Done = (u64, String, String, Vec<Event>, Option<String>);

/// One segment's bytes to its cache events inside `window`, on a worker: the fill's decode.
/// Segments fetched ahead of the feed, in key order: fetchers take the
/// next key from a shared counter and send its bytes under its index; the feed takes
/// them in order through a small reorder map. A fetcher without a client of its own
/// shares the source's, which then takes turns — correct, and no faster.
struct FetchAhead {
    rx: std::sync::mpsc::Receiver<(usize, Result<Vec<u8>, Error>)>,
    pending: std::collections::BTreeMap<usize, Result<Vec<u8>, Error>>,
    next: usize,
}

/// Segments in flight ahead of the feed.
const FETCHERS: usize = 8;

impl FetchAhead {
    fn start<'scope, 'env: 'scope, S: SegmentStore>(
        scope: &'scope std::thread::Scope<'scope, 'env>,
        source: &'env ParquetSource<S>,
        keys: &'env [&'env String],
    ) -> Self {
        let (tx, rx) = std::sync::mpsc::sync_channel(FETCHERS * 2);
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..FETCHERS.min(keys.len()).max(1) {
            let tx = tx.clone();
            let counter = std::sync::Arc::clone(&counter);
            scope.spawn(move || {
                let own = source.store.sibling();
                loop {
                    let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(key) = keys.get(i) else { break };
                    let got = match &own {
                        Some(store) => source.cached_bytes_with(store.as_ref(), key),
                        None => source.cached_bytes(key),
                    };
                    if tx.send((i, got)).is_err() {
                        break;
                    }
                }
            });
        }
        Self {
            rx,
            pending: std::collections::BTreeMap::new(),
            next: 0,
        }
    }

    /// The next key's bytes, in key order.
    fn next(&mut self) -> Result<Vec<u8>, Error> {
        loop {
            if let Some(got) = self.pending.remove(&self.next) {
                self.next += 1;
                return got;
            }
            let (i, got) = self
                .rx
                .recv()
                .map_err(|_| Error::Derivation("parquet_source: a fetcher is gone".to_string()))?;
            self.pending.insert(i, got);
        }
    }
}

fn decode_cache_segment(
    (seq, key, batch_id, bytes): Job,
    fleet_id: &str,
    selection: crate::source::InputSelection,
) -> Done {
    let mut events: Vec<Event> = Vec::new();
    let mut decode_err: Option<Error> = None;
    let delivery = Some(batch_id.clone()).filter(|d| !d.is_empty());
    let streamed = analyzer_segments::columnar::decode_segment_projected_into(
        &key,
        fleet_id,
        bytes,
        Projection {
            columns: CACHE_PROJECTION,
            kinds: CACHE_KINDS,
            window: selection.bounds(),
        },
        &mut |rows| {
            if decode_err.is_some() {
                return Ok(());
            }
            for row in &rows {
                match cache_event_of(row, delivery.clone()) {
                    Ok(Some(ev)) => {
                        if selection.contains(ev.at_ms) {
                            events.push(ev);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        decode_err = Some(e);
                        break;
                    }
                }
            }
            Ok(())
        },
    );
    let err = match (streamed, decode_err) {
        (Err(e), _) => Some(e.to_string()),
        (Ok(_), Some(e)) => Some(e.to_string()),
        (Ok(_), None) => None,
    };
    (seq, key, batch_id, events, err)
}

/// The lifecycle projection: every field `LifecycleEvent` carries, and nothing else.
/// `attributed` and `dropped_bytes` are `segment_recovered`'s two declared facts (spec 2.5) —
/// typed columns in the shipped schema; the shaping routes them through `extra` either way,
/// and the shared row reader accepts both spellings. `extra` is projected because
/// `agent_version` rides it — the lifecycle families are one record per segment, heartbeat or
/// boundary, so the map costs nothing at their grain, and projecting it in BOTH lifecycle
/// slices keeps the field uniform across every load path rather than present in one.
const LIFECYCLE_PROJECTION: &[&str] = &[
    "kind",
    "at_ms",
    "instance_id",
    "dropped",
    "oversized",
    "canary",
    "reuse_reporting",
    "endpoints",
    "segment_seq",
    "first_seq",
    "last_seq",
    "refused",
    "window_start_ms",
    "window_end_ms",
    "dp_rank",
    "group_idx",
    "attributed",
    "dropped_bytes",
    "extra",
];

/// The declaration slice's projection: the run-constant declarations and the identity that
/// scopes them, nothing else — what lets `lifecycle_declarations` read the horizon without
/// decoding the whole lifecycle column set for one declaration per run.
const DECLARATION_PROJECTION: &[&str] = &[
    "kind",
    "at_ms",
    "instance_id",
    "incarnation",
    "reuse_reporting",
    "canary",
    // `agent_version` rides the `extra` map (an unrecognised wire field), and it is a
    // declaration in this slice's sense: one run-constant provenance fact per producer run.
    // The `--provenance` keys (`model`, `pod_identity`) ride the same map on the segment
    // header, which is why `segment_open` joins the kinds below.
    "extra",
    // The per-endpoint entries ARE the per-instance lifecycle presence (the roster's
    // rule): the roster fans a liveness record out over its endpoint sources, and the
    // topic is the relayed channel name a recognizer may read a model from. The projection
    // names the record-model FIELD — the decoder resolves the nested element columns
    // itself, and a dotted leaf name here matches nothing and is silently skipped, which
    // is exactly how E2's real captures reached the roster with no endpoints at all.
    "endpoints",
];
const DECLARATION_KINDS: &[&str] = &["agent_start", "heartbeat", "agent_stop", "segment_open"];

/// The lifecycle kinds the seam's lifecycle load admits. ONE list, shared with the test twin
/// (`analyzer_harness::bucket_source`) so the two sources cannot drift on it.
pub use analyzer_segments::LIFECYCLE_KINDS;

/// The corpus forms the test seam admits beside the shipped suffix: the write-ahead form and
/// hand-assembled archives. Never consulted unless [`ParquetSource::admitting_corpus_forms`]
/// was called — the production candidate rule is `dated::is_shipped_key`, ingest's own.
const CORPUS_SUFFIXES: &[&str] = &[".jsonl", ".ndjson"];

const HOUR_MS: f64 = 3_600_000.0;

/// One segment's fold step: its shaped rows beside its delivery identity. Named so the fold
/// parameter reads as what it is rather than as a type puzzle.
type SegmentFold<'a> = dyn FnMut(&[Value], &str, &str) -> Result<(), Error> + 'a;

/// Distinguishes concurrent fetch temporaries within one process; the process id
/// distinguishes across processes sharing a cache directory.
static FETCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// The ONE derivation of the content cache's location under a data directory. The tick's
/// source and the read path's source must open the SAME directory — fetch-once is a property
/// of the directory — so the location is derived once rather than assembled at each site.
#[must_use]
pub fn segment_cache_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("segment-cache")
}

/// The byte cache's SIZE bound, beside the age bound that already existed — the rig
/// one is not the other: age eviction keys to `horizon + margin`, so under a 30-day horizon
/// nothing becomes evictable for a month and an unbudgeted cache fills the volume it shares
/// with the store until the disk ladder refuses intake — the ladder governing the volume while
/// the cache on the same volume answers to no ladder at all. Age remains
/// the CORRECTNESS bound (never evict what discovery still lists... cheaply); this is
/// the CAPACITY bound, LRU past the budget, and a cache loss is a re-fetch, never an
/// answer. 8 GiB holds the incremental fill's working set — recent, unledgered
/// segments — with an order of magnitude to spare; the number is a labeled analytic,
/// re-derived when a deployment's segment rate demands it.
pub const DEFAULT_SEGMENT_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Hours of discovery beyond a window's own: the skew between a segment's ship hour and its
/// records' clock, which a shipper may run behind by.
const LISTING_MARGIN_HOURS: i64 = 48;

/// The horizon that reaches a stamp from now, plus the listing margin: how far back a source
/// over segments with fixed stamps must list, however old an install finds them.
#[must_use]
#[allow(clippy::cast_possible_truncation)] // an age in hours fits i64
pub fn horizon_reaching(at_ms: f64) -> i64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    (((now_ms - at_ms) / analyzer_analysis::HOUR_MS).ceil() as i64).max(0) + LISTING_MARGIN_HOURS
}

impl<S: SegmentStore> ParquetSource<S> {
    /// Attach the lifecycle bank's reader. Builder-shaped, like
    /// [`Self::admitting_corpus_forms`].
    #[must_use]
    pub fn with_lifecycle_bank(mut self, bank: LifecycleBank) -> Self {
        self.lifecycle_bank = Some(bank);
        self
    }

    /// Attach the lifecycle fetch scope — see `LifecycleScope`. Builder-shaped, like
    /// [`Self::admitting_corpus_forms`].
    #[must_use]
    pub fn with_lifecycle_scope(mut self, scope: LifecycleScope) -> Self {
        self.lifecycle_scope = Some(scope);
        self
    }

    /// The exclusion a lifecycle fetch runs under: the provider's include set when it
    /// can answer, announced either way — the family's loudness rule, applied at the
    /// fetch instead of trusted to the caller.
    fn lifecycle_fetch_exclusion(
        &self,
        lo_ms: f64,
        hi_ms: f64,
    ) -> Result<crate::source::FillExclusion, Error> {
        let Some(provider) = &self.lifecycle_scope else {
            return Ok(crate::source::FillExclusion::default());
        };
        if let Some(keys) = provider(lo_ms, hi_ms)? {
            log::info!(
                "lifecycle fetch: scoped to {} ledgered segment \
                 key(s) for [{lo_ms:.0}..{hi_ms:.0}]",
                keys.len()
            );
            Ok(crate::source::FillExclusion {
                only_keys: Some(keys),
                ..Default::default()
            })
        } else {
            log::info!(
                "lifecycle fetch: window scoping unavailable (no \
                 keyed committed batches) — walking the listing"
            );
            Ok(crate::source::FillExclusion::default())
        }
    }

    /// A source over `store` under `key_prefix`, caching under `cache_dir` at the default
    /// budget, the horizon fixed at `horizon_hours`; no lifecycle scope or bank and no corpus
    /// forms until the builders attach them.
    #[must_use]
    pub fn new(
        store: S,
        cache_dir: impl Into<PathBuf>,
        key_prefix: impl Into<String>,
        horizon_hours: i64,
    ) -> Self {
        Self {
            store,
            lifecycle_scope: None,
            lifecycle_bank: None,
            cache_dir: cache_dir.into(),
            cache_budget_bytes: DEFAULT_SEGMENT_CACHE_BYTES,
            key_prefix: key_prefix.into(),
            horizon_hours,
            corpus_forms: false,
            quarantined: AtomicU64::new(0),
        }
    }

    /// The corpus seam — see [`SourceConfig::admit_corpus_forms`]. Consumes and returns, so a
    /// production construction cannot flip it after the fact by accident.
    #[must_use]
    pub fn admitting_corpus_forms(mut self) -> Self {
        self.corpus_forms = true;
        self
    }

    /// The byte cache's budget, the daemon's knob (`--segment-cache-bytes`).
    #[must_use]
    pub fn with_cache_budget(mut self, bytes: u64) -> Self {
        self.cache_budget_bytes = bytes;
        self
    }

    /// Objects this source has skipped as undecodable (quarantine-and-declare). Zero on a
    /// healthy source; each increment was logged with its key.
    #[must_use]
    pub fn quarantined(&self) -> u64 {
        self.quarantined.load(Ordering::Relaxed)
    }

    /// The candidate rule: ingest's shipped-suffix rule, plus the corpus forms where the test
    /// seam admits them.
    fn is_candidate(&self, key: &str) -> bool {
        dated::is_shipped_key(key)
            || (self.corpus_forms && CORPUS_SUFFIXES.iter().any(|s| key.ends_with(s)))
    }

    /// Skip one undecodable object, loudly and counted. The pricing is structural and free:
    /// the skipped segment's records never enter the IR, so its `segment_seq` never appears
    /// and the existing sequence-gap machinery degrades the window. Where not even the header
    /// was readable there is no incarnation to scope a claim to, and this alarm plus the
    /// counter is the WHOLE surface — a span claim for an unlocatable span would invent a
    /// fact.
    fn quarantine(&self, key: &str, error: &str) {
        self.quarantined.fetch_add(1, Ordering::Relaxed);
        log::warn!(
            "QUARANTINED segment object {key} — it does not decode and is \
             skipped; its absent segment_seq prices the loss through the sequence-gap ledger \
             where the stream around it survives ({error})"
        );
    }

    /// Every retained key: the whole retention horizon's dated hour prefixes, listed per
    /// prefix, no clip from the caller's window — plus the undated keys.
    ///
    /// This is the ONLY sound discovery for a load whose bounds are in the ENGINE clock domain
    /// (`cache_events` — its window is over the cache records' own stamps) or in no single
    /// domain. A key's hour is its SHIP hour — the producer's wall clock at upload — and spec
    /// Spec 2.6 permits unbounded displacement between the engine and producer clocks: the
    /// vendored `clock_the_domains_never_meet` fixture ships a stream displaced thirty days.
    /// No wall-clock margin maps such a window to ship hours, so clipping prefixes by one
    /// silently loses a legally displaced segment. Narrowing happens where the stamps are the
    /// records' own instead: the decode prunes row groups by their `at_ms` footer statistics
    /// against the window, and the folds apply the exact half-open edge per event.
    ///
    /// **The cost is the price, paid knowingly** (the flip's own posture):
    /// ~336 hour-prefix LISTs per load at a 14-day horizon, most of them empty.
    /// LIST-level pruning for an engine-domain question returns with the manifest sitting
    /// (endgame), and only once the manifest carries engine-domain extents — its
    /// footer-sourced `at_ms` time bounds are the producer clock.
    fn discover_horizon(&self) -> Result<Vec<String>, Error> {
        let now = wall_hour()?;
        #[allow(clippy::cast_precision_loss)] // epoch hours are far below 2^53
        let hour_ms = |h: i64| h as f64 * HOUR_MS;
        // Displacement is symmetric: a producer whose wall clock runs AHEAD of this reader's
        // ships into hour prefixes this reader has not reached yet, and a one-hour ceiling
        // would lose them the same way the old wall margin lost a displaced past. The horizon
        // bounds both sides — a producer displaced further ahead than retention is as
        // invisible as data older than retention, and the bound is stated rather than guessed.
        self.list_hours(
            now,
            hour_ms(now - self.horizon_hours),
            hour_ms(now + self.horizon_hours),
        )
    }

    /// The keys a load bounded in the producer's WALL clock must consider: the range's dated
    /// hour prefixes, widened by the shipper margin on each side. The margin — the shipper's
    /// upload deadline plus skew allowance — brackets ship hours COMPLETELY here, because the
    /// bounds and the key hours are the same clock; that soundness is a property of the
    /// domain, which is why the domain is in this signature rather than guessed per caller.
    /// `lifecycle_events` qualifies by the seam's own contract: its range is an
    /// already-clipped wall range over records that carry the producer clock (spec 2.6).
    fn discover_producer_wall(&self, lo_ms: f64, hi_ms: f64) -> Result<Vec<String>, Error> {
        let now = wall_hour()?;
        #[allow(clippy::cast_precision_loss)] // a margin in hours is far below 2^53 ms
        let margin = dated::LIST_MARGIN_HOURS as f64 * HOUR_MS;
        // The horizon clamps the range: segments older than retention do not exist, so a load
        // stated wider than retention lists exactly retention — which is what lets a
        // retention-scoped caller pass an unbounded range without enumerating 10^300 hours.
        #[allow(clippy::cast_precision_loss)] // epoch hours are far below 2^53
        let now_ms = now as f64 * HOUR_MS;
        #[allow(clippy::cast_precision_loss)] // horizons in hours are far below 2^53 ms
        let horizon_ms = self.horizon_hours as f64 * HOUR_MS;
        let (lo_ms, hi_ms) = (lo_ms.max(now_ms - horizon_ms), hi_ms.min(now_ms + HOUR_MS));
        self.list_hours(now, lo_ms - margin, hi_ms + margin)
    }

    /// One LIST per hour prefix in `[lo_ms, hi_ms]`, plus ONE list of the whole fleet prefix
    /// kept to its UNDATED keys, filtered to the candidate rule — the shared tail of both
    /// discovery paths.
    ///
    /// The undated list exists because ingest deliberately accepts operator archive drops
    /// (keys with no dated segment; their age is unknowable from the name), and a source that
    /// enumerated only dated hours would derive from less than the store ingested. It is one
    /// extra LIST per load, filtered client-side by `dated::key_hour` — the same machinery
    /// ingest's retention gate reads — and undated keys join EVERY load, because with no hour
    /// there is nothing to clip by; the decode's footer pruning and the per-event edge still
    /// narrow them.
    ///
    /// `now_hour` is the one clock reading this whole pass runs on: eviction below and the
    /// listing bounds above derive from the same instant, so an hour tick between them cannot
    /// evict a key the listing is about to return.
    fn list_hours(&self, now_hour: i64, lo_ms: f64, hi_ms: f64) -> Result<Vec<String>, Error> {
        self.evict_aged(now_hour);
        let mut keys = Vec::new();
        for hour in dated::hour_prefixes(lo_ms, hi_ms) {
            keys.extend(self.store.list(&format!("{}{hour}/", self.key_prefix))?);
        }
        keys.extend(
            self.store
                .list(&self.key_prefix)?
                .into_iter()
                .filter(|k| dated::key_hour(k).is_none()),
        );
        keys.retain(|k| self.is_candidate(k));
        // Sorted, so the set is deterministic before the IR's own sort runs.
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// The bytes for `key`, through the cache: a hit never touches the store, a miss fetches
    /// ONCE and lands the bytes before returning them. Immutable objects make this safe and
    /// I7 makes it obligatory — the cache holds bytes, not derivations.
    ///
    /// **The cache is the DIRECTORY, and several sources share one.** The tick and the read
    /// path each hold a `ParquetSource` over the same `cache_dir`, so a segment either of
    /// them lands is a hit for the other — fetch-once is a property of the directory, not of
    /// an instance. That sharing is what the per-fetch temporary name below exists for: two
    /// concurrent misses on one key must not interleave on one temporary, and the rename
    /// commits whole bytes whichever fetch wins.
    ///
    /// A cache read that FAILS — deleted between check and read, unreadable, half-lost — is a
    /// miss, never an error: the cache is disposable by definition (I7), so its failure mode
    /// is a re-fetch and only the store itself can fail a load.
    /// Keep the byte cache inside its budget: oldest-touched first, down to seven
    /// eighths, best-effort throughout (a cache loss is a re-fetch, never an answer).
    /// Called after each insert; the walk's cost is bounded by the budget it enforces.
    fn enforce_cache_budget(&self) {
        let mut files = Vec::new();
        if walk(&self.cache_dir, &mut files).is_err() {
            return;
        }
        let mut sized: Vec<(std::time::SystemTime, u64, std::path::PathBuf)> = files
            .into_iter()
            .filter_map(|p| {
                let meta = std::fs::metadata(&p).ok()?;
                Some((
                    meta.modified().unwrap_or(std::time::UNIX_EPOCH),
                    meta.len(),
                    p,
                ))
            })
            .collect();
        let mut total: u64 = sized.iter().map(|(_, len, _)| len).sum();
        if total <= self.cache_budget_bytes {
            return;
        }
        sized.sort_by_key(|(mtime, _, _)| *mtime);
        let floor = self.cache_budget_bytes - self.cache_budget_bytes / 8;
        let over = total;
        let mut evicted_files = 0u64;
        let mut evicted_bytes = 0u64;
        let mut failed = 0u64;
        for (_, len, path) in sized {
            if total <= floor {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                total = total.saturating_sub(len);
                evicted_files += 1;
                evicted_bytes += len;
            } else {
                failed += 1;
            }
        }
        // Says what it did, every time: the enforcer's absence and its failure would otherwise
        // look identical from outside.
        log::info!(
            "segment cache evicted {evicted_files} file(s) / \
             {evicted_bytes} bytes ({failed} undeletable): {over} -> {total} against \
             the {}-byte budget",
            self.cache_budget_bytes
        );
    }

    fn cached_bytes(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.cached_bytes_with(&self.store, key)
    }

    /// [`Self::cached_bytes`] through a given client — a fetcher thread's own sibling;
    /// the cache dir is shared, and a key's file is written under a
    /// temporary name and renamed into place, so two fetchers of one key both land it.
    fn cached_bytes_with(&self, store: &dyn SegmentStore, key: &str) -> Result<Vec<u8>, Error> {
        if key
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == "..")
        {
            return Err(Error::Derivation(format!(
                "object key {key} does not name a cache path"
            )));
        }
        let path = self.cache_dir.join(key);
        if let Ok(bytes) = std::fs::read(&path) {
            // The LRU's clock: a hit refreshes mtime, best-effort, so the budget
            // eviction below removes the least-recently-USED segment, not merely the
            // least-recently-fetched one.
            if let Ok(f) = std::fs::File::options().write(true).open(&path) {
                let now = std::fs::FileTimes::new().set_modified(std::time::SystemTime::now());
                let _ = f.set_times(now);
            }
            return Ok(bytes);
        }
        let bytes = store.fetch(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Derivation(format!("cache dir {key}: {e}")))?;
        }
        // Write-then-rename, so a crash mid-write leaves no half segment answering to a key.
        let fetch = FETCH_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = path.with_extension(format!("fetch-tmp-{}-{fetch}", std::process::id()));
        std::fs::write(&tmp, &bytes)
            .map_err(|e| Error::Derivation(format!("cache write {key}: {e}")))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| Error::Derivation(format!("cache commit {key}: {e}")))?;
        self.enforce_cache_budget();
        Ok(bytes)
    }

    /// Drop cached segments older than every listing's reach: the horizon PLUS the listing
    /// margin, strictly — a wall-domain discovery lists `horizon + margin` back, so evicting
    /// at the bare horizon deleted keys the next load would list and re-fetch, and fetch-once
    /// broke exactly at the edge. Age is the key's own dated hour where it has one, the
    /// file's modification time where it does not; a cache loss is a re-fetch, so the failure
    /// mode of evicting wrongly is cost, never an answer. `now_hour` is the caller's pass
    /// clock, shared with the listing bounds.
    fn evict_aged(&self, now_hour: i64) {
        let floor_hour = now_hour - self.horizon_hours - dated::LIST_MARGIN_HOURS;
        let mut files = Vec::new();
        if walk(&self.cache_dir, &mut files).is_err() {
            return; // no cache directory yet: nothing to evict
        }
        for path in files {
            let key = path
                .strip_prefix(&self.cache_dir)
                .map(|rel| {
                    rel.components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/")
                })
                .unwrap_or_default();
            let hour = dated::key_hour(&key).or_else(|| mtime_hour(&path));
            if hour.is_some_and(|h| h < floor_hour) {
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    /// The cache facts of `keys` clipped to `w` — the fold behind both cache reads.
    fn fold_cache_window(
        &self,
        fleet_id: &str,
        keys: &[String],
        w: &Window,
        exclude: &crate::source::FillExclusion,
    ) -> Result<std::sync::Arc<Vec<Event>>, Error> {
        let mut events = Vec::new();
        self.fold_segments(
            fleet_id,
            keys,
            Projection {
                columns: CACHE_PROJECTION,
                kinds: CACHE_KINDS,
                window: Some((w.start_ms, w.end_ms)),
            },
            exclude,
            &mut |rows, delivery, _| {
                // Built whole before extending, so a row that refuses drops the SEGMENT's
                // events rather than half of them.
                let mut segment = Vec::new();
                for row in rows {
                    let Some(ev) =
                        cache_event_of(row, Some(delivery.to_string()).filter(|d| !d.is_empty()))?
                    else {
                        continue;
                    };
                    if ev.at_ms < w.start_ms || ev.at_ms >= w.end_ms {
                        continue;
                    }
                    segment.push(ev);
                }
                events.extend(segment);
                Ok(())
            },
        )?;
        analyzer_analysis::events::sort(&mut events);
        Ok(std::sync::Arc::new(events))
    }

    /// The fill's pipeline: this thread fetches in key order and feeds the workers, the
    /// workers decode, and this thread drains the decoded segments into `sink` in the
    /// keys' order, the next chunk's fetch overlapping the current chunk's decode.
    ///
    /// The second of the source's two drivers. [`Self::fold_segments`] streams one segment
    /// at a time in bounded batches, for the loads whose rows are shaped and folded as they
    /// pass; this one decodes whole segments to events on workers, for the fill, whose unit
    /// is the segment. They share the admission rule ([`admitted`]) and the quarantine.
    fn fill_in_parallel(
        &self,
        fleet_id: &str,
        keys: &[String],
        selection: crate::source::InputSelection,
        exclude: &crate::source::FillExclusion,
        sink: &mut crate::source::SegmentSink<'_>,
    ) -> Result<Vec<(String, String)>, Error> {
        let workers = std::thread::available_parallelism().map_or(2, |n| n.get().min(4));
        let chunk = workers * 2;
        let mut folded: HashSet<String> = exclude.batch_ids.clone();
        let mut folded_pairs: Vec<(String, String)> = Vec::new();
        let todo: Vec<&String> = admitted(keys, exclude).collect();
        let (job_tx, job_rx) = std::sync::mpsc::sync_channel::<Job>(chunk * 2);
        let (out_tx, out_rx) = std::sync::mpsc::sync_channel::<Done>(chunk * 2);
        let job_rx = std::sync::Mutex::new(job_rx);
        let mut sink_err: Option<Error> = None;
        std::thread::scope(|scope| -> Result<(), Error> {
            for _ in 0..workers {
                let out_tx = out_tx.clone();
                let job_rx = &job_rx;
                scope.spawn(move || loop {
                    let job = {
                        let rx = job_rx
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        rx.recv()
                    };
                    let Ok(job) = job else { break };
                    if out_tx
                        .send(decode_cache_segment(job, fleet_id, selection))
                        .is_err()
                    {
                        break;
                    }
                });
            }
            drop(out_tx);
            // Feed chunk i, then drain chunk i-1 in order: the reorder map holds a chunk.
            let mut pending: std::collections::BTreeMap<u64, Done> =
                std::collections::BTreeMap::new();
            let mut next: u64 = 0;
            let mut sent: u64 = 0;
            let mut drain_to = |upto: u64,
                                pending: &mut std::collections::BTreeMap<u64, Done>,
                                next: &mut u64|
             -> Result<(), Error> {
                while *next < upto {
                    let done = if let Some(done) = pending.remove(next) {
                        done
                    } else {
                        let done = out_rx.recv().map_err(|_| {
                            Error::Derivation("parquet_source: a decode worker is gone".to_string())
                        })?;
                        if done.0 != *next {
                            pending.insert(done.0, done);
                            continue;
                        }
                        done
                    };
                    let (_, key, batch_id, events, err) = done;
                    if let Some(e) = err {
                        self.quarantine(&key, &e);
                    } else {
                        if sink_err.is_none() {
                            if let Err(e) = sink(events, &key) {
                                sink_err = Some(e.into());
                            }
                        }
                        folded_pairs.push((batch_id, key));
                    }
                    *next += 1;
                }
                Ok(())
            };
            // The fetch ahead of the feed: a pool of fetchers, each on a client
            // of its own, keeps FETCHERS segments in flight while this thread feeds the
            // decoders in key order — one fetch at a time is a round trip per segment with the
            // box idle.
            let mut fetched = FetchAhead::start(scope, self, &todo);
            for keys_chunk in todo.chunks(chunk) {
                let chunk_start = sent;
                for key in keys_chunk {
                    let bytes = fetched.next()?;
                    let batch_id = analyzer_segments::content_batch_id(&bytes);
                    if !folded.insert(batch_id.clone()) {
                        continue;
                    }
                    job_tx
                        .send((sent, (*key).clone(), batch_id, bytes))
                        .map_err(|_| {
                            Error::Derivation("parquet_source: a decode worker is gone".to_string())
                        })?;
                    sent += 1;
                }
                // The previous chunk has had a chunk's worth of decode time; take it.
                drain_to(chunk_start, &mut pending, &mut next)?;
            }
            drop(job_tx);
            drain_to(sent, &mut pending, &mut next)
        })?;
        sink_err.map_or(Ok(folded_pairs), Err)
    }

    /// Decode every discovered segment under one projection and fold each segment's shaped
    /// rows — all-or-nothing PER SEGMENT, with three per-object outcomes:
    ///
    /// - transport failure fetching the bytes: a hard error (per-infrastructure, never
    ///   quarantined);
    /// - an object that does not decode, or whose rows do not read as their declared types:
    ///   quarantined — skipped, logged with its key, counted — and the fold sees none of its
    ///   rows (spec 5.5: ingestion is all-or-nothing per segment);
    /// - the same BYTES already folded under another key: skipped silently. The delivery
    ///   identity is the content hash, the same identity ingest's ledger dedups on — a
    ///   segment shipped again under a second dated key is one segment, and folding it twice
    ///   doubled every mass it carried.
    fn fold_segments(
        &self,
        fleet_id: &str,
        keys: &[String],
        projection: Projection<'_>,
        exclude: &crate::source::FillExclusion,
        fold: &mut SegmentFold,
    ) -> Result<Vec<(String, String)>, Error> {
        let mut folded: HashSet<String> = exclude.batch_ids.clone();
        let mut folded_pairs: Vec<(String, String)> = Vec::new();
        for key in admitted(keys, exclude) {
            let bytes = self.cached_bytes(key)?;
            // The identity BEFORE the decode: the dedup and the fold both key on it, and
            // computing it first is what lets the rows stream instead of materialize.
            let batch_id = analyzer_segments::content_batch_id(&bytes);
            if !folded.insert(batch_id.clone()) {
                continue;
            }
            // **The fold's unit is the BOUNDED BATCH, not the segment**.
            // Never materialize a whole segment: a shared-prefix segment's dictionary-compressed
            // repetition decodes to tens of GiB of owned rows, far past any cgroup ceiling.
            // Consumers all extend or push per call, so many calls per segment under one
            // batch_id fold identically — and a segment that refuses mid-stream may have
            // folded earlier batches, which stored data cannot reach: the ingest gate
            // decodes whole and refuses there first.
            let mut fold_err: Option<Error> = None;
            let streamed = analyzer_segments::columnar::decode_segment_projected_into(
                key,
                fleet_id,
                bytes,
                projection,
                &mut |rows| {
                    if fold_err.is_none() {
                        if let Err(e) = fold(&rows, &batch_id, key) {
                            fold_err = Some(e);
                        }
                    }
                    Ok(())
                },
            );
            match (streamed, fold_err) {
                (Err(e), _) => self.quarantine(key, &e.to_string()),
                (Ok(_), Some(e)) => self.quarantine(key, &e.to_string()),
                (Ok(_), None) => folded_pairs.push((batch_id, key.clone())),
            }
        }
        Ok(folded_pairs)
    }
}

/// The keys a load admits, decided BEFORE any fetch: inside the window's own segment set when
/// one is given, and not already in the ledger. A skipped key costs a lookup, never a GET,
/// which is also what keeps the byte cache's working set at recent segments instead of the
/// horizon.
fn admitted<'a>(
    keys: &'a [String],
    exclude: &'a crate::source::FillExclusion,
) -> impl Iterator<Item = &'a String> {
    keys.iter().filter(move |key| {
        exclude
            .only_keys
            .as_ref()
            .is_none_or(|only| only.contains(*key))
            && !exclude.object_keys.contains(*key)
    })
}

/// The wall clock's epoch hour — the ONE clock read every discovery and eviction shares, and
/// the ONE policy for a clock before the epoch: refuse the load with a stated error, exactly
/// as everything else here fails closed. A silent fallback would turn an impossible clock into an
/// unbounded enumeration or an unbounded cache.
///
/// # Errors
/// A wall clock reading before the Unix epoch — no retention horizon can be stated from it.
fn wall_hour() -> Result<i64, Error> {
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| {
            Error::Derivation(format!(
                "the wall clock reads before the epoch ({e}) — no retention horizon can be \
                 stated, refusing the load"
            ))
        })?
        .as_secs_f64()
        * 1000.0;
    #[allow(clippy::cast_possible_truncation)] // the wall clock's epoch hour fits i64
    Ok((ms / HOUR_MS).floor() as i64)
}

fn mtime_hour(path: &Path) -> Option<i64> {
    let ms = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs_f64()
        * 1000.0;
    #[allow(clippy::cast_possible_truncation)] // a file mtime's epoch hour fits i64
    Some((ms / HOUR_MS).floor() as i64)
}

impl<S: SegmentStore> EventSource for ParquetSource<S> {
    /// The window's cache facts. **The predicate is kind and a HALF-OPEN window and nothing
    /// else** — no semantic rule, per the seam's law. The window handed to the decode is a
    /// pruning hint over row-group statistics; the exact edge is applied here, per event.
    fn cache_events(
        &self,
        fleet_id: &str,
        w: &Window,
    ) -> Result<std::sync::Arc<Vec<Event>>, analyzer_analysis::Error> {
        w.validate()?;
        // The window is the ENGINE clock domain, so it may not clip the listing;
        // it prunes at the decode and at the per-event edge below.
        let keys = self.discover_horizon()?;
        Ok(self.fold_cache_window(fleet_id, &keys, w, &crate::source::FillExclusion::default())?)
    }
    fn lifecycle_events(
        &self,
        fleet_id: &str,
        lo_ms: f64,
        hi_ms: f64,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        Window::new(lo_ms, hi_ms)?;
        let keys = self.discover_producer_wall(lo_ms, hi_ms)?;
        // The eighth family member's fix: the window was scoped, the FETCH was not —
        // discovery listed retention-wide and this fold fetched everything it listed.
        let mut exclusion = self.lifecycle_fetch_exclusion(lo_ms, hi_ms)?;
        let fetch_started = std::time::Instant::now();
        let mut out = Vec::new();
        // The bank first: the rows the ingest banked for the window, as the
        // tap's own JSON, re-read through the same parser as a walked row; the walk then
        // covers only the segments the bank never saw, where a restart's first tick would
        // otherwise re-read every segment in the horizon.
        if let Some(bank) = &self.lifecycle_bank {
            let (rows, banked) = bank(lo_ms, hi_ms)?;
            for row in &rows {
                let row: Value = serde_json::from_str(row).map_err(|e| {
                    Error::Derivation(format!(
                        "lifecycle bank: a banked row does not re-parse ({e})"
                    ))
                })?;
                if let Some(ev) = lifecycle_event_of(&row)? {
                    if ev.at_ms >= lo_ms && ev.at_ms < hi_ms {
                        out.push(ev);
                    }
                }
            }
            let unbanked: std::collections::HashSet<String> = keys
                .iter()
                .filter(|k| !banked.contains(k.as_str()))
                .map(|k| (*k).clone())
                .collect();
            // What the walk actually reads: the unbanked keys still inside the ledgered
            // scope. A segment in the producer wall but outside the scope is intersected
            // away, so the line reports that count, not the raw unbanked total (which
            // over-reads as a problem — 2,208 "unbanked" that resolve to a zero walk).
            let to_walk: std::collections::HashSet<String> = match exclusion.only_keys.take() {
                Some(scoped) => scoped.intersection(&unbanked).cloned().collect(),
                None => unbanked,
            };
            log::info!(
                "lifecycle bank: {} record(s) from {} banked segment(s); \
                 walking {} unbanked in the window's ledgered scope",
                out.len(),
                banked.len(),
                to_walk.len()
            );
            if to_walk.is_empty() {
                sort_lifecycle(&mut out);
                return Ok(out);
            }
            exclusion.only_keys = Some(to_walk);
        }
        self.fold_segments(
            fleet_id,
            &keys,
            Projection {
                columns: LIFECYCLE_PROJECTION,
                kinds: LIFECYCLE_KINDS,
                window: Some((lo_ms, hi_ms)),
            },
            &exclusion,
            &mut |rows, _, _| {
                let mut segment = Vec::new();
                for row in rows {
                    let Some(ev) = lifecycle_event_of(row)? else {
                        continue;
                    };
                    if ev.at_ms < lo_ms || ev.at_ms >= hi_ms {
                        continue;
                    }
                    segment.push(ev);
                }
                out.extend(segment);
                Ok(())
            },
        )?;
        sort_lifecycle(&mut out);
        log::info!(
            "lifecycle fetch: {} record(s) in {:.1}s",
            out.len(),
            fetch_started.elapsed().as_secs_f64()
        );
        Ok(out)
    }

    /// Every `segment_open` still held: the horizon's whole dated range, no window — a
    /// declared drop's bracket may sit arbitrarily far from any analysis window.
    ///
    /// **The cost is per SEGMENT FETCHED, not per record retained**: a fetch and decode of every
    /// horizon segment. The input log answers the same question fetch-free
    /// (`arrivals::opens_of`); this walk is the fallback for a log with no arrivals, and it
    /// ANNOUNCES itself, entry and completion.
    fn segment_opens_at_retention_horizon(
        &self,
        fleet_id: &str,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        let keys = self.discover_horizon()?;
        log::info!(
            "segment opens: walking {} horizon segment(s) — the \
             fetch-free arrivals read was unavailable",
            keys.len()
        );
        let walk_started = std::time::Instant::now();
        let mut out: Vec<LifecycleEvent> = Vec::new();
        self.fold_segments(
            fleet_id,
            &keys,
            Projection {
                columns: LIFECYCLE_PROJECTION,
                kinds: &["segment_open"],
                window: None,
            },
            &crate::source::FillExclusion::default(),
            &mut |rows, _, _| {
                let mut segment = Vec::new();
                for row in rows {
                    let Some(ev) = lifecycle_event_of(row)? else {
                        continue;
                    };
                    if ev.segment_seq.is_some() {
                        segment.push(ev);
                    }
                }
                out.extend(segment);
                Ok(())
            },
        )?;
        // (incarnation, segment_seq) — the sequence the continuity fold reads.
        out.sort_by(|a, b| {
            a.incarnation
                .cmp(&b.incarnation)
                .then(a.segment_seq.cmp(&b.segment_seq))
        });
        log::info!(
            "segment opens: {} open(s) in {:.1}s",
            out.len(),
            walk_started.elapsed().as_secs_f64()
        );
        Ok(out)
    }

    /// The declaration slice, at declaration cost: the horizon's segments decoded under the
    /// declaration columns alone rather than the whole lifecycle projection — a cost lever,
    /// never a filter (the seam's contract on this method).
    fn lifecycle_declarations(
        &self,
        fleet_id: &str,
        w: &Window,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        // Windowed discovery + pruning: this walked the WHOLE horizon
        // (discover_horizon, window None) at the top of every pass, in front of every
        // scoped fix built behind it. The span-plus-margin is the roster's real need.
        let lo = w.start_ms - analyzer_analysis::completeness::EVIDENCE_CLIP_MARGIN_MS;
        let hi = w.end_ms + analyzer_analysis::completeness::EVIDENCE_CLIP_MARGIN_MS;
        let keys = self.discover_producer_wall(lo, hi)?;
        let exclusion = self.lifecycle_fetch_exclusion(lo, hi)?;
        let mut out = Vec::new();
        self.fold_segments(
            fleet_id,
            &keys,
            Projection {
                columns: DECLARATION_PROJECTION,
                kinds: DECLARATION_KINDS,
                window: Some((lo, hi)),
            },
            &exclusion,
            &mut |rows, _, _| {
                let mut segment = Vec::new();
                for row in rows {
                    if let Some(ev) = lifecycle_event_of(row)? {
                        segment.push(ev);
                    }
                }
                out.extend(segment);
                Ok(())
            },
        )?;
        sort_lifecycle(&mut out);
        Ok(out)
    }
}

impl<S: SegmentStore> crate::source::FillSource for ParquetSource<S> {
    /// The per-segment stream: same projection, same kinds, same per-event edge, same
    /// quarantine and content-hash dedup as the flat load — one decode lineage, two
    /// drivers. A SINK error is a hard error, never a quarantine: the fold's error path
    /// means "these bytes are bad", and an engine refusing an insert says nothing about
    /// the segment, so the first sink failure is stashed and surfaced after the walk
    /// rather than condemning a healthy object.
    fn stream_cache_events_into(
        &self,
        fleet_id: &str,
        selection: crate::source::InputSelection,
        exclude: &crate::source::FillExclusion,
        sink: &mut crate::source::SegmentSink<'_>,
    ) -> Result<Vec<(String, String)>, analyzer_analysis::Error> {
        selection.validate()?;
        // A scoped fill fetches the ledger's keys and lists nothing;
        // only the unscoped fill — the deep audit's whole span — walks the horizon.
        let keys = match &exclude.only_keys {
            Some(only) => {
                let mut keys: Vec<String> = only.iter().cloned().collect();
                keys.sort();
                keys
            }
            None => self.discover_horizon()?,
        };
        // The decode across workers: the fetch stays on this thread
        // and in key order — a segment's bytes arrive in a fifth of a second — and each
        // segment's decode to events, the second of every file, runs on a worker; the sink
        // takes the segments in the keys' order, so a fill is resumable exactly as before.
        // This thread both feeds and drains, in chunks with the next chunk's fetch
        // overlapping the current chunk's decode, because the sink and the store are this
        // thread's alone. A few workers, not the fold's fourteen: a decoded segment is tens
        // of megabytes of events, and two chunks are in flight.
        Ok(self.fill_in_parallel(fleet_id, &keys, selection, exclude, sink)?)
    }
}
