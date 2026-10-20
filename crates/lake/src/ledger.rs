// SPDX-License-Identifier: AGPL-3.0-only

//! The input log (`docs/lake.md`): what arrived, in the lake.
//!
//! One shard of a lineage's log is a [`Head`] holding a [`LedgerHead`] and, beside it, record
//! objects by UTC day. An ingest cycle takes one input position, writes one record object per
//! day it touched, then commits the head once (group commit). A record object is committed
//! when its position is at or below the head's `committed` and its writer's lease epoch is the
//! one that owns that position: a fenced writer's orphans carry a position a later holder may
//! commit, and the epoch tells them apart. A closed day compacts into one manifest.
//!
//! ```text
//! <root>ledger/shards/<shard>/head
//! <root>ledger/shards/<shard>/lease
//! <root>ledger/shards/<shard>/days/<day>/<position>-<epoch>.json
//! <root>ledger/shards/<shard>/days/<day>/manifest.json
//! ```

use crate::head::{Document, Head};
use crate::lineage::Lineage;
use crate::Error;
use analyzer_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The shard v1 runs: one writer per lineage's log. The format carries shards so that
/// splitting intake across writers later is not a format change.
pub const SHARD: u32 = 0;

/// The head's format. A head in another format is refused, never guessed at.
const FORMAT: u32 = 1;

/// A day's standing in the log: what cross-day questions need without listing the day.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DayIndex {
    /// The newest committed input position with arrivals on this day.
    pub newest_position: u64,
    /// The earliest cache-fact instant of any committed batch recorded under this day.
    pub first_at_ms: Option<f64>,
    /// The latest cache-fact instant of any committed batch recorded under this day.
    pub last_at_ms: Option<f64>,
    /// Committed batches with cache facts on this day: the day's arrivals.
    pub batches: u64,
    /// Committed batches recorded under this day: its arrivals, and every batch whose
    /// segments or lifecycle records fall on it.
    pub records: u64,
    /// Whether the day's records are folded into its manifest.
    pub compacted: bool,
    /// The newest committed input position with any record on this day: its arrivals, and
    /// the batches whose segments or lifecycle fall on it.
    pub last_position: u64,
}

/// The head of one shard of the input log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerHead {
    /// The head's format.
    pub format: u32,
    /// Which shard of the lineage's log this is.
    pub shard: u32,
    /// The highest committed input position; `0` before the first commit.
    pub committed: u64,
    /// The newest committed object key: discovery lists from it.
    pub key_watermark: Option<String>,
    /// Committed batches, over the log's life.
    pub batches: u64,
    /// The identities (`key@etag`) of the annotation objects the log holds, so the annotation
    /// walk fetches only what is new. Annotations are operator declarations: few.
    pub annotations: std::collections::BTreeSet<String>,
    /// When the newest committed batch was ingested, epoch milliseconds; `0` before any.
    pub last_ingested_at_ms: u64,
    /// Per UTC day in retention, its standing.
    pub days: BTreeMap<i64, DayIndex>,
    /// The first position each lease epoch committed: a record belongs to the epoch that
    /// owns its first position, or it is an orphan.
    pub epochs: BTreeMap<u64, u64>,
    /// The lease epoch of the last writer to commit.
    pub lease_epoch: u64,
}

impl Default for LedgerHead {
    fn default() -> Self {
        Self {
            format: FORMAT,
            shard: SHARD,
            committed: 0,
            key_watermark: None,
            batches: 0,
            annotations: std::collections::BTreeSet::new(),
            last_ingested_at_ms: 0,
            days: BTreeMap::new(),
            epochs: BTreeMap::new(),
            lease_epoch: 0,
        }
    }
}

impl Document for LedgerHead {
    const NAME: &'static str = "input log head";
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

impl LedgerHead {
    /// The epoch that owns `position`: the latest epoch whose first position is at or before
    /// it.
    #[must_use]
    pub fn owner_of(&self, position: u64) -> Option<u64> {
        self.epochs
            .iter()
            .filter(|(_, first)| **first <= position)
            .map(|(epoch, _)| *epoch)
            .max()
    }

    /// Days with arrivals committed after `position`.
    #[must_use]
    pub fn days_since(&self, position: u64) -> Vec<i64> {
        self.days
            .iter()
            .filter(|(_, index)| index.newest_position > position)
            .map(|(day, _)| *day)
            .collect()
    }

    /// Days with any record committed after `position`.
    #[must_use]
    pub fn days_recorded_since(&self, position: u64) -> Vec<i64> {
        self.days
            .iter()
            .filter(|(_, index)| index.last_position > position)
            .map(|(day, _)| *day)
            .collect()
    }
}

/// One sealed segment's arrival facts, as ingest reads them from the segment's own header.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentArrival {
    /// The tap run.
    pub incarnation: String,
    /// The segment's sequence within the run.
    pub segment_seq: i64,
    /// The instance the segment is for; `None` is the run-level row, which carries the
    /// envelope and keeps a declaration-only segment gradeable.
    pub instance: Option<String>,
    /// The first record's instant.
    pub first_record_at_ms: f64,
    /// The last record's instant.
    pub last_record_at_ms: f64,
    /// The declared heartbeat cadence.
    pub heartbeat_secs: u64,
    /// The declared longest segment.
    pub max_segment_secs: u64,
    /// Heartbeats the segment carries.
    pub hb_count: u64,
    /// The first heartbeat's instant.
    pub hb_first_at_ms: Option<f64>,
    /// The last heartbeat's instant.
    pub hb_last_at_ms: Option<f64>,
    /// The lifecycle stop, when the segment carries one.
    pub stop_at_ms: Option<f64>,
    /// Messages the publisher saw.
    pub msgs_seen: u64,
    /// Messages the publisher dropped.
    pub dropped: u64,
    /// Messages too large to publish.
    pub oversized: u64,
    /// `dropped` as the segment's first heartbeat stated it: the counter is cumulative over the
    /// run, so the movement from the previous segment's last heartbeat to this one falls in the
    /// gap between them, and the rest inside this segment.
    pub dropped_at_hb_first: u64,
    /// `oversized` as the segment's first heartbeat stated it.
    pub oversized_at_hb_first: u64,
    /// Publisher restarts.
    pub publisher_restarts: u64,
    /// The tap's version, where a liveness record stated one.
    pub agent_version: Option<String>,
    /// The model the topic names, where the topic was recognized.
    pub topic_model: Option<String>,
    /// The pod the topic names, where the topic was recognized.
    pub topic_pod: Option<String>,
    /// The model the segment declares, where it declares one.
    pub declared_model: Option<String>,
    /// The pod the segment declares, where it declares one.
    pub declared_pod: Option<String>,
    /// The engine's version, where the segment declares it.
    pub engine_version: Option<String>,
    /// Whether the segment was attributed to an instance.
    pub attributed: bool,
    /// The key-space canary the segment's run declared (spec 3.4), when it declared one.
    pub canary: Option<String>,
}

/// A gap a segment sequence declares.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeclaredDrop {
    /// The tap run.
    pub incarnation: String,
    /// The first missing sequence.
    pub first_seq: i64,
    /// The last missing sequence.
    pub last_seq: i64,
    /// When the gap was declared.
    pub at_ms: f64,
}

/// A lifecycle record, for the roster fold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LifecycleRecord {
    /// The segment it came from.
    pub segment_key: String,
    /// The record's kind.
    pub kind: String,
    /// Its instant.
    pub at_ms: f64,
    /// The record as the segment carried it.
    pub row: String,
}

/// One statement of a declared tag interval (spec 2.8): identity `identity` carried the opaque
/// label `tag` from `from_ms` until `until_ms` (open while absent), stated at `at_ms` under
/// `declaration_id`. Producer clock throughout.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeclaredTag {
    /// The declaration the statement states.
    pub declaration_id: String,
    /// The identity the label is on.
    pub identity: String,
    /// The opaque label.
    pub tag: String,
    /// When the identity began carrying it.
    pub from_ms: f64,
    /// When it stopped; `None` while open.
    pub until_ms: Option<f64>,
    /// When the declarer wrote the statement.
    pub at_ms: f64,
}

/// A deploy an operator declared: where on the trend it sits and who said so.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeclaredAnnotation {
    /// The instant the deploy is about, UTC epoch milliseconds.
    pub at_ms: f64,
    /// The deploy's label.
    pub label: String,
    /// The reader's lens the declaration names, if any.
    pub scope: Option<String>,
    /// Who declared it: `cli`, `api` or `pipeline`.
    pub source: String,
    /// When the declaration was made, epoch milliseconds, where stated.
    pub declared_at_ms: Option<f64>,
}

/// The stored-mass denominator's share of one batch: its cache facts of one kind on one
/// instance on one UTC day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredMass {
    /// The instance the facts were stored on; empty when none was named.
    pub instance: String,
    /// The cache-fact kind: `store`, `evict` or `clear`.
    pub kind: String,
    /// The UTC day.
    pub day: i64,
    /// Facts of the kind.
    pub records: u64,
    /// Their tokens.
    pub tokens: i64,
    /// Of the facts, the stores of the instance's OWN cache (`locality` absent or `LOCAL`):
    /// what makes the instance a holder for a cross-instance question (spec 5.7). Zero for
    /// the other kinds.
    pub own: u64,
    /// Of the own stores, those carrying a portable identity (`content_id`, spec 3.1).
    pub comparable: u64,
}

/// One ingested object and everything ingest learned from it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchRecord {
    /// The batch's identity (content-addressed).
    pub batch_id: String,
    /// What carried it: a segment, an annotation.
    pub carrier: String,
    /// Where it came from.
    pub source: String,
    /// The object's key in the bucket.
    pub object_key: String,
    /// The object's etag.
    pub etag: String,
    /// The input position the commit assigned; `0` until then.
    pub input_position: u64,
    /// When it was ingested, epoch milliseconds.
    pub ingested_at_ms: u64,
    /// Records the object carried.
    pub record_count: u64,
    /// Records of a kind this build does not know.
    pub unknown_kinds: u64,
    /// The earliest cache fact's instant, or `None` when it carries none.
    pub first_at_ms: Option<f64>,
    /// The latest cache fact's instant.
    pub last_at_ms: Option<f64>,
    /// The UTC days its cache facts fall on: its arrival days, which dirty-day accounting
    /// reads.
    pub days: Vec<i64>,
    /// Its segments' arrival facts.
    pub segments: Vec<SegmentArrival>,
    /// Gaps its segments declare.
    pub drops: Vec<DeclaredDrop>,
    /// Its lifecycle records.
    pub lifecycle: Vec<LifecycleRecord>,
    /// Its cache facts' stored mass, per instance, kind and day.
    pub stored: Vec<StoredMass>,
    /// The deploy it declares, for an annotation object.
    pub annotations: Vec<DeclaredAnnotation>,
    /// The tag intervals its declarer stated (spec 2.8), in the segment's order.
    pub tags: Vec<DeclaredTag>,
}

impl BatchRecord {
    /// The UTC days the batch is recorded under: every day its cache-fact span meets (its
    /// arrival days and any quiet day between them), every day its segments' records span,
    /// the days its declared drops and lifecycle records fall on, and, for a batch with none
    /// of those, its ingest day. Every committed batch is recorded somewhere, and a reader of
    /// a window finds every batch whose span meets it by reading the window's days alone.
    #[must_use]
    pub fn placement_days(&self) -> std::collections::BTreeSet<i64> {
        let mut days: std::collections::BTreeSet<i64> = self.days.iter().copied().collect();
        if let (Some(first), Some(last)) = (self.first_at_ms, self.last_at_ms) {
            days.extend(day_of(first)..=day_of(last));
        }
        for a in &self.segments {
            days.extend(day_of(a.first_record_at_ms)..=day_of(a.last_record_at_ms));
        }
        for d in &self.drops {
            days.insert(day_of(d.at_ms));
        }
        for l in self.lifecycle.iter().filter(|l| l.at_ms > 0.0) {
            days.insert(day_of(l.at_ms));
        }
        if days.is_empty() {
            #[allow(clippy::cast_precision_loss)] // epoch milliseconds are exact in f64
            days.insert(day_of(self.ingested_at_ms as f64));
        }
        days
    }
}

/// One UTC day in milliseconds.
const DAY_MS: f64 = 86_400_000.0;

/// The UTC day holding the instant `ms` (epoch milliseconds).
#[must_use]
#[allow(clippy::cast_possible_truncation)] // day indices are small integers
pub fn day_of(ms: f64) -> i64 {
    (ms / DAY_MS).floor() as i64
}

impl BatchRecord {
    /// Whether the batch's cache-fact span meets `[lo_ms, hi_ms)`. A batch without cache
    /// facts meets no window.
    #[must_use]
    pub fn meets(&self, lo_ms: f64, hi_ms: f64) -> bool {
        match (self.first_at_ms, self.last_at_ms) {
            (Some(first), Some(last)) => first < hi_ms && last >= lo_ms,
            _ => false,
        }
    }
}

/// One record object: the batches of one ingest cycle recorded under one day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct RecordObject {
    format: u32,
    shard: u32,
    day: i64,
    epoch: u64,
    position: u64,
    batches: Vec<BatchRecord>,
}

/// A compacted day: every committed batch touching it, and the highest position folded in.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Manifest {
    format: u32,
    shard: u32,
    day: i64,
    through_position: u64,
    batches: Vec<BatchRecord>,
}

/// One day of the log: every committed batch recorded under it (its arrivals, and the
/// batches whose segments or lifecycle fall on it), in position order (a cycle's batches
/// share one position, and order by batch id within it).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DayLedger {
    /// The UTC day.
    pub day: i64,
    /// Its committed batches, each once, in position order.
    pub batches: Vec<BatchRecord>,
}

impl DayLedger {
    /// The batches with cache facts on the day: its arrivals.
    pub fn arrivals(&self) -> impl Iterator<Item = &BatchRecord> {
        self.batches.iter().filter(|b| b.days.contains(&self.day))
    }

    /// The newest committed position with arrivals on the day.
    #[must_use]
    pub fn newest_position(&self) -> Option<u64> {
        self.arrivals().map(|b| b.input_position).max()
    }

    /// Arrivals committed after `position`.
    pub fn since(&self, position: u64) -> impl Iterator<Item = &BatchRecord> {
        self.arrivals().filter(move |b| b.input_position > position)
    }
}

/// One shard of a lineage's input log.
pub struct Ledger {
    head: Head<LedgerHead>,
    store: Box<dyn ObjectStore>,
    dir: String,
    shard: u32,
    open_days: OpenDays,
}

/// The days not yet compacted, as a reader last loaded them, shared by every [`Ledger`] a
/// [`crate::Lake`] hands out. An open day is one record object per ingest cycle, so reading
/// it whole on every question would fetch the day again each time; a reader instead fetches
/// only the objects it has not seen. A compacted day is one manifest and is not kept.
pub(crate) type OpenDays = std::sync::Arc<std::sync::Mutex<BTreeMap<(u32, i64), OpenDay>>>;

/// One open day as loaded: which objects are folded in, and the committed batches.
#[derive(Default)]
pub(crate) struct OpenDay {
    /// The head's record count for the day when it was loaded: unchanged means nothing
    /// was committed to the day since.
    records: u64,
    /// Record objects folded in: committed when read. An object skipped as uncommitted is
    /// not here, and is read again (its writer may commit its position later).
    folded: std::collections::HashSet<String>,
    batches: BTreeMap<(u64, String), BatchRecord>,
    ledger: std::sync::Arc<DayLedger>,
}

impl Ledger {
    /// The log's shard `shard` under a lineage's `root` in `store`, written by `lineage`'s
    /// instance.
    ///
    /// # Errors
    /// The store cannot open a sibling connection.
    pub fn at(
        store: &dyn ObjectStore,
        root: &str,
        shard: u32,
        lineage: Lineage,
    ) -> Result<Self, Error> {
        Self::sharing(store, root, shard, lineage, OpenDays::default())
    }

    pub(crate) fn sharing(
        store: &dyn ObjectStore,
        root: &str,
        shard: u32,
        lineage: Lineage,
        open_days: OpenDays,
    ) -> Result<Self, Error> {
        let dir = format!("{root}ledger/shards/{shard}/");
        Ok(Self {
            head: Head::at(store.sibling()?, &dir, lineage),
            store: store.sibling()?,
            dir,
            shard,
            open_days,
        })
    }

    /// The head as the store holds it now.
    ///
    /// # Errors
    /// The store failed, or the head does not decode.
    pub fn head(&self) -> Result<LedgerHead, Error> {
        self.head.read()
    }

    fn day_dir(&self, day: i64) -> String {
        format!("{}days/{day}/", self.dir)
    }

    /// The position the next cycle commits under: one above the committed position. A cycle
    /// reserves it before it writes anything, so everything the cycle states carries it.
    ///
    /// # Errors
    /// The store failed, or the head does not decode.
    pub fn next_position(&self) -> Result<u64, Error> {
        Ok(self.head.read()?.committed + 1)
    }

    /// Commit one ingest cycle at `position` (from [`Ledger::next_position`]): every batch
    /// carries it, one record object is written per UTC day the batches touch, then the head
    /// is committed once. Returns the committed head. A cycle with no batches writes nothing.
    ///
    /// # Errors
    /// Another instance holds the lease, this writer was superseded, `position` is not above
    /// the committed position, or the store failed. A failure after records were written
    /// leaves orphans, which readers skip.
    pub fn commit_cycle(
        &self,
        position: u64,
        mut batches: Vec<BatchRecord>,
    ) -> Result<LedgerHead, Error> {
        let epoch = self.head.hold_lease()?;
        let before = self.head.read()?;
        if batches.is_empty() {
            return Ok(before);
        }
        if position <= before.committed {
            return Err(Error::Invalid(format!(
                "input log: position {position} is not above the committed {}",
                before.committed
            )));
        }
        for batch in &mut batches {
            batch.input_position = position;
        }
        let mut by_day: BTreeMap<i64, Vec<BatchRecord>> = BTreeMap::new();
        for batch in &batches {
            for day in batch.placement_days() {
                by_day.entry(day).or_default().push(batch.clone());
            }
        }
        for (day, day_batches) in &by_day {
            let object = RecordObject {
                format: FORMAT,
                shard: before.shard,
                day: *day,
                epoch,
                position,
                batches: day_batches.clone(),
            };
            let body = serde_json::to_vec(&object)
                .map_err(|e| Error::Corrupt(format!("input log record: {e}")))?;
            self.store.put(
                &format!("{}{position:020}-{epoch:010}.json", self.day_dir(*day)),
                &body,
            )?;
        }
        self.head.commit(|head| {
            if head.committed >= position {
                return;
            }
            head.epochs.entry(epoch).or_insert(position);
            head.committed = position;
            head.batches += batches.len() as u64;
            for batch in batches.iter().filter(|b| b.carrier == "annotation") {
                head.annotations.insert(batch.batch_id.clone());
            }
            for (day, placed) in &by_day {
                let index = head.days.entry(*day).or_default();
                index.records += placed.len() as u64;
                index.last_position = position;
            }
            for batch in &batches {
                head.last_ingested_at_ms = head.last_ingested_at_ms.max(batch.ingested_at_ms);
                if !batch.object_key.is_empty()
                    && head
                        .key_watermark
                        .as_ref()
                        .is_none_or(|w| batch.object_key > *w)
                {
                    head.key_watermark = Some(batch.object_key.clone());
                }
                for &day in &batch.days {
                    let index = head.days.entry(day).or_default();
                    index.newest_position = index.newest_position.max(position);
                    index.batches += 1;
                }
                // The span on every day the batch is recorded under: a batch of cache facts
                // with no mass-bearing kind (a holder reset alone) still spans them.
                if batch.first_at_ms.is_some() {
                    for day in batch.placement_days() {
                        let index = head.days.entry(day).or_default();
                        index.first_at_ms = min_opt(index.first_at_ms, batch.first_at_ms);
                        index.last_at_ms = max_opt(index.last_at_ms, batch.last_at_ms);
                    }
                }
            }
        })
    }

    /// The day's committed batches: its manifest, and every committed record object beside
    /// it. Orphans (above the committed position, or of an epoch that does not own their
    /// positions) are skipped.
    ///
    /// # Errors
    /// The store failed, or an object does not decode.
    pub fn day(&self, day: i64) -> Result<std::sync::Arc<DayLedger>, Error> {
        let head = self.head.read()?;
        self.day_under(&head, day)
    }

    /// Whether a batch is committed under any of `days`: what ingest asks before it commits
    /// content it may have seen under another key. A committed batch is recorded under every
    /// one of its placement days, so its own days find it.
    ///
    /// # Errors
    /// The store failed, or an object does not decode.
    pub fn holds(
        &self,
        batch_id: &str,
        days: &std::collections::BTreeSet<i64>,
    ) -> Result<bool, Error> {
        let head = self.head.read()?;
        for day in days {
            if head.days.contains_key(day)
                && self
                    .day_under(&head, *day)?
                    .batches
                    .iter()
                    .any(|b| b.batch_id == batch_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// [`Ledger::day`] under a head already read: every day a question reads is then read
    /// under one committed position.
    fn day_under(&self, head: &LedgerHead, day: i64) -> Result<std::sync::Arc<DayLedger>, Error> {
        let index = head.days.get(&day).cloned().unwrap_or_default();
        if index.compacted {
            // One manifest, and any late record beside it: read whole, not kept.
            if let Ok(mut open) = self.open_days.lock() {
                open.remove(&(self.shard, day));
            }
            let mut loaded = OpenDay::default();
            self.fold_new(head, day, &mut loaded)?;
            return Ok(loaded.ledger);
        }
        let mut open = self
            .open_days
            .lock()
            .map_err(|_| Error::Local("input log: the open-day cache was poisoned".into()))?;
        let loaded = open.entry((self.shard, day)).or_default();
        if loaded.records != index.records || loaded.folded.is_empty() {
            self.fold_new(head, day, loaded)?;
            loaded.records = index.records;
        }
        Ok(std::sync::Arc::clone(&loaded.ledger))
    }

    /// Fold into `loaded` the day's committed objects it has not folded yet.
    fn fold_new(&self, head: &LedgerHead, day: i64, loaded: &mut OpenDay) -> Result<(), Error> {
        let mut changed = false;
        for object in self.store.list(&self.day_dir(day))? {
            if loaded.folded.contains(&object.key) {
                continue;
            }
            let body = self.store.get(&object.key)?;
            let batches = if object.key.ends_with("manifest.json") {
                let manifest: Manifest = decode(&object.key, &body)?;
                manifest.batches
            } else {
                let record: RecordObject = decode(&object.key, &body)?;
                let committed = record.position <= head.committed
                    && head.owner_of(record.position) == Some(record.epoch);
                if !committed {
                    continue;
                }
                record.batches
            };
            for b in batches {
                loaded
                    .batches
                    .insert((b.input_position, b.batch_id.clone()), b);
            }
            loaded.folded.insert(object.key);
            changed = true;
        }
        if changed || loaded.folded.is_empty() {
            loaded.ledger = std::sync::Arc::new(DayLedger {
                day,
                batches: loaded.batches.values().cloned().collect(),
            });
        }
        Ok(())
    }

    /// Every batch committed after `position`, each once, in position order.
    ///
    /// # Errors
    /// The store failed, or an object does not decode.
    pub fn committed_since(&self, position: u64) -> Result<Vec<BatchRecord>, Error> {
        let head = self.head.read()?;
        let mut batches: BTreeMap<(u64, String), BatchRecord> = BTreeMap::new();
        for day in head.days_recorded_since(position) {
            for b in &self.day_under(&head, day)?.batches {
                if b.input_position > position {
                    batches
                        .entry((b.input_position, b.batch_id.clone()))
                        .or_insert_with(|| b.clone());
                }
            }
        }
        Ok(batches.into_values().collect())
    }

    /// Every committed batch recorded under a day from `first_day` through `last_day`, each
    /// once, in position order: the batches whose span meets those days, with their
    /// segments and lifecycle records. Days the head does not name are not listed.
    ///
    /// # Errors
    /// The store failed, or an object does not decode.
    pub fn over(&self, first_day: i64, last_day: i64) -> Result<Vec<BatchRecord>, Error> {
        let head = self.head.read()?;
        let mut batches: BTreeMap<(u64, String), BatchRecord> = BTreeMap::new();
        for (&day, _) in head.days.range(first_day..=last_day) {
            for b in &self.day_under(&head, day)?.batches {
                batches
                    .entry((b.input_position, b.batch_id.clone()))
                    .or_insert_with(|| b.clone());
            }
        }
        Ok(batches.into_values().collect())
    }

    /// The committed batches whose cache-fact span meets `[lo_ms, hi_ms)`, each once.
    ///
    /// # Errors
    /// The store failed, or an object does not decode.
    pub fn meeting(&self, lo_ms: f64, hi_ms: f64) -> Result<Vec<BatchRecord>, Error> {
        if hi_ms <= lo_ms {
            return Ok(Vec::new());
        }
        let mut batches = self.over(day_of(lo_ms), day_of(hi_ms))?;
        batches.retain(|b| b.meets(lo_ms, hi_ms));
        Ok(batches)
    }

    /// Visit every day in the head, oldest first, each read under one head: the questions
    /// asked across the whole log (the runs' sequences, every open). A batch recorded under
    /// several days is visited once per day; `visit` folds what it needs and keeps no day.
    ///
    /// # Errors
    /// The store failed, an object does not decode, or `visit` failed.
    pub fn each_day<E: From<Error>>(
        &self,
        mut visit: impl FnMut(&DayLedger) -> Result<(), E>,
    ) -> Result<(), E> {
        let head = self.head.read()?;
        for &day in head.days.keys() {
            visit(&*self.day_under(&head, day)?)?;
        }
        Ok(())
    }

    /// Fold the day's committed batches into its manifest and delete its record objects,
    /// committed or orphaned. Idempotent; a late record written after it lands beside the
    /// manifest and the next compaction folds it in.
    ///
    /// # Errors
    /// Another instance holds the lease, or the store failed.
    pub fn compact(&self, day: i64) -> Result<(), Error> {
        self.head.hold_lease()?;
        let ledger = self.day(day)?;
        let head = self.head.read()?;
        let manifest = Manifest {
            format: FORMAT,
            shard: head.shard,
            day,
            through_position: head.committed,
            batches: ledger.batches.clone(),
        };
        let body = serde_json::to_vec(&manifest)
            .map_err(|e| Error::Corrupt(format!("input log manifest: {e}")))?;
        let dir = self.day_dir(day);
        let records: Vec<String> = self
            .store
            .list(&dir)?
            .into_iter()
            .map(|o| o.key)
            .filter(|k| !k.ends_with("manifest.json"))
            .collect();
        self.store.put(&format!("{dir}manifest.json"), &body)?;
        for key in records {
            self.store.delete(&key)?;
        }
        self.head.commit(|h| {
            if let Some(index) = h.days.get_mut(&day) {
                index.compacted = true;
            }
        })?;
        Ok(())
    }

    /// Drop every day before `first_kept` from the log: each day's objects, then its index
    /// in the head, under the lease. Returns the days dropped. A crash between the two
    /// leaves a day the head still names with fewer objects than it had, and the next call
    /// finishes it; the days a retention drops are ones no pass reads.
    ///
    /// # Errors
    /// Another instance holds the lease, or the store failed.
    pub fn retire_before(&self, first_kept: i64) -> Result<Vec<i64>, Error> {
        self.head.hold_lease()?;
        let head = self.head.read()?;
        let days: Vec<i64> = head.days.range(..first_kept).map(|(d, _)| *d).collect();
        if days.is_empty() {
            return Ok(days);
        }
        for &day in &days {
            for object in self.store.list(&self.day_dir(day))? {
                self.store.delete(&object.key)?;
            }
        }
        self.head
            .commit(|h| h.days.retain(|d, _| *d >= first_kept))?;
        if let Ok(mut open) = self.open_days.lock() {
            open.retain(|(shard, day), _| *shard != self.shard || *day >= first_kept);
        }
        Ok(days)
    }
}

fn decode<T: serde::de::DeserializeOwned>(key: &str, body: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(body).map_err(|e| Error::Corrupt(format!("input log {key}: {e}")))
}

fn min_opt(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn max_opt(a: Option<f64>, b: Option<f64>) -> Option<f64> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchRecord, Ledger, SHARD};
    use crate::lineage::Lineage;
    use analyzer_objectstore::{DirStore, ObjectStore};

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ledger-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch");
        dir
    }

    fn ledger(root: &std::path::Path, instance: &str) -> Ledger {
        Ledger::at(
            &DirStore::new(root),
            "lineages/test/",
            SHARD,
            Lineage::new("test", instance).expect("lineage"),
        )
        .expect("ledger")
    }

    fn batch(id: &str, key: &str, days: &[i64]) -> BatchRecord {
        BatchRecord {
            batch_id: id.into(),
            carrier: "segment".into(),
            source: "bucket".into(),
            object_key: key.into(),
            etag: format!("etag-{id}"),
            input_position: 0,
            ingested_at_ms: 1,
            record_count: 3,
            unknown_kinds: 0,
            first_at_ms: Some(1000.0),
            last_at_ms: Some(2000.0),
            days: days.to_vec(),
            segments: Vec::new(),
            drops: Vec::new(),
            lifecycle: Vec::new(),
            stored: Vec::new(),
            annotations: Vec::new(),
            tags: Vec::new(),
        }
    }

    #[test]
    fn a_cycle_commits_its_batches_once_under_one_position() {
        let root = scratch("cycle");
        let log = ledger(&root, "writer");
        let head = log
            .commit_cycle(
                log.next_position().expect("position"),
                vec![
                    batch("a", "2026/09/22/10/a", &[1]),
                    batch("b", "2026/09/22/10/b", &[1, 2]),
                ],
            )
            .expect("commit");
        assert_eq!(head.committed, 1, "one cycle, one position");
        assert_eq!(head.key_watermark.as_deref(), Some("2026/09/22/10/b"));
        assert_eq!(head.days_since(0), vec![1, 2]);
        assert!(head.days_since(1).is_empty());
        let day1 = log.day(1).expect("day 1");
        assert_eq!(
            day1.batches
                .iter()
                .map(|b| (b.batch_id.as_str(), b.input_position))
                .collect::<Vec<_>>(),
            vec![("a", 1), ("b", 1)]
        );
        assert_eq!(
            log.day(2).expect("day 2").batches.len(),
            1,
            "b is on both days"
        );
        let next = log
            .commit_cycle(2, vec![batch("c", "2026/09/22/11/c", &[2])])
            .expect("next");
        assert_eq!(next.committed, 2);
        assert_eq!(next.days_since(1), vec![2]);
        assert_eq!(log.day(2).expect("day 2").since(1).count(), 1);
        assert!(
            log.commit_cycle(2, vec![batch("d", "k/d", &[2])]).is_err(),
            "a used position"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_batch_without_cache_facts_is_recorded_under_its_segments_day() {
        let root = scratch("placement");
        let log = ledger(&root, "writer");
        let mut lifecycle_only = batch("hb", "k/hb", &[]);
        lifecycle_only.first_at_ms = None;
        lifecycle_only.last_at_ms = None;
        lifecycle_only.segments.push(super::SegmentArrival {
            incarnation: "r".into(),
            segment_seq: 0,
            instance: Some("i1".into()),
            first_record_at_ms: 3.0 * 86_400_000.0 + 5.0,
            last_record_at_ms: 3.0 * 86_400_000.0 + 9.0,
            heartbeat_secs: 10,
            max_segment_secs: 60,
            hb_count: 1,
            hb_first_at_ms: None,
            hb_last_at_ms: None,
            stop_at_ms: None,
            msgs_seen: 0,
            dropped: 0,
            dropped_at_hb_first: 0,
            oversized_at_hb_first: 0,
            oversized: 0,
            publisher_restarts: 0,
            agent_version: None,
            topic_model: None,
            topic_pod: None,
            declared_model: None,
            declared_pod: None,
            engine_version: None,
            attributed: true,
            canary: None,
        });
        let head = log.commit_cycle(1, vec![lifecycle_only]).expect("commit");
        let day3 = &head.days[&3];
        assert_eq!(
            (day3.batches, day3.records),
            (0, 1),
            "recorded, not an arrival"
        );
        assert!(
            head.days_since(0).is_empty(),
            "no arrival makes a day dirty"
        );
        let ledger = log.day(3).expect("day 3");
        assert_eq!(ledger.batches.len(), 1);
        assert_eq!(ledger.arrivals().count(), 0);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_record_above_the_committed_position_is_skipped() {
        let root = scratch("orphan");
        let log = ledger(&root, "writer");
        log.commit_cycle(1, vec![batch("a", "k/a", &[1])])
            .expect("commit");
        // A cycle that wrote its record and died before committing.
        let store = DirStore::new(&root);
        let head = log.head().expect("head");
        let orphan = super::RecordObject {
            format: super::FORMAT,
            shard: SHARD,
            day: 1,
            epoch: head.lease_epoch,
            position: 2,
            batches: vec![batch("b", "k/b", &[1])],
        };
        store
            .put(
                &format!(
                    "lineages/test/ledger/shards/0/days/1/{:020}-{:010}.json",
                    2, head.lease_epoch
                ),
                &serde_json::to_vec(&orphan).unwrap(),
            )
            .unwrap();
        assert_eq!(
            log.day(1).expect("day").batches.len(),
            1,
            "the orphan is skipped"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_fenced_writers_orphan_is_skipped_after_a_takeover_commits_its_positions() {
        let root = scratch("takeover");
        let primary = ledger(&root, "primary");
        primary
            .commit_cycle(1, vec![batch("a", "k/a", &[1])])
            .expect("primary commits 1");
        // The primary writes position 2's record and is fenced before committing it.
        let store = DirStore::new(&root);
        let orphan = super::RecordObject {
            format: super::FORMAT,
            shard: SHARD,
            day: 1,
            epoch: 1,
            position: 2,
            batches: vec![batch("orphan", "k/orphan", &[1])],
        };
        store
            .put(
                &format!(
                    "lineages/test/ledger/shards/0/days/1/{:020}-{:010}.json",
                    2, 1
                ),
                &serde_json::to_vec(&orphan).unwrap(),
            )
            .unwrap();
        // Its lease expires; a standby takes over and commits position 2 itself.
        let lease_key = "lineages/test/ledger/shards/0/lease";
        let mut lease: serde_json::Value =
            serde_json::from_slice(&store.get(lease_key).unwrap()).unwrap();
        lease["expires_ms"] = 0.into();
        store
            .put(lease_key, &serde_json::to_vec(&lease).unwrap())
            .unwrap();
        let standby = ledger(&root, "standby");
        let head = standby
            .commit_cycle(2, vec![batch("b", "k/b", &[1])])
            .expect("standby commits 2");
        assert_eq!((head.committed, head.owner_of(2)), (2, Some(2)));
        let day = standby.day(1).expect("day");
        assert_eq!(
            day.batches
                .iter()
                .map(|b| b.batch_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"],
            "position 2 is the standby's, not the fenced writer's orphan"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_compacted_day_reads_from_its_manifest_and_folds_a_late_record_in() {
        let root = scratch("compact");
        let log = ledger(&root, "writer");
        log.commit_cycle(1, vec![batch("a", "k/a", &[1])])
            .expect("a");
        log.commit_cycle(2, vec![batch("b", "k/b", &[1])])
            .expect("b");
        let before = log.day(1).expect("before");
        log.compact(1).expect("compact");
        let store = DirStore::new(&root);
        let objects: Vec<String> = store
            .list("lineages/test/ledger/shards/0/days/1/")
            .unwrap()
            .into_iter()
            .map(|o| o.key)
            .collect();
        assert_eq!(objects.len(), 1, "{objects:?}");
        assert!(objects[0].ends_with("manifest.json"));
        assert_eq!(log.day(1).expect("after"), before);
        assert!(log.head().expect("head").days[&1].compacted);
        // A late arrival for the compacted day lands beside the manifest.
        log.commit_cycle(3, vec![batch("late", "k/late", &[1])])
            .expect("late");
        assert_eq!(log.day(1).expect("late").batches.len(), 3);
        log.compact(1).expect("again");
        assert_eq!(log.day(1).expect("folded").batches.len(), 3);
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_window_finds_a_batch_whose_span_crosses_a_quiet_day() {
        let root = scratch("quiet-day");
        let log = ledger(&root, "writer");
        let day = 86_400_000.0;
        let mut spanning = batch("long", "k/long", &[1, 3]);
        spanning.first_at_ms = Some(day + 5.0);
        spanning.last_at_ms = Some(3.0 * day + 5.0);
        log.commit_cycle(1, vec![spanning]).expect("commit");
        let quiet = log.meeting(2.0 * day, 3.0 * day).expect("window");
        assert_eq!(
            quiet
                .iter()
                .map(|b| b.batch_id.as_str())
                .collect::<Vec<_>>(),
            vec!["long"],
            "a batch is recorded under every day its span meets"
        );
        assert_eq!(log.over(1, 3).expect("days").len(), 1, "each batch once");
        assert!(log.meeting(4.0 * day, 5.0 * day).expect("after").is_empty());
        let head = log.head().expect("head");
        assert_eq!(
            head.days_since(0),
            vec![1, 3],
            "the quiet day holds the record but no arrival"
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_reader_sharing_the_lake_sees_each_commit_and_an_orphan_its_writer_later_commits() {
        let root = scratch("shared");
        let lake = crate::Lake::new(
            std::sync::Arc::new(DirStore::new(&root)),
            "lineages/test/".into(),
            Lineage::new("test", "writer").expect("lineage"),
        );
        let (writer, reader) = (lake.ledger(SHARD).unwrap(), lake.ledger(SHARD).unwrap());
        writer
            .commit_cycle(1, vec![batch("a", "k/a", &[1])])
            .expect("commit 1");
        assert_eq!(reader.day(1).expect("day").batches.len(), 1);
        // The writer writes position 2's record and fails before its head commit: an
        // orphan the reader skips.
        let orphan = super::RecordObject {
            format: super::FORMAT,
            shard: SHARD,
            day: 1,
            epoch: 1,
            position: 2,
            batches: vec![batch("b", "k/b", &[1])],
        };
        DirStore::new(&root)
            .put(
                &format!(
                    "lineages/test/ledger/shards/0/days/1/{:020}-{:010}.json",
                    2, 1
                ),
                &serde_json::to_vec(&orphan).unwrap(),
            )
            .unwrap();
        assert_eq!(reader.day(1).expect("day").batches.len(), 1, "skipped");
        // Its retry commits position 2 under the same epoch: the same object, now owned.
        writer
            .commit_cycle(2, vec![batch("b", "k/b", &[1])])
            .expect("commit 2");
        assert_eq!(
            reader
                .day(1)
                .expect("day")
                .batches
                .iter()
                .map(|b| (b.batch_id.as_str(), b.input_position))
                .collect::<Vec<_>>(),
            vec![("a", 1), ("b", 2)]
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn retirement_drops_the_old_days_objects_and_index() {
        let root = scratch("retire");
        let log = ledger(&root, "writer");
        log.commit_cycle(1, vec![batch("a", "k/a", &[1])])
            .expect("commit 1");
        log.commit_cycle(2, vec![batch("b", "k/b", &[5])])
            .expect("commit 2");
        assert_eq!(log.retire_before(3).expect("retire"), vec![0, 1]);
        let head = log.head().expect("head");
        assert_eq!(head.days.keys().copied().collect::<Vec<_>>(), vec![5]);
        assert!(log.day(1).expect("day").batches.is_empty());
        assert_eq!(head.committed, 2, "positions are never reused");
        assert!(log.retire_before(3).expect("again").is_empty());
        std::fs::remove_dir_all(root).ok();
    }

    #[test]
    fn a_batch_with_a_span_and_no_arrival_day_still_spans_its_days() {
        let root = scratch("span-only");
        let log = ledger(&root, "writer");
        // Cache facts of no mass-bearing kind (a holder reset alone): a span, no arrival day.
        let mut reset = batch("reset", "k/reset", &[]);
        reset.first_at_ms = Some(86_400_000.0 + 10.0);
        reset.last_at_ms = Some(86_400_000.0 + 20.0);
        let head = log.commit_cycle(1, vec![reset]).expect("commit");
        let index = head.days.get(&1).expect("the span's day is indexed");
        assert_eq!(
            (index.first_at_ms, index.last_at_ms, index.batches),
            (Some(86_400_000.0 + 10.0), Some(86_400_000.0 + 20.0), 0),
            "spanned, with no arrival counted"
        );
        assert!(head.days_since(0).is_empty(), "no day owes a pass for it");
        std::fs::remove_dir_all(root).ok();
    }
}
