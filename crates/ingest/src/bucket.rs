// SPDX-License-Identifier: AGPL-3.0-only

//! Bucket-direct ingest.
//!
//! The analyzer lists and streaming-GETs sealed segments from the customer's S3-API
//! bucket through the object-storage port. No landing zone, no local record file.
//! Rules this module owns:
//!
//! - **Listing never matches what it already knows.** The cycle lists from the input log's
//!   key watermark's hour prefix minus the re-list margin (`start-after`), and skips the
//!   keys the log holds committed on the margin's days.
//! - **The watermark is the log's, not wall clock.** An analyzer that slept a week
//!   lists a week. Late uploads inside the margin are caught by the re-list; later than
//!   the margin is declared loss, named in-band by the seq hole.
//! - **Nothing is pending.** A cycle that did not commit left only orphan records the log
//!   never reads; its objects are above the watermark or inside the margin, absent from
//!   the committed records, and the next cycle fetches them again
//!   (`docs/lake.md`).
//! - **The transport is the object-storage port.** Credentials come from its provider chain
//!   and never reach the engine, and every request is bounded by its timeout and retry
//!   budget, so an unreachable endpoint is a decided error, not a hang inside the cycle.
//! - **No arrival stamps.** Taint and coverage read the producer clock; the
//!   bucket reader passes `arrival = None` unconditionally, which is also what dissolves
//!   the archive/live split — an archive drop and a live segment take one path.

use crate::Error;
use analyzer_lake::dated::{self, LIST_MARGIN_HOURS};
use analyzer_objectstore::ObjectStore;

use super::{IngestReport, LedgerEntry, Run};
use std::collections::HashSet;

/// One object discovery found to fetch: a listed segment the input log does not hold
/// committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovered {
    /// The object's key under the bucket.
    pub key: String,
    /// The listed size in bytes.
    pub size: u64,
    /// The listed etag with its quotes stripped, or empty when the listing carried none.
    pub etag: String,
}

/// The per-file wall-time SPLIT's journal budget: where a cycle's time goes is a production
/// measurement, so the distribution arrives from production, one journal line per file,
/// capped per cycle and SAYING SO when capped (a silent cap reads as full coverage).
const SPLIT_LINES_PER_CYCLE: u32 = 10;

/// The cycle's closing diagnostics: the commit distribution over every file, when the
/// per-file split lines were capped.
fn cycle_diagnostics(split_lines: u32, commit_ms: &mut [f64]) {
    // The suppressed tail, summarized instead of dropped: percentiles over EVERY file keep
    // the distribution's shape visible where the cap quoted only its head.
    if commit_ms.len() > split_lines as usize {
        commit_ms.sort_by(f64::total_cmp);
        let pick = |num: usize, den: usize| commit_ms[(commit_ms.len() - 1) * num / den];
        log::info!(
            "ingest split: all {} file(s) commit p50 {:.0}ms \
             p90 {:.0}ms max {:.0}ms",
            commit_ms.len(),
            pick(1, 2),
            pick(9, 10),
            commit_ms[commit_ms.len() - 1]
        );
    }
}

/// One ingest cycle against the bucket: list what is new, fetch it, commit it under the
/// intent-row protocol. `base_prefix` is the collection URL's own path component as it
/// appears in `_path` (bucket name plus any base directory, trailing slash) — listing
/// returns absolute paths, GETs take collection-relative names, and this is the seam
/// between the two spaces. `key_prefix` scopes the listing glob under the collection
/// (`<key_prefix>**`): a fleet's own prefix in a shared bucket, empty for the whole
/// collection.
///
/// # Errors
/// Engine failures, a listed path outside `base_prefix`, or corrupt (non-tail) input.
pub fn ingest_bucket(
    lake: &analyzer_lake::Lake,
    store: &dyn ObjectStore,
    fleet_id: &str,
    key_prefix: &str,
    admit_corpus_forms: bool,
) -> Result<IngestReport, Error> {
    let fetch = discover(lake, store, key_prefix, admit_corpus_forms)?;
    ingest_discovered(lake, store, fleet_id, fetch, Slice::default(), &mut |_| {})
}

/// The bounds of one ingest slice.
#[derive(Debug, Clone, Copy, Default)]
pub struct Slice {
    /// Past this instant the remaining files are deferred: counted in the report, untouched
    /// on the store, re-listed by the next cycle's own discovery. `None` drains everything,
    /// which is the one-shot commands' slice.
    pub deadline: Option<std::time::Instant>,
    /// The prepare pool's width in workers; `None` is the default sized to the box: two cores
    /// left to the engine, at most eight. A configured width is clamped rather than trusted.
    pub workers: Option<usize>,
}

/// The fetch-and-commit half, over a list [`discover`] produced — public so a caller
/// with retention policy of its own (the daemon) can act on the discovered keys' dates
/// BEFORE any row lands: TTL filters already-expired rows at insert, so a horizon that
/// widens after the fact protects nothing.
///
/// `on_committed` fires after EACH file commits, with that file's ledgered record count.
/// This loop is one blocking call for the whole
/// slice, so per-cycle accounting meant a counter that never moved while thirty-nine
/// of forty files were already committed — the caller's live progress hook is what makes
/// a long cycle observable while it runs.
///
/// `slice` bounds the cycle in time and in width; see [`Slice`].
///
/// # Errors
/// Engine failures, a listed path outside `base_prefix`, or corrupt (non-tail) input.
pub fn ingest_discovered(
    lake: &analyzer_lake::Lake,
    store: &dyn ObjectStore,
    fleet_id: &str,
    fetch: Vec<Discovered>,
    slice: Slice,
    on_committed: &mut dyn FnMut(u64),
) -> Result<IngestReport, Error> {
    let slice_deadline = slice.deadline;
    // **The slice deadline is the tick's guarantee.** Past the deadline the rest is
    // DEFERRED; the first file always runs, so a cycle is never a silent no-op.
    //
    // **The pipeline.** Two stages with different bounds: PREPARE (fetch + decode + hash,
    // CPU-bound) fans out to workers each holding a store sibling; COMMIT (one writer to the
    // input log) stays a single lane, so the log sees one writer at one cadence however wide
    // the prepare side runs. A bounded channel joins them
    // — capacity = workers, so in-flight bytes are bounded by 2 x workers x the
    // segment cap, stated in the resource's unit. Metadata inserts bank per cycle
    // (six per file become two per file plus four per cycle); the claim set that
    // arbitrates duplicate content is consumer-local and needs no lock.
    let workers = ingest_workers(slice.workers, fetch.len());
    if fetch.len() > 1 {
        log::info!(
            "ingest: {workers} preparer(s) over {} file(s), one \
             commit lane",
            fetch.len()
        );
    }
    let queue = std::sync::Mutex::new(std::collections::VecDeque::from(fetch));
    let stop = std::sync::atomic::AtomicBool::new(false);
    let first_claim = std::sync::atomic::AtomicBool::new(false);
    let (tx, rx) = std::sync::mpsc::sync_channel::<Prepared>(workers);

    // Store siblings dialed BEFORE the scope, so a connect failure is the cycle's
    // error and never a worker's panic.
    let mut lanes = Vec::new();
    for _ in 0..workers {
        lanes.push(store.sibling()?);
    }

    let mut cycle = Cycle::default();
    let mut first_error: Option<Error> = None;
    std::thread::scope(|scope| {
        for lane in lanes {
            let tx = tx.clone();
            let (queue, stop, first_claim) = (&queue, &stop, &first_claim);
            scope.spawn(move || {
                prepare_worker(
                    lane.as_ref(),
                    fleet_id,
                    queue,
                    stop,
                    slice_deadline,
                    first_claim,
                    &tx,
                );
            });
        }
        drop(tx);
        // The single commit lane, draining LIVE — the progress hook and the split
        // lines fire as files land. After an error
        // the loop keeps draining (discarding) so no preparer blocks on the bounded
        // channel; the stop flag has already halted dispatch.
        for msg in rx {
            let prepared = match msg {
                Prepared::Failed { size, error } => {
                    cycle.run.report.files_seen += 1;
                    cycle.run.report.bytes_seen += size;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
                Prepared::File(file) => file,
            };
            cycle.run.report.files_seen += 1;
            cycle.run.report.bytes_seen += prepared.size;
            if first_error.is_some() {
                continue;
            }
            if let Err(e) = cycle.commit(lake, *prepared, on_committed) {
                stop.store(true, std::sync::atomic::Ordering::SeqCst);
                first_error = Some(e);
            }
        }
    });
    if let Some(e) = first_error {
        return Err(e);
    }
    // Whatever the deadline left: seen (it was listed) and deferred (never fetched).
    for deferred in queue
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
    {
        cycle.run.report.files_seen += 1;
        cycle.run.report.bytes_seen += deferred.size;
        cycle.run.report.files_deferred += 1;
        cycle.run.report.bytes_deferred += deferred.size;
    }
    cycle.bank.flush(lake)?;
    cycle_diagnostics(cycle.split_lines, &mut cycle.commit_ms);
    Ok(cycle.run.report)
}

/// The pool's PREPARE width, in WORKERS. Each preparer holds at most one file in
/// flight and the channel holds at most `workers` more, so the in-flight byte bound is
/// `2 x workers x the segment size cap` — stated in the resource's own unit. The
/// default leaves two cores to the engine sharing the box and caps at 8; a configured
/// width is clamped rather than trusted. The commit lane is deliberately ONE: the engine
/// is the shared resource, and a single writer at per-file insert cadence is what its
/// merge pipeline is sized for.
fn ingest_workers(configured: Option<usize>, files: usize) -> usize {
    let cores = std::thread::available_parallelism().map_or(2, std::num::NonZeroUsize::get);
    configured
        .unwrap_or_else(|| cores.saturating_sub(2).clamp(1, 8))
        .clamp(1, 64)
        .min(files.max(1))
}

/// One prepared file crossing from a preparer to the commit lane — owned whole, which
/// is why [`crate::PreparedSegment`] owns its bytes.
struct PreparedFile {
    key: String,
    size: u64,
    etag: String,
    prep: crate::PreparedSegment,
    fetch_ms: f64,
    decode_ms: f64,
}

enum Prepared {
    File(Box<PreparedFile>),
    Failed { size: u64, error: Error },
}

/// A preparer's loop: claim the first-file guarantee, respect the deadline and the
/// stop flag at DISPATCH, fetch and decode one file, hand it to the commit lane. The
/// bounded send is the backpressure — a preparer ahead of the engine waits here.
fn prepare_worker(
    store: &dyn ObjectStore,
    fleet_id: &str,
    queue: &std::sync::Mutex<std::collections::VecDeque<Discovered>>,
    stop: &std::sync::atomic::AtomicBool,
    slice_deadline: Option<std::time::Instant>,
    first_claim: &std::sync::atomic::AtomicBool,
    tx: &std::sync::mpsc::SyncSender<Prepared>,
) {
    loop {
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        // The first file always runs, so a cycle is never a silent no-op — the
        // sequential loop's own rule, held by whichever preparer gets there first.
        let first = !first_claim.swap(true, std::sync::atomic::Ordering::SeqCst);
        if !first && slice_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return;
        }
        let popped = queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front();
        let Some(Discovered { key, size, etag }) = popped else {
            return;
        };
        let m0 = std::time::Instant::now();
        // **One fetch shape, because bytes are bytes.** `decode_segment` learns the
        // encoding from the bytes; the dispatch lives in `prepare_segment`.
        let bytes = match store.get(&key) {
            Ok(b) => b,
            Err(e) => {
                stop.store(true, std::sync::atomic::Ordering::SeqCst);
                let _ = tx.send(Prepared::Failed {
                    size,
                    error: Error::File {
                        key: key.clone(),
                        source: Box::new(Error::from(e)),
                    },
                });
                return;
            }
        };
        let m1 = std::time::Instant::now();
        let prep = match crate::prepare_segment(&key, fleet_id, &bytes, false) {
            Ok(p) => p,
            Err(e) => {
                stop.store(true, std::sync::atomic::Ordering::SeqCst);
                let _ = tx.send(Prepared::Failed {
                    size,
                    error: Error::File {
                        key: key.clone(),
                        source: Box::new(e),
                    },
                });
                return;
            }
        };
        let m2 = std::time::Instant::now();
        let file = Prepared::File(Box::new(PreparedFile {
            key,
            size,
            etag,
            prep,
            fetch_ms: m1.saturating_duration_since(m0).as_secs_f64() * 1000.0,
            decode_ms: m2.saturating_duration_since(m1).as_secs_f64() * 1000.0,
        }));
        if tx.send(file).is_err() {
            return;
        }
    }
}

/// The commit lane's per-file half: claim, bank its record, account, announce.
/// One ingest cycle's commit lane: the report and the banked metadata it accumulates, the
/// content identities it has claimed, and the split lines and commit timings it has logged.
#[derive(Default)]
struct Cycle {
    run: Run,
    bank: crate::MetadataBank,
    claimed: HashSet<String>,
    split_lines: u32,
    commit_ms: Vec<f64>,
}

impl Cycle {
    /// Commit one prepared file to the input log, or skip it as a duplicate of content this
    /// cycle already committed.
    fn commit(
        &mut self,
        lake: &analyzer_lake::Lake,
        prepared: PreparedFile,
        on_committed: &mut dyn FnMut(u64),
    ) -> Result<(), Error> {
        let Self {
            run,
            bank,
            claimed,
            split_lines,
            commit_ms: commit_ms_all,
        } = self;
        let PreparedFile {
            key,
            size,
            etag,
            prep,
            fetch_ms,
            decode_ms,
        } = prepared;
        let m3 = std::time::Instant::now();
        // Content the cycle already banked under another key is a duplicate: the log keys on
        // the content hash, and the claim set arbitrates within the cycle, as the log's own
        // records do across cycles.
        if !claimed.insert(prep.batch_id.clone()) {
            run.report.duplicates_skipped += 1;
            return Ok(());
        }
        let entry = LedgerEntry {
            batch_id: prep.batch_id.clone(),
            source: key.clone(),
            object_key: key,
            etag,
            records: prep.records,
            unknown_kinds: prep.unknown_kinds,
        };
        let mut commit_timings: Vec<(&'static str, std::time::Duration)> = Vec::new();
        if !crate::commit_file_facts(
            lake,
            "s3",
            prep.facts,
            &entry,
            run,
            &mut commit_timings,
            bank,
        )? {
            run.report.duplicates_skipped += 1;
            return Ok(());
        }
        on_committed(entry.records);
        let commit_ms = m3.elapsed().as_secs_f64() * 1000.0;
        commit_ms_all.push(commit_ms);
        if *split_lines < SPLIT_LINES_PER_CYCLE {
            *split_lines += 1;
            let commit_split: String = commit_timings
                .iter()
                .map(|(label, d)| format!("{label} {:.0}ms", d.as_secs_f64() * 1000.0))
                .collect::<Vec<_>>()
                .join(" ");
            log::info!(
                "ingest split: fetch {fetch_ms:.0}ms decode+hash \
                 {decode_ms:.0}ms commit {commit_ms:.0}ms [{commit_split}] — {} record(s), \
                 {size} byte(s)",
                entry.records,
            );
            if *split_lines == SPLIT_LINES_PER_CYCLE {
                log::info!(
                    "ingest split: further per-file lines summarized \
                     at cycle end"
                );
            }
        }
        Ok(())
    }
}

/// What this cycle must fetch: the listing from the input log's watermark less the margin,
/// less the keys the log holds committed on the margin's days.
///
/// # Errors
/// The input log or the store failed.
pub fn discover(
    lake: &analyzer_lake::Lake,
    store: &dyn ObjectStore,
    key_prefix: &str,
    admit_corpus_forms: bool,
) -> Result<Vec<Discovered>, Error> {
    let log = lake.ledger(analyzer_lake::ledger::SHARD)?;
    let head = log.head()?;
    let watermark = head.key_watermark.clone().unwrap_or_default();
    let floor = dated::hour_floor(&watermark, LIST_MARGIN_HOURS);
    // Committed keys the listing must not match: the margin's days (a day before the
    // floor's for a batch whose events lead its key), or the whole young log while no dated
    // watermark exists yet.
    let first_day = floor.as_deref().and_then(dated::key_day).map_or_else(
        || head.days.keys().next().copied().unwrap_or(0),
        |day| day - 1,
    );
    let last_day = head.days.keys().next_back().copied().unwrap_or(first_day);
    let known: Vec<String> = log
        .over(first_day, last_day)?
        .into_iter()
        .map(|b| b.object_key)
        .filter(|k| !k.is_empty())
        .collect();

    // **The watermark is a seek, not a filter.** `list_after` reaches S3's own `start-after`, so
    // a fleet with a year of segments does not pay to enumerate the year on each cycle. The floor is an hour DIRECTORY (`fleet/2026-08-06/18`) and never a whole object
    // key, and a string that is a prefix of another sorts before it — so `>= floor` and
    // `start-after floor` select the same objects here.
    let listed = match &floor {
        Some(fl) => store.list_after(key_prefix, fl)?,
        None => store.list(key_prefix)?,
    };

    // Candidate rule, unchanged in meaning and now expressed as a predicate rather than a glob:
    // only the shipped form is ever fetched — `.parquet`, the sole shipped suffix (contract 2.1).
    // Everything else under the prefix — preflight probes, operator notes, a shipper's manifests,
    // a mis-shipped `.parquet.tmp` — stays invisible to ingest by construction. The suffix is
    // `dated::SHIPPED_SUFFIX`, the same constant the event source reads, so the two transports
    // cannot come to disagree about what a segment is (`dated::shipped_rules_agree` is the
    // ratchet).
    let known: std::collections::HashSet<String> = known.into_iter().collect();
    let mut fetch: Vec<Discovered> = Vec::new();
    for m in listed {
        // The corpus seam, same as the derivation source's: a RIG may plant the write-ahead
        // JSONL form directly in a bucket, because authoring Parquet in a test would
        // re-implement the tap's converter. Production never sets it — the contract's
        // ship rule is Parquet-only, and the flag rides DaemonConfig where the other
        // corpus admissions already live.
        // `dated::is_shipped_key` is the rule, not the suffix alone: it also refuses the
        // analyzer-owned root (`_analyzer/`), where the derived tier's
        // objects are Parquet by design and would otherwise be fetched as segments.
        let candidate = dated::is_shipped_key(&m.key)
            || (admit_corpus_forms
                && !dated::is_analyzer_owned(&m.key)
                && std::path::Path::new(&m.key)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("jsonl")));
        if !candidate || known.contains(&m.key) {
            continue;
        }
        fetch.push(Discovered {
            key: m.key,
            size: m.size,
            etag: m.etag.unwrap_or_default(),
        });
    }
    // Sorted, so a replay walks the same objects in the same sequence — the property the
    // directory reader had, carried across one more transport.
    fetch.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(fetch)
}

/// The oldest calendar day named by any discovered key's dated hour prefix, as days
/// since the epoch — what a retention gate needs before a byte is fetched. Keys without
/// a dated segment (operator archive drops) contribute nothing: their rows' ages are
/// unknowable before parse, and the post-landing gate owns whatever survives.
#[must_use]
pub fn oldest_key_day(fetch: &[Discovered]) -> Option<i64> {
    fetch.iter().filter_map(|d| dated::key_day(&d.key)).min()
}
