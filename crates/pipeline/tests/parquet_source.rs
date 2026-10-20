// SPDX-License-Identifier: AGPL-3.0-only

//! `ParquetSource`'s engine-free acceptance: the
//! reads are PROVEN, not trusted.
//!
//! - **Pruning**: an instrumented `ChunkReader` observes which byte ranges the decode touched,
//!   mapped to row groups through the footer's own chunk offsets. A segment wholly outside the
//!   window decodes ZERO row groups, and perturbing the window bound makes the same counter
//!   catch the decode — so a broken counter cannot satisfy the zero.
//! - **Projection**: the same shim counts distinct columns touched in a row group; widening
//!   the projection by one field moves the count by one.
//! - **The cache is fetch-once**(endgame, invariant I7): two passes fetch each segment
//!   exactly once, and a cold-cache re-derivation equals the warm one.
//! - **The two file sources agree**: `ParquetSource` over a dated tree and the test-tree
//!   `BucketSource` over the same bytes yield the same IR, with no engine anywhere.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use analyzer_analysis::events::EventSource;
use analyzer_analysis::Window;
use analyzer_pipeline::parquet_source::{FsStore, ParquetSource, SegmentStore};
use analyzer_segments::columnar::{projected_rows, Projection};
use bytes::Bytes;
use harness::bucket_source::BucketSource;
use harness::parquet::twin_of_jsonl;
use parquet::file::reader::{ChunkReader, Length};

// ================================================================================================
// The instrumented reader: every byte range the decode asks for, attributed to (row group,
// column) through the footer's chunk offsets. Observation, not trust.
// ================================================================================================

#[derive(Clone)]
struct CountingReader {
    bytes: Bytes,
    reads: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl CountingReader {
    fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: Bytes::copy_from_slice(bytes),
            reads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The distinct `(row group, column)` chunks any recorded read overlapped.
    fn chunks_touched(&self, bytes: &[u8]) -> BTreeSet<(usize, usize)> {
        use parquet::file::reader::FileReader;
        let meta = parquet::file::serialized_reader::SerializedFileReader::new(
            Bytes::copy_from_slice(bytes),
        )
        .expect("fixture opens")
        .metadata()
        .clone();
        let reads = self.reads.lock().expect("reads lock");
        let mut touched = BTreeSet::new();
        for group in 0..meta.num_row_groups() {
            let rg = meta.row_group(group);
            for column in 0..rg.num_columns() {
                let (start, len) = rg.column(column).byte_range();
                let hit = reads.iter().any(|(rs, rl)| {
                    // A zero-length record is a `get_read` cursor: it touches the chunk that
                    // contains its start offset.
                    if *rl == 0 {
                        *rs >= start && *rs < start + len
                    } else {
                        *rs < start + len && rs + rl > start
                    }
                });
                if hit {
                    touched.insert((group, column));
                }
            }
        }
        touched
    }
}

impl Length for CountingReader {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

impl ChunkReader for CountingReader {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        self.reads.lock().expect("reads lock").push((start, 0));
        self.bytes.get_read(start)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.reads
            .lock()
            .expect("reads lock")
            .push((start, length as u64));
        self.bytes.get_bytes(start, length)
    }
}

/// Six rows in three row groups: `at_ms` 1000..=1005, two rows to a group, so the footer
/// carries three disjoint `at_ms` ranges for the pruning to read.
fn three_group_fixture() -> Vec<u8> {
    let body = "{\"kind\":\"segment_open\",\"instance_id\":\"i1\",\"incarnation\":\"run-a\",\"segment_seq\":0,\"at_ms\":1000.0}\n\
                {\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"b1\",\"n_tokens\":8,\"at_ms\":1001.0}\n\
                {\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"b2\",\"n_tokens\":8,\"at_ms\":1002.0}\n\
                {\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"b3\",\"n_tokens\":8,\"at_ms\":1003.0}\n\
                {\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"b4\",\"n_tokens\":8,\"at_ms\":1004.0}\n\
                {\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"b5\",\"n_tokens\":8,\"at_ms\":1005.0}\n";
    twin_of_jsonl(body, 2)
}

const CACHE_KINDS: &[&str] = &["store", "evict", "clear"];

#[test]
fn a_segment_wholly_outside_the_window_decodes_zero_row_groups() {
    let bytes = three_group_fixture();

    // Every group's `at_ms` maximum is below the window: the footer proves the file empty for
    // this load, so nothing — not even the header probe — may read a data page.
    let reader = CountingReader::new(&bytes);
    let (rows, _) = projected_rows(
        "seg-a-0.parquet",
        "f",
        "b",
        reader.clone(),
        Projection {
            columns: &["instance_id", "block_id"],
            kinds: CACHE_KINDS,
            window: Some((2000.0, 3000.0)),
        },
    )
    .expect("pruned decode");
    assert!(rows.is_empty(), "a pruned-out segment yields no rows");
    assert_eq!(
        reader.chunks_touched(&bytes),
        BTreeSet::new(),
        "zero row groups decoded: the footer is the only read"
    );

    // The bound perturbed to ADMIT the last group: the same counter must now catch a decode,
    // or the zero above was the counter failing, not the pruning working.
    let reader = CountingReader::new(&bytes);
    let (rows, _) = projected_rows(
        "seg-a-0.parquet",
        "f",
        "b",
        reader.clone(),
        Projection {
            columns: &["instance_id", "block_id"],
            kinds: CACHE_KINDS,
            window: Some((1004.0, 3000.0)),
        },
    )
    .expect("perturbed decode");
    assert_eq!(rows.len(), 2, "the admitted group's rows decode: {rows:#?}");
    let groups: BTreeSet<usize> = reader
        .chunks_touched(&bytes)
        .into_iter()
        .map(|(g, _)| g)
        .collect();
    assert!(
        groups.contains(&2),
        "the admitted group was decoded: {groups:?}"
    );
    assert!(
        groups.contains(&0),
        "the header probe reads row zero's group: {groups:?}"
    );
    assert!(
        !groups.contains(&1),
        "the group the statistics still exclude stays undecoded: {groups:?}"
    );
}

#[test]
fn the_projection_bounds_the_columns_decoded_and_widening_is_caught() {
    let bytes = three_group_fixture();
    let columns_in_group_one = |projection: &[&str]| -> usize {
        let reader = CountingReader::new(&bytes);
        projected_rows(
            "seg-a-0.parquet",
            "f",
            "b",
            reader.clone(),
            Projection {
                columns: projection,
                kinds: CACHE_KINDS,
                window: None,
            },
        )
        .expect("projected decode");
        // Group one, not group zero: the header probe legitimately reads every column of row
        // zero's group, so the projection's effect is observed where the probe never reaches.
        reader
            .chunks_touched(&bytes)
            .into_iter()
            .filter(|(g, _)| *g == 1)
            .count()
    };

    // The projected pair plus the two columns that always ride (`kind`, `at_ms`).
    assert_eq!(
        columns_in_group_one(&["instance_id", "block_id"]),
        4,
        "the projection is the column bound"
    );
    // Widening the projection by one field is observed as exactly one more column decoded.
    assert_eq!(
        columns_in_group_one(&["instance_id", "block_id", "n_tokens"]),
        5,
        "a widened projection is caught by the counter"
    );
}

// ================================================================================================
// The operational half: dated discovery, the fetch-once cache, cold equals warm.
// ================================================================================================

/// A [`SegmentStore`] that counts every fetch by key — the observation fetch-once is proven
/// against.
struct CountingStore {
    inner: FsStore,
    fetches: Arc<Mutex<BTreeMap<String, usize>>>,
}

impl SegmentStore for CountingStore {
    fn list(&self, prefix: &str) -> Result<Vec<String>, analyzer_pipeline::Error> {
        self.inner.list(prefix)
    }

    fn fetch(&self, key: &str) -> Result<Vec<u8>, analyzer_pipeline::Error> {
        *self
            .fetches
            .lock()
            .expect("fetch lock")
            .entry(key.to_string())
            .or_insert(0) += 1;
        self.inner.fetch(key)
    }
}

use harness::{dated_hour_of as hour_of, plant_dated as plant};

/// A two-segment mixed-form corpus: one JSONL, one Parquet twin — so the engine-free path
/// exercises the content dispatch too. Returns `(root, window)`.
fn mixed_corpus(base: f64) -> (PathBuf, Window) {
    let root = harness::fresh_dir("parquet-source-store");
    let hour = hour_of(base);
    let a = format!(
        "{{\"kind\":\"segment_open\",\"instance_id\":\"i1\",\"incarnation\":\"run-a\",\"segment_seq\":0,\"at_ms\":{}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\"n_tokens\":16,\"at_ms\":{}}}\n\
         {{\"kind\":\"evict\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\"at_ms\":{}}}",
        base,
        base + 10.0,
        base + 20.0
    );
    let b = format!(
        "{{\"kind\":\"segment_open\",\"instance_id\":\"i2\",\"incarnation\":\"run-a\",\"segment_seq\":1,\"at_ms\":{}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i2\",\"dp_rank\":2,\"group_idx\":1,\"block_id\":\"q1\",\"n_tokens\":8,\"at_ms\":{}}}",
        base + 30.0,
        base + 40.0
    );
    plant(&root, &hour, "seg-run-a-0.jsonl", a.as_bytes());
    plant(&root, &hour, "seg-run-a-1.parquet", &twin_of_jsonl(&b, 2));
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 1_000.0,
    };
    (root, w)
}

#[test]
fn the_cache_fetches_each_segment_once_and_cold_equals_warm() {
    let base = harness::rebase_offset() + 1_000_000.0;
    let (root, w) = mixed_corpus(base);
    let cache = harness::fresh_dir("parquet-source-cache");
    let fetches = Arc::new(Mutex::new(BTreeMap::new()));
    let source = ParquetSource::new(
        CountingStore {
            inner: FsStore::new(&root),
            fetches: Arc::clone(&fetches),
        },
        &cache,
        "tap/",
        24 * 30,
    )
    .admitting_corpus_forms();

    let warm = source.cache_events("f", &w).expect("first pass");
    assert_eq!(warm.len(), 3, "the corpus's cache facts: {warm:#?}");
    let again = source.cache_events("f", &w).expect("second pass");
    assert_eq!(warm, again, "a warm pass changes no answer");
    // The lifecycle and horizon loads ride the same cache and cost no further fetch.
    source
        .lifecycle_events("f", w.start_ms, w.end_ms)
        .expect("lifecycle");
    source
        .segment_opens_at_retention_horizon("f")
        .expect("opens");
    {
        let counts = fetches.lock().expect("fetch lock");
        assert_eq!(
            counts.len(),
            2,
            "both segments were discovered: {counts:#?}"
        );
        assert!(
            counts.values().all(|&n| n == 1),
            "each segment is fetched EXACTLY once, ever: {counts:#?}"
        );
    }

    // I7: a cold-cache re-derivation is identical to the warm one — the cache holds bytes,
    // not derivations, so losing it costs a re-fetch and never an answer.
    std::fs::remove_dir_all(&cache).expect("drop the cache");
    let cold = source.cache_events("f", &w).expect("cold pass");
    assert_eq!(warm, cold, "cold re-derivation equals warm (I7)");
    let counts = fetches.lock().expect("fetch lock");
    assert!(
        counts.values().all(|&n| n == 2),
        "the cold pass re-fetched each segment once: {counts:#?}"
    );
}

#[test]
fn aged_cache_entries_evict_and_recent_ones_stay() {
    let base = harness::rebase_offset() + 1_000_000.0;
    let root = harness::fresh_dir("parquet-evict-store");
    let cache = harness::fresh_dir("parquet-evict-cache");
    // Horizon of one day; the cache holds a segment three days old and one from now.
    let old_key = format!("tap/{}/n1/seg-old-0.jsonl", hour_of(base));
    let new_key = format!("tap/{}/n1/seg-new-0.jsonl", hour_of(harness::now_ms()));
    for key in [&old_key, &new_key] {
        let path = cache.join(key);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, b"{}\n").expect("plant cache entry");
    }
    let source =
        ParquetSource::new(FsStore::new(&root), &cache, "tap/", 24).admitting_corpus_forms();
    let w = Window {
        start_ms: harness::now_ms(),
        end_ms: harness::now_ms() + 1.0,
    };
    source
        .cache_events("f", &w)
        .expect("a pass sweeps the cache");
    assert!(
        !cache.join(&old_key).exists(),
        "a segment past the horizon evicts by its key's own dated hour"
    );
    assert!(
        cache.join(&new_key).exists(),
        "a segment inside the horizon stays"
    );
}

/// A counting source over a planted corpus, for the load-behaviour tests below. Returns the
/// source, the fetch counter, and the list counter.
#[allow(clippy::type_complexity)]
fn counting_source(
    root: &std::path::Path,
    cache: &std::path::Path,
    horizon_hours: i64,
) -> (
    ParquetSource<CountingListStore>,
    Arc<Mutex<BTreeMap<String, usize>>>,
    Arc<Mutex<usize>>,
) {
    let fetches = Arc::new(Mutex::new(BTreeMap::new()));
    let lists = Arc::new(Mutex::new(0usize));
    let source = ParquetSource::new(
        CountingListStore {
            inner: FsStore::new(root),
            fetches: Arc::clone(&fetches),
            lists: Arc::clone(&lists),
        },
        cache,
        "tap/",
        horizon_hours,
    )
    .admitting_corpus_forms();
    (source, fetches, lists)
}

/// Counts fetches per key AND listings — the observation the dedup, fetch-once and memo
/// claims are proven against.
struct CountingListStore {
    inner: FsStore,
    fetches: Arc<Mutex<BTreeMap<String, usize>>>,
    lists: Arc<Mutex<usize>>,
}

impl SegmentStore for CountingListStore {
    fn list(&self, prefix: &str) -> Result<Vec<String>, analyzer_pipeline::Error> {
        *self.lists.lock().expect("list lock") += 1;
        self.inner.list(prefix)
    }

    fn fetch(&self, key: &str) -> Result<Vec<u8>, analyzer_pipeline::Error> {
        *self
            .fetches
            .lock()
            .expect("fetch lock")
            .entry(key.to_string())
            .or_insert(0) += 1;
        self.inner.fetch(key)
    }
}

/// One two-record segment body: an open (seq `seq`, run `run`) and one store of 16 tokens.
fn segment_body(run: &str, seq: u64, instance: &str, block: &str, at: f64) -> String {
    format!(
        "{{\"kind\":\"segment_open\",\"instance_id\":\"{instance}\",\"incarnation\":\"{run}\",\"segment_seq\":{seq},\"at_ms\":{at}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"{instance}\",\"block_id\":\"{block}\",\"n_tokens\":16,\"at_ms\":{}}}\n",
        at + 10.0
    )
}

// ================================================================================================
// Content dedup: the ledger dedups by content hash; the source must too.
// ================================================================================================

/// The same segment BYTES shipped under a second dated key fold once. The two-source
/// differential is structurally blind to this — both sources would double-fold identically —
/// so the assertion is direct: one store event, one token mass, however many keys carried it.
#[test]
fn duplicate_bytes_under_a_second_key_fold_once() {
    let base = harness::rebase_offset() + 2_000_000.0;
    let root = harness::fresh_dir("dedup-store");
    let body = segment_body("run-a", 0, "i1", "d1", base);
    // Same bytes, two keys — one in the same hour, and once more an hour later, the re-ship
    // shape a shipper retry produces.
    plant(&root, &hour_of(base), "seg-run-a-0.jsonl", body.as_bytes());
    plant(
        &root,
        &hour_of(base + 3_600_000.0),
        "reshipped-run-a-0.jsonl",
        body.as_bytes(),
    );
    let (source, _, _) = counting_source(&root, &harness::fresh_dir("dedup-cache"), 24 * 30);
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 7_200_000.0,
    };

    let events = source.cache_events("f", &w).expect("load");
    assert_eq!(
        events.len(),
        1,
        "one segment's records, however many keys shipped its bytes: {events:#?}"
    );
    let mass: i64 = events.iter().map(|e| e.n_tokens).sum();
    assert_eq!(mass, 16, "the token mass is singular");
    let opens = source
        .segment_opens_at_retention_horizon("f")
        .expect("opens");
    assert_eq!(opens.len(), 1, "one open, not one per key: {opens:#?}");
}

// ================================================================================================
// Quarantine-and-declare.
// ================================================================================================

/// A corrupt object does not fail the load: it is skipped and counted, and the hole its
/// missing `segment_seq` leaves degrades the window through the EXISTING sequence-gap
/// machinery — `Register::Indeterminate` via the ledger, no new cause, no fabrication.
#[test]
fn a_corrupt_object_quarantines_and_the_sequence_gap_prices_it() {
    let base = harness::rebase_offset() + 3_000_000.0;
    let root = harness::fresh_dir("quarantine-store");
    let hour = hour_of(base);
    plant(
        &root,
        &hour,
        "seg-run-q-0.jsonl",
        segment_body("run-q", 0, "i1", "q0", base).as_bytes(),
    );
    // Mid-file corruption: an unparseable line that is NOT the final line fails closed
    // (spec 5.5), so this segment decodes on no path — and its seq (1) goes missing.
    plant(
        &root,
        &hour,
        "seg-run-q-1.jsonl",
        b"garbage that is not JSON\n{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"q1\",\"at_ms\":1.0}\n",
    );
    plant(
        &root,
        &hour,
        "seg-run-q-2.jsonl",
        segment_body("run-q", 2, "i1", "q2", base + 2_000.0).as_bytes(),
    );
    let (source, _, _) = counting_source(&root, &harness::fresh_dir("quarantine-cache"), 24 * 30);
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 10_000.0,
    };

    // Every pass-level load stays up.
    let events = source.cache_events("f", &w).expect("the load survives");
    assert_eq!(events.len(), 2, "the two decodable segments' stores");
    let _lifecycle = source
        .lifecycle_events("f", w.start_ms, w.end_ms)
        .expect("lifecycle survives");
    let opens = source
        .segment_opens_at_retention_horizon("f")
        .expect("opens survive");
    assert_eq!(source.quarantined(), 3, "one object, counted once per load");

    // The pricing is structural: seq 1 never appears, so the EXISTING gap machinery fires.
    let holes = analyzer_analysis::degradation::sequence_holes_from_events(&opens);
    assert_eq!(
        holes.len(),
        1,
        "the quarantined segment is a hole: {holes:?}"
    );
    assert_eq!((holes[0].missing_from, holes[0].missing_to), (1, 1));
    let ledger = analyzer_analysis::degradation::Ledger {
        entries: vec![analyzer_analysis::degradation::Degradation {
            scope: analyzer_analysis::degradation::Scope::Run {
                incarnation: holes[0].incarnation.clone(),
                instances: vec!["i1".to_string()],
            },
            cause: analyzer_analysis::degradation::Cause::SequenceGap {
                first_missing: holes[0].missing_from,
                last_missing: holes[0].missing_to,
            },
            producer_bracket: analyzer_analysis::degradation::ProducerBracket {
                start_ms: Some(holes[0].bracket_start_ms),
                end_ms: Some(holes[0].bracket_end_ms),
            },
            engine_extent: None,
            incidents: 1,
        }],
    };
    let holder = analyzer_analysis::intervals::Holder {
        instance: "i1".to_string(),
        dp_rank: 0,
        group_idx: 0,
    };
    assert_eq!(
        ledger.register_for(&[holder], &w),
        Some(analyzer_analysis::degradation::Register::Indeterminate),
        "the window degrades through the existing gap path"
    );
}

/// The unattributable case: an object so corrupt not even an incarnation is knowable. The
/// load survives, the counter increments, and NOTHING is claimed — no event, no open, no
/// hole; a span claim for an unlocatable span would invent a fact.
#[test]
fn an_unattributable_corrupt_object_counts_and_claims_nothing() {
    let base = harness::rebase_offset() + 4_000_000.0;
    let root = harness::fresh_dir("unattr-store");
    plant(
        &root,
        &hour_of(base),
        "seg-run-x-7.jsonl",
        b"not json at all\nnot the final line either\n{\"kind\":\"store\"}\n",
    );
    let (source, _, _) = counting_source(&root, &harness::fresh_dir("unattr-cache"), 24 * 30);
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 10_000.0,
    };

    assert!(source
        .cache_events("f", &w)
        .expect("load survives")
        .is_empty());
    let opens = source
        .segment_opens_at_retention_horizon("f")
        .expect("opens survive");
    assert!(opens.is_empty(), "no fabricated open");
    assert!(
        analyzer_analysis::degradation::sequence_holes_from_events(&opens).is_empty(),
        "no fabricated hole — there is no sequence to have one in"
    );
    assert_eq!(source.quarantined(), 2, "counted per load, alarm-only");
}

// ================================================================================================
// u32_of fails closed: a present cell that does not read refuses the SEGMENT.
// ================================================================================================

/// `dp_rank: -1` read as ABSENT would be the single-worker zero, silently merging two
/// holders into one. It fails the segment's decode instead, and the quarantine path applies
/// per-object; the test twin propagates the same refusal, so the differential can see the
/// class instead of both sources sharing it.
#[test]
fn a_negative_dp_rank_quarantines_the_segment_rather_than_merging_holders() {
    let base = harness::rebase_offset() + 5_000_000.0;
    let root = harness::fresh_dir("negrank-store");
    let bad = format!(
        "{{\"kind\":\"segment_open\",\"instance_id\":\"i1\",\"incarnation\":\"run-n\",\"segment_seq\":0,\"at_ms\":{base}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"dp_rank\":-1,\"block_id\":\"n1\",\"n_tokens\":8,\"at_ms\":{}}}\n",
        base + 10.0
    );
    plant(&root, &hour_of(base), "seg-run-n-0.jsonl", bad.as_bytes());
    let (source, _, _) = counting_source(&root, &harness::fresh_dir("negrank-cache"), 24 * 30);
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 10_000.0,
    };

    let events = source.cache_events("f", &w).expect("the load survives");
    assert!(
        events.is_empty(),
        "the segment refused whole — no holder-zero event was minted: {events:#?}"
    );
    assert_eq!(source.quarantined(), 1);

    // The twin reads the SAME suite and fails its load outright — the naive reader has no
    // quarantine, which is exactly the difference that lets the differential see the class.
    let flat = harness::fresh_dir("negrank-flat");
    std::fs::write(flat.join("0001.jsonl"), bad.as_bytes()).expect("flat corpus");
    let err = BucketSource::new(&flat)
        .cache_events("f", &w)
        .expect_err("the twin must refuse the same cell");
    assert!(
        matches!(&err, analyzer_analysis::Error::Source(m) if m.contains("dp_rank")),
        "the twin's refusal is a source error that names the field: {err}"
    );
}

// ================================================================================================
// Candidate rules: production admits the shipped form only; the seam widens; undated
// archive drops are discovered on both.
// ================================================================================================

#[test]
fn production_discovery_matches_ingests_candidate_rule_and_the_seam_widens_it() {
    let base = harness::rebase_offset() + 6_000_000.0;
    let root = harness::fresh_dir("candidate-store");
    let hour = hour_of(base);
    let body = segment_body("run-c", 0, "i1", "c1", base);
    plant(&root, &hour, "seg-run-c-0.jsonl", body.as_bytes());
    plant(
        &root,
        &hour,
        "seg-run-c-0.parquet",
        &twin_of_jsonl(&body, 2),
    );
    plant(
        &root,
        &hour,
        "seg-run-c-0.parquet.tmp",
        b"conversion residue",
    );
    // An undated operator archive drop, under the fleet prefix but no dated hour — ingest
    // accepts these (`bucket.rs` retention gate note), so discovery must list them too.
    let archive = segment_body("run-arch", 0, "i2", "a1", base + 100.0);
    let dir = root.join("tap/archive");
    std::fs::create_dir_all(&dir).expect("archive dir");
    std::fs::write(dir.join("drop.parquet"), twin_of_jsonl(&archive, 2)).expect("archive drop");
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 10_000.0,
    };

    // Production: `.parquet` only — the write-ahead form and the residue are invisible by
    // construction — and the undated drop IS discovered.
    let production = ParquetSource::new(
        FsStore::new(&root),
        harness::fresh_dir("candidate-cache-prod"),
        "tap/",
        24 * 30,
    );
    let events = production.cache_events("f", &w).expect("production load");
    let instances: BTreeSet<&str> = events.iter().map(|e| e.instance.as_str()).collect();
    assert_eq!(
        instances,
        ["i1", "i2"].into_iter().collect(),
        "the shipped twin and the undated archive drop, nothing else: {events:#?}"
    );
    assert_eq!(
        events.len(),
        2,
        "the JSONL sibling is not a candidate, so nothing double-folds"
    );
    assert_eq!(
        production.quarantined(),
        0,
        "the .tmp residue was never fetched"
    );

    // The corpus seam admits the write-ahead form — and content dedup still folds the
    // JSONL/Parquet twin pair once, because they are different BYTES but the twin test
    // above pins byte-identical pairs; here the two forms carry the same records and the
    // fold sees both files.
    let corpus = ParquetSource::new(
        FsStore::new(&root),
        harness::fresh_dir("candidate-cache-corpus"),
        "tap/",
        24 * 30,
    )
    .admitting_corpus_forms();
    let widened = corpus.cache_events("f", &w).expect("corpus load");
    assert!(
        widened.len() > events.len(),
        "the seam admits the write-ahead form the production rule refuses"
    );
}

// ================================================================================================
// Fetch-once at the horizon edge, and the cache-read fallback.
// ================================================================================================

/// A segment whose SHIP hour sits behind the horizon but inside the listing margin is
/// listable by every wall-domain load — so it must not be evictable, or every pass pays a
/// re-fetch for it. Two passes, every key fetched exactly once, including the edge.
#[test]
fn fetch_once_holds_for_a_segment_at_the_horizon_edge_across_two_passes() {
    let horizon = 24 * 30i64;
    let now = harness::now_ms();
    #[allow(clippy::cast_precision_loss)] // horizons in hours are far below 2^53
    let edge_ms = now - (horizon as f64) * 3_600_000.0 - 3_600_000.0; // one hour past the horizon
    let root = harness::fresh_dir("edge-store");
    plant(
        &root,
        &hour_of(edge_ms),
        "seg-run-e-0.jsonl",
        segment_body("run-e", 0, "i1", "e1", edge_ms).as_bytes(),
    );
    plant(
        &root,
        &hour_of(now),
        "seg-run-e-1.jsonl",
        segment_body("run-e", 1, "i1", "e2", now).as_bytes(),
    );
    let (source, fetches, _) = counting_source(&root, &harness::fresh_dir("edge-cache"), horizon);

    for _ in 0..2 {
        // The wall-domain load lists horizon + margin, which reaches the edge segment.
        source
            .lifecycle_events("f", edge_ms - 1.0, now + 1.0)
            .expect("wall load");
        source
            .segment_opens_at_retention_horizon("f")
            .expect("horizon load");
    }
    let counts = fetches.lock().expect("fetch lock");
    assert_eq!(
        counts.len(),
        2,
        "both segments were discovered: {counts:#?}"
    );
    assert!(
        counts.values().all(|&n| n == 1),
        "fetch-once holds across passes, edge segment included — an eviction floor at the \
         bare horizon deletes what the margin-widened listing re-lists: {counts:#?}"
    );
}

/// A cache entry that exists but cannot be READ is a miss, not an error: the read falls back
/// to a re-fetch and the load succeeds. (The old `is_file()`-then-read pair turned a file
/// deleted or wrecked between check and read into a failed load.)
#[test]
#[cfg(unix)]
fn an_unreadable_cache_entry_refetches_instead_of_failing() {
    use std::os::unix::fs::PermissionsExt;
    let base = harness::rebase_offset() + 7_000_000.0;
    let root = harness::fresh_dir("toctou-store");
    let key = format!("tap/{}/n1/seg-run-t-0.jsonl", hour_of(base));
    plant(
        &root,
        &hour_of(base),
        "seg-run-t-0.jsonl",
        segment_body("run-t", 0, "i1", "t1", base).as_bytes(),
    );
    let cache = harness::fresh_dir("toctou-cache");
    let (source, fetches, _) = counting_source(&root, &cache, 24 * 30);
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 10_000.0,
    };

    let warm = source.cache_events("f", &w).expect("warm pass");
    assert_eq!(warm.len(), 1);
    // Wreck the cached entry between passes: it exists, and it does not read.
    let cached = cache.join(&key);
    std::fs::set_permissions(&cached, std::fs::Permissions::from_mode(0o000))
        .expect("chmod the cache entry");
    if std::fs::read(&cached).is_ok() {
        // Running as root: the entry is readable regardless and the fallback is
        // unobservable here; the deleted-entry case is covered by the cold-cache test.
        return;
    }
    let again = source
        .cache_events("f", &w)
        .expect("an unreadable entry is a miss");
    assert_eq!(warm, again, "the re-fetched answer is the same answer");
    let counts = fetches.lock().expect("fetch lock");
    assert_eq!(
        counts.values().sum::<usize>(),
        2,
        "the unreadable entry cost exactly one re-fetch: {counts:#?}"
    );
}

// ================================================================================================
// The recovered truncation reaches the ledger (the new cause's guard).
// ================================================================================================

/// **The flattering direction this closes:** a store whose deciding close died in an
/// attributed recovery's torn tail leaves NO sequence hole — the recovered segment keeps its
/// `segment_seq` — so before this cause the window read clean and the store read cold.
/// The producer's own `dropped_bytes` declaration now degrades the recovered segment's span
/// to indeterminate.
#[test]
#[allow(clippy::too_many_lines)] // one narrative: the incident, its register, and the controls
fn an_attributed_recovery_with_dropped_bytes_reads_indeterminate_not_clean() {
    let base = harness::rebase_offset() + 8_000_000.0;
    let root = harness::fresh_dir("recovery-store");
    let hour = hour_of(base);
    // The recovered segment: its own header survived (attributed), the recovery marker rides
    // inside it, and the evict that would have closed p1 died in the discarded tail.
    let recovered = format!(
        "{{\"kind\":\"segment_open\",\"instance_id\":\"i1\",\"incarnation\":\"run-r\",\"segment_seq\":0,\"at_ms\":{base}}}\n\
         {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"p1\",\"n_tokens\":16,\"at_ms\":{}}}\n\
         {{\"kind\":\"segment_recovered\",\"at_ms\":{},\"attributed\":true,\"dropped_bytes\":137}}\n",
        base + 10.0,
        base + 5_000.0
    );
    plant(&root, &hour, "seg-run-r-0.jsonl", recovered.as_bytes());
    let source = ParquetSource::new(
        FsStore::new(&root),
        harness::fresh_dir("recovery-cache"),
        "tap/",
        24 * 30,
    )
    .admitting_corpus_forms();
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 10_000.0,
    };

    let lifecycle = source
        .lifecycle_events("f", w.start_ms, w.end_ms)
        .expect("lifecycle");
    let opens = source
        .segment_opens_at_retention_horizon("f")
        .expect("opens");
    assert!(
        analyzer_analysis::degradation::sequence_holes_from_events(&opens).is_empty(),
        "an attributed recovery participates in continuity normally — NO hole exists, which \
         is why this cause has to"
    );

    let incidents = analyzer_analysis::degradation::recovered_truncation_incidents_from_events(
        &lifecycle, &opens,
    );
    assert_eq!(incidents.len(), 1, "one declared truncation: {incidents:?}");
    assert_eq!(incidents[0].incarnation, "run-r");
    assert_eq!(
        incidents[0].cause,
        analyzer_analysis::degradation::Cause::RecoveredTruncation { dropped_bytes: 137 }
    );
    assert_eq!(
        incidents[0].cause.register(),
        analyzer_analysis::degradation::Register::Indeterminate,
        "indeterminate, never cold-by-silence"
    );
    // The bracket is the recovered segment's own span: its open to the recovery stamp.
    assert_eq!(incidents[0].bracket.start_ms, Some(base));
    assert_eq!(incidents[0].bracket.end_ms, Some(base + 5_000.0));

    // And through the ledger's own composition: the run's instance reads indeterminate.
    let entry = analyzer_analysis::degradation::Degradation {
        scope: analyzer_analysis::degradation::Scope::Run {
            incarnation: incidents[0].incarnation.clone(),
            instances: vec!["i1".to_string()],
        },
        cause: incidents[0].cause.clone(),
        producer_bracket: incidents[0].bracket,
        engine_extent: None,
        incidents: 1,
    };
    let holder = analyzer_analysis::intervals::Holder {
        instance: "i1".to_string(),
        dp_rank: 0,
        group_idx: 0,
    };
    assert_eq!(
        analyzer_analysis::degradation::Ledger {
            entries: vec![entry]
        }
        .register_for(&[holder], &w),
        Some(analyzer_analysis::degradation::Register::Indeterminate)
    );

    // The negative controls the cause ratchet demands: an unattributed recovery claims no
    // span — as the pre-1.11 headerless orphan file, admitted under 4.1's tolerance, AND as
    // the 1.11 form, the recovering incarnation's `attributed: false` declaration inside its
    // own headered segment (spec 4.3), whose stamped incarnation must not tempt a claim —
    // and a clean recovery (zero bytes dropped) claims nothing at all.
    let unattributed = format!(
        "{{\"kind\":\"segment_recovered\",\"at_ms\":{},\"attributed\":false,\"dropped_bytes\":9}}\n",
        base + 6_000.0
    );
    std::fs::write(
        root.join("tap")
            .join(&hour)
            .join("n1")
            .join("orphan-1.jsonl"),
        unattributed.as_bytes(),
    )
    .expect("orphan");
    let controls = format!(
        "{{\"kind\":\"segment_open\",\"instance_id\":\"i1\",\"incarnation\":\"run-s\",\"segment_seq\":0,\"at_ms\":{base}}}\n\
         {{\"kind\":\"segment_recovered\",\"at_ms\":{},\"attributed\":false,\"dropped_bytes\":9}}\n\
         {{\"kind\":\"segment_recovered\",\"at_ms\":{},\"attributed\":true,\"dropped_bytes\":0}}\n",
        base + 6_100.0,
        base + 6_200.0
    );
    plant(&root, &hour, "seg-run-s-0.jsonl", controls.as_bytes());
    let source2 = ParquetSource::new(
        FsStore::new(&root),
        harness::fresh_dir("recovery-cache-2"),
        "tap/",
        24 * 30,
    )
    .admitting_corpus_forms();
    let lifecycle2 = source2
        .lifecycle_events("f", w.start_ms, w.end_ms)
        .expect("lifecycle");
    let incidents2 = analyzer_analysis::degradation::recovered_truncation_incidents_from_events(
        &lifecycle2,
        &opens,
    );
    assert_eq!(
        incidents2.len(),
        1,
        "neither unattributed shape nor a zero-byte recovery mints an incident — only the \
         original declared truncation stands: {incidents2:?}"
    );
    assert_eq!(incidents2[0].incarnation, "run-r");
}

// ================================================================================================
// The memo: one derive-lifetime wrapper, each load shape discovered and decoded once.
// ================================================================================================

#[test]
fn the_memo_runs_each_load_shape_once() {
    let base = harness::rebase_offset() + 9_000_000.0;
    let root = harness::fresh_dir("memo-store");
    plant(
        &root,
        &hour_of(base),
        "seg-run-m-0.jsonl",
        segment_body("run-m", 0, "i1", "m1", base).as_bytes(),
    );
    let (source, fetches, lists) =
        counting_source(&root, &harness::fresh_dir("memo-cache"), 24 * 30);
    let memo = analyzer_pipeline::memo::MemoSource::new(&source);
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 10_000.0,
    };

    let first = memo.cache_events("f", &w).expect("first load");
    memo.lifecycle_events("f", w.start_ms, w.end_ms)
        .expect("lifecycle");
    memo.segment_opens_at_retention_horizon("f").expect("opens");
    memo.lifecycle_declarations(
        "f",
        &analyzer_analysis::Window {
            start_ms: 0.0,
            end_ms: 4.0e12,
        },
    )
    .expect("declarations");
    let after_first: usize = *lists.lock().expect("list lock");
    assert!(after_first > 0, "the shapes ran discovery at all");

    // The fan-out: every shape re-asked, several times — no further discovery, no further
    // fetch, identical answers.
    for _ in 0..5 {
        assert_eq!(memo.cache_events("f", &w).expect("memoized"), first);
        memo.lifecycle_events("f", w.start_ms, w.end_ms)
            .expect("memoized");
        memo.segment_opens_at_retention_horizon("f")
            .expect("memoized");
        memo.lifecycle_declarations(
            "f",
            &analyzer_analysis::Window {
                start_ms: 0.0,
                end_ms: 4.0e12,
            },
        )
        .expect("memoized");
    }
    assert_eq!(
        *lists.lock().expect("list lock"),
        after_first,
        "a repeated load shape runs NO further discovery"
    );
    assert!(
        fetches
            .lock()
            .expect("fetch lock")
            .values()
            .all(|&n| n == 1),
        "and no further fetch"
    );

    // A DIFFERENT shape is a different load: the memo must not serve a stale answer to it.
    let narrower = Window {
        start_ms: base + 5.0,
        end_ms: base + 6.0,
    };
    assert!(
        memo.cache_events("f", &narrower)
            .expect("new shape")
            .is_empty(),
        "a new window is a new key, answered from the source"
    );
}

/// The live horizon: one owner writes, a later `source()` construction reads the moved value.
#[test]
fn the_shared_horizon_is_live() {
    let horizon = analyzer_pipeline::parquet_source::HorizonHours::fixed(24 * 14);
    let holder = horizon.clone(); // the API's long-lived copy
    horizon.set(24 * 3); // the ladder sheds
    assert_eq!(holder.hours(), 24 * 3, "every clone reads the moved value");
}

// ================================================================================================
// The two file sources over one corpus, no engine anywhere.
// ================================================================================================

#[test]
fn the_two_file_sources_yield_the_same_ir_over_one_corpus() {
    let base = harness::rebase_offset() + 1_000_000.0;
    let hour = hour_of(base);
    let flat = harness::fresh_dir("two-file-flat");
    let dated = harness::fresh_dir("two-file-dated");
    // File order deliberately not the IR's order, so the sort is under test here too.
    let bodies = [
        (
            "0001.jsonl",
            format!(
                "{{\"kind\":\"segment_open\",\"instance_id\":\"i1\",\"incarnation\":\"run-a\",\"segment_seq\":0,\"at_ms\":{}}}\n\
                 {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"s2\",\"n_tokens\":2,\"at_ms\":{}}}\n\
                 {{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"s1\",\"n_tokens\":2,\"at_ms\":{}}}\n\
                 {{\"kind\":\"evict\",\"instance_id\":\"i1\",\"block_id\":\"s1\",\"at_ms\":{}}}",
                base,
                base + 30.0,
                base + 10.0,
                base + 10.0
            ),
        ),
        (
            "0002.jsonl",
            format!(
                "{{\"kind\":\"segment_open\",\"instance_id\":\"i2\",\"incarnation\":\"run-b\",\"segment_seq\":0,\"at_ms\":{}}}\n\
                 {{\"kind\":\"agent_start\",\"instance_id\":\"i2\",\"incarnation\":\"run-b\",\"at_ms\":{}}}\n\
                 {{\"kind\":\"store\",\"instance_id\":\"i2\",\"dp_rank\":1,\"group_idx\":0,\"block_id\":\"t1\",\"tier\":\"gpu\",\"n_tokens\":4,\"at_ms\":{}}}",
                base + 40.0,
                base + 41.0,
                base + 50.0
            ),
        ),
    ];
    for (name, body) in &bodies {
        std::fs::write(flat.join(name), body).expect("flat corpus");
        plant(&dated, &hour, name, body.as_bytes());
    }

    let bucket = BucketSource::new(&flat);
    let source = ParquetSource::new(
        FsStore::new(&dated),
        harness::fresh_dir("two-file-cache"),
        "tap/",
        24 * 30,
    )
    .admitting_corpus_forms();
    let w = Window {
        start_ms: base - 1.0,
        end_ms: base + 1_000.0,
    };

    // Same bytes on both sides, so everything must agree — the delivery identity included,
    // because it is a function of the bytes alone.
    assert_eq!(
        bucket.cache_events("f", &w).expect("bucket cache"),
        source.cache_events("f", &w).expect("parquet cache"),
        "the two file sources must build the same cache-fact IR"
    );
    assert_eq!(
        bucket
            .lifecycle_events("f", w.start_ms, w.end_ms)
            .expect("bucket lifecycle"),
        source
            .lifecycle_events("f", w.start_ms, w.end_ms)
            .expect("parquet lifecycle"),
        "the two file sources must build the same lifecycle IR"
    );
    assert_eq!(
        bucket
            .segment_opens_at_retention_horizon("f")
            .expect("bucket opens"),
        source
            .segment_opens_at_retention_horizon("f")
            .expect("parquet opens"),
        "the two file sources must observe the same segment opens"
    );
}
