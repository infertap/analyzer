// SPDX-License-Identifier: AGPL-3.0-only

//! The bucket reader against a real S3 endpoint: listing, pruning, commit, and the re-list
//! margin that recovers a cycle's uncommitted objects, into an input log of its own, over
//! the endpoint `INFERTAP_TEST_S3_URL` / `_KEY` / `_SECRET` name.
//!
//! The objects below are written in the write-ahead JSONL form and admitted through the
//! corpus seam (`admit_corpus_forms`), the same admission the derivation's source carries —
//! authoring Parquet here would re-implement the tap's converter to test a transport that
//! dispatches on content anyway. This header once carried a STALE notice: the candidate
//! rule had gone Parquet-only under it and nothing noticed for as long as no environment
//! ran the test, which the release battery now does on every tag.
//!
//! Skips loudly when the environment carries no bucket, so a bucket-less `--ignored`
//! sweep stays green — the reference rig is the named trigger for making this
//! unconditional. Test objects are written through the store, the transport the product
//! uses.

mod common;

use analyzer_ingest::bucket::ingest_bucket;
use analyzer_objectstore::{S3Store, StaticCredentials};

/// The same endpoint and credential `storage::testing` injects into the collection, as a
/// store. `None` without the env: env-gated means SKIP, never panic — a credential-less
/// `--ignored` sweep must be able to run the hermetic set without these tests failing it
/// for the crime of having no live bucket. Each test early-returns on `None`.
fn test_store() -> Option<S3Store> {
    let Ok(url) = std::env::var("INFERTAP_TEST_S3_URL") else {
        eprintln!("skipped: INFERTAP_TEST_S3_URL not set — this test needs a live bucket");
        return None;
    };
    let creds = StaticCredentials {
        access_key_id: std::env::var("INFERTAP_TEST_S3_KEY").unwrap_or_default(),
        secret_access_key: std::env::var("INFERTAP_TEST_S3_SECRET").unwrap_or_default(),
        session_token: None,
    };
    Some(S3Store::open(&url, Some(&creds)).expect("a store over the test endpoint"))
}

/// **Writes through the store, not through the engine**: the test carries the same transport the
/// product does.
fn put(store: &S3Store, rel: &str, lines: &[&str]) {
    use analyzer_objectstore::ObjectStore as _;
    let mut body = String::new();
    for l in lines {
        body.push_str(l);
        body.push('\n');
    }
    store.put(rel, body.as_bytes()).expect("put object");
}

fn store_line(at_ms: f64, block: &str) -> String {
    format!(
        "{{\"kind\":\"store\",\"instance_id\":\"i1\",\"block_id\":\"{block}\",\
         \"n_tokens\":4,\"at_ms\":{at_ms}}}"
    )
}

#[test]
#[ignore = "live bucket: needs a live bucket: INFERTAP_TEST_S3_URL/_KEY/_SECRET; skips loudly when absent"]
fn a_spent_slice_deadline_defers_the_rest_and_the_next_cycle_continues() {
    let Some(store) = test_store() else { return };
    let lake = common::fresh_lake("bucket-live");
    let pid = std::process::id();
    let prefix = format!("bd-{pid}/");
    let fleet = format!("bd{pid}");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    for i in 0..3u32 {
        let line = store_line(now + f64::from(i) * 1000.0, &format!("bd{i}"));
        put(
            &store,
            &format!("{prefix}2026-08-06/10/n1/orphan-{i}.jsonl"),
            &[line.as_str()],
        );
    }
    let fetch = analyzer_ingest::bucket::discover(&lake, &store, &prefix, true).expect("discover");
    assert_eq!(fetch.len(), 3);
    // A deadline already in the past: the FIRST file still runs (a cycle is never a
    // silent no-op), the rest are counted deferred and never fetched.
    let r = analyzer_ingest::bucket::ingest_discovered(
        &lake,
        &store,
        &fleet,
        fetch,
        analyzer_ingest::bucket::Slice {
            // now() is already spent by the time the second file checks it
            deadline: Some(std::time::Instant::now()),
            workers: None,
        },
        &mut |_| {},
    )
    .expect("budgeted cycle");
    assert_eq!(
        (r.files_seen, r.files_ingested, r.files_deferred),
        (3, 1, 2),
        "one file of progress, the rest deferred and counted"
    );
    assert!(r.bytes_deferred > 0, "deferred bytes ride the report too");
    // Deferred means untouched: the next cycle's own discovery re-lists exactly the
    // remainder and finishes the job.
    let r2 = ingest_bucket(&lake, &store, &fleet, &prefix, true).expect("continuation");
    assert_eq!((r2.files_ingested, r2.files_deferred), (2, 0));
}

#[test]
#[ignore = "live bucket: needs a live bucket: INFERTAP_TEST_S3_URL/_KEY/_SECRET; skips loudly when absent"]
fn bucket_ingest_lists_fetches_commits_and_repairs() {
    let Ok(_url) = std::env::var("INFERTAP_TEST_S3_URL") else {
        eprintln!("SKIPPED: no INFERTAP_TEST_S3_URL — the bucket reader ran against nothing");
        return;
    };
    let Some(store) = test_store() else { return };
    let lake = common::fresh_lake("bucket-live");
    // Per-run key prefix and fleet: runs never see each other's objects or ledgers.
    let pid = std::process::id();
    let prefix = format!("bt-{pid}/");
    let fleet = format!("bt{pid}");
    // Producer clock = now: the input log retires days by event time, so a fixture stamped
    // past the retention horizon is forgotten at the first maintenance and every count reads
    // zero — which this test learned the measured way.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);

    // Two sealed segments under dated hour prefixes.
    let header = |seq: u32, at: f64| {
        format!(
            "{{\"kind\":\"segment_open\",\"at_ms\":{at},\"instance_id\":\"i1\",\
             \"incarnation\":\"a\",\"segment_seq\":{seq}}}"
        )
    };
    let seg_a = [header(1, now), store_line(now + 1000.0, "b1")];
    let seg_b = [
        header(2, now + 3_600_000.0),
        store_line(now + 3_600_000.0, "b2"),
    ];
    let a_refs: Vec<&str> = seg_a.iter().map(String::as_str).collect();
    let b_refs: Vec<&str> = seg_b.iter().map(String::as_str).collect();
    put(
        &store,
        &format!("{prefix}2026-08-06/10/n1/seg-a-1.jsonl"),
        &a_refs,
    );
    put(
        &store,
        &format!("{prefix}2026-08-06/11/n1/seg-a-2.jsonl"),
        &b_refs,
    );

    // First cycle: both listed, both committed, no arrival stamps, identity recorded.
    let r1 = ingest_bucket(&lake, &store, &fleet, &prefix, true).expect("first cycle");
    assert_eq!((r1.files_seen, r1.files_ingested), (2, 2));
    assert_eq!(r1.records_inserted, 4);
    let identified = common::committed(&lake)
        .iter()
        .filter(|b| !b.object_key.is_empty() && !b.etag.is_empty() && b.carrier == "s3")
        .count();
    let stored: u64 = common::stored_mass_of(&lake)
        .lines()
        .filter_map(|l| l.split('\t').nth(3)?.parse::<u64>().ok())
        .sum();
    assert_eq!(
        (stored, identified),
        (2, 2),
        "both stores landed in the denominator; both records carry (key, etag)"
    );

    // Second cycle: the listing predicate prunes everything — nothing is even SEEN,
    // which is the measured property the whole design stands on.
    let r2 = ingest_bucket(&lake, &store, &fleet, &prefix, true).expect("second cycle");
    assert_eq!(
        (r2.files_seen, r2.files_ingested, r2.duplicates_skipped),
        (0, 0, 0),
        "a steady-state cycle with no new objects touches no object at all"
    );

    // A new object under a later hour: it alone is fetched.
    let seg_c = [store_line(now + 7_200_000.0, "b3")];
    let c_refs: Vec<&str> = seg_c.iter().map(String::as_str).collect();
    put(
        &store,
        &format!("{prefix}2026-08-06/12/n1/orphan-1.jsonl"),
        &c_refs,
    );
    let r3 = ingest_bucket(&lake, &store, &fleet, &prefix, true).expect("third cycle");
    assert_eq!((r3.files_seen, r3.files_ingested), (1, 1));

    assert_margin_recovery(&lake, &store, (&fleet, &prefix), now);
}

/// A cycle that fetched an object and never committed leaves no pending state: the log's
/// watermark did not move past it, so the object sits inside the re-list margin, absent from
/// the committed records, and the next cycle lists it and records it once.
fn assert_margin_recovery(
    lake: &analyzer_lake::Lake,
    store: &S3Store,
    (fleet, prefix): (&str, &str),
    now: f64,
) {
    let missed = [store_line(now + 7_300_000.0, "b4")];
    let refs: Vec<&str> = missed.iter().map(String::as_str).collect();
    put(
        store,
        &format!("{prefix}2026-08-06/12/n1/orphan-2.jsonl"),
        &refs,
    );
    let recovered = ingest_bucket(lake, store, fleet, prefix, true).expect("recovery cycle");
    assert_eq!(
        recovered.files_ingested, 1,
        "the object inside the margin is listed and recorded"
    );
    // The fleet's whole denominator: b1, b2, b3 from the cycles before, b4 once.
    let stored: u64 = common::stored_mass_of(lake)
        .lines()
        .filter_map(|l| l.split('\t').nth(3)?.parse::<u64>().ok())
        .sum();
    assert_eq!(stored, 4, "four stores in the denominator — never doubled");
}

/// **the fast path over the real transport**: a shipped-form Parquet
/// segment — the conformance corpus's own sealed fixture — planted in the bucket,
/// listed, fetched, and committed through the projected walk, in the production
/// posture (`admit_corpus_forms = false`: shipped Parquet is admitted by suffix, no rig
/// seam involved). The corpus equivalence pin proves the two shapings agree; this proves
/// the wired path actually ENGAGES end to end — listing, GET, dispatch, the projected walk
/// — and `projected_files` is the witness that it was the fast path and not a silent
/// fall-through to the reference.
#[test]
#[ignore = "live bucket: needs a live bucket: INFERTAP_TEST_S3_URL/_KEY/_SECRET; skips loudly when absent"]
fn a_shipped_parquet_segment_rides_the_projected_walk_end_to_end() {
    use analyzer_objectstore::ObjectStore as _;
    if std::env::var("INFERTAP_TEST_S3_URL").is_err() {
        eprintln!("SKIPPED: no INFERTAP_TEST_S3_URL — the projected walk ran against nothing");
        return;
    }
    let Some(store) = test_store() else { return };
    let lake = common::fresh_lake("bucket-live");
    // The fixture's event times age, and nothing here needs a widened horizon any more:
    // ingest lands no event row, the denominator carries the rollup's
    // own retention, and the arrivals rows key their TTL on the ingest clock.

    let pid = std::process::id();
    let prefix = format!("pt-{pid}/");
    let fleet = format!("pt{pid}");
    let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../../contract/inference-events/conformance/delivery/seg-1785153670000-4242-0.parquet",
    );
    let bytes = std::fs::read(&fixture).expect("the corpus's sealed Parquet fixture");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
    let hour = analyzer_lake::dated::hour_prefixes(now, now)
        .pop()
        .expect("a dated hour for the plant");
    let key = format!("{prefix}{hour}/n1/seg-1785153670000-4242-0.parquet");
    store.put(&key, &bytes).expect("put the sealed segment");

    let report = ingest_bucket(&lake, &store, &fleet, &prefix, false).expect("ingest cycle");
    assert_eq!(
        report.files_ingested, 1,
        "the shipped form is admitted by suffix"
    );
    assert_eq!(
        report.projected_files, 1,
        "a sealed Parquet segment with no unknown kinds must ride the fast path — a \
         fall-through here means the dispatch silently declined and nothing measured why"
    );
    let batches = common::committed(&lake);
    let stored: u64 = batches
        .iter()
        .flat_map(|b| &b.stored)
        .map(|m| m.records)
        .sum();
    let segments = batches.iter().map(|b| b.segments.len()).sum::<usize>();
    assert_eq!(
        (stored, segments),
        (6, 3),
        "the fixture's six cache facts land in the denominator through the projected walk, \
         which also derives the arrivals rows (the run-level row plus instances i0 and i1)"
    );
}
