// SPDX-License-Identifier: AGPL-3.0-only

//! The ingest worker pool against a memory-backed store: every file commits exactly
//! once, duplicate content under two keys commits once however the workers race, a
//! second cycle is all duplicates, and a missing object fails the cycle rather than
//! vanishing. Hermetic — an input log of its own and no network — because the pool's
//! correctness claims (the claim set, the per-worker siblings, the coordinator fold)
//! are about OUR code, not about S3.

mod common;

use analyzer_objectstore::{ObjectMeta, ObjectStore};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

/// The store the pool's claims are tested against: a map behind a mutex, sibling by
/// `Arc` clone — the cheapest honest implementation of the contract, including its
/// `Send + Sync` supertraits.
struct MemStore(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

impl MemStore {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(BTreeMap::new())))
    }
    fn put_object(&self, key: &str, body: &[u8]) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(key.to_string(), body.to_vec());
    }
}

impl ObjectStore for MemStore {
    fn location(&self) -> String {
        format!("mem://{:p}", std::sync::Arc::as_ptr(&self.0))
    }
    fn sibling(&self) -> Result<Box<dyn ObjectStore>, analyzer_objectstore::Error> {
        Ok(Box::new(Self(Arc::clone(&self.0))))
    }
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, analyzer_objectstore::Error> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| ObjectMeta {
                key: k.clone(),
                size: v.len() as u64,
                etag: None,
            })
            .collect())
    }
    fn list_after(
        &self,
        prefix: &str,
        after: &str,
    ) -> Result<Vec<ObjectMeta>, analyzer_objectstore::Error> {
        Ok(self
            .list(prefix)?
            .into_iter()
            .filter(|m| m.key.as_str() > after)
            .collect())
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, analyzer_objectstore::Error> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(key)
            .cloned()
            .ok_or_else(|| analyzer_objectstore::Error::Other(format!("no such object: {key}")))
    }
    fn put(&self, key: &str, body: &[u8]) -> Result<(), analyzer_objectstore::Error> {
        self.put_object(key, body);
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), analyzer_objectstore::Error> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(key);
        Ok(())
    }
}

/// One distinct minimal segment: an attributed open and two stores — distinct block
/// ids and instants per index, so every file is distinct content with two records.
fn segment(i: u32) -> String {
    // A CURRENT clock, not an epoch-zero one: the deltas table's retention TTL filters
    // already-expired rows AT INSERT, so a 1970-era fixture would land zero delta rows
    // while the ledger counted a successful commit — the first draft of this test
    // proved that the hard way.
    #[allow(clippy::cast_precision_loss)]
    let base_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_millis() as f64);
    let open = format!(
        r#"{{"kind":"segment_open","at_ms":{base_ms},"contract_version":"1.10","incarnation":"1700000000000-{i}","segment_seq":{i},"heartbeat_secs":60,"max_segment_secs":300,"key_epoch":0,"gpu":"fixture","vllm_version":"0.27.1","model":"m","pod_identity":"pod-{i}"}}"#
    );
    let store_a = format!(
        r#"{{"kind":"store","at_ms":{at},"instance_id":"n{i}","block_id":"b{i}a","n_tokens":16,"tier":"GPU","dp_rank":0,"group_idx":0,"seq":1}}"#,
        at = base_ms + 500.0 + f64::from(i)
    );
    let store_b = format!(
        r#"{{"kind":"store","at_ms":{at},"instance_id":"n{i}","block_id":"b{i}b","n_tokens":16,"tier":"GPU","dp_rank":0,"group_idx":0,"seq":2}}"#,
        at = base_ms + 600.0 + f64::from(i)
    );
    // One liveness record, so the arrivals fold has a clock: a segment with no
    // liveness rows correctly banks NO arrivals row (the cache-only orphan rule), and
    // the first draft of this fixture proved that by asserting arrivals against a
    // fixture that could never produce them.
    let hb = format!(
        r#"{{"kind":"heartbeat","at_ms":{at},"msgs_seen":2,"dropped":0,"oversized":0,"endpoints":[{{"source":"n{i}","endpoint":"tcp://h:1","msgs_seen":2,"dropped":0}}]}}"#,
        at = base_ms + 700.0 + f64::from(i)
    );
    format!("{open}\n{store_a}\n{store_b}\n{hb}\n")
}

fn fetch_list(store: &MemStore) -> Vec<analyzer_ingest::bucket::Discovered> {
    store
        .list("")
        .expect("list")
        .into_iter()
        .map(|m| analyzer_ingest::bucket::Discovered {
            key: m.key,
            size: m.size,
            etag: String::new(),
        })
        .collect()
}

#[test]
fn the_pool_commits_every_file_once_and_the_claim_set_holds() {
    let lake = common::fresh_lake("bucket-pool");

    let store = MemStore::new();
    // Built once and reused for the duplicate keys: `segment` stamps the current
    // clock, so a second call is different BYTES — a duplicate must share the exact
    // bytes or it is not one.
    let segments: Vec<String> = (0..12u32).map(segment).collect();
    for (i, seg) in segments.iter().enumerate() {
        store.put_object(&format!("seg/{i:02}.jsonl"), seg.as_bytes());
    }
    // Two more keys carrying content the store already holds: however the workers
    // race, the claim set must let exactly one commit per content hash.
    store.put_object("seg/dup-of-03.jsonl", segments[3].as_bytes());
    store.put_object("seg/dup-of-07.jsonl", segments[7].as_bytes());

    let mut committed_records = 0u64;
    let report = analyzer_ingest::bucket::ingest_discovered(
        &lake,
        &store,
        "pool-fleet",
        fetch_list(&store),
        analyzer_ingest::bucket::Slice::default(),
        &mut |n| committed_records += n,
    )
    .expect("first cycle");
    assert_eq!(report.files_seen, 14);
    assert_eq!(
        report.files_ingested, 12,
        "distinct content commits once each"
    );
    assert_eq!(
        report.duplicates_skipped, 2,
        "same content under a new key skips"
    );
    assert_eq!(report.records_inserted, 12 * 4);
    assert_eq!(
        committed_records,
        12 * 4,
        "the progress hook saw every commit"
    );

    let batches = common::committed(&lake);
    assert_eq!(
        batches.len(),
        12,
        "one committed batch per distinct content"
    );
    let denominator: u64 = common::stored_mass_of(&lake)
        .lines()
        .filter_map(|l| l.split('\t').nth(3)?.parse::<u64>().ok())
        .sum();
    assert_eq!(
        denominator, 24,
        "two stores per distinct segment, each segment's denominator landed once"
    );
    // The cycle's bank committed once: every distinct segment's arrivals ride its record.
    assert!(
        batches.iter().all(|b| !b.segments.is_empty()),
        "one cycle commit carries every batch's arrivals"
    );
    assert_eq!(
        batches
            .iter()
            .map(|b| b.input_position)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1,
        "the cycle committed under one position"
    );

    // Second cycle over the same keys: the input log answers for every one of them.
    let second = analyzer_ingest::bucket::ingest_discovered(
        &lake,
        &store,
        "pool-fleet",
        fetch_list(&store),
        analyzer_ingest::bucket::Slice::default(),
        &mut |_| {},
    )
    .expect("second cycle");
    assert_eq!(second.files_ingested, 0);
    assert_eq!(second.duplicates_skipped, 14);
}

#[test]
fn a_missing_object_fails_the_cycle_instead_of_vanishing() {
    let lake = common::fresh_lake("bucket-pool-err");

    let store = MemStore::new();
    store.put_object("seg/00.jsonl", segment(0).as_bytes());
    let mut fetch = fetch_list(&store);
    fetch.push(analyzer_ingest::bucket::Discovered {
        key: "seg/ghost.jsonl".to_string(),
        size: 64,
        etag: String::new(),
    });

    let err = analyzer_ingest::bucket::ingest_discovered(
        &lake,
        &store,
        "pool-err",
        fetch,
        analyzer_ingest::bucket::Slice::default(),
        &mut |_| {},
    )
    .expect_err("a listed object that cannot be fetched is a cycle error");
    assert!(
        matches!(&err, analyzer_ingest::Error::File { key, .. } if key.contains("ghost")),
        "the error names the object: {err}"
    );
}
