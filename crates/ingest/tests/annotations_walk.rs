// SPDX-License-Identifier: AGPL-3.0-only

//! The annotations walk into an input log of its own: objects in, declarations recorded in the input
//! log, deduped by the log's head — and the property the whole design exists for, pinned end
//! to end: an object written by `declare` (the CLI's and the API's door) and one a pipeline
//! wrote are recorded the same way, so a declaration and its replay cannot drift. The core
//! states each recorded declaration as its marker (`pipeline::inputs`).
//!
//! The store is an in-memory [`ObjectStore`]: the transport itself is `bucket_live.rs`'s
//! subject; everything here is about what the walk does with objects once it can list them.

mod common;

use std::collections::BTreeMap;
use std::sync::Mutex;

use analyzer_ingest::annotations::{
    declare, encode, ingest_annotations, object_key, DeployAnnotation, DeploySource,
    ANNOTATIONS_PREFIX, ANNOTATION_MAX_BYTES,
};
use analyzer_objectstore::{ObjectMeta, ObjectStore};

/// A bucket in a map. The etag is the content hash, which is exactly the property the walk's
/// pre-fetch dedup leans on a real store for.
#[derive(Default)]
struct MemStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    gets: Mutex<u64>,
}

impl ObjectStore for MemStore {
    fn location(&self) -> String {
        format!("mem://{:p}", std::ptr::from_ref(self))
    }
    fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, analyzer_objectstore::Error> {
        self.list_after(prefix, "")
    }

    fn sibling(&self) -> Result<Box<dyn ObjectStore>, analyzer_objectstore::Error> {
        Err(analyzer_objectstore::Error::Other(
            "the walk never takes a sibling; a test that hits this found a new caller".to_string(),
        ))
    }

    fn list_after(
        &self,
        prefix: &str,
        after: &str,
    ) -> Result<Vec<ObjectMeta>, analyzer_objectstore::Error> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with(prefix) && k.as_str() > after)
            .map(|(k, v)| ObjectMeta {
                key: k.clone(),
                size: v.len() as u64,
                etag: Some(analyzer_ingest::content_batch_id(v)),
            })
            .collect())
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, analyzer_objectstore::Error> {
        *self.gets.lock().unwrap() += 1;
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| analyzer_objectstore::Error::Other(format!("absent: {key}")))
    }

    fn put(&self, key: &str, body: &[u8]) -> Result<(), analyzer_objectstore::Error> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), body.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), analyzer_objectstore::Error> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

fn ann(at_ms: f64, label: &str) -> DeployAnnotation {
    DeployAnnotation {
        at_ms,
        label: label.to_string(),
        scope: Some("rag-batch".to_string()),
        source: DeploySource::Pipeline,
        declared_at_ms: Some(at_ms + 90_000.0),
    }
}

/// The declarations the input log holds, each once, newest record per object last.
fn declared(lake: &analyzer_lake::Lake) -> Vec<analyzer_lake::ledger::DeclaredAnnotation> {
    common::committed(lake)
        .into_iter()
        .flat_map(|b| b.annotations)
        .collect()
}

#[test]
fn a_declared_and_a_pipeline_object_record_alike_and_the_walk_costs_no_repeat_fetches() {
    let lake = common::fresh_lake("annotations");
    let store = MemStore::default();
    let at = 1_787_616_000_000.0;

    // Door 1: a pipeline's object, put directly. Door 2: `declare`, the CLI's and the API's.
    let a = ann(at, "affinity v2 rollout");
    store
        .put(&object_key(a.at_ms, &a.label), encode(&a).as_bytes())
        .expect("put");
    let b = ann(at + 60_000.0, "router weights v9");
    declare(&store, &b).expect("declare");
    let walk = ingest_annotations(&lake, &store).expect("walk");
    assert_eq!((walk.ingested, walk.refused), (2, 0), "{walk:?}");
    let mut recorded = declared(&lake);
    recorded.sort_by(|x, y| x.at_ms.total_cmp(&y.at_ms));
    assert_eq!(
        recorded,
        vec![a.record(), b.record()],
        "one spelling, both doors"
    );

    // Idempotent, and cheap: the second walk fetches NOTHING (the head's dedup is
    // pre-fetch, by key@etag) and records nothing.
    let fetched_before = *store.gets.lock().unwrap();
    let again = ingest_annotations(&lake, &store).expect("re-walk");
    assert_eq!((again.ingested, again.refused), (0, 0));
    assert_eq!(
        *store.gets.lock().unwrap(),
        fetched_before,
        "a settled prefix costs zero GETs per cycle — the T1 lesson"
    );
    assert_eq!(declared(&lake).len(), 2, "no doubling");

    // A re-declared deploy: same key, new content, new etag -> re-recorded, and the marker
    // it states replaces the earlier one on the table's key.
    let mut a2 = a.clone();
    a2.scope = Some("chat-prod".to_string());
    declare(&store, &a2).expect("re-declare");
    let third = ingest_annotations(&lake, &store).expect("third walk");
    assert_eq!((third.ingested, third.refused), (1, 0));
    assert_eq!(
        declared(&lake)
            .last()
            .and_then(|d| d.scope.clone())
            .as_deref(),
        Some("chat-prod"),
        "the re-declaration is recorded"
    );
}

#[test]
fn a_bad_object_is_refused_recorded_and_cannot_wedge_the_walk() {
    let lake = common::fresh_lake("annotations");
    let store = MemStore::default();

    store
        .put(
            &format!("{ANNOTATIONS_PREFIX}ann-1-junk.json"),
            b"not json at all",
        )
        .expect("put junk");
    let oversized = vec![b'x'; usize::try_from(ANNOTATION_MAX_BYTES + 1).expect("fits")];
    store
        .put(&format!("{ANNOTATIONS_PREFIX}ann-2-big.json"), &oversized)
        .expect("put oversized");
    let good = ann(1_787_616_000_000.0, "the one good deploy");
    declare(&store, &good).expect("declare");

    let walk = ingest_annotations(&lake, &store).expect("walk");
    assert_eq!((walk.ingested, walk.refused), (1, 2), "{walk:?}");
    assert_eq!(declared(&lake).len(), 1);

    // The refusals are RECORDED facts, so the next walk skips them — one malformed upload
    // costs one fetch ever, not one per cycle forever.
    let again = ingest_annotations(&lake, &store).expect("re-walk");
    assert_eq!((again.ingested, again.refused), (0, 0), "{again:?}");
    let refused = lake
        .ledger(analyzer_lake::ledger::SHARD)
        .expect("input log")
        .committed_since(0)
        .expect("committed")
        .iter()
        .filter(|b| b.carrier == "annotation" && b.unknown_kinds == 1)
        .count();
    assert_eq!(refused, 2, "each refusal is a record somebody can read");
}
