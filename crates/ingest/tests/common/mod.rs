// SPDX-License-Identifier: AGPL-3.0-only

//! Shared by ingest's integration tests.

#![allow(dead_code)] // each test binary uses a subset

/// An input log of a test's own over a directory store, removed when dropped: what ingest
/// writes needs no engine.
pub struct TestLake {
    pub lake: analyzer_lake::Lake,
    root: std::path::PathBuf,
}

impl std::ops::Deref for TestLake {
    type Target = analyzer_lake::Lake;
    fn deref(&self) -> &Self::Target {
        &self.lake
    }
}

impl Drop for TestLake {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// A fresh input log under the system's temporary directory.
pub fn fresh_lake(name: &str) -> TestLake {
    fresh_lake_over(name, |root| {
        std::sync::Arc::new(analyzer_objectstore::DirStore::new(root))
    })
}

/// A fresh input log over the store `store_of` makes from its directory.
pub fn fresh_lake_over(
    name: &str,
    store_of: impl FnOnce(
        &std::path::Path,
    ) -> std::sync::Arc<dyn analyzer_objectstore::ObjectStore + Send + Sync>,
) -> TestLake {
    // A counter, not the clock alone: the clock's grain is a microsecond on some hosts, and two
    // parallel tests in one microsecond shared a directory, and so one input log.
    static FRESH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let root = std::env::temp_dir().join(format!(
        "ingest-{name}-{}-{}-{}",
        std::process::id(),
        FRESH.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("scratch");
    // An instance of its own: the lake keeps the leases a process holds by lease key and
    // instance, and every test log shares the root `r/`.
    let instance = root
        .file_name()
        .map_or_else(|| name.to_string(), |n| n.to_string_lossy().into_owned());
    let lineage = analyzer_lake::lineage::Lineage::new("test", &instance).expect("lineage");
    TestLake {
        lake: analyzer_lake::Lake::new(store_of(&root), "r/".to_string(), lineage),
        root,
    }
}

/// The input log's committed batches, in position order.
pub fn committed(lake: &analyzer_lake::Lake) -> Vec<analyzer_lake::ledger::BatchRecord> {
    lake.ledger(analyzer_lake::ledger::SHARD)
        .expect("input log")
        .committed_since(0)
        .expect("committed batches")
}

/// [`stored_mass`] of a lake.
pub fn stored_mass_of(lake: &analyzer_lake::Lake) -> String {
    let mut sums: std::collections::BTreeMap<(String, String, i64), (u64, i64)> =
        std::collections::BTreeMap::new();
    for b in committed(lake) {
        for m in b.stored {
            let slot = sums.entry((m.instance, m.kind, m.day)).or_insert((0, 0));
            slot.0 += m.records;
            slot.1 += m.tokens;
        }
    }
    let mut out = String::new();
    for ((instance, kind, day), (records, tokens)) in sums {
        use std::fmt::Write as _;
        writeln!(out, "{instance}\t{kind}\t{day}\t{records}\t{tokens}").expect("a string");
    }
    out
}
