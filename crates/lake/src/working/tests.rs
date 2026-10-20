// SPDX-License-Identifier: AGPL-3.0-only

//! The working state's tests: the keyed relations, the staging, the unit rollback and the
//! crash/restore property.

use super::{is_sst, Declared, Index, Relation, Scope, WorkingState};
use crate::keyed::Record;
use analyzer_objectstore::{DirStore, ObjectStore};

const FRONTIER: Declared = Declared {
    name: "frontier",
    dated: false,
    index: Index::Plain,
};
const SPANS: Declared = Declared {
    name: "spans",
    dated: true,
    index: Index::Plain,
};
const FACTS: Declared = Declared {
    name: "facts",
    dated: true,
    index: Index::Instants,
};
const ALL: [Declared; 3] = [FRONTIER, SPANS, FACTS];

fn frontier() -> Relation<'static> {
    Relation {
        name: "frontier",
        scope: Scope::Lineage,
    }
}

fn spans(day: i64) -> Relation<'static> {
    Relation {
        name: "spans",
        scope: Scope::Day(day),
    }
}

fn facts(day: i64) -> Relation<'static> {
    Relation {
        name: "facts",
        scope: Scope::Day(day),
    }
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "working-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("scratch");
    dir
}

fn lake(root: &std::path::Path) -> crate::Lake {
    crate::Lake::new(
        std::sync::Arc::new(DirStore::new(root.join("store"))),
        "r/".into(),
        crate::lineage::Lineage::new("t", "w").expect("lineage"),
    )
}

fn open(root: &std::path::Path, dir: &str, lake: &crate::Lake) -> std::sync::Arc<WorkingState> {
    std::sync::Arc::new(
        WorkingState::open(&root.join(dir), lake, &ALL, super::DEFAULT_MEMORY_BYTES).expect("open"),
    )
}

/// One write to the frontier: a record under `hash`.
fn put(state: &WorkingState, hash: u64, payload: &str) {
    write(
        state,
        &frontier(),
        false,
        &[rec(hash, &format!("k{hash}"), 1.0, false, payload)],
        None,
    );
}

fn rec(hash: u64, key: &str, version: f64, gone: bool, payload: &str) -> Record {
    Record {
        hash,
        key: key.as_bytes().to_vec(),
        version,
        gone,
        payload: payload.as_bytes().to_vec(),
    }
}

/// One revision of `relation`: its records staged as one part per stripe they fall in.
fn write(
    state: &WorkingState,
    relation: &Relation<'_>,
    base: bool,
    records: &[Record],
    instants: Option<&[f64]>,
) {
    let revision = state.reserve();
    for (i, r) in records.iter().enumerate() {
        let stripe = crate::stripe::of(r.hash);
        let part = u32::try_from(i).expect("part");
        state
            .stage(
                relation,
                revision,
                (stripe, part),
                std::slice::from_ref(r),
                instants.map(|at| &at[i..=i]),
            )
            .expect("stage");
    }
    state
        .commit(relation, revision, base, None)
        .expect("commit");
}

fn all(state: &WorkingState, relation: &Relation<'_>) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = (0..crate::stripe::STRIPES)
        .flat_map(|stripe| state.read(relation, stripe, None).expect("read"))
        .map(|r| {
            (
                String::from_utf8(r.key).expect("utf8"),
                String::from_utf8(r.payload).expect("utf8"),
            )
        })
        .collect();
    out.sort();
    out
}

fn probe(state: &WorkingState, relation: &Relation<'_>, hash: u64) -> Vec<String> {
    let stripe = crate::stripe::of(hash);
    state
        .read(relation, stripe, Some(&[hash]))
        .expect("probe")
        .into_iter()
        .map(|r| String::from_utf8(r.payload).expect("utf8"))
        .collect()
}

/// The keyed rule, in hash order: the highest version wins a key and a later write wins a
/// tie, so an older day's restatement loses to the frontier it would overwrite; a `gone`
/// winner is no record; a probe takes only its hash; an abandoned revision writes nothing.
#[test]
fn the_keyed_rule_holds_in_hash_order() {
    let root = scratch("rule");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    let f = frontier();
    write(
        &state,
        &f,
        true,
        &[
            rec(1, "a", 5.0, false, "a@5"),
            rec(2, "b", 5.0, false, "b@5"),
        ],
        None,
    );
    write(
        &state,
        &f,
        false,
        &[
            rec(1, "a", 4.0, false, "a@4"),
            rec(2, "b", 5.0, false, "b@5-later"),
            rec(3, "c", 6.0, false, "c@6"),
        ],
        None,
    );
    write(&state, &f, false, &[rec(3, "c", 7.0, true, "")], None);
    assert_eq!(
        all(&state, &f),
        vec![("a".into(), "a@5".into()), ("b".into(), "b@5-later".into())]
    );
    assert_eq!(probe(&state, &f, 2), vec!["b@5-later".to_string()]);
    assert!(
        probe(&state, &f, 3).is_empty(),
        "a gone winner is no record"
    );

    let abandoned = state.reserve();
    state
        .stage(
            &f,
            abandoned,
            (0, 0),
            &[rec(16, "q", 9.0, false, "q")],
            None,
        )
        .expect("stage");
    state.abandon(abandoned).expect("abandon");
    state
        .commit(&f, abandoned, false, None)
        .expect("an empty commit");
    assert_eq!(
        all(&state, &f).len(),
        2,
        "an abandoned revision wrote nothing"
    );
    std::fs::remove_dir_all(root).ok();
}

/// The same rule in a dated relation: a probe takes its hash's records, a base clears its
/// day and no other.
#[test]
fn the_keyed_rule_holds_in_a_dated_relation() {
    let root = scratch("rule-key");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    let (d1, d2) = (spans(1), spans(2));
    write(
        &state,
        &d1,
        true,
        &[
            rec(7, "x\0k1", 0.0, false, "x1"),
            rec(7, "x\0k2", 0.0, false, "x2"),
        ],
        None,
    );
    write(&state, &d2, true, &[rec(8, "y\0k", 0.0, false, "y")], None);
    write(&state, &d1, false, &[rec(7, "x\0k1", 0.0, true, "")], None);
    assert_eq!(probe(&state, &d1, 7), vec!["x2".to_string()], "by hash");
    write(&state, &d1, true, &[rec(9, "z\0k", 0.0, false, "z")], None);
    assert_eq!(
        all(&state, &d1),
        vec![("z\0k".into(), "z".into())],
        "a base clears"
    );
    assert_eq!(all(&state, &d2).len(), 1, "another day's scope untouched");
    assert!(probe(&state, &d1, 7).is_empty(), "the base cleared it");

    std::fs::remove_dir_all(root).ok();
}

/// A stripe streams in hash order and holds only its hashes, a stream opened before a
/// commit reads the state it opened on, and a record staged outside its hash's stripe is
/// refused.
#[test]
fn a_stream_is_ordered_and_one_view() {
    let root = scratch("stream");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    let d = spans(3);
    // Hashes of stripe 0, their keys in the reverse of their hash order, and one of stripe 1.
    let mut records: Vec<Record> = (0..40u64)
        .map(|i| i * 16)
        .map(|h| rec(h, &format!("{:03}", 999 - h), 0.0, false, "v"))
        .collect();
    records.push(rec(crate::stripe::first_hash(1), "next", 0.0, false, "v"));
    write(&state, &d, true, &records, None);
    let mut stream = state.stream(&d, 0).expect("stream");
    let first = stream.next().expect("a record").expect("read");
    write(&state, &d, true, &[], None);
    let rest: Vec<u64> = stream.map(|r| r.expect("read").hash).collect();
    let mut hashes = vec![first.hash];
    hashes.extend(rest);
    assert_eq!(
        hashes.len(),
        40,
        "stripe 0 alone, its view kept across the commit"
    );
    assert!(hashes.windows(2).all(|w| w[0] < w[1]), "hash order");
    assert!(all(&state, &d).is_empty(), "the commit itself landed");
    let revision = state.reserve();
    assert!(
        state
            .stage(
                &d,
                revision,
                (0, 0),
                &[rec(crate::stripe::first_hash(2), "x", 0.0, false, "v")],
                None
            )
            .is_err(),
        "a record outside its stripe is refused"
    );
    std::fs::remove_dir_all(root).ok();
}

/// A read from an instant takes the records whose writers stated an instant at or after it,
/// every stripe, and not a record a later revision tombstoned.
#[test]
fn a_read_from_an_instant_takes_what_lies_at_or_after_it() {
    // Through write batches, then through ingested files and their index's files.
    for ingest in [u64::MAX, 0] {
        reads_from_an_instant(ingest);
    }
}

fn reads_from_an_instant(ingest_bytes: u64) {
    let root = scratch("since");
    let lake = lake(&root);
    let state = open_with(
        &root,
        "db",
        &lake,
        super::durable::Limits {
            ingest_bytes,
            ..super::durable::Limits::default()
        },
    );
    let d = facts(4);
    let records: Vec<Record> = (0..8u64)
        .map(|i| rec(i, &format!("f{i}"), 1.0, false, &format!("{i}")))
        .collect();
    let instants: Vec<f64> = (0..8u32).map(|i| f64::from(i) * 100.0).collect();
    write(&state, &d, true, &records, Some(&instants));
    write(
        &state,
        &d,
        false,
        &[rec(6, "f6", 2.0, true, "")],
        Some(&[600.0]),
    );
    let mut since: Vec<String> = state
        .since(&d, 450.0)
        .expect("since")
        .into_iter()
        .map(|r| String::from_utf8(r.payload).expect("utf8"))
        .collect();
    since.sort();
    assert_eq!(since, vec!["5", "7"]);
    let revision = state.reserve();
    assert!(
        state
            .stage(&d, revision, (0, 0), &[rec(0, "g", 1.0, false, "g")], None)
            .is_err(),
        "a record without its instant is refused"
    );
    std::fs::remove_dir_all(root).ok();
}

/// A tracked commit counts what it changed against the state it is committed over: a
/// delta by the keys it writes, a base against the whole scope.
#[test]
fn a_commit_counts_what_it_changed() {
    let root = scratch("changes");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    let d = spans(5);
    let commit = |base: bool, records: &[Record]| {
        let mut terms = crate::keyed::Terms::new();
        let revision = state.reserve();
        for (i, r) in records.iter().enumerate() {
            terms.insert(
                (r.hash, r.key.clone()),
                (!r.gone).then(|| crate::keyed::term(r)),
            );
            state
                .stage(
                    &d,
                    revision,
                    (crate::stripe::of(r.hash), u32::try_from(i).expect("part")),
                    std::slice::from_ref(r),
                    None,
                )
                .expect("stage");
        }
        state
            .commit(&d, revision, base, Some(&terms))
            .expect("commit")
            .expect("tracked")
    };
    commit(
        true,
        &[
            rec(1, "a", 0.0, false, "a1"),
            rec(2, "b", 0.0, false, "b1"),
            rec(3, "c", 0.0, false, "c1"),
        ],
    );
    let delta = commit(
        false,
        &[
            rec(2, "b", 0.0, true, ""),
            rec(1, "a", 0.0, false, "a2"),
            rec(3, "c", 0.0, false, "c1"),
            rec(4, "d", 0.0, false, "d1"),
        ],
    );
    assert_eq!(
        (delta.written, delta.inserted, delta.updated, delta.deleted),
        (4, 1, 1, 1)
    );
    let base = commit(true, &[rec(1, "a", 0.0, false, "a2")]);
    assert_eq!(
        (base.inserted, base.updated, base.deleted),
        (0, 0, 2),
        "a base deletes what it does not write"
    );
    std::fs::remove_dir_all(root).ok();
}

/// Retirement runs once per boundary, its boundary kept beside the data.
#[test]
fn retirement_runs_once_per_boundary() {
    let root = scratch("retire");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    let f = frontier();
    write(
        &state,
        &f,
        false,
        &[
            rec(1, "old", 1.0, false, "o"),
            rec(2, "new", 1.0, false, "n"),
        ],
        None,
    );
    assert!(state.retire(&f, 5, |r| r.key != b"old").expect("retire"));
    write(&state, &f, false, &[rec(3, "old2", 2.0, false, "o2")], None);
    assert!(!state
        .retire(&f, 5, |r| !r.key.starts_with(b"old"))
        .expect("again"));
    assert_eq!(
        all(&state, &f).len(),
        2,
        "the same boundary retires nothing"
    );
    assert!(
        state.retire(&spans(1), 5, |_| true).is_err(),
        "a dated relation"
    );
    std::fs::remove_dir_all(root).ok();
}

/// A relation is stated once committed, empty or not, and a drop forgets it; a dropped
/// day's relations are empty and unstated, another day's untouched.
#[test]
fn a_drop_forgets_the_scope() {
    let root = scratch("drop");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    assert!(!state.stated(&spans(1)).expect("stated"));
    write(&state, &spans(1), true, &[], None);
    assert!(
        state.stated(&spans(1)).expect("stated"),
        "an empty base states"
    );
    assert!(!state.holds(&spans(1)).expect("holds"));
    write(
        &state,
        &spans(1),
        false,
        &[rec(1, "a", 0.0, false, "a")],
        None,
    );
    write(
        &state,
        &spans(2),
        false,
        &[rec(1, "a", 0.0, false, "a")],
        None,
    );
    assert!(state.holds(&spans(1)).expect("holds"));
    state.drop_day(1).expect("drop");
    assert!(!state.stated(&spans(1)).expect("stated"));
    assert!(!state.holds(&spans(1)).expect("holds"));
    assert!(
        probe(&state, &spans(1), 1).is_empty(),
        "the index went with it"
    );
    assert!(state.holds(&spans(2)).expect("holds"), "another day stays");
    std::fs::remove_dir_all(root).ok();
}

/// Wait out a background checkpoint's upload.
fn settled_upload(state: &WorkingState) {
    while state.uploading.load(std::sync::atomic::Ordering::Acquire) {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// A checkpoint makes what was written durable and names the position it holds: a fresh
/// directory restores its writes and its position, and not a write after it.
#[test]
fn a_checkpoint_makes_writes_durable_at_its_position() {
    let root = scratch("checkpoint");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    assert_eq!(state.position(), 0, "a new state holds nothing");
    state.begin(&lake).expect("begin");
    put(&state, 1, "a");
    put(&state, 2, "b");
    state.end(&lake, 7).expect("end");
    assert_eq!(state.position(), 7, "an ended unit's position is held");
    state.checkpoint(&lake).expect("checkpoint");
    let durable = all(&state, &frontier());
    state.begin(&lake).expect("begin");
    put(&state, 3, "after");
    state.end(&lake, 9).expect("end");
    assert_eq!(state.position(), 9);
    let restored = open(&root, "fresh", &lake);
    assert_eq!(all(&restored, &frontier()), durable);
    assert_eq!(
        restored.position(),
        7,
        "the checkpoint's position, not the live one"
    );
    std::fs::remove_dir_all(root).ok();
}

/// A checkpoint restores on a fresh directory; a file whose size is not the checkpoint's
/// fails the restore and leaves no copy behind.
#[test]
fn a_checkpoint_restores_whole_or_not_at_all() {
    let root = scratch("restore");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    for i in 0..2_000 {
        put(&state, i, &format!("v{i}"));
    }
    assert!(state.checkpoint(&lake).expect("checkpoint") > 0);
    let head = lake.working_head().expect("head").read().expect("read");
    assert_eq!(head.checkpoints.len(), 1);
    let expected = all(&state, &frontier());
    assert_eq!(all(&open(&root, "fresh", &lake), &frontier()), expected);
    let sst = head.checkpoints[0]
        .files
        .iter()
        .find(|f| is_sst(&f.name))
        .expect("an sst")
        .key
        .clone();
    DirStore::new(root.join("store"))
        .put(&sst, b"short")
        .expect("truncate");
    let broken = root.join("broken");
    assert!(WorkingState::open(&broken, &lake, &ALL, super::DEFAULT_MEMORY_BYTES).is_err());
    assert!(
        !broken.join(super::CHECKPOINT).exists(),
        "a failed download left a copy"
    );
    std::fs::remove_dir_all(root).ok();
}

/// Reopening a directory reads the newest checkpoint from its local copy: with the bucket's
/// copies gone, it still opens, and a write after the checkpoint is not in it, as a process
/// never trusts the live database it did not open.
#[test]
fn a_reopen_uses_the_local_copy_and_drops_what_no_checkpoint_holds() {
    let root = scratch("local");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    put(&state, 1, "a");
    put(&state, 2, "b");
    state.checkpoint(&lake).expect("checkpoint");
    put(&state, 3, "after");
    state.crash();
    drop(state);
    let store = DirStore::new(root.join("store"));
    for object in store.list("r/state-db/0").expect("list") {
        store.delete(&object.key).expect("delete");
    }
    let reopened = open(&root, "db", &lake);
    assert_eq!(
        all(&reopened, &frontier()),
        vec![
            ("k1".to_string(), "a".to_string()),
            ("k2".to_string(), "b".to_string())
        ]
    );
    std::fs::remove_dir_all(root).ok();
}

/// A unit that began and never ended is rolled back to the newest checkpoint, and the
/// position with it: the units ended since the checkpoint go too, which is what the replay
/// owes. While a unit is open the position is the checkpoint's. Nothing rolls back while a
/// stream of the failed unit is open.
#[test]
fn a_unit_that_did_not_end_is_rolled_back_to_the_newest_checkpoint() {
    let root = scratch("unit");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    assert!(!state.begin(&lake).expect("begin"), "nothing to roll back");
    put(&state, 1, "a");
    state.end(&lake, 3).expect("end");
    state.checkpoint(&lake).expect("checkpoint");
    state.begin(&lake).expect("begin");
    put(&state, 2, "b");
    state.end(&lake, 5).expect("end");
    assert!(
        !state.begin(&lake).expect("begin"),
        "an ended unit is not rolled back"
    );
    assert_eq!(
        state.position(),
        3,
        "an open unit holds the checkpoint's position for certain"
    );
    put(&state, 3, "failed");
    let open_stream = state.stream(&frontier(), 0).expect("stream");
    assert!(
        state.settle(&lake).is_err(),
        "not while a stream of it is open"
    );
    assert_eq!(
        state.position(),
        3,
        "a failed unit's position is the checkpoint's"
    );
    drop(open_stream);
    assert!(state.settle(&lake).expect("the rollback"), "it rolled back");
    assert_eq!(
        all(&state, &frontier()),
        vec![("k1".to_string(), "a".to_string())],
        "back to the checkpoint, the unit ended at 5 with it"
    );
    assert_eq!(state.position(), 3);
    assert!(!state.settle(&lake).expect("settle"), "once");
    state.begin(&lake).expect("begin");
    put(&state, 4, "d");
    state.end(&lake, 6).expect("end");
    assert_eq!(state.position(), 6, "units go on from the checkpoint");
    std::fs::remove_dir_all(root).ok();
}

/// The probe path's cost on a state much larger than its cache, after a whole scan has
/// passed through the cache (measured: 40 s for 403,000 rows, every seek re-reading a whole
/// filter block). A measurement, not a check: run it by hand, in
/// release, and read its line.
#[test]
#[ignore = "benchmark: cargo test --release -p analyzer-lake --lib probe_cost -- --ignored --nocapture"]
fn probe_cost_on_a_state_larger_than_its_cache() {
    let root = scratch("probe-bench");
    let lake = lake(&root);
    let state = std::sync::Arc::new(
        WorkingState::open(&root.join("db"), &lake, &ALL, 64 << 20).expect("open"),
    );
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let keys: u64 = 6_000_000;
    let mut hashes = Vec::new();
    for round in 0..6u64 {
        let revision = state.reserve();
        let mut by_stripe: std::collections::BTreeMap<usize, Vec<Record>> =
            std::collections::BTreeMap::new();
        for _ in 0..keys / 6 {
            let hash = rng.next();
            hashes.push(hash);
            by_stripe
                .entry(crate::stripe::of(hash))
                .or_default()
                .push(rec(
                    hash,
                    &format!("k{hash:x}"),
                    1.0,
                    false,
                    &"p".repeat(48),
                ));
        }
        for (stripe, mut records) in by_stripe {
            records.sort_by_key(|r| r.hash);
            let part = u32::try_from(round).expect("part");
            state
                .stage(&frontier(), revision, (stripe, part), &records, None)
                .expect("stage");
        }
        state
            .commit(&frontier(), revision, false, None)
            .expect("commit");
    }
    {
        let db = state.live().expect("live");
        let cf = WorkingState::cf(&db, frontier().name).expect("cf");
        db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
    }
    let scanned = std::time::Instant::now();
    let mut streamed = 0usize;
    for stripe in 0..crate::stripe::STRIPES {
        streamed += state.stream(&frontier(), stripe).expect("stream").count();
    }
    let scan_s = scanned.elapsed().as_secs_f64();
    let mut probes: Vec<u64> = (0..200_000)
        .map(|_| hashes[usize::try_from(rng.below(keys)).expect("index")])
        .collect();
    probes.sort_unstable();
    let probing = std::time::Instant::now();
    let mut found = 0usize;
    for stripe in 0..crate::stripe::STRIPES {
        found += state
            .read(&frontier(), stripe, Some(&probes))
            .expect("read")
            .len();
    }
    println!(
        "probe bench: {keys} keys, 64 MiB; scan {streamed} in {scan_s:.1}s; {} probes found {found} in {:.2}s",
        probes.len(),
        probing.elapsed().as_secs_f64()
    );
    drop(state);
    std::fs::remove_dir_all(root).ok();
}

/// A clean close reopens on the same box at its position with every write it held, no
/// checkpoint needed; its marker is taken once, so a crash after the reopen restores.
#[test]
fn a_clean_close_reopens_at_its_position_once() {
    let root = scratch("close");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    put(&state, 1, "checkpointed");
    state.checkpoint(&lake).expect("checkpoint");
    state.begin(&lake).expect("begin");
    put(&state, 2, "past it");
    state.end(&lake, 4).expect("end");
    let held = all(&state, &frontier());
    assert!(state.close().expect("close"), "closed cleanly");
    drop(state);
    let reopened = open(&root, "db", &lake);
    assert_eq!(
        all(&reopened, &frontier()),
        held,
        "the close kept every write"
    );
    assert_eq!(reopened.position(), 4, "and its position");
    reopened.begin(&lake).expect("begin");
    put(&reopened, 3, "unclosed");
    reopened.end(&lake, 5).expect("end");
    reopened.crash();
    drop(reopened);
    let crashed = open(&root, "db", &lake);
    assert_eq!(
        all(&crashed, &frontier()),
        vec![("k1".to_string(), "checkpointed".to_string())],
        "a crash after a reopen restores the checkpoint"
    );
    assert_eq!(crashed.position(), 0);
    std::fs::remove_dir_all(root).ok();
}

/// A clean close is not trusted once the head names a checkpoint it does not descend from:
/// another writer moved the lineage on, and the reopen restores that checkpoint.
#[test]
fn a_clean_close_yields_to_a_newer_checkpoint() {
    let root = scratch("close-newer");
    let lake = lake(&root);
    let state = open(&root, "a", &lake);
    put(&state, 1, "from a");
    state.end(&lake, 2).expect("end");
    assert!(state.close().expect("close"));
    drop(state);
    let other = open(&root, "b", &lake);
    put(&other, 7, "from b");
    other.end(&lake, 3).expect("end");
    other.checkpoint(&lake).expect("b checkpoints");
    drop(other);
    let reopened = open(&root, "a", &lake);
    assert_eq!(
        all(&reopened, &frontier()),
        vec![("k7".to_string(), "from b".to_string())],
        "the newer checkpoint, not the closed database"
    );
    assert_eq!(reopened.position(), 3);
    std::fs::remove_dir_all(root).ok();
}

/// A clean close is one lake's: a lake with the same root in another store shares the
/// directory and restores its own state instead.
#[test]
fn a_clean_close_is_one_lakes() {
    let root = scratch("close-lakes");
    let lake_in = |store: &str| {
        crate::Lake::new(
            std::sync::Arc::new(DirStore::new(root.join(store))),
            "r/".into(),
            crate::lineage::Lineage::new("t", "w").expect("lineage"),
        )
    };
    let (first, second) = (lake_in("one"), lake_in("two"));
    let state = open(&root, "db", &first);
    put(&state, 1, "the first lake's");
    assert!(state.close().expect("close"));
    drop(state);
    let other = open(&root, "db", &second);
    assert!(
        all(&other, &frontier()).is_empty(),
        "another lake trusted the first's close"
    );
    std::fs::remove_dir_all(root).ok();
}

/// A unit that did not end is never closed cleanly: the reopen restores.
#[test]
fn a_failed_unit_is_not_closed_cleanly() {
    let root = scratch("close-failed");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    state.begin(&lake).expect("begin");
    put(&state, 1, "failed");
    assert!(
        !state.close().expect("close"),
        "a failed unit closed cleanly"
    );
    drop(state);
    let reopened = open(&root, "db", &lake);
    assert!(all(&reopened, &frontier()).is_empty());
    std::fs::remove_dir_all(root).ok();
}

/// A unit that ends past the checkpoint's bytes starts one in the background, at the
/// position it ended at.
#[test]
fn a_unit_past_the_checkpoint_bytes_starts_one() {
    let root = scratch("due");
    let lake = lake(&root);
    let state = open_with(
        &root,
        "db",
        &lake,
        super::durable::Limits {
            checkpoint_bytes: 8 << 10,
            ..super::durable::Limits::default()
        },
    );
    state.begin(&lake).expect("begin");
    put(&state, 1, "small");
    state.end(&lake, 1).expect("end");
    settled_upload(&state);
    assert!(
        lake.working_head()
            .expect("head")
            .read()
            .expect("read")
            .checkpoints
            .is_empty(),
        "under the limit"
    );
    state.begin(&lake).expect("begin");
    for i in 2..40 {
        put(&state, i, &"v".repeat(400));
    }
    state.end(&lake, 2).expect("end");
    settled_upload(&state);
    let head = lake.working_head().expect("head").read().expect("read");
    assert_eq!(head.checkpoints.len(), 1, "past the limit");
    assert_eq!(head.checkpoints[0].position, 2);
    let expected = all(&state, &frontier());
    assert_eq!(all(&open(&root, "fresh", &lake), &frontier()), expected);
    std::fs::remove_dir_all(root).ok();
}

/// Two lakes on one data directory keep apart working states, and one lake opened twice
/// shares one: what one writes, the other does not read.
#[test]
fn a_working_state_is_one_lakes() {
    let root = scratch("lakes");
    let store = std::sync::Arc::new(DirStore::new(root.join("store")));
    let lineage = crate::lineage::Lineage::new("t", "w").expect("lineage");
    let a = crate::Lake::new(store.clone(), "ns-a/".into(), lineage.clone());
    let b = crate::Lake::new(store, "ns-b/".into(), lineage);
    let dir = root.join("data");
    let state_a = WorkingState::shared(&dir, &a, &ALL, super::DEFAULT_MEMORY_BYTES).expect("a");
    let state_b = WorkingState::shared(&dir, &b, &ALL, super::DEFAULT_MEMORY_BYTES).expect("b");
    assert!(
        !std::sync::Arc::ptr_eq(&state_a, &state_b),
        "two lakes, two states"
    );
    let again = WorkingState::shared(&dir, &a, &ALL, super::DEFAULT_MEMORY_BYTES).expect("a again");
    assert!(
        std::sync::Arc::ptr_eq(&state_a, &again),
        "one lake, one state"
    );
    write(
        &state_a,
        &frontier(),
        false,
        &[rec(1, "a", 1.0, false, "a")],
        None,
    );
    assert!(
        all(&state_b, &frontier()).is_empty(),
        "b reads nothing of a's"
    );
    std::fs::remove_dir_all(root).ok();
}

/// The newest checkpoints stay named; a checkpoint past them is queued, and its files
/// deleted once they have been unnamed long enough, never before.
#[test]
fn garbage_waits_its_minimum_age_then_leaves() {
    let root = scratch("garbage");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    for i in 0..6u64 {
        put(&state, i, "v");
        state.checkpoint(&lake).expect("checkpoint");
    }
    let head = lake.working_head().expect("head").read().expect("read");
    assert_eq!(head.checkpoints.len(), super::RETAINED);
    assert!(
        !head.garbage.is_empty(),
        "a retired checkpoint's files are queued"
    );
    let store = DirStore::new(root.join("store"));
    let queued = head.garbage[0].key.clone();
    assert!(
        store.get(&queued).is_ok(),
        "not deleted before its minimum age"
    );
    lake.working_head()
        .expect("head")
        .commit(|h| h.garbage.iter_mut().for_each(|g| g.since_ms = 0))
        .expect("age");
    put(&state, 9, "v");
    state.checkpoint(&lake).expect("checkpoint");
    assert!(
        store.get(&queued).is_err(),
        "collected past its minimum age"
    );
    let head = lake.working_head().expect("head").read().expect("read");
    let named: Vec<String> = head
        .checkpoints
        .iter()
        .flat_map(|c| c.files.iter().map(|f| f.key.clone()))
        .collect();
    assert!(
        named.iter().all(|k| store.get(k).is_ok()),
        "a named file was deleted"
    );
    // An upload nothing names, a failed checkpoint's: the next checkpoint queues it, and
    // once its minimum age is past, the one after deletes it.
    let orphan = "r/state-db/00000000000000000001/sst/orphan.sst".to_string();
    store.put(&orphan, b"x").expect("orphan");
    put(&state, 10, "v");
    state.checkpoint(&lake).expect("checkpoint");
    let head = lake.working_head().expect("head").read().expect("read");
    assert!(
        head.garbage.iter().any(|g| g.key == orphan),
        "the orphan is queued"
    );
    assert!(store.get(&orphan).is_ok(), "not before its minimum age");
    lake.working_head()
        .expect("head")
        .commit(|h| h.garbage.iter_mut().for_each(|g| g.since_ms = 0))
        .expect("age");
    put(&state, 11, "v");
    state.checkpoint(&lake).expect("checkpoint");
    assert!(store.get(&orphan).is_err(), "the orphan is collected");
    std::fs::remove_dir_all(root).ok();
}

/// A small deterministic generator for the property test: xorshift, seeded.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// One to four records on random keys, versions and tombstones, their payload the step.
    fn records(&mut self, step: usize) -> Vec<Record> {
        (0..=self.below(4))
            .map(|_| {
                let hash = self.below(24);
                #[allow(clippy::cast_precision_loss)] // small integers
                let version = self.below(5) as f64;
                let gone = self.below(6) == 0;
                rec(
                    hash,
                    &format!("k{hash}"),
                    version,
                    gone,
                    &format!("s{step}"),
                )
            })
            .collect()
    }
}

/// The keyed rule as a model: each key's winner, the highest version, a later write
/// winning a tie; the live records are the winners not gone.
#[derive(Clone, Default)]
struct Model(std::collections::BTreeMap<String, (f64, bool, String)>);

impl Model {
    fn apply(&mut self, r: &Record) {
        let key = String::from_utf8(r.key.clone()).expect("utf8");
        let wins = self
            .0
            .get(&key)
            .is_none_or(|(v, _, _)| r.version.total_cmp(v).is_ge());
        if wins {
            let payload = String::from_utf8(r.payload.clone()).expect("utf8");
            self.0.insert(key, (r.version, r.gone, payload));
        }
    }

    fn live(&self) -> Vec<(String, String)> {
        self.0
            .iter()
            .filter(|(_, (_, gone, _))| !gone)
            .map(|(k, (_, _, p))| (k.clone(), p.clone()))
            .collect()
    }
}

/// The durability claims, over random runs against the keyed rule's model: what a
/// checkpoint made durable, its writes and its position, is exactly what a crash, a fresh
/// directory and a failed unit's rollback come back to, and nothing written after it; a
/// clean restart comes back to everything.
#[test]
fn crashes_restores_and_rollbacks_return_to_exactly_what_a_checkpoint_holds() {
    // Every commit through write batches, then every commit through ingested files.
    for (ingest, seed) in [false, true].into_iter().flat_map(|ingest| {
        [
            0x9e37_79b9_7f4a_7c15_u64,
            0x2545_f491_4f6c_dd1d,
            0xdead_beef_cafe_f00d,
        ]
        .map(|seed| (ingest, seed))
    }) {
        let open = |root: &std::path::Path, dir: &str, lake: &crate::Lake| {
            open_with(root, dir, lake, ingesting(ingest))
        };
        let root = scratch("property");
        let lake = lake(&root);
        let mut state = open(&root, "db", &lake);
        let mut rng = Rng(seed);
        let (mut live, mut durable) = (Model::default(), Model::default());
        let (mut position, mut durable_position) = (0u64, 0u64);
        for step in 0..160 {
            let op = rng.below(100);
            if op < 55 {
                let records = rng.records(step);
                // One write in five restates the relation whole.
                let base = rng.below(5) == 0;
                write(&state, &frontier(), base, &records, None);
                if base {
                    live.0.clear();
                }
                for r in &records {
                    live.apply(r);
                }
            } else if op < 72 {
                // A unit ends: held, not durable.
                state.begin(&lake).expect("begin");
                position += 1;
                state.end(&lake, position).expect("end");
            } else if op < 77 {
                state.checkpoint(&lake).expect("checkpoint");
                durable = live.clone();
                durable_position = position;
            } else if op < 81 {
                // A clean stop: the close, or the drop that runs it.
                if rng.below(2) == 0 {
                    assert!(state.close().expect("close"));
                }
                drop(state);
                state = open(&root, "db", &lake);
                assert_eq!(
                    all(&state, &frontier()),
                    live.live(),
                    "seed {seed:x} step {step}: a clean restart"
                );
            } else if op < 87 {
                state.crash();
                drop(state);
                state = open(&root, "db", &lake);
                live = durable.clone();
                position = durable_position;
                assert_eq!(
                    all(&state, &frontier()),
                    durable.live(),
                    "ingest {ingest} seed {seed:x} step {step}: a crash"
                );
            } else if op < 94 {
                state.begin(&lake).expect("begin");
                let hash = rng.below(24);
                let r = rec(hash, &format!("k{hash}"), 9.0, false, "failed");
                write(&state, &frontier(), false, std::slice::from_ref(&r), None);
                assert!(state.settle(&lake).expect("the rollback"));
                live = durable.clone();
                position = durable_position;
                assert_eq!(
                    all(&state, &frontier()),
                    durable.live(),
                    "seed {seed:x} step {step}: a rollback"
                );
            } else {
                let fresh = open(&root, &format!("fresh-{step}"), &lake);
                assert_eq!(
                    all(&fresh, &frontier()),
                    durable.live(),
                    "seed {seed:x} step {step}: a fresh restore"
                );
                assert_eq!(
                    fresh.position(),
                    durable_position,
                    "seed {seed:x} step {step}"
                );
            }
            assert_eq!(
                all(&state, &frontier()),
                live.live(),
                "seed {seed:x} step {step}: the live state"
            );
            assert_eq!(
                state.position(),
                position,
                "seed {seed:x} step {step}: the position"
            );
        }
        std::fs::remove_dir_all(root).ok();
    }
}

/// A writer superseded by another cannot checkpoint: nothing it uploaded is named, and a
/// restore holds the new writer's state without the old one's late write.
#[test]
fn a_superseded_writer_cannot_checkpoint() {
    let root = scratch("fence");
    let store = std::sync::Arc::new(DirStore::new(root.join("store")));
    let lake_of = |instance: &str| {
        crate::Lake::new(
            store.clone(),
            "r/".into(),
            crate::lineage::Lineage::new("t", instance).expect("lineage"),
        )
    };
    let (a_lake, b_lake) = (lake_of("a"), lake_of("b"));
    let a = std::sync::Arc::new(
        WorkingState::open(&root.join("a"), &a_lake, &ALL, super::DEFAULT_MEMORY_BYTES)
            .expect("a opens"),
    );
    put(&a, 1, "from a");
    a.checkpoint(&a_lake).expect("a checkpoints");
    // A's lease expires: B takes the lineage over at the next epoch.
    let lease_key = format!("{}lease", a_lake.journal_dir());
    let expired = crate::lineage::Lease {
        holder: "a".into(),
        epoch: 1,
        expires_ms: 0,
    };
    store
        .put(&lease_key, &serde_json::to_vec(&expired).expect("lease"))
        .expect("expire");
    let b = std::sync::Arc::new(
        WorkingState::open(&root.join("b"), &b_lake, &ALL, super::DEFAULT_MEMORY_BYTES)
            .expect("b opens"),
    );
    assert_eq!(
        all(&b, &frontier()).len(),
        1,
        "b restores what a checkpointed"
    );
    put(&b, 2, "from b");
    b.checkpoint(&b_lake).expect("b checkpoints");
    put(&a, 3, "late from a");
    assert!(
        matches!(a.checkpoint(&a_lake), Err(crate::Error::Fenced { .. })),
        "a superseded writer checkpointed"
    );
    let restored = open(&root, "fresh", &b_lake);
    assert_eq!(
        all(&restored, &frontier()),
        vec![
            ("k1".to_string(), "from a".to_string()),
            ("k2".to_string(), "from b".to_string())
        ]
    );
    std::fs::remove_dir_all(root).ok();
}

/// Limits that send every commit through ingested files, or none.
fn ingesting(ingest: bool) -> super::durable::Limits {
    super::durable::Limits {
        ingest_bytes: if ingest { 0 } else { u64::MAX },
        ..super::durable::Limits::default()
    }
}

/// A state whose checkpoint goes by `limits`.
fn open_with(
    root: &std::path::Path,
    dir: &str,
    lake: &crate::Lake,
    limits: super::durable::Limits,
) -> std::sync::Arc<WorkingState> {
    let mut state =
        WorkingState::open(&root.join(dir), lake, &ALL, super::DEFAULT_MEMORY_BYTES).expect("open");
    state.limits = limits;
    std::sync::Arc::new(state)
}

/// A tombstone the revision's own later record overwrites is never written: the commit
/// applies each key's winner alone, one write where there were two; a tombstone nothing
/// overwrites still deletes.
#[test]
fn a_tombstone_its_own_revision_overwrites_is_not_written() {
    let root = scratch("winners");
    let lake = lake(&root);
    let state = open(&root, "db", &lake);
    put(&state, 1, "a");
    put(&state, 2, "b");
    let before = state.live().expect("live").latest_sequence_number();
    write(
        &state,
        &frontier(),
        false,
        &[
            rec(1, "k1", 1.0, true, ""),
            rec(2, "k2", 1.0, true, ""),
            rec(1, "k1", 1.0, false, "a-again"),
        ],
        None,
    );
    let after = state.live().expect("live").latest_sequence_number();
    // k1's record, k2's tombstone, the relation's statement: three writes, not four.
    assert_eq!(after - before, 3);
    assert_eq!(
        all(&state, &frontier()),
        vec![("k1".to_string(), "a-again".to_string())]
    );
    std::fs::remove_dir_all(root).ok();
}
