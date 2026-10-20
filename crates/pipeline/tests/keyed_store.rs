// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed operators' durable state (`keyed::Store`): a driver reopened from its store
//! between ticks classifies and places what one that never stopped does, and a unit of work
//! that did not end is rolled back with the working state and replayed from the head the
//! rollback left.
//!
//! The examined history is an hour over a nine-hour stream, so frontiers retire as their closes
//! leave it, in the reopened driver as in the one that never stopped.

mod keyed_log;

use analyzer_analysis::class_band::ClassBandCells;
use analyzer_analysis::classify::Frontier;
use analyzer_analysis::dup_cells::DupCells;
use analyzer_analysis::module::Mergeable;
use analyzer_analysis::module::Value;
use analyzer_analysis::Params;
use analyzer_pipeline::keyed::{
    Batch, Driver, Emission, IdentityKey, PlacementCompute, RamState, RepeatCompute, Seeds,
    SegmentSource, Store, Watermark,
};
use keyed_log::{segments, stream, Chunks, Log};
use std::collections::BTreeMap;

fn repeat() -> RepeatCompute {
    RepeatCompute::new(
        4,
        Emission::Watermarked {
            delay_ms: Params::default().delta_skew_ms,
        },
    )
}

/// The parameters every run here classifies under: an hour of examined history.
fn params() -> Params {
    Params::default().with_examined_history_ms(3_600_000.0)
}

fn placement() -> PlacementCompute<Seeds> {
    PlacementCompute::new(
        4,
        Emission::Watermarked {
            delay_ms: Params::default().delta_skew_ms / 2.0,
        },
        RamState::default,
    )
}

fn open(lake: &analyzer_pipeline::Lake) -> (Driver<Seeds>, Store) {
    Driver::open(lake, &params(), repeat(), placement()).expect("open")
}

/// The driver's state: the frontiers, what placement carries and the holders' rings.
type States = (
    BTreeMap<IdentityKey, Vec<Frontier>>,
    Carried,
    BTreeMap<analyzer_analysis::intervals::Holder, Vec<analyzer_analysis::emitted::ClassifiedFact>>,
);

/// Each identity's seeds and mass, and each identity's whale day.
type Carried = (
    BTreeMap<String, (String, i64)>,
    BTreeMap<(i64, String), analyzer_analysis::dup_cells::WhaleSum>,
);

/// What placement carries, as a replay must agree on it: each identity's open seeds and mass,
/// and every identity's whale day through the emitted edge. How much of a day a still identity
/// has stored rather than carries in its rates depends on when it began to hold still, which a
/// replay places afresh, so the days are compared whole.
fn carried(placement: &analyzer_pipeline::keyed::PlacementCompute<Seeds>) -> Carried {
    let residency = placement
        .states()
        .filter(|(_, s)| !s.seeds.is_empty())
        .map(|(k, s)| (k.clone(), (format!("{:?}", s.seeds), s.n_tokens)))
        .collect();
    let (from, to) = (
        placement.emitted_from_ms().unwrap_or(0.0),
        placement.emitted_to_ms().unwrap_or(0.0),
    );
    let mut whales = BTreeMap::new();
    for day in
        analyzer_analysis::emitted::day_index(from)..=analyzer_analysis::emitted::day_index(to)
    {
        for (id, w) in placement.whale_totals(day) {
            whales.insert((day, id), w);
        }
    }
    (residency, whales)
}

fn states(driver: &Driver<Seeds>) -> States {
    (
        driver
            .repeat()
            .states()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        carried(driver.placement()),
        driver
            .ring()
            .holders()
            .map(|(h, r)| (h.clone(), r.iter().cloned().collect()))
            .collect(),
    )
}

/// The watermark a tick after `segment` sees: behind its newest record, as a seal makes it.
fn watermark(segment: &[analyzer_analysis::events::Event]) -> Watermark {
    Watermark::At(segment.last().map_or(0.0, |e| e.at_ms) - 150_000.0)
}

/// What serving holds after the published batches: each cell's latest whole value by table and
/// key, a tombstone removing it.
type Served = BTreeMap<(String, Vec<String>), Vec<String>>;

fn publish(served: &mut Served, batch: &Batch) {
    for row in batch
        .classes
        .iter()
        .chain(&batch.placement)
        .chain(&batch.coverage)
        .chain(&batch.roster)
        .chain(&batch.blocks)
        .chain(&batch.tags)
        .chain(&batch.declarations)
    {
        let key = (
            row.table.to_string(),
            row.key.iter().map(Value::text).collect(),
        );
        if row.gone {
            served.remove(&key);
        } else {
            served.insert(key, row.measures.iter().map(Value::text).collect());
        }
    }
}

/// One driver that never stops, over every segment: its cells, what it published, and its final
/// state.
fn uninterrupted(
    parts: &[Vec<analyzer_analysis::events::Event>],
) -> (ClassBandCells, DupCells, Served, States) {
    let params = params();
    let log = Log::new("store-reference");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let mut driver = Driver::new(repeat(), placement());
    let mut total = ClassBandCells::default();
    let mut placed = DupCells::default();
    let mut served = Served::new();
    for (seq, part) in parts.iter().enumerate() {
        log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        let run = driver
            .tick(&log.lake, &source, &params, watermark(part))
            .expect("tick");
        total.merge(run.cells);
        placed.merge(run.placement.cells);
        publish(&mut served, &driver.drain());
    }
    (total, placed, served, states(&driver))
}

#[test]
fn a_driver_reopened_from_its_store_classifies_what_one_that_never_stopped_does() {
    let params = params();
    let parts = segments(&stream(9.0, 400));
    let (reference, reference_placed, reference_served, reference_states) = uninterrupted(&parts);

    let log = Log::new("store-reopened");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = open(&log.lake);
    let mut total = ClassBandCells::default();
    let mut placed = DupCells::default();
    let mut served = Served::new();
    for (seq, part) in parts.iter().enumerate() {
        log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        if seq % 3 == 2 {
            // A new process: everything in memory gone, the store all that is left.
            (driver, store) = open(&log.lake);
        }
        working.begin(&log.lake).expect("begin");
        let run = driver
            .tick(&log.lake, &source, &params, watermark(part))
            .expect("tick");
        let batch = driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
        publish(&mut served, &batch);
        total.merge(run.cells);
        placed.merge(run.placement.cells);
    }
    // Nine hours under a one-hour history: a close older than the history before the edge decides
    // nothing more, so the frontier holds none of them.
    let edge = driver.repeat().classified_to_ms().expect("classified");
    let cutoff = edge - params.examined_history_ms;
    let stale: Vec<_> = driver
        .repeat()
        .states()
        .flat_map(|(_, f)| f)
        .filter(|f| !f.open && f.last_close_ms.is_some_and(|c| c < cutoff))
        .collect();
    assert!(stale.is_empty(), "frontiers did not retire: {stale:?}");
    assert_eq!(total.buckets, reference.buckets, "class counts");
    assert_eq!(total.cells, reference.cells, "the class and band cells");
    assert!(!placed.is_empty(), "nothing was placed");
    assert_eq!(placed, reference_placed, "the placement cells");
    assert_eq!(served, reference_served, "what was published");
    assert_eq!(states(&driver), reference_states, "the frontiers and seeds");
}

#[test]
fn a_unit_that_did_not_end_is_rolled_back_and_replayed_from_the_head_it_left() {
    let params = params();
    let parts = segments(&stream(4.0, 400));
    let (_, _, reference_served, reference_states) = uninterrupted(&parts);
    let (checkpointed, failed) = (parts.len() / 3, parts.len() / 2);

    let log = Log::new("store-rollback");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = open(&log.lake);
    let mut served = Served::new();
    let mut reloads = 0;
    for (seq, part) in parts.iter().enumerate() {
        log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        let rolled_back = working.begin(&log.lake).expect("begin");
        if !store.current(&log.lake).expect("head") {
            // Memory is ahead of the store, and the store is the record.
            assert!(rolled_back, "only a rollback moves the head back");
            (driver, store) = open(&log.lake);
            reloads += 1;
            assert!(
                store.tick() > checkpointed as u64 && store.tick() <= failed as u64,
                "the rollback returned to a checkpoint before the failed unit: tick {}",
                store.tick()
            );
        }
        driver
            .tick(&log.lake, &source, &params, watermark(part))
            .expect("tick");
        let batch = driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        if seq == failed {
            // The unit dies before its end, so nothing of it is published: the next begin
            // rolls it back.
            continue;
        }
        log.lake.end_unit(working.position()).expect("end");
        publish(&mut served, &batch);
        if seq == checkpointed {
            working.checkpoint(&log.lake).expect("checkpoint");
        }
    }
    assert_eq!(reloads, 1, "one rollback, one reload");
    // The replay restated what the ticks since the checkpoint had published, whole, from
    // cells restored with the operators' state: nothing is added twice.
    assert_eq!(served, reference_served, "what was published");
    assert_eq!(states(&driver), reference_states, "the frontiers and seeds");
}

#[test]
fn a_batch_stored_but_never_published_is_published_from_the_outbox() {
    let params = params();
    let parts = segments(&stream(4.0, 400));
    let (_, _, reference_served, reference_states) = uninterrupted(&parts);
    let died = parts.len() / 2;

    let log = Log::new("store-outbox");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = open(&log.lake);
    let mut served = Served::new();
    for (seq, part) in parts.iter().enumerate() {
        log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        if seq == died + 1 {
            // A new process after a death between the unit's end and the publish: the head's
            // batch goes out again, under the head's publication, before anything else.
            (driver, store) = open(&log.lake);
            assert_eq!(store.publication(), died as u64 + 1);
            publish(&mut served, &store.outbox(&log.lake).expect("outbox"));
        }
        working.begin(&log.lake).expect("begin");
        driver
            .tick(&log.lake, &source, &params, watermark(part))
            .expect("tick");
        let batch = driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
        if seq != died {
            publish(&mut served, &batch);
        }
    }
    assert_eq!(served, reference_served, "what was published");
    assert_eq!(states(&driver), reference_states, "the frontiers and seeds");
}
