// SPDX-License-Identifier: AGPL-3.0-only

//! The log partitioned by block (`keyed::block_history`): what the driver writes as it reads is
//! one block's records and the scope closers, whole, before and after the hours compact; a
//! rolled-back tick's objects are never read and leave at compaction; retired hours are gone.
//! The driver's own maintenance compacts what both operators passed, never an hour holding
//! objects it has not saved, and retires what its retention dropped.

mod keyed_log;

use analyzer_analysis::events::{Event, Kind};
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::keyed::{
    block_history, collect_held_garbage, compact, hour_of, hours_with_ticks, retire_before, Driver,
    Emission, PlacementCompute, RamState, RepeatCompute, Seeds, SegmentSource, SpineLayout,
    Watermark, Written,
};
use keyed_log::{segments, stream, Chunks, Log};

const LAYOUT: SpineLayout = SpineLayout {
    partitions: 8,
    retention_days: 7,
};

/// The stream, with every instance clearing its cache every 50 minutes.
fn events() -> Vec<Event> {
    let mut events = stream(3.0, 40);
    let (first, last) = (events[0].at_ms, events[events.len() - 1].at_ms);
    let mut at = first + 600_000.0;
    while at < last {
        for instance in ["i0", "i1", "i2"] {
            let mut clear = Event::new(Kind::Clear, instance, at);
            clear.dp_rank = Some(0);
            clear.group_idx = Some(0);
            clear.incarnation = "r1".into();
            events.push(clear);
        }
        at += 3_000_000.0;
    }
    analyzer_analysis::events::sort(&mut events);
    events
}

/// What a record is, for comparing: kind, holder, block, instant.
fn facts(events: &[Event]) -> Vec<(String, String, Option<String>, u64)> {
    let mut out: Vec<_> = events
        .iter()
        .map(|e| {
            (
                e.kind.as_str().to_string(),
                e.instance.clone(),
                e.block_id.clone(),
                e.at_ms.to_bits(),
            )
        })
        .collect();
    out.sort();
    out
}

fn expected(events: &[Event], block: &str) -> Vec<(String, String, Option<String>, u64)> {
    facts(
        &events
            .iter()
            .filter(|e| e.identity() == Some(block) || e.kind == Kind::Clear)
            .cloned()
            .collect::<Vec<_>>(),
    )
}

/// The objects of `hour` in the log partitioned by block that are not compacted ones.
fn tick_objects(lake: &analyzer_pipeline::Lake, hour: i64) -> Vec<String> {
    lake.store()
        .list(&format!("{}keyed-spine/{hour}/", lake.root()))
        .expect("list")
        .into_iter()
        .map(|o| o.key)
        // A tick object's name starts `t`; a compacted one's, its generation `g`.
        .filter(|k| k.rsplit('/').next().unwrap_or_default().starts_with('t'))
        .collect()
}

#[test]
fn a_blocks_history_is_its_records_and_the_closers_before_and_after_compaction() {
    let params = Params::default();
    let all = events();
    let parts = segments(&all);
    let log = Log::new("spine");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = open(&log);
    let failed = parts.len() / 2;
    for (seq, part) in parts.iter().enumerate() {
        log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        if working.begin(&log.lake).expect("begin") || !store.current(&log.lake).expect("head") {
            (driver, store) = open(&log);
        }
        let watermark = Watermark::At(part.last().map_or(0.0, |e| e.at_ms) - 150_000.0);
        driver
            .tick(&log.lake, &source, &params, watermark)
            .expect("tick");
        driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        if seq == failed {
            // Rolled back at the next begin: the objects this tick wrote are named nowhere.
            continue;
        }
        log.lake.end_unit(working.position()).expect("end");
        if seq == failed - 2 {
            working.checkpoint(&log.lake).expect("checkpoint");
        }
    }
    let settled = Watermark::At(all[all.len() - 1].at_ms + 10_000.0);
    working.begin(&log.lake).expect("begin");
    driver
        .tick(&log.lake, &source, &params, settled)
        .expect("the settled tick");
    driver
        .save(&log.lake, &mut store, parts.len() as u64 + 1)
        .expect("save");
    log.lake.end_unit(working.position()).expect("end");

    let window = Window {
        start_ms: all[0].at_ms,
        end_ms: all[all.len() - 1].at_ms + 1.0,
    };
    for block in ["b3", "b17", "b39"] {
        assert_eq!(
            facts(&block_history(&log.lake, block, window, &Written::default()).expect("history")),
            expected(&all, block),
            "{block}: its records and the closers, once each"
        );
    }

    let hours = hour_of(window.start_ms)..=hour_of(window.end_ms);
    for hour in hours.clone() {
        working.begin(&log.lake).expect("begin");
        compact(&log.lake, hour, 1e9).expect("compact");
        log.lake.end_unit(working.position()).expect("end");
        // No checkpoint holds the compaction yet: a rollback would return a manifest naming the
        // tick objects, so they stay.
        assert!(
            !tick_objects(&log.lake, hour).is_empty(),
            "hour {hour}: tick objects deleted before a checkpoint held the compaction"
        );
    }
    working.checkpoint(&log.lake).expect("checkpoint");
    working.begin(&log.lake).expect("begin");
    collect_held_garbage(&log.lake, 1e9 + 0.5).expect("collect");
    log.lake.end_unit(working.position()).expect("end");
    for hour in hours {
        let left = tick_objects(&log.lake, hour);
        assert!(left.is_empty(), "hour {hour}: tick objects left: {left:?}");
    }
    for block in ["b3", "b17", "b39"] {
        assert_eq!(
            facts(&block_history(&log.lake, block, window, &Written::default()).expect("history")),
            expected(&all, block),
            "{block}: unchanged by compaction"
        );
    }

    working.begin(&log.lake).expect("begin");
    retire_before(&log.lake, hour_of(window.end_ms), 1e9 + 1.0).expect("retire");
    log.lake.end_unit(working.position()).expect("end");
    let kept = block_history(&log.lake, "b3", window, &Written::default()).expect("history");
    assert!(
        kept.iter()
            .all(|e| hour_of(e.at_ms) == hour_of(window.end_ms)),
        "only the last hour is left"
    );
}

/// The driver over `log`'s store, laid out as the test writes the log.
fn open(log: &Log) -> (Driver<Seeds>, analyzer_pipeline::keyed::Store) {
    let (repeat, placement) = driver_parts();
    let (driver, store) =
        Driver::open(&log.lake, &Params::default(), repeat, placement).expect("open");
    (driver.with_layout(LAYOUT), store)
}

fn driver_parts() -> (RepeatCompute, PlacementCompute<Seeds>) {
    let skew = Params::default().delta_skew_ms;
    (
        RepeatCompute::new(2, Emission::Watermarked { delay_ms: skew }),
        PlacementCompute::new(
            2,
            Emission::Watermarked {
                delay_ms: skew / 2.0,
            },
            RamState::default,
        ),
    )
}

#[test]
fn maintenance_compacts_what_both_operators_passed_and_retires_what_retention_dropped() {
    let params = Params::default();
    let all = stream(30.0, 12);
    let parts = segments(&all);
    let log = Log::new("maintain");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (repeat, placement) = driver_parts();
    let (driver, mut store) = Driver::open(&log.lake, &params, repeat, placement).expect("open");
    let mut driver = driver.with_layout(SpineLayout {
        partitions: 8,
        retention_days: 1,
    });
    let mut classified_to = 0.0;
    for (seq, part) in parts.iter().enumerate() {
        log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        working.begin(&log.lake).expect("begin");
        let run = driver
            .tick(
                &log.lake,
                &source,
                &params,
                Watermark::At(part.last().map_or(0.0, |e| e.at_ms) - 150_000.0),
            )
            .expect("tick");
        classified_to = run.classified_to_ms.unwrap_or(classified_to);
        driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
        working.begin(&log.lake).expect("begin");
        driver.maintain(&log.lake, &store).expect("maintain");
        log.lake.end_unit(working.position()).expect("end");
        let passed = hour_of(classified_to);
        assert!(
            hours_with_ticks(&log.lake)
                .expect("hours")
                .iter()
                .all(|h| *h >= passed - 1),
            "tick {seq}: an hour both operators passed is left uncompacted"
        );
    }

    // A second engine's records land late, in two batches, in an hour long compacted. The first
    // is saved, so the hour holds tick objects again; maintenance between the second's tick and
    // its save leaves the hour alone, since the second's objects are named by no manifest yet.
    let late_hour = hour_of(classified_to) - 3;
    let late = late_stores(late_hour);
    log.commit_run(&chunks, ("r2", "i9", ""), 0, late[..10].to_vec());
    working.begin(&log.lake).expect("begin");
    driver
        .tick(&log.lake, &source, &params, Watermark::At(classified_to))
        .expect("tick");
    let next = store.tick() + 1;
    driver.save(&log.lake, &mut store, next).expect("save");
    log.lake.end_unit(working.position()).expect("end");
    assert!(
        hours_with_ticks(&log.lake)
            .expect("hours")
            .contains(&late_hour),
        "the saved late batch left tick objects in its hour"
    );
    log.commit_run(&chunks, ("r2", "i9", ""), 1, late[10..].to_vec());
    working.begin(&log.lake).expect("begin");
    driver
        .tick(&log.lake, &source, &params, Watermark::At(classified_to))
        .expect("tick");
    let compacted = driver.maintain(&log.lake, &store).expect("maintain");
    assert!(
        !compacted.contains(&late_hour),
        "an hour holding unsaved objects was compacted"
    );
    let next = store.tick() + 1;
    driver.save(&log.lake, &mut store, next).expect("save");
    log.lake.end_unit(working.position()).expect("end");

    let mut kept: Vec<Event> = all.clone();
    kept.extend(late);
    let from = hour_start(hour_of(classified_to) - 24);
    kept.retain(|e| e.at_ms >= from);
    // The log holds what the driver has read: everything before the classified edge.
    let window = Window {
        start_ms: all[0].at_ms,
        end_ms: classified_to,
    };
    kept.retain(|e| e.at_ms < classified_to);
    for block in ["b3", "b7"] {
        assert_eq!(
            facts(&block_history(&log.lake, block, window, &Written::default()).expect("history")),
            expected(&kept, block),
            "{block}: the late records are kept, and nothing older than the retention"
        );
    }
}

/// An hour's first instant.
fn hour_start(hour: i64) -> f64 {
    f64::from(i32::try_from(hour).expect("an hour since the epoch")) * 3_600_000.0
}

/// Twenty stores of a second engine, in `hour`.
fn late_stores(hour: i64) -> Vec<Event> {
    (0..20)
        .map(|k| {
            let mut e = Event::new(
                Kind::Store,
                "i9",
                hour_start(hour) + f64::from(k) * 30_000.0,
            );
            e.dp_rank = Some(0);
            e.group_idx = Some(0);
            e.block_id = Some(format!("b{}", k % 12));
            e.n_tokens = 16;
            e.incarnation = "r2".into();
            e
        })
        .collect()
}
