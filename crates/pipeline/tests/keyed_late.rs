// SPDX-License-Identifier: AGPL-3.0-only

//! The correction path (`keyed::Driver`): records that arrive behind the classified edge are
//! corrected per block from the log partitioned by block, so the held class and band cells and
//! the frontiers end as one tick over the complete log leaves them.
//!
//! A second engine's run shares the blocks, and its segments are committed hours after the
//! watermark passed them: every one of its records arrives late. Placement's cells and seeds
//! end so too, a late evict of a residency opened before the examined history included.

mod keyed_log;

use analyzer_analysis::events::{Event, Kind};
use analyzer_analysis::{Params, DAY_MS};
use analyzer_pipeline::keyed::{Driver, Seeds, SegmentSource, SpineLayout, Watermark};
use keyed_log::{segments, stream, Chunks, Log};
use std::collections::BTreeSet;

fn driver(log: &Log, params: &Params) -> (Driver<Seeds>, analyzer_pipeline::keyed::Store) {
    let (driver, store) = keyed_log::open_driver(&log.lake, params).expect("open");
    (
        driver.with_layout(SpineLayout {
            partitions: 8,
            retention_days: 7,
        }),
        store,
    )
}

/// The second engine: i9 storing and evicting the same twenty blocks for two hours, from an
/// hour in.
fn second_engine() -> Vec<Event> {
    let mut events = Vec::new();
    let mut open = BTreeSet::new();
    let mut at = DAY_MS * 100.0 + 3_600_000.0;
    let mut k: u64 = 7;
    while at < DAY_MS * 100.0 + 3.0 * 3_600_000.0 {
        k = k.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let block = format!("b{}", (k >> 33) % 20);
        let kind = if open.remove(&block) {
            Kind::Evict
        } else {
            open.insert(block.clone());
            Kind::Store
        };
        let mut e = Event::new(kind, "i9", at);
        e.dp_rank = Some(0);
        e.group_idx = Some(0);
        e.block_id = Some(block);
        e.n_tokens = 16;
        e.incarnation = "r2".into();
        e.workload_class = Some("agentic".into());
        events.push(e);
        at += 9_000.0 + f64::from(u32::try_from((k >> 20) % 20_000).expect("small"));
    }
    events
}

type Rows = analyzer_analysis::emitted::DayCells;

/// What a run ends with: its class and band cells, its frontiers, its holders' rings, its
/// placement cells and what it carries, the identities each operator corrected, and its
/// restatement cells.
type Ended = (
    Rows,
    Vec<(
        analyzer_pipeline::keyed::IdentityKey,
        Vec<analyzer_analysis::classify::Frontier>,
    )>,
    std::collections::BTreeMap<
        analyzer_analysis::intervals::Holder,
        Vec<analyzer_analysis::emitted::ClassifiedFact>,
    >,
    (analyzer_analysis::dup_cells::DupCells, Carried),
    (u64, u64),
    // Each family's corrected, too-late and closer counts in the restatement cells.
    std::collections::BTreeMap<String, (i64, i64, i64)>,
);

fn run(late: bool) -> Ended {
    let params = Params::default();
    let first = stream(5.0, 20);
    let second = segments(&second_engine());
    let log = Log::new(if late { "late" } else { "settled" });
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = driver(&log, &params);
    let end = first[first.len() - 1].at_ms + params.delta_skew_ms + 1.0;
    let mut corrected = (0, 0);
    let tick = |driver: &mut Driver<Seeds>,
                store: &mut analyzer_pipeline::keyed::Store,
                seq: u64,
                watermark: f64| {
        working.begin(&log.lake).expect("begin");
        let r = driver
            .tick(&log.lake, &source, &params, Watermark::At(watermark))
            .expect("tick");
        driver.save(&log.lake, store, seq).expect("save");
        log.lake.end_unit(working.position()).expect("end");
        (r.late.corrected, r.placement_late.corrected)
    };
    let mut seq = 0;
    if late {
        for (i, part) in segments(&first).into_iter().enumerate() {
            let last = part.last().map_or(0.0, |e| e.at_ms);
            log.commit_run(
                &chunks,
                ("r1", "i0", ""),
                i64::try_from(i).expect("small"),
                part,
            );
            // Four hours in, the second engine's segments land, every record behind the edge.
            if i == 48 {
                for (j, part) in second.iter().enumerate() {
                    log.commit_run(
                        &chunks,
                        ("r2", "i9", ""),
                        i64::try_from(j).expect("small"),
                        part.clone(),
                    );
                }
            }
            seq += 1;
            let (c, p) = tick(&mut driver, &mut store, seq, last - 150_000.0);
            corrected = (corrected.0 + c, corrected.1 + p);
        }
    } else {
        for (i, part) in segments(&first).into_iter().enumerate() {
            log.commit_run(
                &chunks,
                ("r1", "i0", ""),
                i64::try_from(i).expect("small"),
                part,
            );
        }
        for (j, part) in second.iter().enumerate() {
            log.commit_run(
                &chunks,
                ("r2", "i9", ""),
                i64::try_from(j).expect("small"),
                part.clone(),
            );
        }
    }
    seq += 1;
    let (c, p) = tick(&mut driver, &mut store, seq, end);
    corrected = (corrected.0 + c, corrected.1 + p);
    let states = driver
        .repeat()
        .states()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let rings = driver
        .ring()
        .holders()
        .map(|(h, r)| (h.clone(), r.iter().cloned().collect()))
        .collect();
    let seeds = carried(driver.placement());
    (
        driver.classes().cells().rows(),
        states,
        rings,
        (driver.placed().cells().clone(), seeds),
        corrected,
        restated(&driver),
    )
}

/// Each identity's seeds and mass, and each identity's whale day.
type Carried = (
    Vec<(String, String, i64)>,
    Vec<(i64, String, analyzer_analysis::dup_cells::WhaleSum)>,
);

/// What placement carries, as a re-derivation must agree on it: each identity's open seeds and
/// mass, and every identity's whale day through the emitted edge. How much of a day a still
/// identity has stored rather than carries in its rates depends on when it began to hold still,
/// so the days are compared whole.
fn carried(
    placement: &analyzer_pipeline::keyed::PlacementCompute<analyzer_pipeline::keyed::Seeds>,
) -> Carried {
    let mut residency: Vec<_> = placement
        .states()
        .filter(|(_, s)| !s.seeds.is_empty())
        .map(|(k, s)| (k.clone(), format!("{:?}", s.seeds), s.n_tokens))
        .collect();
    residency.sort();
    let (from, to) = (
        placement.emitted_from_ms().unwrap_or(0.0),
        placement.emitted_to_ms().unwrap_or(0.0),
    );
    let mut whales = Vec::new();
    for day in
        analyzer_analysis::emitted::day_index(from)..=analyzer_analysis::emitted::day_index(to)
    {
        let mut totals = placement.whale_totals(day);
        totals.sort_by(|a, b| a.0.cmp(&b.0));
        whales.extend(totals.into_iter().map(|(id, w)| (day, id, w)));
    }
    (residency, whales)
}

#[test]
fn late_records_are_corrected_to_what_the_complete_log_classifies() {
    let (late_rows, late_states, late_rings, late_placed, corrected, late_restated) = run(true);
    let (settled_rows, settled_states, settled_rings, settled_placed, _, settled_restated) =
        run(false);
    assert!(
        corrected.0 > 0 && corrected.1 > 0,
        "nothing arrived late: {corrected:?}"
    );
    assert_eq!(late_rows, settled_rows, "the class and band cells");
    let mut a = late_states;
    let mut b = settled_states;
    a.sort_by(|x, y| x.0.cmp(&y.0));
    b.sort_by(|x, y| x.0.cmp(&y.0));
    assert_eq!(a, b, "the frontiers");
    assert_eq!(late_rings, settled_rings, "the holders' rings");
    assert!(
        !settled_placed.0.is_empty(),
        "no placement cells to compare"
    );
    assert_eq!(late_placed.0, settled_placed.0, "the placement cells");
    assert_eq!(late_placed.1, settled_placed.1, "the placement seeds");
    // Every record of the second engine arrived late, and each correction path counted it.
    let n = i64::try_from(second_engine().len()).expect("small");
    assert_eq!(
        (late_restated.get("classes"), late_restated.get("placement")),
        (Some(&(n, 0, 0)), Some(&(n, 0, 0))),
        "the restatement cells: {late_restated:?}"
    );
    assert!(settled_restated.is_empty(), "nothing restated in order");
}

/// The restatement cells, summed per family.
fn restated(driver: &Driver<Seeds>) -> std::collections::BTreeMap<String, (i64, i64, i64)> {
    let mut sums = std::collections::BTreeMap::new();
    for ((_, family), c) in driver.restated().cells() {
        let s: &mut (i64, i64, i64) = sums.entry(family.clone()).or_default();
        *s = (s.0 + c.corrected, s.1 + c.too_late, s.2 + c.closers);
    }
    sums
}

/// Placement over an hour of examined history: a second engine stores a block ten minutes in and
/// evicts it four hours in, the evict committed half an hour late, or in order. The engine is
/// heard throughout, its records on another block every twenty minutes, so no silence ends the
/// residency first. What the run ends with: its placement cells and seeds, and the identities
/// placement corrected.
fn old_residency(late: bool) -> (Ended, u64) {
    let params = Params::default().with_examined_history_ms(3_600_000.0);
    let first = stream(5.0, 20);
    let t0 = first[0].at_ms;
    let on = |block: &str, ms: f64, kind: Kind| {
        let mut e = Event::new(kind, "i9", t0 + ms);
        e.dp_rank = Some(0);
        e.group_idx = Some(0);
        e.block_id = Some(block.into());
        e.n_tokens = 64;
        e.incarnation = "r2".into();
        e.workload_class = Some("agentic".into());
        e
    };
    let opened = vec![on("old", 600_000.0, Kind::Store)];
    let closed = vec![on("old", 4.0 * 3_600_000.0, Kind::Evict)];
    let mut beats: std::collections::VecDeque<Vec<Event>> = (1..15)
        .map(|k| {
            let ms = f64::from(k) * 1_200_000.0;
            vec![
                on("beat", ms, Kind::Store),
                on("beat", ms + 60_000.0, Kind::Evict),
            ]
        })
        .collect();
    let mut seq = 0;
    let mut commit_i9 = |log: &Log, chunks: &Chunks, events: Vec<Event>| {
        seq += 1;
        log.commit_run(chunks, ("r2", "i9", ""), seq, events);
    };
    let log = Log::new(if late { "old-late" } else { "old-settled" });
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = driver(&log, &params);
    let mut corrected = 0;
    commit_i9(&log, &chunks, opened);
    let mut closed = Some(closed);
    if !late {
        commit_i9(&log, &chunks, closed.take().expect("once"));
    }
    for (i, part) in segments(&first).into_iter().enumerate() {
        let last = part.last().map_or(0.0, |e| e.at_ms);
        log.commit_run(
            &chunks,
            ("r1", "i0", ""),
            i64::try_from(i).expect("small"),
            part,
        );
        while beats
            .front()
            .is_some_and(|b| b.iter().all(|e| e.at_ms <= last))
        {
            commit_i9(&log, &chunks, beats.pop_front().expect("a beat"));
        }
        if last > t0 + 4.5 * 3_600_000.0 {
            if let Some(closed) = closed.take() {
                commit_i9(&log, &chunks, closed);
            }
        }
        working.begin(&log.lake).expect("begin");
        let r = driver
            .tick(&log.lake, &source, &params, Watermark::At(last - 150_000.0))
            .expect("tick");
        corrected += r.placement_late.corrected;
        let next = store.tick() + 1;
        driver.save(&log.lake, &mut store, next).expect("save");
        log.lake.end_unit(working.position()).expect("end");
    }
    let seeds = carried(driver.placement());
    (
        (
            driver.classes().cells().rows(),
            Vec::new(),
            std::collections::BTreeMap::new(),
            (driver.placed().cells().clone(), seeds),
            (0, 0),
            restated(&driver),
        ),
        corrected,
    )
}

#[test]
fn a_late_evict_closes_a_residency_opened_before_the_examined_history() {
    let (late, corrected) = old_residency(true);
    let (settled, _) = old_residency(false);
    assert!(corrected > 0, "the evict did not arrive late");
    assert_eq!(late.3 .0, settled.3 .0, "the placement cells");
    assert_eq!(late.3 .1, settled.3 .1, "the placement seeds");
}
