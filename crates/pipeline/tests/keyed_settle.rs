// SPDX-License-Identifier: AGPL-3.0-only

//! Settled days and meanings (`docs/pipeline.md`): the driver reports
//! each day settled once, after every edge has passed it by the settle delay, and again when a
//! correction restates it; a store written under one meaning refuses another, and a reset
//! starts the new meaning's state with the generations carried. A late record of a day no
//! longer open to change is counted, never corrected, across a restart too.

mod keyed_log;

use analyzer_analysis::{Params, DAY_MS};
use analyzer_pipeline::keyed::{Driver, Seeds, SegmentSource, SpineLayout, Store, Watermark};
use keyed_log::{segments, stream, Chunks, Log};

fn open(log: &Log, params: &Params) -> Result<(Driver<Seeds>, Store), analyzer_pipeline::Error> {
    let (driver, store) = keyed_log::open_driver(&log.lake, params)?;
    Ok((
        driver
            .with_layout(SpineLayout {
                partitions: 8,
                retention_days: 7,
            })
            .with_settle_delay(60_000.0),
        store,
    ))
}

/// Tick `parts` in, from `from`, and return every day each tick reported settled.
fn tick_through(
    log: &Log,
    chunks: &Chunks,
    (driver, store): (&mut Driver<Seeds>, &mut Store),
    params: &Params,
    parts: &[Vec<analyzer_analysis::events::Event>],
) -> Vec<i64> {
    let source = SegmentSource::new(&log.lake, chunks);
    let working = log.lake.working().expect("a working state");
    let mut settled = Vec::new();
    for (seq, part) in parts.iter().enumerate() {
        log.commit(chunks, i64::try_from(seq).expect("small"), part.clone());
        working.begin(&log.lake).expect("begin");
        let run = driver
            .tick(
                &log.lake,
                &source,
                params,
                Watermark::At(part.last().map_or(0.0, |e| e.at_ms) - 150_000.0),
            )
            .expect("tick");
        settled.extend(run.settled_days);
        driver
            .save(&log.lake, store, store.tick() + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
    }
    settled
}

#[test]
fn each_day_settles_once_after_every_edge_has_passed_it() {
    let params = Params::default();
    let parts = segments(&stream(30.0, 12));
    let log = Log::new("settle");
    let chunks = Chunks::default();
    let (mut driver, mut store) = open(&log, &params).expect("open");
    let settled = tick_through(&log, &chunks, (&mut driver, &mut store), &params, &parts);
    let first_day = 100;
    assert_eq!(
        settled,
        vec![first_day],
        "day 100 settled once; day 101 is still open"
    );
    assert_eq!(driver.settled_through_day(), Some(first_day));
    let generation = &driver.generations()[0];
    assert_eq!(
        (generation.first_day, generation.last_day),
        (first_day, first_day)
    );

    // A second engine's segment lands late in day 100's second hour: the correction restates
    // the settled day, which is reported again.
    let late = second_engine(DAY_MS * 100.0 + 2.0 * 3_600_000.0);
    log.commit_run(&chunks, ("r2", "i9", ""), 0, late);
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    working.begin(&log.lake).expect("begin");
    let run = driver
        .tick(
            &log.lake,
            &source,
            &params,
            Watermark::At(DAY_MS * 100.0 + 30.0 * 3_600_000.0),
        )
        .expect("tick");
    let next = store.tick() + 1;
    driver.save(&log.lake, &mut store, next).expect("save");
    log.lake.end_unit(working.position()).expect("end");
    assert!(run.late.corrected > 0, "the late segment was corrected");
    assert_eq!(
        run.settled_days,
        vec![first_day],
        "the restated day is reported again"
    );

    // Reopened, the store keeps what settled.
    let (reopened, _) = open(&log, &params).expect("reopen");
    assert_eq!(reopened.settled_through_day(), Some(first_day));
    assert_eq!(reopened.generations(), driver.generations());
}

/// Twenty stores of a second engine, from `from_ms`.
fn second_engine(from_ms: f64) -> Vec<analyzer_analysis::events::Event> {
    (0..20)
        .map(|k| {
            let mut e = analyzer_analysis::events::Event::new(
                analyzer_analysis::events::Kind::Store,
                "i9",
                from_ms + f64::from(k) * 30_000.0,
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

#[test]
fn a_late_record_of_a_day_no_longer_open_is_counted_and_never_corrected() {
    use analyzer_analysis::module::DayPartitioned;
    // A day closes once every edge is the correction horizon past it: after fifty hours, day
    // 100 is closed to change.
    let params = Params::default();
    let parts = segments(&stream(50.0, 12));
    let log = Log::new("too-late");
    let chunks = Chunks::default();
    let (mut driver, mut store) = open(&log, &params).expect("open");
    tick_through(&log, &chunks, (&mut driver, &mut store), &params, &parts);
    assert!(
        !driver.classes().cells().days().contains(&100)
            && !driver.covered().cells().days().contains(&100),
        "day 100 is no longer held, its coverage included"
    );

    // Reopened, the store still knows which days are open.
    let (mut driver, mut store) = open(&log, &params).expect("reopen");
    log.commit_run(
        &chunks,
        ("r2", "i9", ""),
        0,
        second_engine(DAY_MS * 100.0 + 2.0 * 3_600_000.0),
    );
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    working.begin(&log.lake).expect("begin");
    let run = driver
        .tick(
            &log.lake,
            &source,
            &params,
            Watermark::At(DAY_MS * 100.0 + 50.0 * 3_600_000.0),
        )
        .expect("tick");
    let next = store.tick() + 1;
    driver.save(&log.lake, &mut store, next).expect("save");
    log.lake.end_unit(working.position()).expect("end");
    assert_eq!((run.late.too_late, run.late.corrected), (20, 0));
    assert!(
        !driver.classes().cells().days().contains(&100),
        "no difference is held for day 100 as if it were the day's whole value"
    );
}

#[test]
fn another_meaning_is_refused_until_a_reset_carries_the_generations_over() {
    let first = Params::default();
    let parts = segments(&stream(30.0, 12));
    let log = Log::new("meanings");
    let chunks = Chunks::default();
    let (mut driver, mut store) = open(&log, &first).expect("open");
    tick_through(&log, &chunks, (&mut driver, &mut store), &first, &parts);

    let mut second = Params::default();
    second.delta_skew_ms = first.delta_skew_ms * 2.0;
    assert!(
        matches!(
            open(&log, &second),
            Err(analyzer_pipeline::Error::IncompatibleState)
        ),
        "a store written under one meaning is not read under another"
    );
    Store::reset(&log.lake).expect("reset");
    let (mut fresh, mut fresh_store) = open(&log, &second).expect("open the new meaning");
    assert_eq!(fresh_store.tick(), 0, "the new meaning starts fresh");
    assert_eq!(
        fresh.roster().cells().runs.len(),
        driver.roster().cells().runs.len(),
        "the roster stays"
    );
    // The new meaning re-derives the log from its start: its first tick delivers everything.
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    working.begin(&log.lake).expect("begin");
    let run = fresh
        .tick(
            &log.lake,
            &source,
            &second,
            Watermark::At(DAY_MS * 100.0 + 30.0 * 3_600_000.0),
        )
        .expect("tick");
    fresh.save(&log.lake, &mut fresh_store, 1).expect("save");
    log.lake.end_unit(working.position()).expect("end");
    assert_eq!(
        run.settled_days,
        vec![100],
        "the new meaning settles day 100 again"
    );
    let meanings: Vec<&str> = fresh
        .generations()
        .iter()
        .map(|g| g.discriminator.as_str())
        .collect();
    assert_eq!(
        meanings.len(),
        2,
        "both meanings' days are kept: {meanings:?}"
    );
}
