// SPDX-License-Identifier: AGPL-3.0-only

//! The whale register over a run long enough for a day to settle: the driver's placement cells
//! are one global fold's, the settled day's published register is that fold's per-identity day
//! totals cut at `WHALE_CUT`, and most ticks folded fewer identities than they held.

mod keyed_log;

use analyzer_analysis::dup_cells::{DupCells, DupCtx, Duplication};
use analyzer_analysis::events::ScopeClosers;
use analyzer_analysis::module::{run_window, Persisted, Row, Value};
use analyzer_analysis::whale_days::{Day, WhaleDays, WHALE_CUT};
use analyzer_analysis::{Params, Window, DAY_MS};
use analyzer_pipeline::keyed::{Driver, Seeds, SegmentSource, SpineLayout, Store, Watermark};
use keyed_log::{segments, stream, Chunks, Log};
use std::collections::BTreeMap;

type Keyed = BTreeMap<(String, String), Vec<f64>>;

fn driver(log: &Log, params: &Params) -> (Driver<Seeds>, Store) {
    let (driver, store) = keyed_log::open_driver(&log.lake, params).expect("open");
    let driver = driver
        .with_layout(SpineLayout {
            partitions: 8,
            retention_days: 7,
        })
        .with_settle_delay(0.0);
    (driver, store)
}

/// Rows by table and key; the last write of a key wins and a tombstone removes it.
fn keyed(rows: &[Row]) -> Keyed {
    let number = |v: &Value| match v {
        Value::F64(x) => *x,
        #[allow(clippy::cast_precision_loss)] // test sums stay far below 2^52
        Value::I64(x) => *x as f64,
        Value::U32(x) => f64::from(*x),
        other => panic!("a measure that is not a number: {other:?}"),
    };
    let mut out = BTreeMap::new();
    for row in rows {
        let key = (row.table.to_string(), format!("{:?}", row.key));
        if row.gone {
            out.remove(&key);
        } else {
            out.insert(key, row.measures.iter().map(number).collect());
        }
    }
    out
}

fn agree(what: &str, want: &Keyed, have: &Keyed) {
    assert_eq!(
        want.keys().collect::<Vec<_>>(),
        have.keys().collect::<Vec<_>>(),
        "{what}: the keys"
    );
    for (key, w) in want {
        for (a, b) in w.iter().zip(&have[key]) {
            let scale = a.abs().max(b.abs()).max(1.0);
            assert!(
                ((a - b) / scale).abs() < 1e-9,
                "{what} {key:?}: {w:?} vs {:?}",
                have[key]
            );
        }
    }
}

/// One fold of every record over `emitted`, from no seeds.
fn global_fold(
    all: &[analyzer_analysis::events::Event],
    emitted: Window,
    params: &Params,
) -> analyzer_analysis::module::Outcome<Duplication> {
    let closers = ScopeClosers::of(all);
    let mut grouped = BTreeMap::new();
    for e in all {
        if let Some(id) = e.identity() {
            grouped
                .entry(id.to_string())
                .or_insert_with(Vec::new)
                .push(e.clone());
        }
    }
    run_window(
        &Duplication,
        grouped,
        &BTreeMap::new(),
        emitted,
        &DupCtx {
            closers: &closers,
            half_skew: params.delta_skew_ms / 2.0,
        },
    )
}

#[test]
fn a_settled_day_states_the_global_fold_s_whales_and_still_identities_cost_no_fold() {
    let params = Params::default();
    let all = stream(30.0, 40);
    let parts = segments(&all);
    let log = Log::new("whales");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store) = driver(&log, &params);
    let end = all[all.len() - 1].at_ms + params.delta_skew_ms + 1.0;
    let ticks: Vec<Watermark> = parts
        .iter()
        .map(|p| Watermark::At(p.last().map_or(0.0, |e| e.at_ms) - 150_000.0))
        .chain([Watermark::At(end)])
        .collect();
    let (mut published, mut quiet_ticks) = (Vec::new(), 0);
    for (seq, watermark) in ticks.into_iter().enumerate() {
        if let Some(part) = parts.get(seq) {
            log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        }
        working.begin(&log.lake).expect("begin");
        let run = driver
            .tick(&log.lake, &source, &params, watermark)
            .expect("tick");
        let seeded = driver
            .placement()
            .states()
            .filter(|(_, s)| !s.seeds.is_empty())
            .count() as u64;
        if run.placement.identities < seeded {
            quiet_ticks += 1;
        }
        let batch = driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        published.extend(batch.whales);
        log.lake.end_unit(working.position()).expect("end");
    }
    assert!(
        quiet_ticks > 100,
        "only {quiet_ticks} ticks left identities to the sum"
    );

    // One fold of every record over what placement emitted.
    let emitted = Window::new(
        driver.placement().emitted_from_ms().expect("emitted"),
        driver.placement().emitted_to_ms().expect("emitted"),
    )
    .expect("window");
    let reference = global_fold(&all, emitted, &params);
    let cells = |c: &DupCells| {
        keyed(
            &c.days()
                .into_iter()
                .flat_map(|day| c.render_day(day))
                .collect::<Vec<_>>(),
        )
    };
    agree(
        "the cells",
        &cells(&reference.cells),
        &cells(driver.placed().cells()),
    );

    #[allow(clippy::cast_possible_truncation)] // the stream's first day index
    let day = (all[0].at_ms / DAY_MS).floor() as i64;
    assert!(
        driver.settled_through_day().is_some_and(|d| d >= day),
        "the first day did not settle"
    );
    let mut want = WhaleDays::default();
    want.set_day(
        day,
        Day::of(
            reference
                .states
                .iter()
                .filter_map(|(id, s)| s.whale.days.get(&day).map(|w| (id.clone(), *w))),
            WHALE_CUT,
        ),
    );
    let want = keyed(&want.day_rows(day));
    let marker = format!("Date({day})");
    let have: Keyed = keyed(&published)
        .into_iter()
        .filter(|((_, key), _)| key.contains(&marker))
        .collect();
    assert!(
        want.len() > 2,
        "the day held duplicated identities: {want:?}"
    );
    agree("the whale day", &want, &have);
}
