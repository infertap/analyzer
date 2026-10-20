// SPDX-License-Identifier: AGPL-3.0-only

//! One block re-derived from the log partitioned by block (`keyed::refold_block`): over the
//! whole run, each block's re-fold states exactly the facts repeat compute stated for it as the
//! log grew, the drill's answer and the correction's reference.

mod keyed_log;

use analyzer_analysis::emitted::ClassifiedFact;
use analyzer_analysis::events::Kind;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::keyed::{
    refold_block, Driver, Seeds, SegmentSource, SpineLayout, Watermark, Written,
};
use keyed_log::{segments, stream, Chunks, Log};

/// A fact as compared: holder, block, instant, class, gap and tokens.
fn key(f: &ClassifiedFact) -> (String, u32, String, u64, String, Option<u64>, i64) {
    (
        f.holder.instance.clone(),
        f.holder.dp_rank,
        f.block.clone(),
        f.at_ms.to_bits(),
        f.class.as_str().to_string(),
        f.gap_ms.map(f64::to_bits),
        f.n_tokens,
    )
}

/// The stream `all` run through a driver a segment a tick under `params`, every tick saved, and
/// `late`'s records committed as another engine's run on the first tick whose watermark passes
/// its instant: the log, the driver, and every fact it stated.
fn run(
    name: &str,
    all: &[analyzer_analysis::events::Event],
    params: &Params,
    late: Option<(f64, Vec<analyzer_analysis::events::Event>)>,
) -> (Log, Driver<Seeds>, Vec<ClassifiedFact>) {
    let mut late = late;
    let parts = segments(all);
    let log = Log::new(name);
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let skew = params.delta_skew_ms;
    let (driver, mut store) = keyed_log::open_driver(&log.lake, params).expect("open");
    let mut driver: Driver<Seeds> = driver.with_layout(SpineLayout {
        partitions: 8,
        retention_days: 7,
    });
    let mut stated: Vec<ClassifiedFact> = Vec::new();
    let end = all[all.len() - 1].at_ms + skew + 1.0;
    let ticks: Vec<Watermark> = parts
        .iter()
        .map(|p| Watermark::At(p.last().map_or(0.0, |e| e.at_ms) - 150_000.0))
        .chain([Watermark::At(end)])
        .collect();
    for (seq, watermark) in ticks.into_iter().enumerate() {
        if let Some(part) = parts.get(seq) {
            log.commit(&chunks, i64::try_from(seq).expect("small"), part.clone());
        }
        let Watermark::At(at) = watermark else {
            unreachable!("every tick is at an instant")
        };
        if let Some((_, records)) = late.take_if(|(arrives, _)| at >= *arrives) {
            log.commit_run(&chunks, ("r9", "i9", ""), 0, records);
        }
        working.begin(&log.lake).expect("begin");
        let run = driver
            .tick(&log.lake, &source, params, watermark)
            .expect("tick");
        stated.extend(run.cells.facts);
        driver
            .save(&log.lake, &mut store, seq as u64 + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
    }
    (log, driver, stated)
}

/// Each named block re-derived over `window`, compared with what was stated for it there.
/// Returns how many facts were compared.
fn compare(
    (log, driver, stated): &(Log, Driver<Seeds>, Vec<ClassifiedFact>),
    params: &Params,
    window: Window,
    blocks: &[String],
) -> usize {
    let mut checked = 0;
    for block in blocks {
        let refold = refold_block(
            &log.lake,
            block,
            window,
            (params, &driver.roster().cells().models()),
            &Written::default(),
        )
        .expect("refold");
        let mut got: Vec<_> = refold.facts.iter().map(key).collect();
        let mut want: Vec<_> = stated
            .iter()
            .filter(|f| &f.block == block && f.at_ms >= window.start_ms && f.at_ms < window.end_ms)
            .map(key)
            .collect();
        got.sort();
        want.sort();
        assert_eq!(got, want, "{block}: the re-fold's facts");
        checked += want.len();
    }
    checked
}

#[test]
fn a_blocks_refold_states_what_repeat_compute_stated_for_it() {
    let params = Params::default();
    let all = stream(6.0, 30);
    let skew = params.delta_skew_ms;
    let end = all[all.len() - 1].at_ms + skew + 1.0;
    let ran = run("refold", &all, &params, None);
    let window = Window {
        start_ms: all[0].at_ms,
        end_ms: end - skew,
    };
    let blocks: Vec<String> = ["b0", "b3", "b7", "b11", "b19", "b23", "b29"]
        .map(String::from)
        .to_vec();
    let checked = compare(&ran, &params, window, &blocks);
    assert!(checked > 100, "only {checked} facts compared");
}

#[test]
fn a_refold_from_a_later_day_sees_residencies_open_since_the_days_before() {
    // Sixty hours under a one-hour history: a window late on the third day is re-derived from
    // the frontiers carried at its day start, which hold residencies opened a day and more
    // before it. Read from the boundary alone, those holders would be invisible, and a store
    // the stream stated RE against one would come back cold.
    let params = Params::default().with_examined_history_ms(3_600_000.0);
    let mut all = stream(60.0, 30);
    // One block held from the stream's first minute to its end, and stored again elsewhere on
    // the third day: RE only to a reader that sees a residency opened two days before.
    let day = analyzer_analysis::DAY_MS;
    let held = |kind, instance: &str, at_ms| {
        let mut e = all[0].clone();
        e.kind = kind;
        e.instance = instance.to_string();
        e.block_id = Some("hot".to_string());
        e.at_ms = at_ms;
        e
    };
    let start = all[0].at_ms;
    let third = (start / day).floor() * day + 2.0 * day;
    all.extend([
        held(Kind::Store, "i9", start + 1.0),
        held(Kind::Store, "i8", third + 5.0 * 3_600_000.0),
        held(Kind::Evict, "i8", third + 5.0 * 3_600_000.0 + 60_000.0),
    ]);
    analyzer_analysis::events::sort(&mut all);
    let skew = params.delta_skew_ms;
    let end = all[all.len() - 1].at_ms + skew + 1.0;
    let ran = run("refold-carried", &all, &params, None);
    let window = Window {
        start_ms: third + 3.0 * 3_600_000.0,
        end_ms: end - skew,
    };
    let hot: Vec<_> = ran.2.iter().filter(|f| f.block == "hot").collect();
    assert_eq!(hot.len(), 2, "the two stores of the held block");
    assert_eq!(hot[1].class.as_str(), "RE", "i9 holds it still");
    let blocks: Vec<String> = (0..30)
        .map(|b| format!("b{b}"))
        .chain(["hot".to_string()])
        .collect();
    let checked = compare(&ran, &params, window, &blocks);
    assert!(checked > 100, "only {checked} facts compared");
}

#[test]
fn a_correction_restates_what_the_day_boundary_carried() {
    // i9's evict of the held block, half an hour before the third day, lands twenty minutes
    // after the stream crossed into it. The correction ends i9's residency in the frontiers and
    // in what the boundary carried, so the store on i8 that day is not RE, and a re-derivation
    // from that boundary says so too.
    let params = Params::default().with_examined_history_ms(3_600_000.0);
    let mut all = stream(60.0, 30);
    let day = analyzer_analysis::DAY_MS;
    let held = |kind, instance: &str, at_ms| {
        let mut e = all[0].clone();
        e.kind = kind;
        e.instance = instance.to_string();
        e.block_id = Some("hot".to_string());
        e.at_ms = at_ms;
        e
    };
    let start = all[0].at_ms;
    let third = (start / day).floor() * day + 2.0 * day;
    let late = vec![held(Kind::Evict, "i9", third - 1_800_000.0)];
    all.extend([
        held(Kind::Store, "i9", start + 1.0),
        held(Kind::Store, "i8", third + 5.0 * 3_600_000.0),
        held(Kind::Evict, "i8", third + 5.0 * 3_600_000.0 + 60_000.0),
    ]);
    analyzer_analysis::events::sort(&mut all);
    let skew = params.delta_skew_ms;
    let end = all[all.len() - 1].at_ms + skew + 1.0;
    let ran = run(
        "refold-recarried",
        &all,
        &params,
        Some((third + 1_200_000.0, late)),
    );
    let hot: Vec<_> = ran.2.iter().filter(|f| f.block == "hot").collect();
    assert_eq!(hot.len(), 2, "the two stores of the held block");
    assert_ne!(hot[1].class.as_str(), "RE", "i9 evicted it before the day");
    let window = Window {
        start_ms: third + 3.0 * 3_600_000.0,
        end_ms: end - skew,
    };
    assert_eq!(
        compare(&ran, &params, window, &["hot".to_string()]),
        1,
        "the store on i8"
    );
}
