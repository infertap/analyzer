// SPDX-License-Identifier: AGPL-3.0-only

//! Coverage is what the producer observed, not when its segments arrived: a source whose
//! segments ship an hour late (a shipper outage, stated uncovered once the grace passed) ends
//! with the coverage, and the pair basis, it would have had delivered in order, while a source
//! that produced nothing over the same hour stays uncovered.

mod keyed_log;

use analyzer_analysis::coverage_sums::{MinuteKey, Observed};
use analyzer_analysis::events::Event;
use analyzer_analysis::Params;
use analyzer_pipeline::keyed::{Driver, Seeds, SegmentSource, Watermark};
use keyed_log::{segments, stream, Chunks, Log};
use std::collections::BTreeMap;

/// How the second source's segments of the held hour reach the log.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Held {
    /// In order, as produced.
    InOrder,
    /// Committed together an hour late, after the grace has passed them.
    Late,
    /// Never: the source produced nothing, and its numbering carries on after.
    Silent,
}

/// The ticks that hold back the second source: the fourth hour, at five minutes a segment.
const HELD: std::ops::Range<usize> = 36..48;
/// The tick the held segments are committed at, when late.
const RELEASED: usize = 60;

/// What a run states: the second source's minute cells, the coverage arrivals restated, the
/// sources restated, and the pair basis's spans.
type Stated = (
    BTreeMap<MinuteKey, Observed>,
    u64,
    i64,
    Vec<(i64, u64, u64)>,
);

/// Commit the second source's segments for tick `step` as `held` delivers them.
fn commit_second(log: &Log, chunks: &Chunks, held: Held, step: usize, second: &[Vec<Event>]) {
    let seq = |s: usize| i64::try_from(s).expect("small");
    match held {
        Held::Late | Held::Silent if HELD.contains(&step) => {}
        Held::InOrder => {
            log.commit_run(chunks, ("r2", "i1", ""), seq(step), second[step].clone());
        }
        Held::Late => {
            if step == RELEASED {
                for s in HELD {
                    log.commit_run(chunks, ("r2", "i1", ""), seq(s), second[s].clone());
                }
            }
            log.commit_run(chunks, ("r2", "i1", ""), seq(step), second[step].clone());
        }
        Held::Silent => {
            let renumbered = if step < HELD.start {
                step
            } else {
                step - HELD.len()
            };
            log.commit_run(
                chunks,
                ("r2", "i1", ""),
                seq(renumbered),
                second[step].clone(),
            );
        }
    }
}

fn run(held: Held) -> Stated {
    let params = Params::default();
    let all = stream(8.0, 12);
    let by = |instance: &str| -> Vec<Vec<Event>> {
        let own: Vec<Event> = all
            .iter()
            .filter(|e| e.instance == instance)
            .cloned()
            .collect();
        segments(&own)
    };
    let (first, second) = (by("i0"), by("i1"));
    let log = Log::new("coverage");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (mut driver, mut store): (Driver<Seeds>, _) =
        keyed_log::open_driver(&log.lake, &params).expect("open");
    let mut restated = 0;
    for (step, own) in first.iter().enumerate().take(second.len()) {
        log.commit_run(
            &chunks,
            ("r1", "i0", ""),
            i64::try_from(step).expect("small"),
            own.clone(),
        );
        commit_second(&log, &chunks, held, step, &second);
        working.begin(&log.lake).expect("begin");
        let at = own.last().map_or(0.0, |e| e.at_ms);
        let run = driver
            .tick(&log.lake, &source, &params, Watermark::At(at - 150_000.0))
            .expect("tick");
        restated += run.coverage_restated;
        let next = store.tick() + 1;
        driver.save(&log.lake, &mut store, next).expect("save");
        log.lake.end_unit(working.position()).expect("end");
    }
    let of_second = driver
        .covered()
        .cells()
        .minutes()
        .iter()
        .filter(|(k, _)| k.source == "i1")
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let late_arrivals = driver
        .restated()
        .cells()
        .iter()
        .filter(|((_, family), _)| family == "coverage")
        .map(|(_, c)| c.corrected)
        .sum();
    let spans = driver
        .pair_basis()
        .cells()
        .iter()
        .filter(|((_, s, _), _)| s == "i1")
        .map(|((day, _, start), end)| (*day, *start, end.to_bits()))
        .collect();
    (of_second, restated, late_arrivals, spans)
}

fn covered(cells: &BTreeMap<MinuteKey, Observed>) -> f64 {
    cells.values().map(|o| o.covered_ms).sum()
}

#[test]
fn a_late_backlog_restates_coverage_and_true_silence_stays_uncovered() {
    let (in_order, _, none, in_order_spans) = run(Held::InOrder);
    let (late, restated, arrivals, late_spans) = run(Held::Late);
    let (silent, _, _, _) = run(Held::Silent);
    assert!(restated > 0, "the late backlog restated the source");
    assert_eq!(
        (arrivals, none),
        (i64::try_from(HELD.len()).expect("small"), 0),
        "each late segment counts once, in order nothing"
    );
    assert_eq!(
        late, in_order,
        "the late segments restate what was stated uncovered"
    );
    assert!(!in_order_spans.is_empty());
    assert_eq!(late_spans, in_order_spans, "and the pair basis with it");
    // The silent hour less its grace is uncovered: a stretch the in-order source covers.
    assert!(
        covered(&silent) + 25.0 * 60_000.0 < covered(&in_order),
        "silence stays uncovered: {} against {}",
        covered(&silent),
        covered(&in_order)
    );
}
