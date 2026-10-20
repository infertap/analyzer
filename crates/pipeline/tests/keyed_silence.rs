// SPDX-License-Identifier: AGPL-3.0-only

//! A holder whose producer goes silent (`docs/classification-semantics.md` §4): past its liveness
//! bound, its residencies close, reason `silent`. A store elsewhere after the silence starts is
//! no longer RE against the silent holder's copies, the holder's own next store of a block is
//! `indeterminate`, and a re-derivation from the log says the same as the stream.

mod keyed_log;

use analyzer_analysis::emitted::ClassifiedFact;
use analyzer_analysis::events::{Event, Kind};
use analyzer_analysis::{Params, Window, DAY_MS};
use analyzer_pipeline::keyed::{
    refold_block, Driver, Seeds, SegmentSource, SpineLayout, Watermark, Written,
};
use keyed_log::{Chunks, Log};

const MINUTE: f64 = 60_000.0;
const T0: f64 = DAY_MS * 100.0 + 3_600_000.0;

fn event(kind: Kind, instance: &str, run: &str, block: &str, at_ms: f64) -> Event {
    let mut e = Event::new(kind, instance, at_ms);
    e.dp_rank = Some(0);
    e.group_idx = Some(0);
    e.block_id = Some(block.to_string());
    e.n_tokens = 16;
    e.incarnation = run.into();
    e.workload_class = Some("agentic".into());
    e
}

/// A producer's steady records from `from` to `to`, a store and its evict of its own block every
/// two minutes: what keeps its segments, and so its heartbeats, coming.
fn steady(instance: &str, run: &str, (from, to): (f64, f64)) -> Vec<Event> {
    let mut events = Vec::new();
    let mut at = from;
    while at < to {
        events.push(event(
            Kind::Store,
            instance,
            run,
            &format!("own-{instance}"),
            at,
        ));
        events.push(event(
            Kind::Evict,
            instance,
            run,
            &format!("own-{instance}"),
            at + MINUTE,
        ));
        at += 2.0 * MINUTE;
    }
    events
}

/// `events` of one run cut into five-minute segments, each with the instant it ends.
fn segments_of(events: &[Event]) -> Vec<(f64, Vec<Event>)> {
    let mut sorted = events.to_vec();
    analyzer_analysis::events::sort(&mut sorted);
    let mut out: Vec<(f64, Vec<Event>)> = Vec::new();
    for e in sorted {
        let end = ((e.at_ms / (5.0 * MINUTE)).floor() + 1.0) * 5.0 * MINUTE;
        match out.last_mut() {
            Some((last, part)) if last.to_bits() == end.to_bits() => part.push(e),
            _ => out.push((end, vec![e])),
        }
    }
    out
}

fn key(f: &ClassifiedFact) -> (String, String, u64, String) {
    (
        f.holder.instance.clone(),
        f.block.clone(),
        f.at_ms.to_bits(),
        f.class.as_str().to_string(),
    )
}

/// How `ia` beat between one hour and two hundred minutes.
#[derive(Clone, Copy, PartialEq)]
enum Between {
    /// Not at all: silent.
    Silent,
    /// It beat, and those segments land only at 205 minutes.
    Late,
    /// It beat, and those segments land as they end.
    OnTime,
}

/// A run of both producers, a tick at each segment end. `ia` holds y and z from the start, beats
/// for an hour, stores y again at 110 minutes, and runs again later: silent between, its second
/// run starts at 100 and makes that store; otherwise its first run beats on to 200 and makes it,
/// and the second starts there. `ib` beats throughout, storing z at 30 minutes and y at 95.
/// Returns the log, the driver, and what the ticks stated.
fn run(name: &str, between: Between) -> (Log, Driver<Seeds>, Vec<ClassifiedFact>) {
    let params = Params::default();
    let mut ia = vec![
        event(Kind::Store, "ia", "ra", "y", T0),
        event(Kind::Store, "ia", "ra", "z", T0),
    ];
    ia.extend(steady("ia", "ra", (T0, T0 + 60.0 * MINUTE)));
    let second_from = if between == Between::Silent {
        100.0
    } else {
        200.0
    };
    ia.extend(steady(
        "ia",
        "ra2",
        (T0 + second_from * MINUTE, T0 + 330.0 * MINUTE),
    ));
    if between == Between::Silent {
        ia.push(event(Kind::Store, "ia", "ra2", "y", T0 + 110.0 * MINUTE));
    }
    let mut ib = steady("ib", "rb", (T0, T0 + 340.0 * MINUTE));
    ib.extend([
        event(Kind::Store, "ib", "rb", "z", T0 + 30.0 * MINUTE),
        event(Kind::Evict, "ib", "rb", "z", T0 + 31.0 * MINUTE),
        event(Kind::Store, "ib", "rb", "y", T0 + 95.0 * MINUTE),
        event(Kind::Evict, "ib", "rb", "y", T0 + 96.0 * MINUTE),
    ]);
    let mut commits: Vec<(f64, (&str, &str), Vec<Event>)> = Vec::new();
    for (run, instance, events) in [("ra", "ia", &ia), ("rb", "ib", &ib)] {
        for (end, part) in segments_of(events) {
            let run = if part.iter().all(|e| e.incarnation == "ra2") {
                "ra2"
            } else {
                run
            };
            commits.push((end, (run, instance), part));
        }
    }
    if between != Between::Silent {
        let mut beats = steady("ia", "ra", (T0 + 60.0 * MINUTE, T0 + 200.0 * MINUTE));
        beats.push(event(Kind::Store, "ia", "ra", "y", T0 + 110.0 * MINUTE));
        for (end, part) in segments_of(&beats) {
            let lands = if between == Between::Late {
                T0 + 205.0 * MINUTE
            } else {
                end
            };
            commits.push((lands, ("ra", "ia"), part));
        }
    }
    commits.sort_by(|a, b| a.0.total_cmp(&b.0));

    let log = Log::new(name);
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (driver, mut store) = keyed_log::open_driver(&log.lake, &params).expect("open");
    let mut driver: Driver<Seeds> = driver.with_layout(SpineLayout {
        partitions: 8,
        retention_days: 7,
    });
    let mut stated: Vec<ClassifiedFact> = Vec::new();
    let mut seqs = std::collections::BTreeMap::<&str, i64>::new();
    // Every producer's segment ending at an instant lands before the tick at it.
    let mut ends: Vec<f64> = commits.iter().map(|c| c.0).collect();
    ends.dedup_by(|a, b| a.to_bits() == b.to_bits());
    let mut commits = commits.into_iter().peekable();
    for (tick, end) in ends.into_iter().enumerate() {
        while let Some((_, (run, instance), part)) =
            commits.next_if(|c| c.0.to_bits() == end.to_bits())
        {
            let seq = seqs.entry(run).or_default();
            log.commit_run(&chunks, (run, instance, ""), *seq, part);
            *seq += 1;
        }
        working.begin(&log.lake).expect("begin");
        let ran = driver
            .tick(&log.lake, &source, &params, Watermark::At(end - 150_000.0))
            .expect("tick");
        stated.extend(ran.cells.facts);
        driver
            .save(&log.lake, &mut store, tick as u64 + 1)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
    }
    (log, driver, stated)
}

/// `block` re-derived over the run, as `(holder, block, instant, class)`, sorted.
fn refolded(log: &Log, driver: &Driver<Seeds>, block: &str) -> Vec<(String, String, u64, String)> {
    let window = Window {
        start_ms: T0,
        end_ms: T0 + 320.0 * MINUTE,
    };
    let refold = refold_block(
        &log.lake,
        block,
        window,
        (&Params::default(), &driver.roster().cells().models()),
        &Written::default(),
    )
    .expect("refold");
    let mut got: Vec<_> = refold.facts.iter().map(key).collect();
    got.sort();
    got
}

#[test]
fn a_silent_holders_copies_stop_counting_and_its_return_is_indeterminate() {
    let (log, driver, stated) = run("silence", Between::Silent);
    let class_of = |instance: &str, block: &str| -> Vec<&str> {
        stated
            .iter()
            .filter(|f| f.holder.instance == instance && f.block == block)
            .map(|f| f.class.as_str())
            .collect()
    };
    assert_eq!(class_of("ib", "z"), ["RE"], "ia held z, heard from");
    assert_eq!(class_of("ib", "y"), ["cold"], "ia was silent by then");
    assert_eq!(
        class_of("ia", "y"),
        ["cold", "indeterminate"],
        "ia's own return to y after its silence"
    );
    for block in ["y", "z"] {
        let mut want: Vec<_> = stated
            .iter()
            .filter(|f| f.block == block)
            .map(key)
            .collect();
        want.sort();
        assert_eq!(
            refolded(&log, &driver, block),
            want,
            "{block}: the re-derivation"
        );
    }
}

#[test]
fn segments_landing_inside_a_silence_reopen_what_it_closed() {
    // ia's beats from one hour to two hundred minutes land at two hundred and five: the silence
    // the stream drew at 89 minutes was never one. Re-taint re-derives every block ia held, so y
    // is open on ia again, ib's store of it at 95 is RE, and the frontiers agree with a
    // re-derivation.
    let (log, driver, stated) = run("silence-revoked", Between::Late);
    let ib_y: Vec<_> = stated
        .iter()
        .filter(|f| f.holder.instance == "ib" && f.block == "y")
        .map(|f| f.class.as_str())
        .collect();
    assert_eq!(
        ib_y,
        ["cold"],
        "as the stream first stated it, before the beats landed"
    );
    let ib_y_now: Vec<_> = refolded(&log, &driver, "y")
        .into_iter()
        .filter(|(holder, ..)| holder == "ib")
        .map(|(.., class)| class)
        .collect();
    assert_eq!(ib_y_now, ["RE"], "ia held y throughout");
    let y_on_ia: Vec<_> = driver
        .repeat()
        .states()
        .filter(|((block, _), _)| block == "y")
        .flat_map(|(_, frontiers)| frontiers)
        .filter(|f| f.key.instance == "ia")
        .map(|f| (f.open, f.open_since_ms.map(|o| (o - T0) / MINUTE)))
        .collect();
    assert_eq!(
        y_on_ia,
        [(true, Some(0.0))],
        "y open on ia since its first store, no silence ever closing it"
    );
}

/// What a run ends with, as two runs over the same log must agree on it: the class and band
/// cells, the frontiers, the duplication cells, and each identity's placement seeds and mass.
fn ended(driver: &Driver<Seeds>) -> (String, String, String, String) {
    let mut frontiers: Vec<_> = driver.repeat().states().collect();
    frontiers.sort_by(|a, b| a.0.cmp(&b.0));
    let mut seeds: Vec<_> = driver
        .placement()
        .states()
        .filter(|(_, s)| !s.seeds.is_empty())
        .map(|(k, s)| (k.clone(), format!("{:?}", s.seeds), s.n_tokens))
        .collect();
    seeds.sort();
    (
        format!("{:?}", driver.classes().cells().rows()),
        format!("{frontiers:?}"),
        format!("{:?}", driver.placed().cells()),
        format!("{seeds:?}"),
    )
}

#[test]
fn a_revoked_silence_ends_as_if_the_beats_had_never_been_late() {
    let (_, late, _) = run("silence-late", Between::Late);
    let (_, on_time, _) = run("silence-on-time", Between::OnTime);
    let (late, on_time) = (ended(&late), ended(&on_time));
    assert_eq!(late.0, on_time.0, "the class and band cells");
    assert_eq!(late.1, on_time.1, "the frontiers");
    assert_eq!(late.2, on_time.2, "the duplication cells");
    assert_eq!(late.3, on_time.3, "placement's seeds");
}

/// The minute cells holding token-time in two or more caches from `from_ms` on.
fn duplicated_from(driver: &Driver<Seeds>, from_ms: i64) -> usize {
    driver
        .placed()
        .cells()
        .minute
        .iter()
        .filter(|(k, sum)| k.band != "1" && k.bucket >= from_ms && sum.token_ms > 0.0)
        .count()
}

#[test]
fn a_silent_holders_copies_are_no_longer_duplicates() {
    // ib holds y for a minute at 95: ia's copy counts toward duplication only if ia was heard.
    let (_, silent, _) = run("silence-dup", Between::Silent);
    let (_, heard, _) = run("silence-dup-heard", Between::OnTime);
    #[allow(clippy::cast_possible_truncation)] // a minute boundary in milliseconds
    let from = (T0 + 90.0 * MINUTE) as i64;
    assert_eq!(
        duplicated_from(&silent, from),
        0,
        "ia went silent at 89 minutes"
    );
    assert!(duplicated_from(&heard, from) > 0, "ia held y and was heard");
}
