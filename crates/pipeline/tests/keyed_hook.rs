// SPDX-License-Identifier: AGPL-3.0-only

//! The settled-day hook (`keyed::Hook`): a public module runs over each settled day with the
//! day's coverage as the published `coverage_daily` states it; the batch keeps its days, so a
//! replay states them again; the generation seams are what the generations derive, a seam that
//! no longer holds tombstoned.

mod keyed_log;

use analyzer_analysis::emitted::{Meaning, COVERAGE_DAILY, MARKERS};
use analyzer_analysis::events::{Event, EventSource, LifecycleEvent};
use analyzer_analysis::module::{
    Column, Contribution, DayOf, DerivationModule, Initialization, Kind, Mergeable, Row, Table,
    Value,
};
use analyzer_analysis::{Discriminator, Params, Window};
use analyzer_pipeline::adapter::{Adapter, Cadence, Context, Dependencies, Registration};
use analyzer_pipeline::keyed::{
    Batch, Driver, Generation, Hook, Seeds, SegmentSource, SpineLayout, Watermark,
};
use analyzer_pipeline::modules::ModuleSet;
use analyzer_pipeline::publish::{Head, Publish};
use analyzer_pipeline::state::LakePublish;
use keyed_log::{segments, stream, Chunks, Log};

/// Each source's unobserved milliseconds of a day.
#[derive(Default)]
struct Unseen(Vec<(String, f64)>);
impl Mergeable for Unseen {
    fn merge(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

struct UnseenModule;
impl DerivationModule for UnseenModule {
    type Key = String;
    type State = ();
    type Event = f64;
    type Cells = Unseen;
    type Ctx<'a> = ();
    const NAME: &'static str = "unseen";
    const INITIALIZATION: Initialization = Initialization::OutputWindow;
    const NEEDS: &'static [&'static str] = &["coverage"];
    fn fold(
        &self,
        source: &String,
        gaps: &[f64],
        (): (),
        _: Window,
        (): &(),
    ) -> Contribution<Unseen, ()> {
        Contribution {
            cells: Unseen(vec![(source.clone(), gaps.iter().sum())]),
            state: (),
            repair_days: Vec::new(),
        }
    }
}

const UNSEEN: Table = Table {
    name: "test_unseen",
    key: &[
        Column::new("day", Kind::I64),
        Column::new("source", Kind::Str),
    ],
    measures: &[Column::new("unobserved_ms", Kind::F64)],
    day: DayOf::Day("day"),
};

struct UnseenAdapter;
impl Adapter for UnseenAdapter {
    type Definition = UnseenModule;
    type Output = Unseen;
    const CADENCE: Cadence = Cadence::Day;
    const TABLES: &'static [Table] = &[UNSEEN];
    fn reconstruct(
        ctx: &Context<'_>,
        deps: &Dependencies<'_>,
    ) -> Result<Unseen, analyzer_pipeline::Error> {
        let mut out = Unseen::default();
        for day in &deps.coverage()?.coverage().per_day {
            out.merge(
                UnseenModule
                    .fold(
                        &day.source,
                        &[day.window_ms - day.covered_ms],
                        (),
                        ctx.output,
                        &(),
                    )
                    .cells,
            );
        }
        Ok(out)
    }
    fn rows(ctx: &Context<'_>, output: &Unseen) -> Vec<Row> {
        let day = analyzer_analysis::emitted::day_index(ctx.output.start_ms);
        output
            .0
            .iter()
            .map(|(source, unseen)| Row {
                table: UNSEEN.name,
                key: vec![Value::I64(day), Value::Str(source.clone())],
                measures: vec![Value::F64(*unseen)],
                gone: false,
            })
            .collect()
    }
}

/// A source the test module never reads: its output comes from coverage alone.
struct NoInput;
impl EventSource for NoInput {
    fn cache_events(
        &self,
        _: &str,
        _: &Window,
    ) -> Result<std::sync::Arc<Vec<Event>>, analyzer_analysis::Error> {
        Ok(std::sync::Arc::new(Vec::new()))
    }
    fn lifecycle_events(
        &self,
        _: &str,
        _: f64,
        _: f64,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        Ok(Vec::new())
    }
    fn segment_opens_at_retention_horizon(
        &self,
        _: &str,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        Ok(Vec::new())
    }
}

fn modules() -> ModuleSet {
    let mut registrations = analyzer_pipeline::modules::registrations();
    registrations.push(Registration::of::<UnseenAdapter>());
    ModuleSet::from_registrations(&registrations, &[]).expect("the modules order")
}

/// Publish a batch's coverage cells to the lake under `meaning`: the family the module is
/// compared with.
fn publish(lake: &LakePublish, meaning: &Meaning, batch: &Batch, publication: u64) {
    let head = Head {
        fleet_id: "f",
        meaning,
    };
    let (tables, rows) = batch.families()[2];
    assert_eq!(tables[1].name, COVERAGE_DAILY.name);
    if !rows.is_empty() {
        lake.apply(&head, tables, publication, rows)
            .expect("publish");
    }
}

#[test]
fn a_module_reads_each_settled_day_as_coverage_daily_states_it_and_a_replay_keeps_the_days() {
    let params = Params::default();
    let meaning = Meaning::current(&Discriminator::of(&params));
    let parts = segments(&stream(30.0, 12));
    let log = Log::new("hook");
    let chunks = Chunks::default();
    let source = SegmentSource::new(&log.lake, &chunks);
    let working = log.lake.working().expect("a working state");
    let (driver, mut store) = keyed_log::open_driver(&log.lake, &params).expect("open");
    let mut driver: Driver<Seeds> = driver
        .with_layout(SpineLayout {
            partitions: 8,
            retention_days: 7,
        })
        .with_settle_delay(60_000.0);
    let lake = LakePublish::of(&log.lake).expect("the state log");
    let modules = modules();
    let hook = Hook {
        lake: &log.lake,
        source: &NoInput,
        fleet: "f",
        params: &params,
        modules: &modules,
        publish: &lake,
    };
    let mut stated = Vec::new();
    for (seq, part) in parts.iter().enumerate() {
        // One segment of day 100 declares records lost: its span is uncertain, so the day's
        // coverage is not whole.
        let dropped = u64::from(seq == 50);
        log.commit_dropping(
            &chunks,
            ("r1", "i0", ""),
            i64::try_from(seq).expect("small"),
            part.clone(),
            dropped,
        );
        working.begin(&log.lake).expect("begin");
        driver
            .tick(
                &log.lake,
                &source,
                &params,
                Watermark::At(part.last().map_or(0.0, |e| e.at_ms) - 150_000.0),
            )
            .expect("tick");
        let publication = seq as u64 + 1;
        let batch = driver
            .save(&log.lake, &mut store, publication)
            .expect("save");
        log.lake.end_unit(working.position()).expect("end");
        assert_eq!(
            store.outbox(&log.lake).expect("outbox").days,
            batch.days,
            "the store keeps the batch's days for a replay"
        );
        publish(&lake, &meaning, &batch, publication);
        hook.state(&batch.days, driver.generations(), publication)
            .expect("the hook");
        stated.extend(batch.days.iter().map(|d| d.day));
    }
    assert_eq!(stated, vec![100], "day 100 settled, once");

    let daily = lake
        .read(&COVERAGE_DAILY, &meaning, Some(100))
        .expect("coverage_daily");
    let unseen = lake.read(&UNSEEN, &meaning, Some(100)).expect("the module");
    assert!(!daily.is_empty(), "the day has coverage rows");
    let f = |v: &Value| match v {
        Value::F64(x) => *x,
        other => panic!("not a float: {other:?}"),
    };
    let want: Vec<(String, f64)> = daily
        .iter()
        .map(|r| (r.key[1].text(), f(&r.measures[1]) - f(&r.measures[0])))
        .collect();
    let got: Vec<(String, f64)> = unseen
        .iter()
        .map(|r| (r.key[1].text(), f(&r.measures[0])))
        .collect();
    assert!(
        want.iter().any(|(_, unseen)| *unseen > 0.0),
        "the hole is unobserved: {want:?}"
    );
    assert_eq!(got, want, "the module read coverage_daily's day");
}

fn generation(sv: &str, disc: &str, first_day: i64, last_day: i64) -> Generation {
    Generation {
        semantics_version: sv.into(),
        discriminator: disc.into(),
        first_day,
        last_day,
    }
}

#[test]
fn the_seams_are_what_the_generations_derive_and_one_that_no_longer_holds_leaves() {
    let log = Log::new("seams");
    let lake = LakePublish::of(&log.lake).expect("the state log");
    let params = Params::default();
    let modules = modules();
    let hook = Hook {
        lake: &log.lake,
        source: &NoInput,
        fleet: "f",
        params: &params,
        modules: &modules,
        publish: &lake,
    };
    let day = analyzer_pipeline::keyed::SettledDay {
        day: 100,
        coverage: Vec::new(),
    };
    let seams = || {
        let mut rows: Vec<(String, String)> = lake
            .read(&MARKERS, &Meaning::generations(), None)
            .expect("markers")
            .into_iter()
            .map(|r| (r.key[1].text(), r.key[2].text()))
            .collect();
        rows.sort();
        rows
    };
    let first = [
        generation("s1", "d1", 100, 101),
        generation("s1", "d2", 102, 103),
    ];
    hook.state(std::slice::from_ref(&day), &first, 1)
        .expect("the hook");
    let stated = seams();
    assert!(
        stated.iter().any(|(kind, _)| kind == "param_change") && stated.len() >= 2,
        "a baseline and a transition: {stated:?}"
    );

    // The second meaning's days are re-derived under the first: its transition no longer holds.
    let second = [generation("s1", "d1", 100, 103)];
    hook.state(std::slice::from_ref(&day), &second, 2)
        .expect("the hook");
    let now = seams();
    assert!(now.len() < stated.len(), "the transition left: {now:?}");
    assert!(now.iter().all(|s| stated.contains(s)), "{now:?}");
}
