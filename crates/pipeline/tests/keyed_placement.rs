// SPDX-License-Identifier: AGPL-3.0-only

//! Placement on block-keyed lanes (`keyed::PlacementCompute`): four lanes ticking every five
//! minutes produce the cells, seeds and whale days the producer's one global fold does over the
//! same windows, though most identities sit still most ticks and are stated from their summed
//! rates. The stream carries clears, which name no identity and reach every lane.

mod keyed_log;

use analyzer_analysis::dup_cells::{DupCells, DupCtx, Duplication, IdentityState};
use analyzer_analysis::events::{Event, Kind, ScopeClosers};
use analyzer_analysis::module::run_window;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::keyed::{Emission, PlacementCompute, RamState};
use std::collections::BTreeMap;

/// The keyed log's stream, with each instance clearing its cache every 90 minutes.
fn events() -> Vec<Event> {
    let mut events = keyed_log::stream(6.0, 40);
    let (first, last) = (events[0].at_ms, events[events.len() - 1].at_ms);
    let mut at = first + 1_800_000.0;
    while at < last {
        for instance in ["i0", "i1", "i2"] {
            let mut clear = Event::new(Kind::Clear, instance, at);
            clear.dp_rank = Some(0);
            clear.group_idx = Some(0);
            clear.incarnation = "r1".into();
            events.push(clear);
        }
        at += 5_400_000.0;
    }
    analyzer_analysis::events::sort(&mut events);
    events
}

/// The producer's fold: every identity with seeds or records, from the global seeds.
fn global_fold(
    events: &[Event],
    seeds: &BTreeMap<String, IdentityState>,
    emitted: Window,
    params: &Params,
) -> (DupCells, BTreeMap<String, IdentityState>) {
    let closers = ScopeClosers::of(events);
    let mut grouped: BTreeMap<String, Vec<Event>> = BTreeMap::new();
    for e in events {
        if let Some(id) = e.identity() {
            grouped.entry(id.to_string()).or_default().push(e.clone());
        }
    }
    let ctx = DupCtx {
        closers: &closers,
        half_skew: params.delta_skew_ms / 2.0,
    };
    let outcome = run_window(&Duplication, grouped, seeds, emitted, &ctx);
    (outcome.cells, outcome.states)
}

#[test]
fn lanes_fold_what_the_producers_global_fold_does() {
    let params = Params::default();
    let half_skew = params.delta_skew_ms / 2.0;
    let events = events();
    let first = events[0].at_ms;
    let end = events[events.len() - 1].at_ms + 1.0;

    let mut lanes = PlacementCompute::new(
        4,
        Emission::Watermarked {
            delay_ms: half_skew,
        },
        RamState::default,
    );
    let (mut lane_cells, mut global_cells) = (DupCells::default(), DupCells::default());
    let mut seeds: BTreeMap<String, IdentityState> = BTreeMap::new();
    let (mut data_from, mut emitted_from) = (first, first);
    while data_from < end {
        let data_to = (data_from + 300_000.0).min(end);
        let emitted =
            Window::new(emitted_from, (data_to - half_skew).max(emitted_from)).expect("window");
        let slice: Vec<Event> = events
            .iter()
            .filter(|e| e.at_ms >= data_from && e.at_ms < data_to)
            .cloned()
            .collect();
        let (cells, next) = global_fold(&slice, &seeds, emitted, &params);
        global_cells.merge(cells);
        seeds = next;
        assert_eq!(lanes.accept(slice), 0, "nothing arrives behind");
        let tick = lanes.advance(data_to, &params, &[]);
        assert_eq!(tick.emitted, Some(emitted), "the emitted window");
        lane_cells.merge(tick.cells);
        (data_from, emitted_from) = (data_to, emitted.end_ms);
    }

    // The residencies the lanes carry, and each identity's whale days: a still identity's
    // stored days end where it began to hold still, and its rates carry the rest.
    let residency = |states: &BTreeMap<String, IdentityState>| {
        states
            .iter()
            .filter(|(_, s)| !s.seeds.is_empty())
            .map(|(k, s)| (k.clone(), (s.seeds.clone(), s.n_tokens)))
            .collect::<BTreeMap<_, _>>()
    };
    let held: BTreeMap<String, IdentityState> = lanes
        .states()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert!(!global_cells.is_empty(), "nothing was emitted");
    assert_eq!(lane_cells, global_cells, "the emitted cells");
    assert_eq!(residency(&held), residency(&seeds), "the seeds");
    #[allow(clippy::cast_possible_truncation)] // the stream's day index
    let day = (first / analyzer_analysis::DAY_MS).floor() as i64;
    let whales: BTreeMap<String, _> = lanes.whale_totals(day).into_iter().collect();
    let want: BTreeMap<String, _> = seeds
        .iter()
        .filter_map(|(k, s)| s.whale.days.get(&day).map(|w| (k.clone(), *w)))
        .collect();
    assert!(!want.is_empty(), "the stream held nothing");
    assert_eq!(whales, want, "each identity's whale day");
}
