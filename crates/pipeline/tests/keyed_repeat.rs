// SPDX-License-Identifier: AGPL-3.0-only

//! Repeat compute on block-keyed lanes computes the same cells as the whole-window classifier
//! (`keyed::RepeatCompute` against one `ClassBand` fold per day). The same stream is classified
//! both ways: whole, each day from a window reaching the examined history back; and in
//! five-minute ticks across four lanes, each tick folding what the watermark allows. Their class
//! and band cells must be equal, `RE` near every tick edge included, which is what the
//! watermark's delay is for.
//!
//! Engine-free: the operator and the classifier are pure over events.

mod keyed_log;

use analyzer_analysis::class_band::{ClassBand, ClassBandCells, ClassCtx};
use analyzer_analysis::completeness::{uncertainty_model_from_parts, AgentBoundaries};
use analyzer_analysis::emitted::Meaning;
use analyzer_analysis::events::{group_by_identity, Event, Kind, ScopeClosers};
use analyzer_analysis::module::run_window;
use analyzer_analysis::{Discriminator, Params, Window, DAY_MS};
use analyzer_pipeline::keyed::{Emission, RepeatCompute, RepeatContext};
use analyzer_pipeline::ledger::day_of;
use keyed_log::Rng;
use std::collections::{BTreeMap, BTreeSet};

/// A small deterministic generator: xorshift, seeded.
fn event(kind: Kind, instance: &str, dp_rank: u32, block: Option<String>, at_ms: f64) -> Event {
    let mut e = Event::new(kind, instance, at_ms);
    e.dp_rank = Some(dp_rank);
    e.group_idx = Some(0);
    e.block_id = block;
    e.n_tokens = 16;
    e.incarnation = format!("run-{instance}");
    e.workload_class = Some("agentic".to_string());
    e
}

/// Two days of three instances, two ranks each, storing and evicting twelve shared blocks so
/// the same block is often open on several holders at once, with a clear now and then. Every
/// holder's stores and evictions alternate per block, as an engine's do.
fn stream(seed: u64) -> Vec<Event> {
    let mut rng = Rng(seed);
    let mut events = Vec::new();
    let mut open: BTreeSet<(String, u32, String)> = BTreeSet::new();
    let mut at = DAY_MS * 100.0;
    let end = at + 2.0 * DAY_MS;
    while at < end {
        at += f64::from(u32::try_from(rng.below(900) + 1).expect("small")) * 100.0;
        let instance = format!("i{}", rng.below(3));
        let rank = u32::try_from(rng.below(2)).expect("rank");
        if rng.below(400) == 0 {
            events.push(event(Kind::Clear, &instance, rank, None, at));
            open.retain(|(i, r, _)| !(*i == instance && *r == rank));
            continue;
        }
        let block = format!("b{}", rng.below(12));
        let key = (instance.clone(), rank, block.clone());
        let kind = if open.contains(&key) {
            open.remove(&key);
            Kind::Evict
        } else {
            open.insert(key);
            Kind::Store
        };
        events.push(event(kind, &instance, rank, Some(block), at));
    }
    events
}

/// The instant a UTC day starts.
fn day_start(day: i64) -> f64 {
    #[allow(clippy::cast_precision_loss)] // day indices are small integers
    let day = day as f64;
    day * DAY_MS
}

fn params() -> Params {
    Params::default()
}

/// The whole-window cells: each day classified from every record before its end, emitting that
/// day's stores. A residency holds its block however long ago it opened, so the window reaches
/// back to the stream's start, not to the examined history.
fn whole(events: &[Event], days: &[i64], ctx: &RepeatContext<'_>) -> ClassBandCells {
    let mut total = ClassBandCells::default();
    for &day in days {
        let emit_day = Window {
            start_ms: day_start(day),
            end_ms: day_start(day + 1),
        };
        let window = Window {
            start_ms: events.first().map_or(emit_day.start_ms, |e| e.at_ms),
            end_ms: emit_day.end_ms,
        };
        let inside: Vec<Event> = events
            .iter()
            .filter(|e| e.at_ms >= window.start_ms && e.at_ms < window.end_ms)
            .cloned()
            .collect();
        let closers = ScopeClosers::of(&inside);
        let grouped = group_by_identity(&inside)
            .into_iter()
            .map(|g| ((g.identity, g.key_epoch), g.keys))
            .collect();
        let class_ctx = ClassCtx {
            closers: &closers,
            model: ctx.model,
            params: ctx.params,
            emit_day,
            coverage: ctx.coverage,
            meaning: ctx.meaning,
            models: ctx.models,
        };
        let out = run_window(&ClassBand, grouped, &BTreeMap::new(), window, &class_ctx);
        analyzer_analysis::module::Mergeable::merge(&mut total, out.cells);
    }
    total
}

/// The lanes' cells: the stream delivered in five-minute ticks, each advanced to the tick's
/// end as the watermark.
fn lanes(events: &[Event], emission: Emission, ctx: &RepeatContext<'_>) -> ClassBandCells {
    let mut operator = RepeatCompute::new(4, emission);
    let mut total = ClassBandCells::default();
    let tick = 300_000.0;
    let first = events.first().map_or(0.0, |e| e.at_ms);
    let last = events.last().map_or(0.0, |e| e.at_ms);
    let mut edge = (first / tick).floor() * tick + tick;
    let mut at = 0;
    while edge <= last + tick {
        let upto = events[at..].partition_point(|e| e.at_ms < edge) + at;
        assert_eq!(
            operator.accept(events[at..upto].to_vec()),
            0,
            "late records"
        );
        at = upto;
        let out = operator.advance(Some(edge), ctx);
        analyzer_analysis::module::Mergeable::merge(&mut total, out.cells);
        edge += tick;
    }
    // Every holder complete: the watermark passes the stream's end.
    let out = operator.advance(Some(last + tick + ctx.params.delta_skew_ms + 1.0), ctx);
    analyzer_analysis::module::Mergeable::merge(&mut total, out.cells);
    total
}

fn with_context<T>(f: impl FnOnce(&RepeatContext<'_>) -> T) -> T {
    let params = params();
    let model = uncertainty_model_from_parts(
        Vec::new(),
        AgentBoundaries::default(),
        BTreeSet::new(),
        &Window {
            start_ms: 0.0,
            end_ms: f64::MAX,
        },
        &params,
    );
    let meaning = Meaning::current(&Discriminator::of(&params));
    let models = BTreeMap::new();
    let ctx = RepeatContext {
        params: &params,
        model: &model,
        meaning: &meaning,
        models: &models,
        coverage: 1.0,
    };
    f(&ctx)
}

#[test]
fn the_lanes_classify_every_store_as_the_whole_window_does() {
    for seed in [
        0x9e37_79b9_7f4a_7c15_u64,
        0x2545_f491_4f6c_dd1d,
        0xdead_beef_cafe_f00d,
    ] {
        let events = stream(seed);
        let days: Vec<i64> = events
            .iter()
            .map(|e| day_of(e.at_ms))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        with_context(|ctx| {
            let delay_ms = ctx.params.delta_skew_ms;
            let expected = whole(&events, &days, ctx);
            let got = lanes(&events, Emission::Watermarked { delay_ms }, ctx);
            assert!(
                expected.buckets.get("RE").copied().unwrap_or(0) > 0,
                "seed {seed:x}: the stream has no RE to check"
            );
            assert_eq!(got.buckets, expected.buckets, "seed {seed:x}: class counts");
            assert_eq!(
                got.cells, expected.cells,
                "seed {seed:x}: the class and band cells"
            );
            assert_eq!(
                got.waste_tokens, expected.waste_tokens,
                "seed {seed:x}: waste"
            );
        });
    }
}

#[test]
fn immediate_emission_classifies_everything_pending_at_once() {
    let events = stream(0x1234_5678_9abc_def1);
    with_context(|ctx| {
        let mut operator = RepeatCompute::new(2, Emission::Immediate);
        assert_eq!(operator.accept(events.clone()), 0);
        let out = operator.advance(None, ctx);
        let stores = events.iter().filter(|e| e.kind == Kind::Store).count() as u64;
        assert_eq!(
            out.cells.buckets.values().sum::<u64>(),
            stores,
            "a store left pending"
        );
        // A record behind what was classified is late, and not folded.
        let behind = event(Kind::Store, "i0", 0, Some("b0".into()), events[0].at_ms);
        assert_eq!(operator.accept(vec![behind]), 1);
    });
}

#[test]
fn the_delay_waits_for_another_holders_close_inside_the_skew() {
    // Holder A holds block b; holder B stores b 500 ms before a tick's edge; A evicts b 700 ms
    // after that store: past the edge, inside the skew allowance. A's interval does not cover
    // B's widened instant, so the whole window does not call B's store RE. At the edge A is
    // still open, and only the delay waits to see the close.
    let edge = DAY_MS * 100.0 + 600_000.0;
    let store_at = edge - 500.0;
    let events = vec![
        event(Kind::Store, "a", 0, Some("b".into()), edge - 300_000.0),
        event(Kind::Store, "b", 0, Some("b".into()), store_at),
        event(Kind::Evict, "a", 0, Some("b".into()), store_at + 700.0),
    ];
    let day = day_of(events[0].at_ms);
    let run = |delay_ms: f64, ctx: &RepeatContext<'_>| {
        let mut operator = RepeatCompute::new(1, Emission::Watermarked { delay_ms });
        let mut total = ClassBandCells::default();
        let (before, after): (Vec<Event>, Vec<Event>) =
            events.iter().cloned().partition(|e| e.at_ms < edge);
        assert_eq!(operator.accept(before), 0);
        analyzer_analysis::module::Mergeable::merge(
            &mut total,
            operator.advance(Some(edge), ctx).cells,
        );
        assert_eq!(operator.accept(after), 0);
        analyzer_analysis::module::Mergeable::merge(
            &mut total,
            operator.advance(Some(edge + 300_000.0), ctx).cells,
        );
        total
    };
    with_context(|ctx| {
        let expected = whole(&events, &[day], ctx);
        assert_eq!(expected.buckets.get("RE"), None, "{:?}", expected.buckets);
        let delayed = run(ctx.params.delta_skew_ms, ctx);
        assert_eq!(delayed.cells, expected.cells, "the delayed lanes differ");
        let hasty = run(0.0, ctx);
        assert_eq!(
            hasty.buckets.get("RE"),
            Some(&1),
            "without the delay the store is decided before A's close arrives, and is RE: {:?}",
            hasty.buckets
        );
    });
}
