// SPDX-License-Identifier: AGPL-3.0-only

//! The seeded slice
//! classifier computes THE SAME function as the whole-window classifier — different
//! traversal, identical output. Every test here runs one event stream both ways and
//! asserts equality of the slice's stores, its residency samples, its aggregates AND its
//! outgoing frontier — the frontier equality is what lets slices chain without drift.
//!
//! No engine: this is the classifier seam itself, which is exactly where a divergence
//! would be invisible to every engine-level golden until day 31.

use std::collections::BTreeSet;

use analyzer_analysis::classify::{
    classify_identity_group, classify_identity_group_seeded, Frontier, GroupClassification,
    ObservedResidency,
};
use analyzer_analysis::completeness::{uncertainty_model_from_parts, AgentBoundaries};
use analyzer_analysis::events::{group_by_identity, Event, Kind, ScopeClosers};
use analyzer_analysis::{Params, Window};

fn ev(kind: Kind, instance: &str, block: &str, at_ms: f64) -> Event {
    let mut event = Event::new(kind, instance, at_ms);
    event.block_id = Some(block.to_string());
    event.n_tokens = 4;
    event.incarnation = "run-1".to_string();
    event.workload_class = Some("agentic".to_string());
    event
}

fn clear(instance: &str, at_ms: f64) -> Event {
    let mut e = ev(Kind::Clear, instance, "unused", at_ms);
    e.block_id = None;
    e
}

/// Classify a stream whole, per identity group.
fn classify_whole(events: &[Event]) -> Vec<GroupClassification> {
    classify_whole_under(events, &Params::default())
}

/// [`classify_whole`] under `params`.
fn classify_whole_under(events: &[Event], params: &Params) -> Vec<GroupClassification> {
    let closers = ScopeClosers::of(events);
    let params = params.clone();
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
    group_by_identity(events)
        .iter()
        .map(|g| classify_identity_group(g, &closers, &model, &params))
        .collect()
}

/// Classify the same stream in two sittings split at `boundary`, seeding the second from
/// the first's frontiers, and return the second sitting's results.
fn classify_sliced(events: &[Event], boundary: f64, params: &Params) -> Vec<GroupClassification> {
    let part1: Vec<Event> = events
        .iter()
        .filter(|e| e.at_ms < boundary)
        .cloned()
        .collect();
    let part2: Vec<Event> = events
        .iter()
        .filter(|e| e.at_ms >= boundary)
        .cloned()
        .collect();
    let params = params.clone();
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

    let closers1 = ScopeClosers::of(&part1);
    let mut frontier: Vec<Frontier> = Vec::new();
    for g in &group_by_identity(&part1) {
        frontier.extend(classify_identity_group(g, &closers1, &model, &params).frontiers);
    }

    let closers2 = ScopeClosers::of(&part2);
    let mut groups = group_by_identity(&part2);
    // The dormant half: seeded-open identities a slice clear reaches but whose own events
    // are silent this slice — the driver's obligation, mirrored here.
    let grouped: BTreeSet<(String, Option<u32>)> = groups
        .iter()
        .map(|g| (g.identity.clone(), g.key_epoch))
        .collect();
    groups.extend(analyzer_analysis::classify::dormant_groups(
        &frontier, &closers2, &grouped,
    ));
    groups
        .iter()
        .map(|g| {
            let seeds: Vec<Frontier> = frontier
                .iter()
                .filter(|f| f.key.identity == g.identity && f.key.key_epoch == g.key_epoch)
                .cloned()
                .collect();
            classify_identity_group_seeded(g, &closers2, &model, &params, &seeds)
        })
        .collect()
}

/// The comparison: the sliced second sitting must equal the whole run restricted to the
/// second window — stores, samples, and the OUTGOING frontier.
fn assert_slice_equals_whole(events: &[Event], boundary: f64) {
    let whole = classify_whole(events);
    let sliced = classify_sliced(events, boundary, &Params::default());

    let mut whole_stores: Vec<_> = whole
        .iter()
        .flat_map(|g| g.stores.iter().filter(|s| s.at_ms >= boundary).cloned())
        .collect();
    let mut slice_stores: Vec<_> = sliced.iter().flat_map(|g| g.stores.clone()).collect();
    let key = |s: &analyzer_analysis::classify::ClassifiedStore| {
        (s.at_ms.to_bits(), s.block.clone(), s.instance.clone())
    };
    whole_stores.sort_by_key(key);
    slice_stores.sort_by_key(key);
    assert_eq!(
        slice_stores, whole_stores,
        "stores diverged at the slice seam"
    );

    let mut whole_res: Vec<_> = whole
        .iter()
        .flat_map(|g| {
            g.residencies
                .iter()
                .filter(|r| r.close_ms >= boundary)
                .cloned()
        })
        .collect();
    let mut slice_res: Vec<_> = sliced.iter().flat_map(|g| g.residencies.clone()).collect();
    let rkey = |r: &ObservedResidency| (r.close_ms.to_bits(), r.key.clone());
    whole_res.sort_by_key(rkey);
    slice_res.sort_by_key(rkey);
    assert_eq!(
        slice_res, whole_res,
        "residency samples diverged at the seam"
    );

    let mut whole_frontier: Vec<_> = whole.iter().flat_map(|g| g.frontiers.clone()).collect();
    let mut slice_frontier: Vec<_> = sliced.iter().flat_map(|g| g.frontiers.clone()).collect();
    let fkey = |f: &Frontier| {
        (
            f.key.identity.clone(),
            f.key.instance.clone(),
            f.key.tier.clone(),
        )
    };
    whole_frontier.sort_by_key(fkey);
    slice_frontier.sort_by_key(fkey);
    assert_eq!(
        slice_frontier, whole_frontier,
        "the OUTGOING frontier diverged — slices would drift from here on"
    );
}

#[test]
fn etr_across_the_seam_keeps_its_class_and_its_gap() {
    assert_slice_equals_whole(
        &[
            ev(Kind::Store, "i1", "p", 10.0),
            ev(Kind::Evict, "i1", "p", 20.0),
            ev(Kind::Store, "i1", "p", 110.0),
        ],
        100.0,
    );
}

#[test]
fn a_readmit_chain_across_the_seam_keeps_its_count_and_its_sample() {
    // Open before the seam, re-admitted after it, closed after that: the residency sample
    // must carry the pre-seam open instant, the opener's class and the FULL re-admit
    // count — the fields the frontier exists to carry.
    assert_slice_equals_whole(
        &[
            ev(Kind::Store, "i1", "p", 10.0),
            ev(Kind::Store, "i1", "p", 50.0),
            ev(Kind::Store, "i1", "p", 120.0),
            ev(Kind::Evict, "i1", "p", 130.0),
        ],
        100.0,
    );
}

#[test]
fn re_across_the_seam_sees_the_other_holder_through_the_seed() {
    // i1 holds p from before the seam and is silent after it; i2 stores p after the seam.
    // RE is only decidable through the seeded open interval of a key with NO slice events
    // — the union half of the seeded fold.
    assert_slice_equals_whole(
        &[
            ev(Kind::Store, "i1", "p", 10.0),
            ev(Kind::Store, "i2", "p", 120.0),
        ],
        100.0,
    );
}

#[test]
fn post_clear_across_the_seam_remembers_the_reason() {
    assert_slice_equals_whole(
        &[
            ev(Kind::Store, "i1", "p", 10.0),
            clear("i1", 20.0),
            ev(Kind::Store, "i1", "p", 110.0),
        ],
        100.0,
    );
}

#[test]
fn a_quiet_slice_round_trips_the_frontier_and_a_third_slice_still_agrees() {
    // Slice 2 has no events for p at all; slice 3 closes it. The chain law: frontier
    // through a seeded, quiet fold must be the identity function.
    let events = [
        ev(Kind::Store, "i1", "p", 10.0),
        ev(Kind::Store, "i1", "q", 150.0),
        ev(Kind::Evict, "i1", "p", 250.0),
        ev(Kind::Store, "i1", "p", 260.0),
    ];
    let whole = classify_whole(&events);
    // three sittings: [0,100), [100,200), [200,inf)
    let params = Params::default();
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
    let mut frontier: Vec<Frontier> = Vec::new();
    let mut all_stores = Vec::new();
    for window in [(0.0, 100.0), (100.0, 200.0), (200.0, f64::MAX)] {
        let part: Vec<Event> = events
            .iter()
            .filter(|e| e.at_ms >= window.0 && e.at_ms < window.1)
            .cloned()
            .collect();
        let closers = ScopeClosers::of(&part);
        let mut next_frontier = Vec::new();
        for g in &group_by_identity(&part) {
            let seeds: Vec<Frontier> = frontier
                .iter()
                .filter(|f| f.key.identity == g.identity && f.key.key_epoch == g.key_epoch)
                .cloned()
                .collect();
            let out = classify_identity_group_seeded(g, &closers, &model, &params, &seeds);
            all_stores.extend(out.stores);
            next_frontier.extend(out.frontiers);
        }
        // Identities the slice never touched carry their frontier forward unchanged —
        // exactly what the banked table does between passes.
        let touched: BTreeSet<(String, Option<u32>)> = next_frontier
            .iter()
            .map(|f| (f.key.identity.clone(), f.key.key_epoch))
            .collect();
        frontier.retain(|f| !touched.contains(&(f.key.identity.clone(), f.key.key_epoch)));
        frontier.extend(next_frontier);
    }
    let mut whole_stores: Vec<_> = whole.iter().flat_map(|g| g.stores.clone()).collect();
    let key = |s: &analyzer_analysis::classify::ClassifiedStore| {
        (s.at_ms.to_bits(), s.block.clone(), s.instance.clone())
    };
    whole_stores.sort_by_key(key);
    all_stores.sort_by_key(key);
    assert_eq!(all_stores, whole_stores, "three sittings diverged from one");

    let mut whole_frontier: Vec<_> = whole.iter().flat_map(|g| g.frontiers.clone()).collect();
    let fkey = |f: &Frontier| {
        (
            f.key.identity.clone(),
            f.key.instance.clone(),
            f.key.tier.clone(),
        )
    };
    whole_frontier.sort_by_key(fkey);
    frontier.sort_by_key(fkey);
    assert_eq!(frontier, whole_frontier, "the chained frontier drifted");
}

#[test]
fn a_close_older_than_the_examined_history_decides_no_store_carried_or_read() {
    // The law is the store's own: a close older than the examined history before the store is
    // no close. A carried frontier says the same as a read of the history does, wherever the
    // slice boundary falls, so the store classifies cold either way and surfaces no gap.
    let events = [
        ev(Kind::Store, "i1", "p", 10.0),
        ev(Kind::Evict, "i1", "p", 20.0),
        ev(Kind::Store, "i1", "p", 110.0),
    ];
    let params = Params::default().with_examined_history_ms(80.0);
    let whole: Vec<_> = classify_whole_under(&events, &params)
        .into_iter()
        .flat_map(|g| g.stores)
        .filter(|s| s.at_ms >= 100.0)
        .collect();
    for boundary in [15.0, 50.0, 100.0] {
        let sliced: Vec<_> = classify_sliced(&events, boundary, &params)
            .into_iter()
            .flat_map(|g| g.stores)
            .filter(|s| s.at_ms >= 100.0)
            .collect();
        assert_eq!(sliced, whole, "boundary {boundary}");
    }
    assert_eq!(whole.len(), 1);
    assert_eq!(
        whole[0].class,
        analyzer_analysis::classify::Class::Cold,
        "the close is 90 ms back, past the 80 ms history"
    );
    assert_eq!(whole[0].gap_ms, None, "and surfaces no gap");
    // Inside the history, the same close decides.
    let wide = Params::default().with_examined_history_ms(100.0);
    let decided: Vec<_> = classify_sliced(&events, 50.0, &wide)
        .into_iter()
        .flat_map(|g| g.stores)
        .collect();
    assert_eq!(decided[0].class, analyzer_analysis::classify::Class::Etr);
}

#[test]
fn a_clear_after_the_seam_closes_a_dormant_seed() {
    // p opens before the seam and is SILENT after it; the clear lands after the seam. The
    // whole window closes p (PostClear context, a residency sample, a closed frontier);
    // the slice can only do the same through the dormant-group driver — no event of p's
    // ever appears in the slice stream.
    assert_slice_equals_whole(
        &[
            ev(Kind::Store, "i1", "p", 10.0),
            clear("i1", 110.0),
            ev(Kind::Store, "i1", "p", 120.0),
        ],
        100.0,
    );
}

#[test]
fn a_clear_after_the_seam_closes_the_other_holders_seed_inside_a_live_group() {
    // The union-half variant: identity p IS streamed this slice (i2 stores it), but i1's
    // residency exists only as a seed — and i1's clear must still close it, through the
    // covering closers the seed-only fold now merges.
    assert_slice_equals_whole(
        &[
            ev(Kind::Store, "i1", "p", 10.0),
            clear("i1", 110.0),
            ev(Kind::Store, "i2", "p", 120.0),
        ],
        100.0,
    );
}

#[test]
fn a_residency_open_longer_than_the_examined_history_still_holds_the_block_elsewhere() {
    // The history bounds how far back a close decides a store, not how long a block may stay
    // resident: i1 has held the block since 10 s, far past an 80 s history, so i2's store at
    // 200 s is RE whether the residency was carried across a boundary or read in one pass.
    let events = [
        ev(Kind::Store, "i1", "p", 10_000.0),
        ev(Kind::Store, "i2", "p", 200_000.0),
    ];
    let params = Params::default().with_examined_history_ms(80_000.0);
    let late = |groups: Vec<GroupClassification>| -> Vec<_> {
        groups
            .into_iter()
            .flat_map(|g| g.stores)
            .filter(|s| s.at_ms >= 100_000.0)
            .collect()
    };
    let whole = late(classify_whole_under(&events, &params));
    for boundary in [15_000.0, 100_000.0, 199_000.0] {
        assert_eq!(
            late(classify_sliced(&events, boundary, &params)),
            whole,
            "boundary {boundary}"
        );
    }
    assert_eq!(whole.len(), 1);
    assert_eq!(whole[0].class, analyzer_analysis::classify::Class::Re);
}
