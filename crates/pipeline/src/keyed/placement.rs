// SPDX-License-Identifier: AGPL-3.0-only

//! Placement on block-keyed lanes (`docs/pipeline.md`).
//!
//! The derivation is the [`Duplication`] module, unchanged: one identity's cache events, from
//! the seeds it carried, to the window's cells of the duplication tables and the seeds the next
//! window resumes from. This operator decides only where an identity's seeds live and which
//! records a tick folds. Identities route to lanes by the same stable hash as repeat compute's
//! blocks, so both operators' state for an identity sits in one lane; scope closers (`clear`,
//! `holder_reset`) name no identity and reach every lane.
//!
//! A tick folds every lane's records before `data_to_ms` and emits from where the last tick
//! stopped to `data_to_ms` less its [`Emission`]'s delay: a closer just past an emitted instant
//! can shorten the residency before it, so a watermarked emission holds back half the skew.
//!
//! **A tick costs what moved, not what is resident.** An open residency adds to the cells of
//! every minute it stays open, but an identity whose holders hold still adds the same per
//! millisecond ([`Rates`]). A lane keeps those identities apart (`still`) with the sum of their
//! rates, and states their cells for the window from that sum, one addition per key and minute.
//! A tick folds one by one only the identities with records in it, those a scope closer reaches,
//! and those whose holders still change (an open not yet in effect past the skew), and each only
//! up to where its holders stop changing inside the window: from there it joins the sum at its
//! rates, so a long window costs its minutes once, not once per identity. An identity a record
//! touches leaves the sum first, bringing its whale days up to the window. The emission's delay
//! is what makes this exact: a record's effect starts no earlier than the window it is folded in.

use super::repeat::Emission;
use super::state::KeyedState;
use analyzer_analysis::dup_cells::{DupCells, DupCtx, Duplication, IdentityState, Rates, WhaleSum};
use analyzer_analysis::events::{Event, ScopeClosers};
use analyzer_analysis::module::DerivationModule;
use analyzer_analysis::{Params, Window};
use std::collections::{BTreeMap, BTreeSet};

/// One lane: its identities' seeds, and their records not yet folded.
struct Lane<S> {
    state: S,
    pending: Vec<Event>,
    /// Identities whose seeds a tick changed since the changes were last taken.
    changed: BTreeSet<String>,
    /// Identities whose holders hold still, each with its rates and since when.
    still: BTreeMap<String, Still>,
    /// The sum of the still identities' rates.
    standing: Rates,
    /// The still identities with a seed on each instance: what a scope closer reaches.
    on_instance: BTreeMap<String, BTreeSet<String>>,
    /// Identities with seeds that do not hold still yet: folded every tick.
    moving: BTreeSet<String>,
    /// Identities a restore brought back and no tick has placed yet.
    unplaced: Vec<String>,
}

/// An identity whose holders hold still.
struct Still {
    rates: Rates,
    /// Since when: its whale days are accrued to here.
    since_ms: f64,
    /// The instances its seeds sit on.
    instances: BTreeSet<String>,
}

impl<S: KeyedState<String, IdentityState>> Lane<S> {
    fn new(state: S) -> Self {
        Self {
            state,
            pending: Vec::new(),
            changed: BTreeSet::new(),
            still: BTreeMap::new(),
            standing: Rates::default(),
            on_instance: BTreeMap::new(),
            moving: BTreeSet::new(),
            unplaced: Vec::new(),
        }
    }

    /// Take `identity` out of the still set, its whale days accrued to `to_ms`. Returns whether
    /// it was still.
    fn release(&mut self, identity: &str, to_ms: f64) -> bool {
        let Some(still) = self.still.remove(identity) else {
            return false;
        };
        self.standing.add(&still.rates, -1.0);
        for instance in &still.instances {
            if let Some(set) = self.on_instance.get_mut(instance) {
                set.remove(identity);
                if set.is_empty() {
                    self.on_instance.remove(instance);
                }
            }
        }
        if let Some(mut held) = self.state.get(&identity.to_string()).cloned() {
            held.whale
                .accrue(&still.rates, held.n_tokens, (still.since_ms, to_ms));
            self.state.put(identity.to_string(), held);
            self.changed.insert(identity.to_string());
        }
        true
    }

    /// Place `identity`, holding `held`, from `from_ms`: still, with its rates in the sum, when
    /// every open is in effect by then; otherwise moving. An identity with no seeds is neither.
    fn place(&mut self, identity: &str, held: &IdentityState, from_ms: f64, half_skew: f64) {
        match Rates::of(held, half_skew, from_ms) {
            Some(rates) => self.hold(identity, held, from_ms, rates),
            None => {
                self.moving.insert(identity.to_string());
            }
        }
    }

    /// Hold `identity` still from `from_ms` at `rates`, its rates in the sum. An identity with
    /// no seeds holds nothing and is neither still nor moving.
    fn hold(&mut self, identity: &str, held: &IdentityState, from_ms: f64, rates: Rates) {
        self.moving.remove(identity);
        if held.seeds.is_empty() {
            return;
        }
        let instances: BTreeSet<String> = held.seeds.keys().map(|k| k.instance.clone()).collect();
        for instance in &instances {
            self.on_instance
                .entry(instance.clone())
                .or_default()
                .insert(identity.to_string());
        }
        self.standing.add(&rates, 1.0);
        self.still.insert(
            identity.to_string(),
            Still {
                rates,
                since_ms: from_ms,
                instances,
            },
        );
    }
}

/// What one placement tick produced.
#[derive(Debug, Default)]
pub struct PlacementTick {
    /// The emitted window's cells.
    pub cells: DupCells,
    /// Days a token mass rose on after cells for them were emitted. Nothing restates them: the
    /// cells emitted before the rise keep the mass they were priced at, a lower bound
    /// (`docs/classification-semantics.md` section 13).
    pub repair_days: BTreeSet<i64>,
    /// Identities folded.
    pub identities: u64,
    /// The window emitted, if the tick emitted one.
    pub emitted: Option<Window>,
}

/// Placement over `lanes` lanes, each keeping its identities' seeds in `S`.
pub struct PlacementCompute<S> {
    lanes: Vec<Lane<S>>,
    emission: Emission,
    /// Where the seeds are exact: the end of the records folded so far.
    data_to_ms: Option<f64>,
    /// Where the first tick began emitting: its first record. Nothing before it was observed,
    /// so no re-derivation reaches before it.
    emitted_from_ms: Option<f64>,
    /// Through where the cells are emitted.
    emitted_to_ms: Option<f64>,
}

impl<S> PlacementCompute<S>
where
    S: KeyedState<String, IdentityState> + Send,
{
    /// An operator over `lanes` lanes (at least one), each with a fresh state from `state`.
    #[must_use]
    pub fn new(lanes: usize, emission: Emission, state: impl Fn() -> S) -> Self {
        Self {
            lanes: (0..lanes.max(1)).map(|_| Lane::new(state())).collect(),
            emission,
            data_to_ms: None,
            emitted_from_ms: None,
            emitted_to_ms: None,
        }
    }

    /// Where the seeds are exact, once a tick has folded.
    #[must_use]
    pub fn data_to_ms(&self) -> Option<f64> {
        self.data_to_ms
    }

    /// Where the cells are emitted from, once a tick has emitted.
    #[must_use]
    pub fn emitted_from_ms(&self) -> Option<f64> {
        self.emitted_from_ms
    }

    /// Through where the cells are emitted, once a tick has emitted.
    #[must_use]
    pub fn emitted_to_ms(&self) -> Option<f64> {
        self.emitted_to_ms
    }

    /// Take records to fold: each to its identity's lane, a scope closer to every lane.
    /// Returns how many arrived behind the records already folded; those are not folded.
    pub fn accept(&mut self, events: Vec<Event>) -> u64 {
        let mut behind = 0;
        let lane_count = self.lanes.len();
        for event in events {
            if self.data_to_ms.is_some_and(|edge| event.at_ms < edge) {
                behind += 1;
                continue;
            }
            match event.identity() {
                Some(identity) => {
                    let target = super::repeat::lane_of(identity, lane_count);
                    self.lanes[target].pending.push(event);
                }
                None if event.kind.closes() => {
                    for lane in &mut self.lanes {
                        lane.pending.push(event.clone());
                    }
                }
                None => {}
            }
        }
        behind
    }

    /// Fold every lane's records before `data_to_ms` from the seeds each identity carries,
    /// and emit from where the last tick stopped (the first record, on the first tick) to
    /// `data_to_ms` less the emission's delay. `silences`, each `(instance, at)` where an
    /// instance went unobserved since the last tick's data edge, close every residency on the
    /// instance as a `holder_reset` would. Nothing moves when `data_to_ms` is not past the
    /// records already folded.
    #[must_use]
    pub fn advance(
        &mut self,
        data_to_ms: f64,
        params: &Params,
        silences: &[(String, f64)],
    ) -> PlacementTick {
        if self.data_to_ms.is_some_and(|done| data_to_ms <= done) {
            return PlacementTick::default();
        }
        let first = self
            .lanes
            .iter()
            .flat_map(|l| l.pending.iter().map(|e| e.at_ms))
            .reduce(f64::min);
        let Some(from) = self.emitted_to_ms.or(first) else {
            self.data_to_ms = Some(data_to_ms);
            return PlacementTick::default();
        };
        let settled = match self.emission {
            Emission::Watermarked { delay_ms } => data_to_ms - delay_ms,
            Emission::Immediate => data_to_ms,
        };
        let emitted = Window {
            start_ms: from,
            end_ms: settled.max(from),
        };
        let half_skew = params.delta_skew_ms / 2.0;
        let lanes = &mut self.lanes;
        let ticks: Vec<PlacementTick> = std::thread::scope(|scope| {
            let handles: Vec<_> = lanes
                .iter_mut()
                .map(|lane| {
                    scope.spawn(move || fold_lane(lane, data_to_ms, emitted, half_skew, silences))
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                .collect()
        });
        let mut tick = PlacementTick::default();
        for t in ticks {
            tick.cells.merge(t.cells);
            tick.repair_days.extend(t.repair_days);
            tick.identities += t.identities;
        }
        self.data_to_ms = Some(data_to_ms);
        self.emitted_from_ms = self.emitted_from_ms.or(Some(emitted.start_ms));
        self.emitted_to_ms = Some(emitted.end_ms);
        tick.emitted = Some(emitted);
        tick
    }

    /// Every identity's seeds across the lanes.
    pub fn states(&self) -> impl Iterator<Item = (&String, &IdentityState)> {
        self.lanes.iter().flat_map(|l| l.state.scan())
    }

    /// What ticks changed since the last call: each changed identity's seeds, `None` for one
    /// that holds none now.
    pub fn take_changes(&mut self) -> Vec<(String, Option<IdentityState>)> {
        let mut changes = Vec::new();
        for lane in &mut self.lanes {
            for identity in std::mem::take(&mut lane.changed) {
                let now = lane.state.get(&identity).cloned();
                changes.push((identity, now));
            }
        }
        changes
    }

    /// `identity`'s seeds, if it holds any.
    #[must_use]
    pub fn seeds_of(&self, identity: &str) -> Option<&IdentityState> {
        let lane = super::repeat::lane_of(identity, self.lanes.len());
        self.lanes[lane].state.get(&identity.to_string())
    }

    /// Replace `identity`'s seeds with a re-derivation's `seeds`, and correct its whale days by
    /// the re-derivation's: `whale.0` what the window held as it was folded, `whale.1` as it is.
    /// The identity is folded one by one from the next tick, which places it again.
    pub fn replace_identity(
        &mut self,
        identity: &str,
        seeds: IdentityState,
        whale: (&BTreeMap<i64, WhaleSum>, &BTreeMap<i64, WhaleSum>),
    ) {
        let emitted_to = self.emitted_to_ms.unwrap_or(f64::NEG_INFINITY);
        let lane = super::repeat::lane_of(identity, self.lanes.len());
        let lane = &mut self.lanes[lane];
        lane.release(identity, emitted_to);
        let mut accrued = lane
            .state
            .get(&identity.to_string())
            .map(|held| held.whale.clone())
            .unwrap_or_default();
        accrued.correct(whale.0, whale.1);
        let state = IdentityState {
            whale: accrued,
            ..seeds
        };
        if state.seeds.is_empty() && state.whale.days.is_empty() {
            lane.state.remove(&identity.to_string());
            lane.moving.remove(identity);
        } else {
            if !state.seeds.is_empty() {
                lane.moving.insert(identity.to_string());
            }
            lane.state.put(identity.to_string(), state);
        }
        lane.changed.insert(identity.to_string());
    }

    /// Every identity's whale totals for `day`, through the emitted edge: what its folds
    /// accrued, and for a still identity what its rates add since. What the whale register
    /// states once the day settles; one pass over the identities.
    #[must_use]
    pub fn whale_totals(&self, day: i64) -> Vec<(String, WhaleSum)> {
        let to = self.emitted_to_ms.unwrap_or(f64::NEG_INFINITY);
        let mut out = Vec::new();
        for lane in &self.lanes {
            for (identity, held) in lane.state.scan() {
                let mut whale = held.whale.clone();
                if let Some(still) = lane.still.get(identity) {
                    whale.accrue(&still.rates, held.n_tokens, (still.since_ms, to));
                }
                if let Some(sum) = whale.days.get(&day) {
                    out.push((identity.clone(), *sum));
                }
            }
        }
        out
    }

    /// Drop every identity's whale days before `day`, and the identities left holding nothing.
    /// One pass over the identities, when the first day open to change moves.
    pub fn retire_whales_before(&mut self, day: i64) {
        for lane in &mut self.lanes {
            let retired: Vec<(String, IdentityState)> = lane
                .state
                .scan()
                .filter(|(_, held)| held.whale.days.keys().next().is_some_and(|d| *d < day))
                .map(|(identity, held)| {
                    let mut held = held.clone();
                    held.whale.retire_before(day);
                    (identity.clone(), held)
                })
                .collect();
            for (identity, held) in retired {
                if held.seeds.is_empty() && held.whale.days.is_empty() {
                    lane.state.remove(&identity);
                } else {
                    lane.state.put(identity.clone(), held);
                }
                lane.changed.insert(identity);
            }
        }
    }

    /// An operator resumed from what a store holds: `seeds` routed to their lanes, exact at
    /// `data_to_ms`, the cells emitted over `[emitted_from_ms, emitted_to_ms)`. The next tick
    /// places each identity from where its whale days are accrued to, which for an identity that
    /// held still is where it began to. Nothing counts as changed.
    #[must_use]
    pub fn restored(
        mut self,
        seeds: impl IntoIterator<Item = (String, IdentityState)>,
        (data_to_ms, emitted_from_ms, emitted_to_ms): (Option<f64>, Option<f64>, Option<f64>),
    ) -> Self {
        let lane_count = self.lanes.len();
        for (identity, held) in seeds {
            let lane = super::repeat::lane_of(&identity, lane_count);
            if !held.seeds.is_empty() {
                self.lanes[lane].unplaced.push(identity.clone());
            }
            self.lanes[lane].state.put(identity, held);
        }
        self.data_to_ms = data_to_ms;
        self.emitted_from_ms = emitted_from_ms;
        self.emitted_to_ms = emitted_to_ms;
        self
    }
}

/// Fold one lane over `emitted`: the still identities' cells from their summed rates, and one by
/// one every identity with records before `data_to_ms`, every still identity a scope closer
/// reaches, and every moving one. The records after `data_to_ms` stay pending.
fn fold_lane<S>(
    lane: &mut Lane<S>,
    data_to_ms: f64,
    emitted: Window,
    half_skew: f64,
    silences: &[(String, f64)],
) -> PlacementTick
where
    S: KeyedState<String, IdentityState>,
{
    for identity in std::mem::take(&mut lane.unplaced) {
        if let Some(held) = lane.state.get(&identity).cloned() {
            let from = held.whale.through_ms.unwrap_or(emitted.start_ms);
            lane.place(&identity, &held, from, half_skew);
        }
    }
    let (inside, rest): (Vec<Event>, Vec<Event>) =
        lane.pending.drain(..).partition(|e| e.at_ms < data_to_ms);
    lane.pending = rest;
    let closers = ScopeClosers::of(&inside).with_silences(silences.iter().cloned());
    let mut grouped: BTreeMap<String, Vec<Event>> = BTreeMap::new();
    let mut reached: BTreeSet<String> = BTreeSet::new();
    for (instance, _) in silences {
        if let Some(on) = lane.on_instance.get(instance) {
            reached.extend(on.iter().cloned());
        }
    }
    for event in inside {
        match event.identity() {
            Some(identity) => grouped.entry(identity.to_string()).or_default().push(event),
            None if event.kind.closes() => {
                if let Some(on) = lane.on_instance.get(&event.instance) {
                    reached.extend(on.iter().cloned());
                }
            }
            None => {}
        }
    }
    let mut folded: BTreeSet<String> = grouped.keys().cloned().collect();
    folded.extend(reached);
    folded.extend(lane.moving.iter().cloned());
    for identity in &folded {
        lane.release(identity, emitted.start_ms);
    }
    let mut tick = PlacementTick::default();
    tick.cells
        .add_rates(&lane.standing, (emitted.start_ms, emitted.end_ms));
    let ctx = DupCtx {
        closers: &closers,
        half_skew,
    };
    let mut joins = Vec::new();
    for identity in folded {
        let events = grouped.remove(&identity).unwrap_or_default();
        let held = lane.state.get(&identity).cloned().unwrap_or_default();
        // An identity whose holders stop changing inside the window is folded up to there and
        // joins the sum at its rates for the rest.
        let joining = settles_at(&events, &held, &closers, half_skew, emitted).and_then(|at| {
            let changing = Window {
                start_ms: emitted.start_ms,
                end_ms: at,
            };
            let next = Duplication.fold(&identity, &events, held.clone(), changing, &ctx);
            Rates::of(&next.state, half_skew, at).map(|rates| (next, at, rates))
        });
        let (next, joined) = match joining {
            Some((next, at, rates)) => (next, Some((at, rates))),
            None => (
                Duplication.fold(&identity, &events, held.clone(), emitted, &ctx),
                None,
            ),
        };
        tick.cells.merge(next.cells);
        tick.repair_days.extend(next.repair_days);
        tick.identities += 1;
        if next.state != held {
            lane.changed.insert(identity.clone());
            if next.state == IdentityState::default() {
                lane.state.remove(&identity);
            } else {
                lane.state.put(identity.clone(), next.state.clone());
            }
        }
        match joined {
            Some((at, rates)) => {
                joins.push((at, rates.clone()));
                lane.hold(&identity, &next.state, at, rates);
            }
            None => lane.place(&identity, &next.state, emitted.end_ms, half_skew),
        }
    }
    tick.cells
        .add_rates_joining(&Rates::default(), joins, (emitted.start_ms, emitted.end_ms));
    tick
}

/// Where `held`'s holders stop changing inside `emitted` under `events`: the last edge of its
/// intervals there, each moved inward by `half_skew`. `None` where an edge falls at or past the
/// window's end, so the holders over the window's tail are not the seeds the fold ends with.
fn settles_at(
    events: &[Event],
    held: &IdentityState,
    closers: &ScopeClosers,
    half_skew: f64,
    emitted: Window,
) -> Option<f64> {
    let keys = analyzer_analysis::events::group_by_interval_key(events);
    let intervals = analyzer_analysis::intervals::residency_intervals_for_keys_seeded(
        &keys,
        closers,
        &held.seeds,
    );
    let mut last = emitted.start_ms;
    for iv in &intervals {
        let edges = [
            iv.open_ms.map(|open| open + half_skew),
            iv.close_ms.map(|close| close - half_skew),
        ];
        for edge in edges.into_iter().flatten() {
            if edge >= emitted.end_ms {
                return None;
            }
            last = last.max(edge);
        }
    }
    Some(last)
}
