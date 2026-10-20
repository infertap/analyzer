// SPDX-License-Identifier: AGPL-3.0-only

//! Repeat compute on block-keyed lanes (`docs/pipeline.md`).
//!
//! The classification is the [`ClassBand`] derivation module, unchanged: one block and every
//! holder's intervals for it are one key, folded from the frontiers the key carried. This
//! operator decides only when a record is folded and where its key's state lives. Records are
//! routed to lanes by their block, so a lane holds every holder's state for its blocks and a
//! holder's records reach it in delivery order; scope closers (`clear`, `holder_reset`) name no
//! block and reach every lane. A tick folds each lane's records before the edge its
//! [`Emission`] allows, one UTC day at a time, and carries each key's frontiers to the next.

use super::frontiers::LaneFrontiers;
use analyzer_analysis::class_band::{ClassBand, ClassBandCells, ClassCtx};
use analyzer_analysis::classify::{dormant_groups, Frontier};
use analyzer_analysis::completeness::UncertaintyModel;
use analyzer_analysis::emitted::Meaning;
use analyzer_analysis::events::{group_by_identity, Event, IntervalKey, ScopeClosers};
use analyzer_analysis::intervals::CloseReason;
use analyzer_analysis::module::{run_window, Mergeable};
use analyzer_analysis::{Params, Window, DAY_MS};
use std::collections::{BTreeMap, BTreeSet};

/// A classify group's key: the block's identity and its key epoch.
pub type IdentityKey = (String, Option<u32>);

/// When a record may be classified.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Emission {
    /// Once the watermark has passed its instant by `delay_ms`. `RE` reads another holder's
    /// open interval over the skew-widened instant, so a store is final only once every
    /// holder's records are complete that far past it: the register's policy, with the delay
    /// set to the skew allowance.
    Watermarked {
        /// How far past a record's instant the watermark must be, in milliseconds.
        delay_ms: f64,
    },
    /// As soon as it arrives, on the view it arrives to: a store near the edge may be
    /// classified before another holder's covering record arrives.
    Immediate,
}

/// A deployment's choice of when operators emit; each operator resolves it against the delay
/// its own semantics need.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EmissionPolicy {
    /// Emit only what the watermark has settled, so emitted cells are never restated.
    #[default]
    Watermarked,
    /// Emit everything folded so far, on the view it arrived to.
    Immediate,
}

impl EmissionPolicy {
    /// The emission for an operator whose output settles `delay_ms` behind the watermark.
    #[must_use]
    pub fn resolve(self, delay_ms: f64) -> Emission {
        match self {
            Self::Watermarked => Emission::Watermarked { delay_ms },
            Self::Immediate => Emission::Immediate,
        }
    }
}

/// What classification reads beside the records: the parameters, the completeness model the
/// taint rule consults, and what each fact row is stamped with.
pub struct RepeatContext<'a> {
    /// The classification parameters: the skew allowance and the examined history.
    pub params: &'a Params,
    /// The drop windows classification taint consults.
    pub model: &'a UncertaintyModel,
    /// The semantics version and discriminator every fact row is stamped with.
    pub meaning: &'a Meaning,
    /// The run-scoped model relay, keyed by (instance, incarnation).
    pub models: &'a BTreeMap<(String, String), String>,
    /// The coverage figure and path each fact row carries.
    pub coverage: f64,
}

/// What one tick classified.
#[derive(Debug, Default)]
pub struct Tick {
    /// The class and band cells of the stores classified.
    pub cells: ClassBandCells,
    /// The edge classified through, if anything has been.
    pub classified_to_ms: Option<f64>,
}

/// One lane: the blocks whose hash maps to it, their frontiers, and their records not yet
/// folded.
#[derive(Default)]
struct Lane {
    state: LaneFrontiers,
    pending: Vec<Event>,
    /// Keys whose state a tick changed since the changes were last taken.
    changed: BTreeSet<IdentityKey>,
}

/// Repeat compute over `lanes` block-keyed lanes, each keeping its keys' frontiers.
pub struct RepeatCompute {
    lanes: Vec<Lane>,
    emission: Emission,
    classified_to_ms: Option<f64>,
}

/// What a tick changed, for a store to write: each changed key's frontiers, `None` for a key
/// that left.
#[derive(Debug, Default)]
pub struct Changes {
    /// Changed keys and their state now.
    pub frontiers: Vec<(IdentityKey, Option<Vec<Frontier>>)>,
}

/// The lane a block's records and state belong to: the spine's identity hash, stable across
/// builds, so a restored state routes as it was written.
pub(super) fn lane_of(identity: &str, lanes: usize) -> usize {
    #[allow(clippy::cast_possible_truncation)] // reduced below `lanes`
    let lane = (crate::spine::identity_hash(identity) % lanes as u64) as usize;
    lane
}

impl RepeatCompute {
    /// An operator over `lanes` lanes (at least one), each holding no frontiers yet.
    #[must_use]
    pub fn new(lanes: usize, emission: Emission) -> Self {
        Self {
            lanes: (0..lanes.max(1)).map(|_| Lane::default()).collect(),
            emission,
            classified_to_ms: None,
        }
    }

    /// Take a delivery's records: each to its block's lane, a scope closer to every lane.
    /// Returns how many arrived behind the edge already classified; those are not folded, and
    /// belong to the correction path.
    pub fn accept(&mut self, events: Vec<Event>) -> u64 {
        let mut late = 0;
        let lane_count = self.lanes.len();
        for event in events {
            if self.classified_to_ms.is_some_and(|edge| event.at_ms < edge) {
                late += 1;
                continue;
            }
            match event.identity() {
                Some(identity) => {
                    let target = lane_of(identity, lane_count);
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
        late
    }

    /// Classify every lane's records before the edge the emission policy allows at
    /// `watermark_ms`, one UTC day at a time, carrying each key's frontiers; return the cells.
    #[must_use]
    pub fn advance(&mut self, watermark_ms: Option<f64>, ctx: &RepeatContext<'_>) -> Tick {
        match self.advance_carrying(watermark_ms, ctx, |_, _| {
            Ok::<(), std::convert::Infallible>(())
        }) {
            Ok(tick) => tick,
            Err(never) => match never {},
        }
    }

    /// [`Self::advance`], calling `at_boundary` with the operator as it stands at each UTC day
    /// boundary the tick classifies through, before anything after it is folded: where
    /// [`Self::carried`] reads the frontiers a re-derivation from that boundary starts with.
    ///
    /// # Errors
    /// What `at_boundary` returns.
    pub fn advance_carrying<E>(
        &mut self,
        watermark_ms: Option<f64>,
        ctx: &RepeatContext<'_>,
        mut at_boundary: impl FnMut(&Self, f64) -> Result<(), E>,
    ) -> Result<Tick, E> {
        let newest = self
            .lanes
            .iter()
            .flat_map(|l| l.pending.iter().map(|e| e.at_ms))
            .reduce(f64::max);
        let edge = match self.bound(watermark_ms) {
            Some(bound) if bound.is_finite() => Some(bound),
            Some(_) => newest.map(|n| n + 1.0),
            None => None,
        };
        let mut tick = Tick {
            classified_to_ms: self.classified_to_ms,
            ..Tick::default()
        };
        let Some(edge) = edge.filter(|e| self.classified_to_ms.is_none_or(|done| *e > done)) else {
            return Ok(tick);
        };
        // From the edge already classified, records or none: an idle stretch still crosses day
        // boundaries and the silences in it.
        let first = self
            .lanes
            .iter()
            .flat_map(|l| l.pending.iter().map(|e| e.at_ms))
            .reduce(f64::min);
        let Some(start) = (match (first, self.classified_to_ms) {
            (Some(first), Some(done)) => Some(first.min(done)),
            (first, done) => first.or(done),
        }) else {
            self.classified_to_ms = Some(edge);
            tick.classified_to_ms = Some(edge);
            return Ok(tick);
        };
        // One UTC day at a time: the module scopes a store to the day it emits.
        let mut day_start = (start / DAY_MS).floor() * DAY_MS;
        while day_start < edge {
            let slice = Window {
                start_ms: day_start.max(start),
                end_ms: (day_start + DAY_MS).min(edge),
            };
            let day = Window {
                start_ms: day_start,
                end_ms: day_start + DAY_MS,
            };
            let lanes = &mut self.lanes;
            let cells: Vec<ClassBandCells> = std::thread::scope(|scope| {
                let handles: Vec<_> = lanes
                    .iter_mut()
                    .map(|lane| scope.spawn(move || fold_lane(lane, slice, day, ctx)))
                    .collect();
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                    .collect()
            });
            for c in cells {
                tick.cells.merge(c);
            }
            day_start += DAY_MS;
            if edge >= day_start {
                at_boundary(self, day_start)?;
            }
        }
        // A close older than the examined history decides nothing past the edge: it leaves.
        for lane in &mut self.lanes {
            let retired = lane.state.retire(
                edge - ctx.params.examined_history_ms,
                edge - analyzer_analysis::CORRECTION_HORIZON_MS,
            );
            lane.changed.extend(retired);
        }
        self.classified_to_ms = Some(edge);
        tick.classified_to_ms = Some(edge);
        Ok(tick)
    }

    /// The blocks with a frontier entry on `instance`, open or closed, across the lanes.
    #[must_use]
    pub fn blocks_on(&self, instance: &str) -> Vec<String> {
        self.lanes
            .iter()
            .flat_map(|l| l.state.blocks_on(instance))
            .collect()
    }

    /// How many lanes the operator keeps.
    #[must_use]
    pub fn lanes(&self) -> usize {
        self.lanes.len()
    }

    /// Lane `lane`'s frontiers as a re-derivation from `at_ms` needs them: every key's open
    /// residencies, its closes within `history_ms` before it, and the closes a silence made
    /// within the correction horizon, which a late segment may yet take back. Read while the
    /// operator stands at `at_ms`, from [`Self::advance_carrying`]'s callback.
    pub fn carried(
        &self,
        lane: usize,
        at_ms: f64,
        history_ms: f64,
    ) -> impl Iterator<Item = (IdentityKey, Vec<Frontier>)> + '_ {
        let cutoff = at_ms - history_ms;
        let silent_cutoff = at_ms - analyzer_analysis::CORRECTION_HORIZON_MS;
        self.lanes[lane]
            .state
            .scan()
            .filter_map(move |(key, frontiers)| {
                let kept: Vec<Frontier> = frontiers
                    .into_iter()
                    .filter(|f| {
                        let from = if f.last_close_reason == Some(CloseReason::Silent) {
                            silent_cutoff
                        } else {
                            cutoff
                        };
                        f.open || f.last_close_ms.is_some_and(|c| c >= from)
                    })
                    .collect();
                (!kept.is_empty()).then_some((key, kept))
            })
    }

    /// What ticks changed since the last call: each changed key's frontiers, for a store to
    /// write.
    pub fn take_changes(&mut self) -> Changes {
        let mut changes = Changes::default();
        for lane in &mut self.lanes {
            for key in std::mem::take(&mut lane.changed) {
                let now = lane.state.get(&key);
                changes.frontiers.push((key, now));
            }
        }
        changes
    }

    /// Replace every key of `identity` with `frontiers`, a re-derivation's end state: a key
    /// the re-derivation does not hold leaves.
    pub fn replace_identity(
        &mut self,
        identity: &str,
        frontiers: impl IntoIterator<Item = (IdentityKey, Vec<Frontier>)>,
    ) {
        let lane_count = self.lanes.len();
        let lane = &mut self.lanes[lane_of(identity, lane_count)];
        lane.changed.extend(lane.state.remove_block(identity));
        for (key, now) in frontiers {
            lane.state.put(&key, &now);
            lane.changed.insert(key);
        }
    }

    /// An operator resumed from what a store holds: `frontiers` routed to their lanes,
    /// classified through `classified_to_ms`. Nothing counts as changed.
    #[must_use]
    pub fn restored(
        mut self,
        frontiers: impl IntoIterator<Item = (IdentityKey, Vec<Frontier>)>,
        classified_to_ms: Option<f64>,
    ) -> Self {
        let lane_count = self.lanes.len();
        for (key, held) in frontiers {
            self.lanes[lane_of(&key.0, lane_count)]
                .state
                .put(&key, &held);
        }
        self.classified_to_ms = classified_to_ms;
        self
    }

    /// The bound a tick at `watermark_ms` classifies up to under the emission policy: the
    /// watermark less the delay, `None` while the watermark is unknown; unbounded for
    /// [`Emission::Immediate`], which classifies everything accepted.
    #[must_use]
    pub fn bound(&self, watermark_ms: Option<f64>) -> Option<f64> {
        match self.emission {
            Emission::Watermarked { delay_ms } => watermark_ms.map(|w| w - delay_ms),
            Emission::Immediate => Some(f64::INFINITY),
        }
    }

    /// The edge classified through, if anything has been.
    #[must_use]
    pub fn classified_to_ms(&self) -> Option<f64> {
        self.classified_to_ms
    }

    /// The earliest instant among the records not yet classified, or the classified edge
    /// where that is earlier: where the next tick's window starts.
    #[must_use]
    pub fn earliest_ms(&self) -> Option<f64> {
        let pending = self
            .lanes
            .iter()
            .flat_map(|l| l.pending.iter().map(|e| e.at_ms))
            .reduce(f64::min)?;
        Some(
            self.classified_to_ms
                .map_or(pending, |done| done.min(pending)),
        )
    }

    /// Every key's state across the lanes, for a checkpoint or a test.
    pub fn states(&self) -> impl Iterator<Item = (IdentityKey, Vec<Frontier>)> + '_ {
        self.lanes.iter().flat_map(|l| l.state.scan())
    }
}

/// Fold one lane's records inside `slice`, emitting into `day`: the touched keys and the
/// dormant ones a scope closer reaches, each from its carried frontiers.
fn fold_lane(
    lane: &mut Lane,
    slice: Window,
    day: Window,
    ctx: &RepeatContext<'_>,
) -> ClassBandCells {
    let (inside, rest): (Vec<Event>, Vec<Event>) = lane
        .pending
        .drain(..)
        .partition(|e| e.at_ms >= slice.start_ms && e.at_ms < slice.end_ms);
    lane.pending = rest;
    let silences: Vec<(String, f64)> = ctx
        .model
        .silences_in(slice.start_ms, slice.end_ms)
        .collect();
    if inside.is_empty() && silences.is_empty() {
        return ClassBandCells::default();
    }
    let closers = ScopeClosers::of(&inside).with_silences(silences);
    let mut grouped: BTreeMap<IdentityKey, Vec<(IntervalKey, Vec<Event>)>> =
        group_by_identity(&inside)
            .into_iter()
            .map(|g| ((g.identity, g.key_epoch), g.keys))
            .collect();
    let touched: BTreeSet<IdentityKey> = grouped.keys().cloned().collect();
    // The dormant half: keys open in the state that a scope closer here reaches but whose own
    // records are silent this slice. Bounded by the closers: none, and nothing is scanned.
    if !closers.is_empty() {
        let seeds = lane.state.open_under(&closers);
        for g in dormant_groups(&seeds, &closers, &touched) {
            grouped.insert((g.identity, g.key_epoch), g.keys);
        }
    }
    let carried: BTreeMap<IdentityKey, Vec<Frontier>> = grouped
        .keys()
        .filter_map(|k| lane.state.get(k).map(|f| (k.clone(), f)))
        .collect();
    let folded: Vec<IdentityKey> = grouped.keys().cloned().collect();
    let class_ctx = ClassCtx {
        closers: &closers,
        model: ctx.model,
        params: ctx.params,
        emit_day: day,
        coverage: ctx.coverage,
        meaning: ctx.meaning,
        models: ctx.models,
    };
    let mut outcome = run_window(&ClassBand, grouped, &carried, slice, &class_ctx);
    for key in folded {
        lane.changed.insert(key.clone());
        match outcome.states.remove(&key) {
            Some(frontiers) => lane.state.put(&key, &frontiers),
            None => lane.state.remove(&key),
        }
    }
    outcome.cells
}
