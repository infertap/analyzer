// SPDX-License-Identifier: AGPL-3.0-only

//! One block re-derived from the log partitioned by block (`docs/pipeline.md`):
//! the drill's answer, the audit's reference, and the corrections' two halves.
//!
//! A block's classification reads only its own records, the scope closers and the arrivals'
//! completeness model, so it is re-derived by the same operator that classifies the fleet, on
//! one lane: from the frontiers repeat compute carried at the window's UTC day start, and the
//! block's history from there. The carried frontiers hold every residency open at the boundary
//! however long before it opened, so what the re-derivation states for the window's stores is
//! what repeat compute stated.

use super::repeat::{Emission, IdentityKey, RepeatCompute, RepeatContext};
use crate::Error;
use analyzer_analysis::classify::Frontier;
use analyzer_analysis::emitted::{ClassifiedFact, Meaning};
use analyzer_analysis::events::Event;
use analyzer_analysis::{Discriminator, Params, Window};
use std::collections::BTreeMap;

/// One block's re-derivation.
#[derive(Debug, Default)]
pub struct Refold {
    /// The window's stores, as classified.
    pub facts: Vec<ClassifiedFact>,
    /// The block's frontiers at the window's end.
    pub frontiers: BTreeMap<IdentityKey, Vec<Frontier>>,
    /// The block's frontiers at each UTC day boundary past the window's day start that the
    /// re-derivation crossed, as repeat compute carries them there.
    pub carried: Vec<(f64, Vec<super::spine::Carried>)>,
}

/// The carried frontiers a correction restates: at each boundary its re-derivations crossed,
/// each corrected key as it is now, and each key the derivation before carried that the one now
/// does not, as leaving.
#[derive(Debug, Default)]
pub(crate) struct Recarry(BTreeMap<i64, (f64, BTreeMap<IdentityKey, Vec<Frontier>>)>);

impl Recarry {
    /// Take one block's correction: `before` re-derived as it was, `now` as it is.
    pub(crate) fn add(&mut self, before: &Refold, now: &Refold) {
        for (at, rows) in &before.carried {
            let (_, keys) = self.at(*at);
            for (key, _) in rows {
                keys.entry(key.clone()).or_default();
            }
        }
        for (at, rows) in &now.carried {
            let (_, keys) = self.at(*at);
            for (key, frontiers) in rows {
                keys.insert(key.clone(), frontiers.clone());
            }
        }
    }

    fn at(&mut self, at: f64) -> &mut (f64, BTreeMap<IdentityKey, Vec<Frontier>>) {
        self.0
            .entry(analyzer_analysis::emitted::day_index(at))
            .or_insert_with(|| (at, BTreeMap::new()))
    }

    /// Write each boundary's restated keys to the log, one object a boundary.
    ///
    /// # Errors
    /// The store refused a write.
    pub(crate) fn write(
        self,
        lake: &analyzer_lake::Lake,
        layout: super::spine::SpineLayout,
    ) -> Result<super::spine::Written, Error> {
        let mut written = super::spine::Written::default();
        for (at, keys) in self.0.into_values() {
            written.absorb(super::spine::write_carry(lake, layout, at, keys)?);
        }
        Ok(written)
    }
}

/// A block's keys and their frontiers.
type Keys = BTreeMap<IdentityKey, Vec<Frontier>>;

/// Each `(instance, incarnation)`'s model: what a store's model dimension reads
/// ([`analyzer_analysis::roster::RosterCells::models`]).
pub type Models = BTreeMap<(String, String), String>;

/// Re-derive `identity` over `window`, under `params`, each store's model from `models`: its
/// history from the examined history before the window through the skew past it (the objects
/// the manifest names and the `unsaved` ones), folded on one lane with an exact frontier under
/// the completeness model of every batch. The window's stores come back as classified, and the
/// block's frontiers as they stand at the window's end.
///
/// # Errors
/// The log, the working state or the arrivals could not be read.
pub fn refold_block(
    lake: &analyzer_lake::Lake,
    identity: &str,
    window: Window,
    (params, models): (&Params, &Models),
    unsaved: &super::spine::Written,
) -> Result<Refold, Error> {
    let model = crate::arrivals::classifying_model(lake, &history_of(window, params), params)?;
    refold_block_under(lake, identity, window, (params, models), &model, unsaved)
}

/// The UTC day boundary a re-derivation over `window` starts from: the window's day start, where
/// repeat compute carried the frontiers.
#[must_use]
pub fn carried_from(window: Window) -> f64 {
    (window.start_ms / analyzer_analysis::DAY_MS).floor() * analyzer_analysis::DAY_MS
}

/// The completeness model's reach for a re-derivation over `window`: from the examined history
/// before the day start it classifies from through the skew past it, the stretch every store it
/// classifies looks back over for taint and the silences it crosses.
#[must_use]
pub fn history_of(window: Window, params: &Params) -> Window {
    Window {
        start_ms: carried_from(window) - params.examined_history_ms,
        end_ms: window.end_ms + params.delta_skew_ms,
    }
}

/// [`refold_block`] under `model`, the completeness model of [`history_of`] the window, which a
/// caller re-deriving several blocks over one window builds once.
///
/// # Errors
/// The log or the working state could not be read.
pub fn refold_block_under(
    lake: &analyzer_lake::Lake,
    identity: &str,
    window: Window,
    (params, models): (&Params, &Models),
    model: &analyzer_analysis::completeness::UncertaintyModel,
    unsaved: &super::spine::Written,
) -> Result<Refold, Error> {
    let (seeds, events) = read_block(lake, identity, window, params, unsaved)?;
    Ok(classify_history(
        seeds,
        events,
        window,
        (params, models),
        model,
    ))
}

/// What a re-derivation over `window` starts from: the block's frontiers carried at the
/// window's day start, and its records and the scope closers from there through the skew past
/// the window.
fn read_block(
    lake: &analyzer_lake::Lake,
    identity: &str,
    window: Window,
    params: &Params,
    unsaved: &super::spine::Written,
) -> Result<(Keys, Vec<Event>), Error> {
    let from = carried_from(window);
    let seeds = super::spine::carried(lake, identity, from, unsaved)?;
    let events = super::spine::block_history(
        lake,
        identity,
        Window {
            start_ms: from,
            end_ms: history_of(window, params).end_ms,
        },
        unsaved,
    )?;
    Ok((seeds, events))
}

/// A block re-derived over `window` twice from one read of its history, for a correction: as
/// it was, under `before` and without the records `left_out` names, and as it is, under `now`
/// with every record. Each model is the completeness model of a window holding
/// [`history_of`] this one, which a correction over several blocks builds once.
///
/// # Errors
/// The log or the working state could not be read.
pub fn refold_before_and_now(
    lake: &analyzer_lake::Lake,
    identity: &str,
    window: Window,
    (params, models): (&Params, &Models),
    (before, now): (
        &analyzer_analysis::completeness::UncertaintyModel,
        &analyzer_analysis::completeness::UncertaintyModel,
    ),
    left_out: &dyn Fn(&Event) -> bool,
    unsaved: &super::spine::Written,
) -> Result<(Refold, Refold), Error> {
    let (seeds, events) = read_block(lake, identity, window, params, unsaved)?;
    let kept: Vec<Event> = events.iter().filter(|e| !left_out(e)).cloned().collect();
    Ok((
        classify_history(seeds.clone(), kept, window, (params, models), before),
        classify_history(seeds, events, window, (params, models), now),
    ))
}

/// A block's history classified on one lane from `seeds`, the frontiers carried at the window's
/// day start: the window's stores as classified, and the frontiers at each boundary crossed and
/// at the history's end.
fn classify_history(
    seeds: Keys,
    events: Vec<Event>,
    window: Window,
    (params, models): (&Params, &Models),
    model: &analyzer_analysis::completeness::UncertaintyModel,
) -> Refold {
    let history = history_of(window, params);
    let meaning = Meaning::current(&Discriminator::of(params));
    let mut operator = RepeatCompute::new(
        1,
        Emission::Watermarked {
            delay_ms: params.delta_skew_ms,
        },
    )
    .restored(seeds, Some(carried_from(window)));
    operator.accept(events);
    let mut carried = Vec::new();
    let Ok(tick) = operator.advance_carrying(
        Some(history.end_ms),
        &RepeatContext {
            params,
            model,
            meaning: &meaning,
            models,
            coverage: 1.0,
        },
        |repeat, at| {
            carried.push((
                at,
                repeat.carried(0, at, params.examined_history_ms).collect(),
            ));
            Ok::<(), std::convert::Infallible>(())
        },
    );
    Refold {
        facts: tick
            .cells
            .facts
            .into_iter()
            .filter(|f| f.at_ms >= window.start_ms && f.at_ms < window.end_ms)
            .collect(),
        frontiers: operator.states().collect(),
        carried,
    }
}

/// One identity's placement re-derivation.
#[derive(Debug, Default)]
pub struct PlacementRefold {
    /// The window's placement cells.
    pub cells: analyzer_analysis::dup_cells::DupCells,
    /// The identity's seeds at the data edge.
    pub state: analyzer_analysis::dup_cells::IdentityState,
}

/// The day boundary a placement re-derivation over a window from `start_ms` seeds from: the
/// window's day start, or repeat compute's classified edge's where that is earlier, since repeat
/// compute carries the frontiers as it crosses a boundary and placement runs ahead of it.
#[must_use]
pub fn placement_from(start_ms: f64, classified_to_ms: f64) -> f64 {
    carried_from(Window {
        start_ms: start_ms.min(classified_to_ms),
        end_ms: start_ms,
    })
}

/// Re-derive `identity`'s placement over `window`: from the residencies open in the frontiers
/// carried at `from_ms` ([`placement_from`]), its records and the scope closers from there to
/// `data_to_ms` (the objects the manifest names and the `unsaved` ones) less the records
/// `leave_out` names, and the `silences` in that stretch, folded with `n_tokens` as the
/// identity's mass. The window's cells come back, and the seeds as they stand at `data_to_ms`.
///
/// # Errors
/// The log or the working state could not be read.
#[allow(clippy::too_many_arguments)]
pub fn refold_placement(
    lake: &analyzer_lake::Lake,
    identity: &str,
    (window, from_ms, data_to_ms): (Window, f64, f64),
    (params, n_tokens): (&Params, i64),
    silences: &[(String, f64)],
    leave_out: &dyn Fn(&Event) -> bool,
    unsaved: &super::spine::Written,
) -> Result<PlacementRefold, Error> {
    use analyzer_analysis::dup_cells::{DupCtx, Duplication, IdentityState};
    use analyzer_analysis::events::IntervalKey;
    use analyzer_analysis::intervals::{FoldSeed, IntervalSeed};
    use analyzer_analysis::module::DerivationModule;
    let history = Window {
        start_ms: from_ms,
        end_ms: data_to_ms,
    };
    let mut events = super::spine::block_history(lake, identity, history, unsaved)?;
    events.retain(|e| !leave_out(e));
    let closers = analyzer_analysis::events::ScopeClosers::of(&events).with_silences(
        silences
            .iter()
            .filter(|(_, at)| *at >= from_ms && *at < data_to_ms)
            .cloned(),
    );
    events.retain(|e| e.identity() == Some(identity));
    // A seed's readmit count enters no placement cell; the carried residencies are the open
    // ones, each where its key's frontier says it opened.
    let carried = IdentityState {
        whale: analyzer_analysis::dup_cells::WhaleAccrual::default(),
        n_tokens,
        seeds: super::spine::carried(lake, identity, from_ms, unsaved)?
            .into_values()
            .flatten()
            .filter(|f| f.open)
            .map(|f| {
                (
                    IntervalKey {
                        instance: f.key.instance,
                        dp_rank: f.key.dp_rank,
                        group_idx: f.key.group_idx,
                        identity: f.key.identity,
                        key_epoch: f.key.key_epoch,
                        tier: f.key.tier,
                        backend_id: f.key.backend_id,
                    },
                    IntervalSeed {
                        open: Some(FoldSeed {
                            open_since_ms: f.open_since_ms,
                            readmits: f.open_readmits,
                            workload_class: f.open_workload_class,
                        }),
                        last_close: None,
                    },
                )
            })
            .collect(),
    };
    let folded = Duplication.fold(
        &identity.to_string(),
        &events,
        carried,
        window,
        &DupCtx {
            closers: &closers,
            half_skew: params.delta_skew_ms / 2.0,
        },
    );
    Ok(PlacementRefold {
        cells: folded.cells,
        state: folded.state,
    })
}
