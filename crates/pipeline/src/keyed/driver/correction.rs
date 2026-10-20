// SPDX-License-Identifier: AGPL-3.0-only

//! The driver's corrections: records behind an operator's edge, a changed completeness model,
//! a changed tag index and a silence revoked, each re-deriving exactly the blocks it reaches.

use super::super::placement::PlacementTick;
use super::super::repeat::RepeatContext;
use super::super::source::Source;
use super::super::state::KeyedState;
use super::{Correct, Driver, Late, Retainted};
use crate::Error;
use analyzer_analysis::completeness::{
    uncertainty_model_from_parts, AgentBoundaries, UncertaintyModel,
};
use analyzer_analysis::dup_cells::IdentityState;
use analyzer_analysis::emitted::ClassifiedFact;
use analyzer_analysis::restated::{Family, Outcome};
use analyzer_analysis::{Params, Window};

impl<P> Driver<P>
where
    P: KeyedState<String, IdentityState> + Send,
{
    /// Take `behind`, records behind an operator's edge, through its correction path `correct`:
    /// those of days still open are corrected, the rest counted.
    pub(super) fn late(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        (behind, family): (Vec<analyzer_analysis::events::Event>, Family),
        correct: Correct<Self>,
    ) -> Result<Late, Error> {
        let records = behind.len() as u64;
        let (open, too_late): (Vec<_>, Vec<_>) = behind.into_iter().partition(|e| {
            self.open_from_day
                .is_none_or(|day| analyzer_analysis::emitted::day_index(e.at_ms) >= day)
        });
        let mut restated = analyzer_analysis::restated::Restated::default();
        for e in &too_late {
            restated.count(family, e.at_ms, Outcome::TooLate);
        }
        for e in &open {
            match e.identity() {
                Some(_) => restated.count(family, e.at_ms, Outcome::Corrected),
                None if e.kind.closes() => restated.count(family, e.at_ms, Outcome::Closer),
                None => {}
            }
        }
        self.restated.add(restated);
        let (corrected, closers) = correct(self, lake, params, open)?;
        Ok(Late {
            records,
            corrected,
            closers,
            too_late: too_late.len() as u64,
        })
    }

    /// Restate behind the edges: the reach of what this tick's batches changed in the model
    /// first, then each operator's late records, each identity from the earlier of its late
    /// records and that reach, then re-taint for the identities the late paths did not take.
    pub(super) fn restate(
        &mut self,
        (lake, source, params): (&analyzer_lake::Lake, &dyn Source, &Params),
        (behind, placement_behind): (
            Vec<analyzer_analysis::events::Event>,
            Vec<analyzer_analysis::events::Event>,
        ),
        (through_before, classified_before): (u64, Option<f64>),
    ) -> Result<(Late, Late), Error> {
        self.reach = self.reach_of(lake, source, params, through_before, classified_before)?;
        let repeat_late = self.late(lake, params, (behind, Family::Classes), Self::correct)?;
        let placement_late = self.late(
            lake,
            params,
            (placement_behind, Family::Placement),
            Self::correct_placement,
        )?;
        self.retaint(lake, params, through_before, classified_before)?;
        Ok((repeat_late, placement_late))
    }

    /// Fold placement to the data edge `data_to_ms` under the silences `model` draws since the
    /// last tick's.
    pub(super) fn place(
        &mut self,
        data_to_ms: f64,
        params: &Params,
        model: &analyzer_analysis::completeness::UncertaintyModel,
    ) -> PlacementTick {
        let from = self.placement.data_to_ms().unwrap_or(f64::NEG_INFINITY);
        let silences: Vec<Silence> = model.silences_in(from, data_to_ms).collect();
        self.placement.advance(data_to_ms, params, &silences)
    }

    /// Classify through the edge `watermark_ms` allows. At each UTC day boundary the frontiers
    /// go to the log, a lane at a time, for every re-derivation from that day to start from.
    pub(super) fn classify(
        &mut self,
        lake: &analyzer_lake::Lake,
        watermark_ms: Option<f64>,
        ctx: &RepeatContext<'_>,
    ) -> Result<super::super::repeat::Tick, Error> {
        let (layout, written) = (self.layout, &mut self.written);
        self.repeat
            .advance_carrying(watermark_ms, ctx, |repeat, at| {
                for lane in 0..repeat.lanes() {
                    let rows = repeat.carried(lane, at, ctx.params.examined_history_ms);
                    written.absorb(super::super::spine::write_carry(lake, layout, at, rows)?);
                }
                Ok(())
            })
    }

    pub(super) fn correct_placement(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        behind: Vec<analyzer_analysis::events::Event>,
    ) -> Result<(u64, u64), Error> {
        let (Some(data_to), Some(emitted_from), Some(emitted_to)) = (
            self.placement.data_to_ms(),
            self.placement.emitted_from_ms(),
            self.placement.emitted_to_ms(),
        ) else {
            return Ok((0, 0));
        };
        let mut by_identity: std::collections::BTreeMap<
            String,
            Vec<analyzer_analysis::events::Event>,
        > = std::collections::BTreeMap::new();
        let mut closers = 0;
        for event in behind {
            match event.identity() {
                Some(identity) => by_identity
                    .entry(identity.to_string())
                    .or_default()
                    .push(event),
                None if event.kind.closes() => closers += 1,
                None => {}
            }
        }
        let corrected = by_identity.len() as u64;
        let classified = self.repeat.classified_to_ms().unwrap_or(emitted_to);
        // A record moves the cells from half the skew before it: an interval's edges shrink by
        // that much. Never from before placement began emitting: nothing was observed there, so
        // a closer before it ends no residency.
        let reached: Vec<_> = by_identity
            .into_iter()
            .map(|(identity, records)| {
                let first = records
                    .iter()
                    .map(|e| e.at_ms)
                    .fold(f64::INFINITY, f64::min);
                let first = self
                    .reach
                    .placement
                    .get(&identity)
                    .map_or(first, |at| first.min(*at));
                let window = Window {
                    start_ms: (first - params.delta_skew_ms).max(emitted_from),
                    end_ms: emitted_to,
                };
                (identity, records, window)
            })
            .collect();
        let Some(earliest) = reached
            .iter()
            .map(|(_, _, w)| super::super::refold::placement_from(w.start_ms, classified))
            .reduce(f64::min)
        else {
            return Ok((corrected, closers));
        };
        let (before, now) = self.placement_silences(lake, params, (earliest, data_to))?;
        for (identity, records, window) in reached {
            let is_late =
                |e: &analyzer_analysis::events::Event| records.iter().any(|l| same_record(l, e));
            self.replace_placement(
                lake,
                params,
                (&identity, window, classified),
                (&before, &now),
                &is_late,
            )?;
            self.placement_corrected.insert(identity);
        }
        Ok((corrected, closers))
    }

    /// The silences placement folded under before this tick's batches and folds under now, from
    /// `from_ms` to the data edge `data_to_ms`.
    pub(super) fn placement_silences(
        &self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        (from_ms, data_to_ms): (f64, f64),
    ) -> Result<(Vec<Silence>, Vec<Silence>), Error> {
        let reach = Window {
            start_ms: from_ms - params.examined_history_ms,
            end_ms: data_to_ms,
        };
        let before =
            crate::arrivals::classifying_model_through(lake, &reach, params, self.model_through)?;
        let now = crate::arrivals::classifying_model(lake, &reach, params)?;
        Ok((
            before.silences_in(from_ms, data_to_ms).collect(),
            now.silences_in(from_ms, data_to_ms).collect(),
        ))
    }

    /// Re-derive `identity`'s placement over `window` as it was, under the silences `before`
    /// and without the records `left_out` names, and as it is, under `now` with every record;
    /// the held cells take the difference and the seeds take the re-derivation's.
    pub(super) fn replace_placement(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        (identity, window, classified): (&str, Window, f64),
        (before, now): (&[Silence], &[Silence]),
        left_out: &dyn Fn(&analyzer_analysis::events::Event) -> bool,
    ) -> Result<(), Error> {
        let Some(data_to) = self.placement.data_to_ms() else {
            return Ok(());
        };
        let from = super::super::refold::placement_from(window.start_ms, classified);
        let n_tokens = self
            .placement
            .seeds_of(identity)
            .map_or(0, |held| held.n_tokens);
        let refold = |silences, left_out| {
            super::super::refold::refold_placement(
                lake,
                identity,
                (window, from, data_to),
                (params, n_tokens),
                silences,
                left_out,
                &self.written,
            )
        };
        let old = refold(before, left_out)?;
        let new = refold(now, &|_| false)?;
        if window.start_ms < window.end_ms {
            self.placed.correct(&old.cells, &new.cells);
            self.note_restated(window);
        }
        self.placement.replace_identity(
            identity,
            new.state.clone(),
            (&old.state.whale.days, &new.state.whale.days),
        );
        Ok(())
    }

    /// One identity re-derived over `window` under the model before and the model now, with
    /// `left_out` records absent from the derivation before, and every state corrected by the
    /// difference: the classes, the ring, the tag cells, the carried frontiers and repeat
    /// compute's own. Returns the facts before and after.
    fn correct_identity(
        &mut self,
        lake: &analyzer_lake::Lake,
        (params, models): (&Params, &super::super::refold::Models),
        (before, now): (&UncertaintyModel, &UncertaintyModel),
        (identity, window): (&str, Window),
        left_out: &dyn Fn(&analyzer_analysis::events::Event) -> bool,
        recarry: &mut super::super::refold::Recarry,
    ) -> Result<(Vec<ClassifiedFact>, Vec<ClassifiedFact>), Error> {
        self.note_restated(window);
        let (old, new) = super::super::refold::refold_before_and_now(
            lake,
            identity,
            window,
            (params, models),
            (before, now),
            left_out,
            &self.written,
        )?;
        self.classes.correct(&old.facts, &new.facts);
        self.ring.restate(&old.facts, &new.facts);
        self.tag_cells.correct(
            &old.facts,
            &new.facts,
            self.tags.index(),
            params.delta_skew_ms,
        );
        recarry.add(&old, &new);
        self.repeat.replace_identity(identity, new.frontiers);
        Ok((old.facts, new.facts))
    }

    pub(super) fn correct(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        behind: Vec<analyzer_analysis::events::Event>,
    ) -> Result<(u64, u64), Error> {
        let Some(edge) = self.repeat.classified_to_ms() else {
            return Ok((0, 0));
        };
        let mut by_identity: std::collections::BTreeMap<
            String,
            Vec<analyzer_analysis::events::Event>,
        > = std::collections::BTreeMap::new();
        let mut closers = 0;
        for event in behind {
            match event.identity() {
                Some(identity) => by_identity
                    .entry(identity.to_string())
                    .or_default()
                    .push(event),
                None if event.kind.closes() => closers += 1,
                None => {}
            }
        }
        let corrected = by_identity.len() as u64;
        let models = self.roster.cells().models();
        // An identity re-taint also reaches is re-derived from the earlier of the two: re-taint
        // leaves it to this path, which must then restate the stretch the model changed too.
        let from_of = |identity: &str, records: &[analyzer_analysis::events::Event]| {
            let first = records
                .iter()
                .map(|e| e.at_ms)
                .fold(f64::INFINITY, f64::min);
            self.reach
                .classes
                .get(identity)
                .map_or(first, |at| first.min(*at))
        };
        let froms: Vec<f64> = by_identity
            .iter()
            .map(|(identity, records)| from_of(identity, records))
            .collect();
        let Some(earliest) = froms.iter().copied().reduce(f64::min) else {
            return Ok((0, closers));
        };
        let (before, now) = correction_models(lake, params, (earliest, edge), self.model_through)?;
        let mut recarry = super::super::refold::Recarry::default();
        for ((identity, records), from) in by_identity.into_iter().zip(froms) {
            let window = Window {
                start_ms: from,
                end_ms: edge,
            };
            let is_late =
                |e: &analyzer_analysis::events::Event| records.iter().any(|l| same_record(l, e));
            let (_, new) = self.correct_identity(
                lake,
                (params, &models),
                (&before, &now),
                (&identity, window),
                &is_late,
                &mut recarry,
            )?;
            for fact in new.iter().filter(|f| {
                records.iter().any(|l| {
                    l.at_ms.to_bits() == f.at_ms.to_bits()
                        && l.instance == f.holder.instance
                        && l.kind == analyzer_analysis::events::Kind::Store
                })
            }) {
                self.blocks.add(
                    analyzer_analysis::emitted::day_index(fact.at_ms),
                    &fact.block,
                    fact.n_tokens,
                    &fact.holder,
                );
            }
            self.corrected_identities.insert(identity);
        }
        self.written.absorb(recarry.write(lake, self.layout)?);
        Ok((corrected, closers))
    }
}

impl<P> Driver<P>
where
    P: KeyedState<String, IdentityState> + Send,
{
    /// The completeness model the tick folds under: the segments' arrivals from the examined
    /// history before the earliest of the records still to fold and either operator's edge,
    /// through `reach_ms`; an empty model before anything was read, which is then never read.
    pub(super) fn model(
        &self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        reach_ms: Option<f64>,
    ) -> Result<analyzer_analysis::completeness::UncertaintyModel, Error> {
        let first = [
            self.repeat.earliest_ms(),
            self.repeat.classified_to_ms(),
            self.placement.emitted_to_ms(),
        ]
        .into_iter()
        .flatten()
        .reduce(f64::min);
        Ok(match (first, reach_ms) {
            (Some(first), Some(end)) => {
                let window = Window {
                    start_ms: first - params.examined_history_ms,
                    end_ms: end,
                };
                crate::arrivals::classifying_model(lake, &window, params)?
            }
            _ => uncertainty_model_from_parts(
                Vec::new(),
                AgentBoundaries::default(),
                std::collections::BTreeSet::new(),
                &Window {
                    start_ms: 0.0,
                    end_ms: 1.0,
                },
                params,
            ),
        })
    }
}

impl<P> Driver<P>
where
    P: KeyedState<String, IdentityState> + Send,
{
    /// Move the stores a restated declaration reaches behind the classified edge from its old
    /// interval's attribution to its new one: the identity re-derived from the log over the
    /// span either interval covers up to the edge.
    pub(super) fn retag(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        restated: &[super::super::tags::Restated],
    ) -> Result<(), Error> {
        let models = self.roster.cells().models();
        let Some(edge) = self.repeat.classified_to_ms() else {
            return Ok(());
        };
        let skew = params.delta_skew_ms;
        for r in restated {
            let from = r
                .before
                .as_ref()
                .map_or(r.now.from_ms, |b| b.from_ms.min(r.now.from_ms))
                - skew;
            if from >= edge {
                continue;
            }
            let window = Window {
                start_ms: from,
                end_ms: edge,
            };
            let facts = super::super::refold::refold_block(
                lake,
                &r.now.identity,
                window,
                (params, &models),
                &self.written,
            )?
            .facts;
            let before = analyzer_analysis::tags::TagIndex::restored(
                r.before
                    .iter()
                    .map(|b| (r.declaration_id.clone(), b.clone())),
            );
            let now = analyzer_analysis::tags::TagIndex::restored([(
                r.declaration_id.clone(),
                r.now.clone(),
            )]);
            self.tag_cells.restate(&facts, (&before, &now), skew);
        }
        Ok(())
    }

    /// Take what `source` delivers, and return what the classified stores were derived under:
    /// the input position before this tick's batches, and the edge they reached. The position
    /// is also the old side every correction in this tick re-derives under.
    pub(super) fn take(&mut self, source: &dyn Source) -> Result<(u64, Option<f64>), Error> {
        let before = (
            self.intake.position().committed(),
            self.repeat.classified_to_ms(),
        );
        self.model_through = Some(before.0);
        self.corrected_identities.clear();
        self.placement_corrected.clear();
        self.intake.take(source)?;
        Ok(before)
    }

    /// The sampled audit of the stores this tick classified (`facts`, from `classified_before`
    /// to the edge now), and the audit edge moved by it.
    pub(super) fn audit_tick(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        facts: &[analyzer_analysis::emitted::ClassifiedFact],
        classified_before: Option<f64>,
    ) -> Result<super::super::audit::Audit, Error> {
        let Some(edge) = self.repeat.classified_to_ms() else {
            return Ok(super::super::audit::Audit::default());
        };
        let start = classified_before
            .or_else(|| facts.iter().map(|f| f.at_ms).reduce(f64::min))
            .unwrap_or(edge);
        if start >= edge {
            return Ok(super::super::audit::Audit::default());
        }
        let window = Window {
            start_ms: start,
            end_ms: edge,
        };
        let blocks = super::super::audit::sample(facts, edge, &self.corrected_identities);
        let audit = super::super::audit::audit(
            lake,
            (facts, window),
            &blocks,
            (params, &self.roster.cells().models()),
            &self.written,
        )?;
        self.audit = self.audit.after(window, &audit);
        Ok(audit)
    }

    /// Where this tick's batches changed the model behind the classified edge, per identity:
    /// the stores a changed drop window reaches, and every block whose holder a moved silence
    /// reaches, each from its earliest instant.
    pub(super) fn reach_of(
        &self,
        lake: &analyzer_lake::Lake,
        source: &dyn Source,
        params: &Params,
        through_before: u64,
        classified_before: Option<f64>,
    ) -> Result<Retainted, Error> {
        let Some(edge) = classified_before else {
            return Ok(Retainted::default());
        };
        #[allow(clippy::cast_precision_loss)] // day indices are small
        let open_from = self
            .open_from_day
            .map_or(edge - analyzer_analysis::CORRECTION_HORIZON_MS, |day| {
                day as f64 * analyzer_analysis::DAY_MS
            });
        if open_from >= edge {
            return Ok(Retainted::default());
        }
        let (spans, silenced) = changed_reach(lake, params, through_before, (open_from, edge))?;
        let mut reach = Retainted {
            classes: if spans.is_empty() {
                std::collections::BTreeMap::new()
            } else {
                reached_identities(lake, source, through_before, &spans)?
            },
            placement: std::collections::BTreeMap::new(),
        };
        // A silence moved: every block its instance held, from where it starts.
        for (instance, at) in silenced {
            for block in self.repeat.blocks_on(&instance) {
                for map in [&mut reach.classes, &mut reach.placement] {
                    map.entry(block.clone())
                        .and_modify(|from: &mut f64| *from = from.min(at))
                        .or_insert(at);
                }
            }
        }
        Ok(reach)
    }

    pub(super) fn retaint(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        through_before: u64,
        classified_before: Option<f64>,
    ) -> Result<(), Error> {
        let Some(edge) = classified_before else {
            return Ok(());
        };
        let Retainted {
            classes: mut froms,
            placement: silence_reached,
        } = std::mem::take(&mut self.reach);
        froms.retain(|identity, _| !self.corrected_identities.contains(identity));
        let models = self.roster.cells().models();
        let mut restated = analyzer_analysis::restated::Restated::default();
        let mut reached = Vec::new();
        let Some(earliest) = froms.values().copied().reduce(f64::min) else {
            return self.reseat_silenced(lake, params, silence_reached);
        };
        let (before, now) =
            correction_models(lake, params, (earliest, edge), Some(through_before))?;
        let mut recarry = super::super::refold::Recarry::default();
        for (identity, from) in froms {
            let window = Window {
                start_ms: from,
                end_ms: edge,
            };
            let (old, new) = self.correct_identity(
                lake,
                (params, &models),
                (&before, &now),
                (&identity, window),
                &|_| false,
                &mut recarry,
            )?;
            for fact in new.iter().filter(|f| {
                !old.iter().any(|o| {
                    o.at_ms.to_bits() == f.at_ms.to_bits()
                        && o.holder == f.holder
                        && o.class == f.class
                })
            }) {
                restated.count(Family::Classes, fact.at_ms, Outcome::Corrected);
            }
            reached.push(identity);
        }
        self.written.absorb(recarry.write(lake, self.layout)?);
        self.corrected_identities.extend(reached);
        self.restated.add(restated);
        self.reseat_silenced(lake, params, silence_reached)
    }

    /// Re-derive the placement of each block a moved silence reached, from where the silence
    /// was drawn, under the silences before this tick's batches and now: the residencies it
    /// closed ended there, or did not. A block placement's correction path re-derived this tick
    /// is left to it.
    pub(super) fn reseat_silenced(
        &mut self,
        lake: &analyzer_lake::Lake,
        params: &Params,
        reached: std::collections::BTreeMap<String, f64>,
    ) -> Result<(), Error> {
        let (Some(emitted_from), Some(emitted_to), Some(data_to)) = (
            self.placement.emitted_from_ms(),
            self.placement.emitted_to_ms(),
            self.placement.data_to_ms(),
        ) else {
            return Ok(());
        };
        let classified = self.repeat.classified_to_ms().unwrap_or(emitted_to);
        let windows: Vec<(String, Window)> = reached
            .into_iter()
            .filter(|(block, _)| !self.placement_corrected.contains(block))
            .map(|(block, at)| {
                let window = Window {
                    start_ms: (at - params.delta_skew_ms).max(emitted_from),
                    end_ms: emitted_to,
                };
                (block, window)
            })
            .collect();
        let Some(earliest) = windows
            .iter()
            .map(|(_, w)| super::super::refold::placement_from(w.start_ms, classified))
            .reduce(f64::min)
        else {
            return Ok(());
        };
        let (before, now) = self.placement_silences(lake, params, (earliest, data_to))?;
        for (block, window) in windows {
            self.replace_placement(
                lake,
                params,
                (&block, window, classified),
                (&before, &now),
                &|_| false,
            )?;
        }
        Ok(())
    }
}

/// The completeness models a correction re-derives its blocks under, over the stores from
/// `earliest` to `edge`: as of the input position `through`, and as of every batch. Built
/// once for every block the correction reaches; like the tick's own model they span more
/// than one block reads, which classification never looks past its own history to see.
fn correction_models(
    lake: &analyzer_lake::Lake,
    params: &Params,
    (earliest, edge): (f64, f64),
    through: Option<u64>,
) -> Result<
    (
        analyzer_analysis::completeness::UncertaintyModel,
        analyzer_analysis::completeness::UncertaintyModel,
    ),
    Error,
> {
    let history = super::super::refold::history_of(
        Window {
            start_ms: earliest,
            end_ms: edge,
        },
        params,
    );
    Ok((
        crate::arrivals::classifying_model_through(lake, &history, params, through)?,
        crate::arrivals::classifying_model_through(lake, &history, params, None)?,
    ))
}

/// Where an instance's heartbeats stopped: the instance and the instant.
type Silence = (String, f64);

/// One changed drop window's reach.
struct Reach {
    /// The window's source.
    source: String,
    /// Whether the reach is that source's own stores (through the examined history after the
    /// window) or every other source's (within the skew band).
    own: bool,
    /// The first instant reached.
    from_ms: f64,
    /// The instant the reach ends, exclusive.
    to_ms: f64,
}

/// Where the model before `through` and the model now differ within `(open_from, edge)`: the
/// stores their drop windows reach (a window reaches its own source's stores through the
/// examined history after it, and every other source's within the skew band), and the silences
/// one has and the other does not, each closing every residency on its instance.
fn changed_reach(
    lake: &analyzer_lake::Lake,
    params: &Params,
    through: u64,
    (open_from, edge): (f64, f64),
) -> Result<(Vec<Reach>, Vec<Silence>), Error> {
    let (skew, history) = (params.delta_skew_ms, params.examined_history_ms);
    let scope = Window {
        start_ms: open_from - history - skew,
        end_ms: edge + skew,
    };
    let windows = |model: &analyzer_analysis::completeness::UncertaintyModel| {
        model
            .drop_windows()
            .map(|s| {
                (
                    s.source.clone(),
                    s.start_at_ms.to_bits(),
                    s.end_at_ms.to_bits(),
                )
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    let starts = |model: &analyzer_analysis::completeness::UncertaintyModel| {
        model
            .silences_in(open_from, edge)
            .map(|(instance, at)| (instance, at.to_bits()))
            .collect::<std::collections::BTreeSet<_>>()
    };
    let before_model =
        crate::arrivals::classifying_model_through(lake, &scope, params, Some(through))?;
    let now_model = crate::arrivals::classifying_model(lake, &scope, params)?;
    let silenced = starts(&before_model)
        .symmetric_difference(&starts(&now_model))
        .map(|(instance, at)| (instance.clone(), f64::from_bits(*at)))
        .collect();
    let (before, now) = (windows(&before_model), windows(&now_model));
    let reach = before
        .symmetric_difference(&now)
        .flat_map(|(source, start, end)| {
            let (start, end) = (f64::from_bits(*start), f64::from_bits(*end));
            [(true, end + skew + history), (false, end + skew)].map(|(own, to)| Reach {
                source: source.clone(),
                own,
                from_ms: (start - skew).max(open_from),
                to_ms: to.min(edge),
            })
        })
        .filter(|r| r.from_ms < r.to_ms)
        .collect();
    Ok((reach, silenced))
}

/// Each identity with a store in `spans`, and its earliest such store, read from the batches
/// committed at or below `through`.
fn reached_identities(
    lake: &analyzer_lake::Lake,
    source: &dyn Source,
    through: u64,
    spans: &[Reach],
) -> Result<std::collections::BTreeMap<String, f64>, Error> {
    let lo = spans
        .iter()
        .map(|r| r.from_ms)
        .fold(f64::INFINITY, f64::min);
    let hi = spans
        .iter()
        .map(|r| r.to_ms)
        .fold(f64::NEG_INFINITY, f64::max);
    let deliveries: Vec<super::super::source::Delivery> = crate::inputs::log(lake)?
        .over(
            analyzer_analysis::emitted::day_index(lo),
            analyzer_analysis::emitted::day_index(hi),
        )?
        .into_iter()
        .filter(|b| b.input_position <= through)
        .filter(|b| {
            b.first_at_ms.is_some_and(|first| first < hi)
                && b.last_at_ms.is_some_and(|last| last >= lo)
        })
        .map(|b| super::super::source::Delivery {
            batch_id: b.batch_id,
            object_key: b.object_key,
            input_position: b.input_position,
            order: super::super::source::Order::InOrder,
            first_at_ms: b.first_at_ms,
            last_at_ms: b.last_at_ms,
        })
        .collect();
    let in_span = |instance: &str, at_ms: f64| {
        spans
            .iter()
            .any(|r| (r.source == instance) == r.own && at_ms >= r.from_ms && at_ms < r.to_ms)
    };
    let mut froms = std::collections::BTreeMap::new();
    let refs: Vec<&super::super::source::Delivery> = deliveries.iter().collect();
    for records in source.records_of(&refs)? {
        for e in records.iter().filter(|e| {
            e.kind == analyzer_analysis::events::Kind::Store && in_span(&e.instance, e.at_ms)
        }) {
            if let Some(identity) = e.identity() {
                let from = froms.entry(identity.to_string()).or_insert(e.at_ms);
                *from = f64::min(*from, e.at_ms);
            }
        }
    }
    Ok(froms)
}

/// Whether `a` and `b` are one record: the same kind, holder, block and instant.
fn same_record(a: &analyzer_analysis::events::Event, b: &analyzer_analysis::events::Event) -> bool {
    a.kind == b.kind
        && a.at_ms.to_bits() == b.at_ms.to_bits()
        && a.instance == b.instance
        && a.dp_rank == b.dp_rank
        && a.group_idx == b.group_idx
        && a.block_id == b.block_id
        && a.content_id == b.content_id
}
