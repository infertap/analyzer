// SPDX-License-Identifier: AGPL-3.0-only

//! Class/band as a derivation module: the classify
//! stream's per-identity fold, behind the boundary.
//!
//! One key is an identity under its key epoch — the classify group. Its carried state is the
//! group's banked frontiers (last close, open since, readmits, the opener's class), the
//! seeds a slice resumes from; a whole fold carries none. Its events are the group's
//! interval-key streams exactly as repeat compute's lanes hand them — the partition of
//! [`crate::events::group_by_interval_key`]'s output the source produces in key order — so the
//! module never re-derives a grouping the engine already stated. Its cells are the eight
//! class/band shapes, and beside them the window's fact rows for the ledger: post-switch the
//! ledger is keyed replace too, so the facts are the fact's FINEST-grain cells, not a second
//! kind of output — one output concept, one write path. What the report
//! keeps (the stores as classified, the observed residencies, the waste and bucket counts)
//! rides in the same cells, day-scoped as the emitter has always scoped them.

use crate::classify::{
    classify_identity_group_seeded, Class, Classification, ClassifiedStore, Frontier,
    ObservedResidency,
};
use crate::completeness::{Coverage, UncertaintyModel};
use crate::emitted::{CellChanges, Cells, ClassifiedFact, EmittedDay, Meaning, StoreKey};
use crate::events::{Event, IdentityGroup, IntervalKey, ScopeClosers};
use crate::module::{Contribution, DerivationModule};
use crate::observed_coverage::ObservedCoverage;
use crate::{Params, Window};
use std::collections::{BTreeMap, BTreeSet};

/// The window's context the class/band fold reads beyond its own group's events.
pub struct ClassCtx<'a> {
    /// The window's scope-closers — every own-copy `clear` and `holder_reset` — held once and
    /// synthesized per key at fold time.
    pub closers: &'a ScopeClosers,
    /// The window's completeness model: the drop windows classification taint consults.
    pub model: &'a UncertaintyModel,
    /// The classification parameters — the skew allowance and the examined history.
    pub params: &'a Params,
    /// The day the window emits: a store scopes to it by its instant, a residency by its
    /// close — the rule the emitter has always applied per store.
    pub emit_day: Window,
    /// The DAY's coverage figure every fact row carries.
    pub coverage: f64,
    /// The semantics version and discriminator every fact row is stamped with.
    pub meaning: &'a Meaning,
    /// The run-scoped model relay, keyed by (instance, incarnation).
    pub models: &'a BTreeMap<(String, String), String>,
}

/// One window's class/band contribution: the eight shapes' cells, the window's fact rows
/// (the ledger's, day-scoped and deduped by the store's replace key — the last statement of
/// a key wins, as the rollup's own dedup has it), and what the report keeps.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ClassBandCells {
    /// The eight class/band shapes' keyed cells, folded from `facts`.
    pub cells: Cells,
    /// The window's fact rows for the ledger: the emitted day's stores, coverage-stamped, the
    /// last statement per replace key.
    pub facts: Vec<ClassifiedFact>,
    /// The stores as classified, those inside the emitted day by their instant — what the
    /// report keeps.
    pub stores: Vec<ClassifiedStore>,
    /// Fully-observed residencies closed inside the emitted day.
    pub residencies: Vec<ObservedResidency>,
    /// `n_tokens` summed over the day's `ETR` stores — the eviction-waste aggregate.
    pub waste_tokens: i64,
    /// `n_tokens` summed over the day's `post_clear` stores.
    pub post_clear_tokens: i64,
    /// Store count per class, keyed by [`Class::as_str`]'s spelling — the report's bucket counts.
    pub buckets: BTreeMap<&'static str, u64>,
}
impl crate::module::Mergeable for ClassBandCells {
    fn merge(&mut self, other: Self) {
        self.cells.merge(other.cells);
        self.facts.extend(other.facts);
        self.stores.extend(other.stores);
        self.residencies.extend(other.residencies);
        self.waste_tokens += other.waste_tokens;
        self.post_clear_tokens += other.post_clear_tokens;
        for (class, n) in other.buckets {
            *self.buckets.entry(class).or_insert(0) += n;
        }
    }
}

impl crate::module::Tracked for ClassBandCells {
    type Changes = CellChanges;
    fn len(&self) -> usize {
        self.cells.len()
    }
    fn drain_changes(&mut self) -> CellChanges {
        self.cells.drain_changes()
    }
    fn discard_changes(&mut self) {
        self.cells.discard_changes();
    }
}

impl crate::module::DayPartitioned for ClassBandCells {
    fn clear_day(&mut self, day: i64) {
        self.cells.clear_day(day);
    }
    fn retire_before(&mut self, before: i64) {
        self.cells.retire_before(before);
    }
    fn days(&self) -> BTreeSet<i64> {
        self.cells.days()
    }
}

/// Class/band as a module.
pub struct ClassBand;

impl DerivationModule for ClassBand {
    type Key = (String, Option<u32>);
    type State = Vec<Frontier>;
    type Event = (IntervalKey, Vec<Event>);
    type Cells = ClassBandCells;
    type Ctx<'a> = ClassCtx<'a>;
    const NAME: &'static str = "class_band";
    const INITIALIZATION: crate::module::Initialization =
        crate::module::Initialization::ExaminedHistory;
    const NEEDS: &'static [&'static str] = &[ObservedCoverage::NAME];

    /// The classify group's fold, as repeat compute runs it: the seeded classification (a whole
    /// fold's empty seeds are the unseeded one), the stores and residencies scoped to the
    /// emitted day per store, the facts built as the flush built them with the day's
    /// coverage, deduped by the store's replace key, and folded into the cells; the group's
    /// outgoing frontiers as the state the next window resumes from.
    fn fold(
        &self,
        key: &Self::Key,
        events: &[Self::Event],
        seeds: Vec<Frontier>,
        _window: Window,
        ctx: &ClassCtx<'_>,
    ) -> Contribution<ClassBandCells, Vec<Frontier>> {
        let group = IdentityGroup {
            identity: key.0.clone(),
            key_epoch: key.1,
            keys: events.to_vec(),
        };
        let out =
            classify_identity_group_seeded(&group, ctx.closers, ctx.model, ctx.params, &seeds);
        let mut c = ClassBandCells::default();
        for store in out.stores {
            if store.at_ms < ctx.emit_day.start_ms || store.at_ms >= ctx.emit_day.end_ms {
                continue;
            }
            match store.class {
                Class::Etr => c.waste_tokens += store.n_tokens,
                Class::PostClear => c.post_clear_tokens += store.n_tokens,
                _ => {}
            }
            *c.buckets.entry(store.class.as_str()).or_insert(0) += 1;
            c.stores.push(store);
        }
        for res in out.residencies {
            if res.close_ms < ctx.emit_day.start_ms || res.close_ms >= ctx.emit_day.end_ms {
                continue;
            }
            c.residencies.push(res);
        }
        // The facts as the flush built them: the day's coverage on every row; the per-day
        // coverage rows are the day's own emission, not a group's.
        let mini = Classification {
            stores: c.stores.clone(),
            eviction_waste_tokens: 0,
            post_clear_tokens: 0,
            bucket_counts: BTreeMap::new(),
            coverage: Coverage {
                fraction: ctx.coverage,
                per_source_covered_ms: Vec::new(),
                uncertain: Vec::new(),
                per_day: Vec::new(),
            },
            residencies: Vec::new(),
        };
        let day = EmittedDay::from_classification(&mini, ctx.meaning, ctx.models);
        let mut last: BTreeMap<StoreKey, ClassifiedFact> = BTreeMap::new();
        for f in day.facts {
            last.insert(StoreKey::of(&f), f);
        }
        c.facts = last.into_values().collect();
        c.cells.add_facts(&c.facts);
        Contribution {
            cells: c,
            state: out.frontiers,
            repair_days: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClassBand, ClassBandCells, ClassCtx};
    use crate::classify::{classify_identity_group, Classification};
    use crate::completeness::{uncertainty_model_from_events, Coverage};
    use crate::emitted::{Cells, EmittedDay, Meaning};
    use crate::events::{group_by_interval_key, Event, IdentityGroup, Kind, ScopeClosers};
    use crate::module::{DerivationModule, Mergeable, Tracked};
    use crate::{Discriminator, Params, Window, DAY_MS};
    use std::collections::BTreeMap;

    fn event(kind: Kind, instance: &str, block: &str, at_ms: f64, n_tokens: i64) -> Event {
        Event {
            kind,
            instance: instance.to_string(),
            dp_rank: Some(0),
            group_idx: Some(0),
            block_id: Some(block.to_string()),
            content_id: None,
            key_epoch: None,
            tier: None,
            backend_id: None,
            at_ms,
            locality: None,
            n_tokens,
            parent_id: None,
            parent_unknown: false,
            delivery_id: None,
            incarnation: "run-1".to_string(),
            seq: None,
            reused: false,
            workload_class: Some("chat".to_string()),
        }
    }

    /// **The module's fold is the emitter's feed and flush**: over one group it yields the
    /// facts the flush would have inserted (day-scoped, coverage-stamped, deduped by key) and
    /// the cells `add_facts` folds from them, and hands the group's frontiers back as the
    /// state — with the day's stores and residencies for the report scoped by the same rule.
    #[test]
    fn the_class_band_module_folds_as_the_emitter_fed_and_flushed() {
        let params = Params::default();
        let meaning = Meaning::current(&Discriminator::of(&params));
        let day = Window {
            start_ms: 0.0,
            end_ms: DAY_MS,
        };
        let events = vec![
            event(Kind::Store, "i1", "p", 60_000.0, 8),
            event(Kind::Evict, "i1", "p", 120_000.0, 0),
            event(Kind::Store, "i1", "p", 180_000.0, 8),
            // The next day's store: outside the emitted day, kept out of the facts.
            event(Kind::Store, "i1", "p", DAY_MS + 60_000.0, 8),
        ];
        let window = Window {
            start_ms: 0.0,
            end_ms: 2.0 * DAY_MS,
        };
        let model = uncertainty_model_from_events(&events, &[], &window, &params).expect("model");
        let closers = ScopeClosers::of(&events);
        let keys = group_by_interval_key(&events);
        let models = BTreeMap::new();
        let ctx = ClassCtx {
            closers: &closers,
            model: &model,
            params: &params,
            emit_day: day,
            coverage: 0.5,
            meaning: &meaning,
            models: &models,
        };
        let c = ClassBand.fold(&("p".to_string(), None), &keys, Vec::new(), window, &ctx);

        // The emitter's path, by hand: the unseeded classification, the day's stores, the
        // flush's facts, the fold's cells.
        let group = IdentityGroup {
            identity: "p".to_string(),
            key_epoch: None,
            keys: keys.clone(),
        };
        let out = classify_identity_group(&group, &closers, &model, &params);
        let stores: Vec<_> = out
            .stores
            .iter()
            .filter(|s| s.at_ms >= day.start_ms && s.at_ms < day.end_ms)
            .cloned()
            .collect();
        assert_eq!(stores.len(), 2, "two stores inside the day");
        let mini = Classification {
            stores: stores.clone(),
            eviction_waste_tokens: 0,
            post_clear_tokens: 0,
            bucket_counts: BTreeMap::new(),
            coverage: Coverage {
                fraction: 0.5,
                per_source_covered_ms: Vec::new(),
                uncertain: Vec::new(),
                per_day: Vec::new(),
            },
            residencies: Vec::new(),
        };
        let expected = EmittedDay::from_classification(&mini, &meaning, &models);
        let mut expected_cells = Cells::default();
        expected_cells.add_facts(&expected.facts);
        assert_eq!(c.cells.stores, stores);
        assert_eq!(c.cells.facts.len(), expected.facts.len());
        assert!(
            c.cells.facts.iter().all(|f| expected.facts.contains(f)),
            "the module's facts are the flush's"
        );
        assert_eq!(
            c.cells.cells, expected_cells,
            "the module's cells are the fold's"
        );
        assert_eq!(c.state, out.frontiers, "the frontiers are the state");
        assert_eq!(
            c.cells.residencies.len(),
            1,
            "one residency closed inside the day"
        );
        assert!(c.repair_days.is_empty());
        // The cells merge as the lanes' do, and drain by touch.
        let mut day_cells = ClassBandCells::default();
        day_cells.merge(c.cells.clone());
        day_cells.merge(c.cells);
        assert_eq!(
            day_cells.facts.len(),
            4,
            "two lanes' rows, both kept for the flush"
        );
        assert!(!day_cells.drain_changes().is_empty());
    }
}
