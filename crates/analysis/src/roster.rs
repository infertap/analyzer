// SPDX-License-Identifier: AGPL-3.0-only

//! The roster as a derivation module: run-keyed, stateless, mergeable, and without a day.
//!
//! **The fleet is who spoke.** One run (a tap incarnation) holds one declaration set for its
//! whole life, so the fold's unit is the run: its span (the earliest and latest liveness
//! sighting), whether a stop was seen, its heartbeat and segment ceilings, the model and pod
//! it declared, the instances it named, and per instance the topic the tap re-states in band
//! — the last sighting is the current one. The row the store keeps is one per (instance,
//! incarnation), materialized from the run ([`RosterCells::rows`]) under the model-dimension
//! rule: recognized topic where present, else the declared value, else null.
//!
//! **One event, two arms.** The pipeline sights a run from the arrivals rows the ingest keeps,
//! or, when the span holds none, from the source's lifecycle declarations; both arms produce
//! [`Sighting`]s and the fold sees one shape. Which arm to take is a store question and stays
//! in the pipeline.
//!
//! **Deliberately not window-scoped**, like every lifecycle-identity fetch: a run's first
//! sighting is whenever it was. Roster cells implement composition and change tracking,
//! without inventing a day axis or providing no-op retirement methods.

use crate::module::{Column, Contribution, DayOf, DerivationModule, Kind, Row, Table, Value};
use crate::Window;
use std::collections::{BTreeMap, BTreeSet};

/// One sighting of a run: what one arrivals row or one lifecycle record says about it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Sighting {
    /// Whether this sighting is a liveness observation (a start, a heartbeat, a stop; every
    /// arrivals row) — the only kind that opens and extends the run's span. A declaration
    /// alone (a segment header) names a run without rostering it.
    pub liveness: bool,
    /// The sighting's producer-clock span: one instant for a record, the first and last
    /// record stamps for an arrivals row.
    pub first_at_ms: f64,
    /// The span's close: equal to `first_at_ms` for a record, the last record stamp for an
    /// arrivals row.
    pub last_at_ms: f64,
    /// A stop was observed.
    pub stop: bool,
    /// The declared heartbeat ceiling (spec 5.8), where a liveness sighting carries it.
    pub heartbeat_secs: Option<u64>,
    /// The declared segment ceiling (spec 5.8), where a liveness sighting carries it.
    pub max_segment_secs: Option<u64>,
    /// The run's declared model and pod (`--provenance`), where this sighting carries them.
    pub declared_model: Option<String>,
    /// The `pod_identity` half of the declaration, beside `declared_model`.
    pub declared_pod: Option<String>,
    /// The instances this sighting names, each with the topic recognized for it (the
    /// recognition is the decoder's; absence is absence, never a guess).
    pub instances: Vec<InstanceSighting>,
}

/// One instance a sighting names, with its recognized topic where the sighting carried one.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InstanceSighting {
    /// The instance label: the record's own, or an endpoint's `source` on a heartbeat.
    pub instance: String,
    /// The model segment of the endpoint's recognized topic; `None` where none was recognized.
    pub topic_model: Option<String>,
    /// The pod segment of the endpoint's recognized topic — the engine's own name for itself.
    pub topic_pod: Option<String>,
}

/// One run's fold: its span, its ceilings, its declarations, and its instances.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Run {
    /// A liveness sighting was seen; a run of declarations only does not roster.
    pub started: bool,
    /// The earliest liveness sighting, producer-clock milliseconds; meaningless until `started`.
    pub first_seen_ms: f64,
    /// The latest liveness sighting, producer-clock milliseconds; meaningless until `started`.
    pub last_seen_ms: f64,
    /// An `agent_stop` (or a stop-stamped arrivals row) was among the sightings.
    pub stop_observed: bool,
    /// The largest declared heartbeat ceiling any liveness sighting carried.
    pub heartbeat_secs: Option<u64>,
    /// The largest declared segment ceiling any liveness sighting carried.
    pub max_segment_secs: Option<u64>,
    /// The earliest declaration of each wins, so a run's declared values are its first ones.
    pub declared_model: Option<String>,
    /// The run's first-declared `pod_identity`, under the same earliest-wins rule.
    pub declared_pod: Option<String>,
    /// When `declared_model` was declared: an earlier declaration replaces it.
    pub declared_model_at_ms: f64,
    /// When `declared_pod` was declared: an earlier declaration replaces it.
    pub declared_pod_at_ms: f64,
    /// The instances the run named, with the latest recognized topic for each.
    pub instances: BTreeMap<String, InstanceCell>,
}

/// One (instance, run) pair's own observation: the recognized topic, re-stated in band on
/// every heartbeat, so the latest sighting is the current one.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InstanceCell {
    /// The model from the latest recognized topic; outranks the run's declared model.
    pub topic_model: Option<String>,
    /// The pod from the latest recognized topic; outranks the run's declared pod.
    pub topic_pod: Option<String>,
    /// When the topic was sighted: a later sighting replaces it.
    pub topic_at_ms: f64,
}

impl Run {
    fn absorb(&mut self, s: &Sighting) {
        if s.liveness {
            if !self.started {
                self.started = true;
                self.first_seen_ms = s.first_at_ms;
                self.last_seen_ms = s.last_at_ms;
            }
            self.first_seen_ms = self.first_seen_ms.min(s.first_at_ms);
            self.last_seen_ms = self.last_seen_ms.max(s.last_at_ms);
            self.stop_observed |= s.stop;
            self.heartbeat_secs = self.heartbeat_secs.max(s.heartbeat_secs);
            self.max_segment_secs = self.max_segment_secs.max(s.max_segment_secs);
        }
        if s.declared_model.is_some() || s.declared_pod.is_some() {
            self.declare(
                s.declared_model.as_ref(),
                s.declared_pod.as_ref(),
                s.first_at_ms,
            );
        }
        for i in &s.instances {
            if i.instance.is_empty() {
                continue;
            }
            let cell = self.instances.entry(i.instance.clone()).or_default();
            if i.topic_model.is_some() || i.topic_pod.is_some() {
                cell.restate(i.topic_model.as_ref(), i.topic_pod.as_ref(), s.last_at_ms);
            }
        }
    }

    /// The earliest declaration of each value wins; a later one never relabels the run.
    fn declare(&mut self, model: Option<&String>, pod: Option<&String>, at_ms: f64) {
        if let Some(m) = model {
            if self.declared_model.is_none() || at_ms < self.declared_model_at_ms {
                self.declared_model = Some(m.clone());
                self.declared_model_at_ms = at_ms;
            }
        }
        if let Some(p) = pod {
            if self.declared_pod.is_none() || at_ms < self.declared_pod_at_ms {
                self.declared_pod = Some(p.clone());
                self.declared_pod_at_ms = at_ms;
            }
        }
    }

    /// Two folds of one run, from different sightings: the span's hull, the ceilings' max,
    /// the earliest declaration, the latest topic per instance.
    fn merge(&mut self, other: Run) {
        if other.started {
            if !self.started {
                self.started = true;
                self.first_seen_ms = other.first_seen_ms;
                self.last_seen_ms = other.last_seen_ms;
            }
            self.first_seen_ms = self.first_seen_ms.min(other.first_seen_ms);
            self.last_seen_ms = self.last_seen_ms.max(other.last_seen_ms);
            self.stop_observed |= other.stop_observed;
            self.heartbeat_secs = self.heartbeat_secs.max(other.heartbeat_secs);
            self.max_segment_secs = self.max_segment_secs.max(other.max_segment_secs);
        }
        self.declare(
            other.declared_model.as_ref(),
            None,
            other.declared_model_at_ms,
        );
        self.declare(None, other.declared_pod.as_ref(), other.declared_pod_at_ms);
        for (instance, cell) in other.instances {
            let mine = self.instances.entry(instance).or_default();
            if cell.topic_model.is_some() || cell.topic_pod.is_some() {
                mine.restate(
                    cell.topic_model.as_ref(),
                    cell.topic_pod.as_ref(),
                    cell.topic_at_ms,
                );
            }
        }
    }
}

impl InstanceCell {
    /// The latest sighting is the current one; an equal stamp re-states.
    fn restate(&mut self, model: Option<&String>, pod: Option<&String>, at_ms: f64) {
        let first = self.topic_model.is_none() && self.topic_pod.is_none();
        if first || at_ms >= self.topic_at_ms {
            self.topic_model = model.cloned();
            self.topic_pod = pod.cloned();
            self.topic_at_ms = at_ms;
        }
    }
}

/// One row the store keeps: one (instance, incarnation), the run's span and ceilings, the
/// model and pod under the model-dimension rule.
#[derive(Debug, Clone, PartialEq)]
pub struct RosterRow {
    /// The instance label; `""` for a run that spoke but named no instance.
    pub instance: String,
    /// The run — the tap incarnation — this row materializes.
    pub incarnation: String,
    /// The run's earliest liveness sighting, producer-clock milliseconds.
    pub first_seen_ms: f64,
    /// The run's latest liveness sighting, producer-clock milliseconds.
    pub last_seen_ms: f64,
    /// Whether the run's stop was seen.
    pub stop_observed: bool,
    /// The run's declared heartbeat ceiling (spec 5.8), the max over its sightings.
    pub heartbeat_secs: Option<u64>,
    /// The run's declared segment ceiling (spec 5.8), the max over its sightings.
    pub max_segment_secs: Option<u64>,
    /// The model dimension: the instance's recognized topic model, else the run's declared
    /// model, else `None`.
    pub model: Option<String>,
    /// The pod, under the same rule: recognized topic pod, else declared pod, else `None`.
    pub pod_identity: Option<String>,
}

/// The roster's served table: one row per (instance, run), keyed changes, undated. The
/// classification's per-run model relay reads it back from the lake.
pub const FLEET_INVENTORY: Table = Table {
    name: "fleet_inventory",
    key: &[
        Column::new("instance_id", Kind::Str),
        Column::new("incarnation", Kind::Str),
    ],
    measures: &[
        Column::new("first_seen_ms", Kind::F64),
        Column::new("last_seen_ms", Kind::F64),
        Column::new("stop_observed", Kind::U32),
        Column::new("model", Kind::NullableStr),
        Column::new("pod_identity", Kind::NullableStr),
        Column::new("heartbeat_secs", Kind::NullableU32),
        Column::new("max_segment_secs", Kind::NullableU32),
    ],
    day: DayOf::Undated,
};

impl RosterRow {
    /// This row as a row of [`FLEET_INVENTORY`].
    #[must_use]
    pub fn row(&self) -> Row {
        let secs =
            |v: Option<u64>| Value::nullable_u32(v.map(|s| u32::try_from(s).unwrap_or(u32::MAX)));
        Row {
            table: FLEET_INVENTORY.name,
            key: vec![
                Value::Str(self.instance.clone()),
                Value::Str(self.incarnation.clone()),
            ],
            measures: vec![
                Value::F64(self.first_seen_ms),
                Value::F64(self.last_seen_ms),
                Value::U32(u32::from(self.stop_observed)),
                Value::nullable(self.model.as_deref()),
                Value::nullable(self.pod_identity.as_deref()),
                secs(self.heartbeat_secs),
                secs(self.max_segment_secs),
            ],
            gone: false,
        }
    }
}

/// The roster's cells: one [`Run`] per incarnation, touched keys tracked for the write.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RosterCells {
    /// One fold per incarnation, keyed by it — the run is the fence's unit.
    pub runs: BTreeMap<String, Run>,
    touched: BTreeSet<String>,
}

impl RosterCells {
    /// Cells holding `runs`, as a store kept them, with nothing touched.
    #[must_use]
    pub fn from_runs(runs: BTreeMap<String, Run>) -> Self {
        Self {
            runs,
            touched: BTreeSet::new(),
        }
    }

    /// Each `(instance, incarnation)`'s model, as its row states it: the recognized topic model,
    /// else the run's declared one. A pair with neither is absent, never defaulted.
    #[must_use]
    pub fn models(&self) -> BTreeMap<(String, String), String> {
        self.rows()
            .into_iter()
            .filter_map(|r| Some(((r.instance, r.incarnation), r.model?)))
            .collect()
    }

    /// The rows to keep, in incarnation then instance order. A run that spoke but named no
    /// instance rosters once under the empty label — the fleet is who spoke.
    #[must_use]
    pub fn rows(&self) -> Vec<RosterRow> {
        let mut out = Vec::new();
        for (incarnation, run) in &self.runs {
            if !run.started {
                continue;
            }
            let fallback = BTreeMap::from([(String::new(), InstanceCell::default())]);
            let instances = if run.instances.is_empty() {
                &fallback
            } else {
                &run.instances
            };
            for (instance, cell) in instances {
                out.push(RosterRow {
                    instance: instance.clone(),
                    incarnation: incarnation.clone(),
                    first_seen_ms: run.first_seen_ms,
                    last_seen_ms: run.last_seen_ms,
                    stop_observed: run.stop_observed,
                    heartbeat_secs: run.heartbeat_secs,
                    max_segment_secs: run.max_segment_secs,
                    model: cell
                        .topic_model
                        .clone()
                        .or_else(|| run.declared_model.clone()),
                    pod_identity: cell.topic_pod.clone().or_else(|| run.declared_pod.clone()),
                });
            }
        }
        out
    }
}
impl crate::module::Mergeable for RosterCells {
    fn merge(&mut self, other: Self) {
        for (incarnation, run) in other.runs {
            self.touched.insert(incarnation.clone());
            self.runs.entry(incarnation).or_default().merge(run);
        }
    }
}

impl crate::module::Tracked for RosterCells {
    /// Each touched run with its rows (empty for a run that no longer rosters).
    type Changes = Vec<(String, Vec<RosterRow>)>;

    fn len(&self) -> usize {
        self.runs.len()
    }

    fn drain_changes(&mut self) -> Self::Changes {
        let touched = std::mem::take(&mut self.touched);
        let rows = self.rows();
        touched
            .into_iter()
            .map(|incarnation| {
                let own: Vec<RosterRow> = rows
                    .iter()
                    .filter(|r| r.incarnation == incarnation)
                    .cloned()
                    .collect();
                (incarnation, own)
            })
            .collect()
    }

    fn discard_changes(&mut self) {
        self.touched.clear();
    }
}

/// The roster fact: who spoke, over what span, declaring what.
pub struct Roster;

impl DerivationModule for Roster {
    /// The incarnation — the run, the fence's unit; the (instance, incarnation) row is a
    /// materialization of it.
    type Key = String;
    type State = ();
    type Event = Sighting;
    type Cells = RosterCells;
    type Ctx<'a> = ();
    const NAME: &'static str = "roster";
    const INITIALIZATION: crate::module::Initialization =
        crate::module::Initialization::OutputWindow;

    /// One run's sightings, in the order given, folded into its [`Run`]. The window is not
    /// consulted: the roster is not window-scoped.
    fn fold(
        &self,
        incarnation: &String,
        sightings: &[Sighting],
        (): (),
        _window: Window,
        (): &(),
    ) -> Contribution<RosterCells, ()> {
        let mut run = Run::default();
        for s in sightings {
            run.absorb(s);
        }
        let mut cells = RosterCells::default();
        cells.touched.insert(incarnation.clone());
        cells.runs.insert(incarnation.clone(), run);
        Contribution {
            cells,
            state: (),
            repair_days: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InstanceSighting, Roster, RosterCells, Sighting};
    use crate::module::{run_window, DerivationModule, Mergeable, Tracked};
    use crate::Window;
    use std::collections::BTreeMap;

    fn liveness(at_ms: f64, instances: &[(&str, Option<&str>)]) -> Sighting {
        Sighting {
            liveness: true,
            first_at_ms: at_ms,
            last_at_ms: at_ms,
            heartbeat_secs: Some(30),
            instances: instances
                .iter()
                .map(|(i, t)| InstanceSighting {
                    instance: (*i).to_string(),
                    topic_model: t.map(|_| "llama".to_string()),
                    topic_pod: t.map(std::string::ToString::to_string),
                })
                .collect(),
            ..Sighting::default()
        }
    }

    fn declaration(at_ms: f64, model: &str) -> Sighting {
        Sighting {
            first_at_ms: at_ms,
            last_at_ms: at_ms,
            declared_model: Some(model.to_string()),
            declared_pod: Some("declared-pod".to_string()),
            ..Sighting::default()
        }
    }

    #[test]
    #[allow(clippy::zero_sized_map_values)] // a stateless fact's carried map is exactly that
    fn a_run_fans_out_over_its_instances_and_the_topic_outranks_the_declaration() {
        let w = Window {
            start_ms: 0.0,
            end_ms: 1.0,
        };
        let mut grouped: BTreeMap<String, Vec<Sighting>> = BTreeMap::new();
        // The real producer shape: a start naming nobody, heartbeats naming endpoints, a
        // header declaring the model, a stop; a later heartbeat re-states one topic.
        grouped.insert(
            "run-1".into(),
            vec![
                liveness(10.0, &[]),
                declaration(11.0, "declared-model"),
                liveness(20.0, &[("gpu-a", Some("pod-a")), ("gpu-b", None)]),
                declaration(21.0, "a-later-declaration"),
                liveness(30.0, &[("gpu-a", Some("pod-a2"))]),
                Sighting {
                    stop: true,
                    ..liveness(40.0, &[])
                },
            ],
        );
        // A run of declarations only never rosters.
        grouped.insert("run-2".into(), vec![declaration(5.0, "m")]);
        let out = run_window(&Roster, grouped, &BTreeMap::new(), w, &());
        assert_eq!(out.keys, 2);
        assert!(out.states.is_empty());
        let rows = out.cells.rows();
        assert_eq!(rows.len(), 2, "{rows:?}");
        let a = &rows[0];
        assert_eq!(
            (a.instance.as_str(), a.incarnation.as_str()),
            ("gpu-a", "run-1")
        );
        assert_eq!(
            (a.first_seen_ms, a.last_seen_ms, a.stop_observed),
            (10.0, 40.0, true)
        );
        assert_eq!(a.heartbeat_secs, Some(30));
        assert_eq!(
            a.model.as_deref(),
            Some("llama"),
            "the recognized topic outranks"
        );
        assert_eq!(
            a.pod_identity.as_deref(),
            Some("pod-a2"),
            "the latest sighting is current"
        );
        let b = &rows[1];
        assert_eq!(b.instance, "gpu-b");
        assert_eq!(
            b.model.as_deref(),
            Some("declared-model"),
            "the earliest declaration"
        );
        assert_eq!(b.pod_identity.as_deref(), Some("declared-pod"));
    }

    #[test]
    fn a_run_that_named_nobody_rosters_under_the_empty_label_and_lanes_merge_to_one_row() {
        let w = Window {
            start_ms: 0.0,
            end_ms: 1.0,
        };
        let whole = Roster.fold(
            &"r".to_string(),
            &[liveness(1.0, &[]), liveness(9.0, &[("x", None)])],
            (),
            w,
            &(),
        );
        let mut split = RosterCells::default();
        split.merge(
            Roster
                .fold(
                    &"r".to_string(),
                    &[liveness(9.0, &[("x", None)])],
                    (),
                    w,
                    &(),
                )
                .cells,
        );
        split.merge(
            Roster
                .fold(&"r".to_string(), &[liveness(1.0, &[])], (), w, &())
                .cells,
        );
        assert_eq!(
            split.rows(),
            whole.cells.rows(),
            "the lanes' order does not matter"
        );
        let changes = split.drain_changes();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].1.len(), 1);
        assert!(split.drain_changes().is_empty());

        let nobody = Roster.fold(&"r".to_string(), &[liveness(1.0, &[])], (), w, &());
        let rows = nobody.cells.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].instance, "", "the fleet is who spoke");
    }
}
