// SPDX-License-Identifier: AGPL-3.0-only

//! The roster on its own clock (`docs/pipeline.md`): who spoke, over what
//! span, declaring what, one run per producer incarnation.
//!
//! The roster reads every batch the input log commits, from its own position, and never waits
//! for the watermark: a heartbeat is news the moment it lands, and the roster's posture (active,
//! or inactive past the staleness grace) is judged against the real now when it is read. Each
//! commit's arrivals rows are sightings of their runs; the [`Roster`] module folds them, and a
//! run's cells merge as its span's hull, so a run folded tick by tick is the run folded whole.

use crate::rowbinary::{Incomplete, Reader, Writer};
use crate::Error;
use analyzer_analysis::module::{run_window, Mergeable, Row, Tracked};
use analyzer_analysis::roster::{
    InstanceCell, InstanceSighting, Roster, RosterCells, Run, Sighting,
};
use analyzer_analysis::Window;
use std::collections::BTreeMap;

/// One arrivals row as a sighting of its run.
pub(crate) fn roster_arrivals_row(
    a: &analyzer_lake::ledger::SegmentArrival,
    grouped: &mut BTreeMap<String, Vec<Sighting>>,
) {
    if a.incarnation.is_empty() {
        return;
    }
    let positive = |n: u64| Some(n).filter(|n| *n > 0);
    let instances = a.instance.as_ref().map_or_else(Vec::new, |instance| {
        vec![InstanceSighting {
            instance: instance.clone(),
            topic_model: a.topic_model.clone(),
            topic_pod: a.topic_pod.clone(),
        }]
    });
    grouped
        .entry(a.incarnation.clone())
        .or_default()
        .push(Sighting {
            liveness: true,
            first_at_ms: a.first_record_at_ms,
            last_at_ms: a.last_record_at_ms,
            stop: a.stop_at_ms.is_some(),
            heartbeat_secs: positive(a.heartbeat_secs),
            max_segment_secs: positive(a.max_segment_secs),
            declared_model: a.declared_model.clone(),
            declared_pod: a.declared_pod.clone(),
            instances,
        });
}

/// The roster's runs and the input position it has read through.
#[derive(Debug, Default)]
pub struct RosterFeed {
    cells: RosterCells,
    position: u64,
}

/// What a roster tick changed: the rows to publish and the runs to store.
#[derive(Debug, Default)]
pub struct RosterChanges {
    /// The changed runs' rows, whole.
    pub rows: Vec<Row>,
    /// The changed runs.
    pub runs: Vec<(String, Run)>,
}

impl RosterFeed {
    /// A feed resumed at `position` over `runs`.
    #[must_use]
    pub fn restored(runs: BTreeMap<String, Run>, position: u64) -> Self {
        Self {
            cells: RosterCells::from_runs(runs),
            position,
        }
    }

    /// The input position read through.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// The runs.
    #[must_use]
    pub fn cells(&self) -> &RosterCells {
        &self.cells
    }

    /// Fold every batch committed since the feed's position.
    ///
    /// # Errors
    /// The input log could not be read.
    pub fn tick(&mut self, lake: &analyzer_lake::Lake) -> Result<(), Error> {
        let mut grouped: BTreeMap<String, Vec<Sighting>> = BTreeMap::new();
        for batch in crate::inputs::log(lake)?.committed_since(self.position)? {
            self.position = self.position.max(batch.input_position);
            for arrival in &batch.segments {
                roster_arrivals_row(arrival, &mut grouped);
            }
        }
        if grouped.is_empty() {
            return Ok(());
        }
        // The roster is not window-scoped: the fold never reads the window.
        let whole = Window {
            start_ms: f64::MIN,
            end_ms: f64::MAX,
        };
        #[allow(clippy::zero_sized_map_values)] // a stateless fact's carried map is exactly that
        let out = run_window(&Roster, grouped, &BTreeMap::new(), whole, &());
        self.cells.merge(out.cells);
        Ok(())
    }

    /// What ticks changed since the last call.
    pub fn drain(&mut self) -> RosterChanges {
        let mut changes = RosterChanges::default();
        for (incarnation, rows) in self.cells.drain_changes() {
            changes
                .rows
                .extend(rows.iter().map(analyzer_analysis::roster::RosterRow::row));
            if let Some(run) = self.cells.runs.get(&incarnation) {
                changes.runs.push((incarnation, run.clone()));
            }
        }
        changes
    }
}

/// A run as bytes.
pub(crate) fn encode_run(run: &Run) -> Vec<u8> {
    let mut w = Writer::default();
    w.u8(u8::from(run.started));
    w.f64(run.first_seen_ms);
    w.f64(run.last_seen_ms);
    w.u8(u8::from(run.stop_observed));
    w.nullable(run.heartbeat_secs, Writer::u64);
    w.nullable(run.max_segment_secs, Writer::u64);
    w.nullable(run.declared_model.as_deref(), Writer::string);
    w.nullable(run.declared_pod.as_deref(), Writer::string);
    w.f64(run.declared_model_at_ms);
    w.f64(run.declared_pod_at_ms);
    w.varint(run.instances.len() as u64);
    for (instance, cell) in &run.instances {
        w.string(instance);
        w.nullable(cell.topic_model.as_deref(), Writer::string);
        w.nullable(cell.topic_pod.as_deref(), Writer::string);
        w.f64(cell.topic_at_ms);
    }
    w.bytes
}

/// A run back from [`encode_run`].
pub(crate) fn decode_run(bytes: &[u8]) -> Result<Run, Error> {
    let mut r = Reader::new(bytes);
    let owned = |r: &mut Reader<'_>| r.string().map(str::to_string);
    let decoded = (|| -> Result<Run, Incomplete> {
        let mut run = Run {
            started: r.u8()? != 0,
            first_seen_ms: r.f64()?,
            last_seen_ms: r.f64()?,
            stop_observed: r.u8()? != 0,
            heartbeat_secs: r.nullable(Reader::u64)?,
            max_segment_secs: r.nullable(Reader::u64)?,
            declared_model: r.nullable(owned)?,
            declared_pod: r.nullable(owned)?,
            declared_model_at_ms: r.f64()?,
            declared_pod_at_ms: r.f64()?,
            instances: BTreeMap::new(),
        };
        for _ in 0..r.varint()? {
            let instance = owned(&mut r)?;
            let cell = InstanceCell {
                topic_model: r.nullable(owned)?,
                topic_pod: r.nullable(owned)?,
                topic_at_ms: r.f64()?,
            };
            run.instances.insert(instance, cell);
        }
        Ok(run)
    })()
    .map_err(|_| Error::Row("a roster run record is truncated".into()))?;
    if !r.at_end() {
        return Err(Error::Row("a roster run record has trailing bytes".into()));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{decode_run, encode_run};
    use analyzer_analysis::roster::{InstanceCell, Run};

    #[test]
    fn a_run_reads_back_as_written() {
        let mut run = Run {
            started: true,
            first_seen_ms: 1.5,
            last_seen_ms: 9.0,
            stop_observed: true,
            heartbeat_secs: Some(30),
            max_segment_secs: None,
            declared_model: Some("llama".into()),
            declared_pod: None,
            declared_model_at_ms: 2.0,
            declared_pod_at_ms: 0.0,
            ..Run::default()
        };
        run.instances.insert(
            "i1".into(),
            InstanceCell {
                topic_model: None,
                topic_pod: Some("pod-7".into()),
                topic_at_ms: 8.0,
            },
        );
        assert_eq!(decode_run(&encode_run(&run)).expect("decodes"), run);
    }
}
