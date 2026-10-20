// SPDX-License-Identifier: AGPL-3.0-only

//! Coverage's sources on their own clock (`docs/pipeline.md`): every
//! segment arrival the input log commits names the source it observed and the span its
//! heartbeats reach, read from the feed's own position whatever the watermark allows.
//!
//! A source is stated from the start of the UTC day of its first evidence, so a day is stated
//! whole: the stretch before its first heartbeat is covered where an `agent_start` attests
//! it and uncertain where nothing does. Coverage is about what the producer observed, not when
//! its segments arrived: a source whose arrivals reach behind the stated edge (a shipper
//! backlog, a late segment) has its stretch stated again from the heartbeats, and only a source
//! that produced none stays uncovered past the grace.

use crate::Error;
use std::collections::BTreeMap;

/// One source the coverage cells state.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StatedSource {
    /// Where its statement starts: the UTC day start of its first evidence.
    pub stated_from_ms: f64,
    /// Its latest evidence: a source with none inside the examined history leaves the feed.
    pub last_evidence_ms: f64,
}

/// One segment arrival's evidence: its source, and the earliest and latest instant it reports.
pub(crate) type Arrival = (String, f64, f64);

/// The sources coverage states and the input position the feed has read through.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CoverageFeed {
    sources: BTreeMap<String, StatedSource>,
    position: u64,
}

impl CoverageFeed {
    /// A feed resumed at `position` over `sources`.
    #[must_use]
    pub fn restored(sources: BTreeMap<String, StatedSource>, position: u64) -> Self {
        Self { sources, position }
    }

    /// The sources stated.
    #[must_use]
    pub fn sources(&self) -> &BTreeMap<String, StatedSource> {
        &self.sources
    }

    /// The input position read through.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Every arrival committed since the feed's position: its source, and the heartbeat span
    /// it reports, or its records' span where it reports no heartbeat.
    ///
    /// # Errors
    /// The input log could not be read.
    pub(crate) fn read(&mut self, lake: &analyzer_lake::Lake) -> Result<Vec<Arrival>, Error> {
        let mut arrivals = Vec::new();
        for batch in crate::inputs::log(lake)?.committed_since(self.position)? {
            self.position = self.position.max(batch.input_position);
            for a in &batch.segments {
                let Some(instance) = &a.instance else {
                    continue; // the run-level row names no source
                };
                arrivals.push((
                    instance.clone(),
                    a.hb_first_at_ms.unwrap_or(a.first_record_at_ms),
                    a.hb_last_at_ms.unwrap_or(a.last_record_at_ms),
                ));
            }
        }
        Ok(arrivals)
    }

    /// Take `arrivals` in: a source never stated starts at its first evidence's day. Returns
    /// the sources whose evidence reaches behind `stated_to`, a new one included, each with
    /// where its statement starts. An arrival reaches back to its source's previous evidence,
    /// where the heartbeat span it closes opens.
    pub(crate) fn take(
        &mut self,
        arrivals: &[Arrival],
        stated_to: Option<f64>,
    ) -> Vec<(String, f64)> {
        let mut evidence: BTreeMap<&str, (f64, f64)> = BTreeMap::new();
        for (source, first, last) in arrivals {
            let seen = evidence.entry(source).or_insert((*first, *last));
            seen.0 = seen.0.min(*first);
            seen.1 = seen.1.max(*last);
        }
        let mut behind = Vec::new();
        for (source, (first, last)) in evidence {
            let known = self
                .sources
                .entry(source.to_string())
                .or_insert_with(|| StatedSource {
                    stated_from_ms: day_start(first),
                    last_evidence_ms: last,
                });
            // A source's new heartbeat span opens at its previous evidence, so the arrival
            // reaches back there: a stretch stated as silence past the grace is stated again
            // from the span the arrival closes. A live source's edge never passes its previous
            // evidence, so only a source whose silence went final is restated this way.
            let reaches = known.stated_from_ms.max(first.min(known.last_evidence_ms));
            known.last_evidence_ms = known.last_evidence_ms.max(last);
            if stated_to.is_some_and(|to| reaches < to) {
                behind.push((source.to_string(), known.stated_from_ms));
            }
        }
        behind
    }

    /// Forget the sources with no evidence since `before`: past the correction horizon, no
    /// arrival of theirs restates what was stated.
    pub(crate) fn forget_before(&mut self, before: f64) {
        self.sources.retain(|_, s| s.last_evidence_ms >= before);
    }
}

/// The UTC day start of `at_ms`.
fn day_start(at_ms: f64) -> f64 {
    (at_ms / analyzer_analysis::DAY_MS).floor() * analyzer_analysis::DAY_MS
}

#[cfg(test)]
mod tests {
    use super::CoverageFeed;

    #[test]
    fn an_arrival_reaches_back_to_its_sources_previous_evidence() {
        let mut feed = CoverageFeed::default();
        assert_eq!(
            feed.take(&[("i0".to_string(), 1_000.0, 2_000.0)], None),
            Vec::<(String, f64)>::new()
        );
        // Silence past the grace: the edge passed i0's last evidence (2 s), and the next
        // arrival's span opens there, so i0 is stated again although its own records start
        // after the edge.
        let behind = feed.take(&[("i0".to_string(), 9_000.0, 9_500.0)], Some(5_000.0));
        assert_eq!(behind, vec![("i0".to_string(), 0.0)]);
        // A live source: the edge stays behind its last evidence, so its next arrival does
        // not reach behind it.
        assert!(feed
            .take(&[("i0".to_string(), 9_600.0, 9_700.0)], Some(9_400.0))
            .is_empty());
    }
}
