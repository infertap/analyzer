// SPDX-License-Identifier: AGPL-3.0-only

//! The arrivals: what the input log's lifecycle records and segment arrivals answer without a
//! fetch.
//!
//! The lifecycle bank for a window, the window's segment keys and every open the
//! store already knows, and the uncertainty model assembled from banked facts — the arrivals-first
//! assembly repeat compute, coverage and the drills read.

use crate::Error;
use analyzer_analysis::{Params, Window, DAY_MS};

/// The lifecycle bank for a window: the banked rows as the tap's JSON and
/// the segment keys the bank holds, sentinels included. The fleet through `quote_literal`;
/// the bounds two floats.
///
/// # Errors
/// Engine failures.
pub fn lifecycle_bank_read(
    lake: &analyzer_lake::Lake,
    lo_ms: f64,
    hi_ms: f64,
) -> Result<(Vec<String>, std::collections::HashSet<String>), Error> {
    let mut rows = Vec::new();
    let mut banked = std::collections::HashSet::new();
    for batch in lifecycle_batches(lake, lo_ms, hi_ms)? {
        if !batch.object_key.is_empty() {
            banked.insert(batch.object_key);
        }
        for record in batch.lifecycle {
            banked.insert(record.segment_key);
            if record.kind != "_segment" && record.at_ms >= lo_ms && record.at_ms < hi_ms {
                rows.push(record.row);
            }
        }
    }
    Ok((rows, banked))
}

/// How far a segment's records reach past its batch's cache-fact span: an hour against a
/// declared `max_segment_secs` of minutes, conservative by an order of magnitude.
const SEGMENT_SPAN_MARGIN_MS: f64 = 3_600_000.0;

/// The committed batches recorded under the days `[lo_ms, hi_ms)` reaches, widened by
/// [`SEGMENT_SPAN_MARGIN_MS`]: every batch whose lifecycle records or segments can fall in
/// the window, since a batch is recorded under each day they fall on.
fn lifecycle_batches(
    lake: &analyzer_lake::Lake,
    lo_ms: f64,
    hi_ms: f64,
) -> Result<Vec<analyzer_lake::ledger::BatchRecord>, Error> {
    use analyzer_lake::ledger::day_of;
    Ok(crate::inputs::log(lake)?.over(
        day_of(lo_ms - SEGMENT_SPAN_MARGIN_MS),
        day_of(hi_ms + SEGMENT_SPAN_MARGIN_MS),
    )?)
}

/// The lifecycle FETCH scope (the eighth unscoped-read family member):
/// segment keys whose committed batches can hold lifecycle rows in `[lo_ms, hi_ms)`,
/// from the ledger's OWN span columns. A batch's span is stamped over its CACHE facts
/// only, so it understates the segment's full extent by at most the segment's sealed
/// duration — the margin below is an hour against a declared `max_segment_secs` of
/// minutes, conservative by an order of magnitude — and a NULL span (a lifecycle-only
/// segment) is INCLUDED unconditionally, because pruning the very segments whose whole
/// content is lifecycle testimony would silence the ledger it feeds. `Ok(None)` when
/// the fleet has no keyed committed batches at all (non-bucket carriers): scoping is
/// blind there and the fetch says so and walks.
///
/// # Errors
/// Engine failures.
pub fn lifecycle_window_keys(
    lake: &analyzer_lake::Lake,
    lo_ms: f64,
    hi_ms: f64,
) -> Result<Option<std::collections::HashSet<String>>, Error> {
    if crate::inputs::log(lake)?.head()?.key_watermark.is_none() {
        return Ok(None);
    }
    let (lo, hi) = (
        (lo_ms - SEGMENT_SPAN_MARGIN_MS).floor(),
        (hi_ms + SEGMENT_SPAN_MARGIN_MS).ceil(),
    );
    Ok(Some(
        lifecycle_batches(lake, lo_ms, hi_ms)?
            .into_iter()
            .filter(|b| !b.object_key.is_empty())
            .filter(|b| b.first_at_ms.is_none() || b.meets(lo, hi))
            .map(|b| b.object_key)
            .collect(),
    ))
}

/// Every segment open the input log already knows, without fetching a segment: a walk of
/// the bucket fetched and decoded every segment to harvest one row each. The segment arrivals
/// carry (incarnation, `segment_seq`, first record stamp), which is exactly what the
/// continuity fold keys on, so the store answers without a single fetch. `Ok(None)`
/// when the fleet has no arrivals at all; the caller announces and walks the source.
///
/// # Errors
/// Engine failures or an unparseable row.
pub fn opens_from_arrivals(
    lake: &analyzer_lake::Lake,
) -> Result<Option<Vec<analyzer_analysis::events::LifecycleEvent>>, Error> {
    let mut first = std::collections::BTreeMap::new();
    crate::inputs::log(lake)?.each_day(|day| -> Result<(), Error> {
        for a in day.batches.iter().flat_map(|b| &b.segments) {
            fold_open(&mut first, a);
        }
        Ok(())
    })?;
    Ok(opens_from(first))
}

/// [`opens_from_arrivals`] over segments a caller already holds: a reader of the served
/// input tables.
#[must_use]
pub fn opens_of<'a>(
    segments: impl IntoIterator<Item = &'a analyzer_lake::ledger::SegmentArrival>,
) -> Option<Vec<analyzer_analysis::events::LifecycleEvent>> {
    let mut first = std::collections::BTreeMap::new();
    for a in segments {
        fold_open(&mut first, a);
    }
    opens_from(first)
}

/// A segment's open, where it carries a sequence claim: the earliest first record per
/// (run, sequence).
fn fold_open(
    first: &mut std::collections::BTreeMap<(String, u64), f64>,
    a: &analyzer_lake::ledger::SegmentArrival,
) {
    let Ok(seq) = u64::try_from(a.segment_seq) else {
        return; // no sequence claim
    };
    if a.incarnation.is_empty() {
        return;
    }
    let at = first
        .entry((a.incarnation.clone(), seq))
        .or_insert(a.first_record_at_ms);
    *at = at.min(a.first_record_at_ms);
}

/// The folded opens as the lifecycle events the continuity fold keys on; `None` when there
/// are none.
fn opens_from(
    first: std::collections::BTreeMap<(String, u64), f64>,
) -> Option<Vec<analyzer_analysis::events::LifecycleEvent>> {
    let opens: Vec<_> = first
        .into_iter()
        .map(
            |((incarnation, seq), at_ms)| analyzer_analysis::events::LifecycleEvent {
                kind: "segment_open".to_string(),
                at_ms,
                instance: String::new(),
                incarnation,
                dropped: None,
                oversized: None,
                heartbeat_secs: None,
                max_segment_secs: None,
                canary: None,
                reuse_reporting: None,
                endpoints: Vec::new(),
                segment_seq: Some(seq),
                first_seq: None,
                last_seq: None,
                refused: None,
                window_start_ms: None,
                window_end_ms: None,
                dp_rank: None,
                group_idx: None,
                attributed: None,
                dropped_bytes: None,
                agent_version: None,
                declared_model: None,
                declared_pod: None,
            },
        )
        .collect();
    (!opens.is_empty()).then_some(opens)
}

/// The window's uncertainty model from the input log: spans and boundaries from the segment
/// arrivals, evidence instances from the batches that carried records over the window's days.
/// `Ok(None)` when no segment arrival meets the window; a keyed tick refuses to classify
/// then, since it cannot say what was observed.
///
/// # Errors
/// The input log or a day's manifest could not be read.
pub fn window_model_from_arrivals(
    lake: &analyzer_lake::Lake,
    w: &Window,
    params: &Params,
) -> Result<Option<analyzer_analysis::completeness::UncertaintyModel>, Error> {
    observed_model(Some(lake), &segments_meeting(lake, w)?, w, params)
}

/// The model a keyed tick classifies under: [`window_model_from_arrivals`], or, where no
/// segment meeting `w` carries a liveness record, the model of no attested span. A log of
/// cache records alone is contract-legal (an orphan tail, a corpus): its sources are stated
/// from their cache evidence over the window's days, with their silence uncovered.
///
/// # Errors
/// The input log could not be read.
pub fn classifying_model(
    lake: &analyzer_lake::Lake,
    w: &Window,
    params: &Params,
) -> Result<analyzer_analysis::completeness::UncertaintyModel, Error> {
    classifying_model_through(lake, w, params, None)
}

/// [`classifying_model`] over the batches committed at or below `through` alone, or over every
/// batch for `None`: the model a tick classified under, before later batches arrived.
///
/// # Errors
/// The input log could not be read.
pub fn classifying_model_through(
    lake: &analyzer_lake::Lake,
    w: &Window,
    params: &Params,
    through: Option<u64>,
) -> Result<analyzer_analysis::completeness::UncertaintyModel, Error> {
    let segments = segments_meeting_through(lake, w, through)?;
    let ModelInputs { spans, boundaries } = model_inputs_of(&segments).unwrap_or_default();
    Ok(
        analyzer_analysis::completeness::uncertainty_model_from_parts(
            spans,
            boundaries,
            evidence(lake, w, through)?,
            w,
            params,
        )
        .with_silences(silences_of(&segments, w.end_ms)),
    )
}

/// Where each instance's heartbeats stopped past their liveness bound, from `segments`: the
/// bound past a segment's last heartbeat where the instance's next segment's first came later,
/// and past its last segment's last heartbeat where that is at or before `through_ms`, the
/// reach of what was read. The instance across its runs: a restarted producer is as unobserved
/// in between as a silent one.
fn silences_of(
    segments: &[analyzer_lake::ledger::SegmentArrival],
    through_ms: f64,
) -> Vec<(String, f64)> {
    let mut beats: std::collections::BTreeMap<&str, Vec<(f64, f64, f64)>> =
        std::collections::BTreeMap::new();
    for a in segments {
        let (Some(first), Some(last)) = (a.hb_first_at_ms, a.hb_last_at_ms) else {
            continue;
        };
        let Some(instance) = a.instance.as_deref() else {
            continue; // the run-level row names no source
        };
        let bound =
            analyzer_analysis::completeness::silence_after_ms(a.heartbeat_secs, a.max_segment_secs);
        beats
            .entry(instance)
            .or_default()
            .push((first, last, bound));
    }
    let mut silences = Vec::new();
    for (instance, mut runs) in beats {
        runs.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.total_cmp(&b.1)));
        let mut heard = f64::NEG_INFINITY;
        let mut bound = 0.0;
        for (first, last, b) in runs {
            if heard.is_finite() && first > heard + bound {
                silences.push((instance.to_string(), heard + bound));
            }
            if last >= heard {
                heard = last;
                bound = b;
            }
        }
        if heard.is_finite() && heard + bound <= through_ms {
            silences.push((instance.to_string(), heard + bound));
        }
    }
    silences
}

/// [`window_model_from_arrivals`] over segments a caller already holds (a reader of the
/// served input tables): those meeting the window are selected here.
///
/// # Errors
/// A day's manifest could not be read for the window's evidence.
pub fn window_model_from_segments(
    lake: &analyzer_lake::Lake,
    segments: Vec<analyzer_lake::ledger::SegmentArrival>,
    w: &Window,
    params: &Params,
) -> Result<Option<analyzer_analysis::completeness::UncertaintyModel>, Error> {
    observed_model(Some(lake), &select_meeting(segments, w), w, params)
}

/// The model from the arrivals, with the evidence instances the spine's days hold when a
/// lake is given.
fn observed_model(
    lake: Option<&analyzer_lake::Lake>,
    segments: &[analyzer_lake::ledger::SegmentArrival],
    w: &Window,
    params: &Params,
) -> Result<Option<analyzer_analysis::completeness::UncertaintyModel>, Error> {
    let Some(ModelInputs { spans, boundaries }) = model_inputs_of(segments) else {
        return Ok(None);
    };
    let evidence = match lake {
        Some(lake) => evidence(lake, w, None)?,
        None => std::collections::BTreeSet::new(),
    };
    Ok(Some(
        analyzer_analysis::completeness::uncertainty_model_from_parts(
            spans, boundaries, evidence, w, params,
        ),
    ))
}

/// The sources with cache evidence over `w`'s days: the instances the input log's records
/// name on those days, whatever segment carried them. A source seen only there has no
/// heartbeat span, and is stated over its days with its silence uncovered.
fn evidence(
    lake: &analyzer_lake::Lake,
    w: &Window,
    through: Option<u64>,
) -> Result<std::collections::BTreeSet<String>, Error> {
    #[allow(clippy::cast_possible_truncation)]
    let lo_day = (w.start_ms / DAY_MS).floor() as i64;
    #[allow(clippy::cast_possible_truncation)]
    let hi_day = ((w.end_ms - 1.0) / DAY_MS).floor() as i64;
    let mut evidence = std::collections::BTreeSet::new();
    for batch in crate::inputs::log(lake)?
        .over(lo_day, hi_day)?
        .into_iter()
        .filter(|b| through.is_none_or(|t| b.input_position <= t))
    {
        evidence.extend(
            batch
                .stored
                .iter()
                .filter(|s| (lo_day..=hi_day).contains(&s.day) && !s.instance.is_empty())
                .map(|s| s.instance.clone()),
        );
    }
    Ok(evidence)
}

/// What the completeness model is built from: the heartbeat spans and the run boundaries.
#[derive(Debug, Default)]
struct ModelInputs {
    spans: Vec<analyzer_analysis::completeness::HeartbeatSpan>,
    boundaries: analyzer_analysis::completeness::AgentBoundaries,
}

/// The model inputs from segments already selected and ordered ([`select_meeting`]), or
/// `None` where there are no segments at all.
fn model_inputs_of(segments: &[analyzer_lake::ledger::SegmentArrival]) -> Option<ModelInputs> {
    let mut spans = Vec::new();
    let mut boundaries = analyzer_analysis::completeness::AgentBoundaries::default();
    let mut prev: Option<(String, Option<String>, f64, i64, i64)> = None;
    let any = !segments.is_empty();
    for a in segments {
        let incarnation = a.incarnation.clone();
        let instance = a.instance.clone();
        let first = a.first_record_at_ms;
        boundaries.start_at_ms = Some(boundaries.start_at_ms.map_or(first, |b| b.min(first)));
        let dropped = i64::try_from(a.dropped).unwrap_or(i64::MAX);
        let oversized = i64::try_from(a.oversized).unwrap_or(i64::MAX);
        if let Some(stop) = a.stop_at_ms {
            boundaries.stop_at_ms = Some(boundaries.stop_at_ms.map_or(stop, |b| b.max(stop)));
            if let Some((pi, ps, _, pd, po)) = &prev {
                if *pi == incarnation && *ps == instance && (dropped > *pd || oversized > *po) {
                    boundaries.stop_span_dropped = true;
                }
            }
        }
        // Rows without a heartbeat clock contribute boundaries only.
        let (Some(hb_first), Some(hb_last)) = (a.hb_first_at_ms, a.hb_last_at_ms) else {
            continue;
        };
        let Some(source) = instance.clone() else {
            continue; // the run-level envelope row; spans are per named source
        };
        let continues = prev
            .as_ref()
            .is_some_and(|(pi, ps, _, _, _)| *pi == incarnation && *ps == instance);
        if continues {
            let (_, _, p_end, pd, po) = prev.as_ref().expect("continues implies prev");
            // The counters are cumulative over the run, and the row states them at its first
            // and last heartbeat: what moved before the first fell in the gap from the previous
            // segment, the rest inside this one. A row with one heartbeat cannot split.
            let interior = hb_first < hb_last;
            let (first_d, first_o) = if interior {
                (
                    i64::try_from(a.dropped_at_hb_first).unwrap_or(i64::MAX),
                    i64::try_from(a.oversized_at_hb_first).unwrap_or(i64::MAX),
                )
            } else {
                (dropped, oversized)
            };
            spans.push(analyzer_analysis::completeness::HeartbeatSpan {
                source: source.clone(),
                start_at_ms: *p_end,
                end_at_ms: if interior { hb_first } else { hb_last },
                dropped_delta: first_d - pd,
                oversized_delta: first_o - po,
                // The quiet stretch is between the runs of beats: the previous segment's last
                // and this one's first. Inside a segment no gap passes the bound, which is never
                // shorter than the segment's own declared seal.
                silence: analyzer_analysis::completeness::silence_between(
                    *p_end,
                    hb_first,
                    analyzer_analysis::completeness::silence_after_ms(
                        a.heartbeat_secs,
                        a.max_segment_secs,
                    ),
                ),
            });
            if interior {
                spans.push(analyzer_analysis::completeness::HeartbeatSpan {
                    source: source.clone(),
                    start_at_ms: hb_first,
                    end_at_ms: hb_last,
                    dropped_delta: dropped - first_d,
                    oversized_delta: oversized - first_o,
                    silence: None,
                });
            }
        } else if hb_first < hb_last {
            // A source's FIRST segment carries its own interior: the row collapses the
            // segment's heartbeats to [hb_first, hb_last], so the span opens here rather than
            // waiting for a second row, or a single-segment run would state no coverage. What
            // the counters moved before the first heartbeat has no span of its own to fall in,
            // so the delta takes the row's cumulative whole: a bound that can only mark a
            // complete span dropped, never a dropped span complete. Coverage under-claims, the
            // same error direction as the residual's budget skip.
            spans.push(analyzer_analysis::completeness::HeartbeatSpan {
                source: source.clone(),
                start_at_ms: hb_first,
                end_at_ms: hb_last,
                dropped_delta: dropped,
                oversized_delta: oversized,
                silence: None,
            });
        }
        prev = Some((incarnation, instance, hb_last, dropped, oversized));
    }
    if !any {
        return None;
    }
    spans.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| a.start_at_ms.total_cmp(&b.start_at_ms))
    });
    Some(ModelInputs { spans, boundaries })
}

/// The segments whose records meet `w` widened by the evidence clip margin, each once, in
/// the fold's order: by run, source, first record, then first heartbeat (none last).
pub(crate) fn segments_meeting(
    lake: &analyzer_lake::Lake,
    w: &Window,
) -> Result<Vec<analyzer_lake::ledger::SegmentArrival>, Error> {
    segments_meeting_through(lake, w, None)
}

/// [`segments_meeting`] from the batches committed at or below `through`, or from every batch.
fn segments_meeting_through(
    lake: &analyzer_lake::Lake,
    w: &Window,
    through: Option<u64>,
) -> Result<Vec<analyzer_lake::ledger::SegmentArrival>, Error> {
    use analyzer_lake::ledger::day_of;
    let margin = analyzer_analysis::completeness::EVIDENCE_CLIP_MARGIN_MS;
    let segments = crate::inputs::log(lake)?
        .over(day_of(w.start_ms - margin), day_of(w.end_ms + margin))?
        .into_iter()
        .filter(|b| through.is_none_or(|t| b.input_position <= t))
        .flat_map(|b| b.segments)
        .collect();
    Ok(select_meeting(segments, w))
}

/// Of `segments`, those whose records meet `w` widened by the evidence clip margin, in the
/// fold's order: by run, source, first record, then first heartbeat (none last).
#[must_use]
pub fn select_meeting(
    segments: Vec<analyzer_lake::ledger::SegmentArrival>,
    w: &Window,
) -> Vec<analyzer_lake::ledger::SegmentArrival> {
    let lo = (w.start_ms - analyzer_analysis::completeness::EVIDENCE_CLIP_MARGIN_MS).floor();
    let hi = (w.end_ms + analyzer_analysis::completeness::EVIDENCE_CLIP_MARGIN_MS).ceil();
    let mut segments: Vec<_> = segments
        .into_iter()
        .filter(|a| a.last_record_at_ms >= lo && a.first_record_at_ms < hi)
        .collect();
    segments.sort_by(|a, b| {
        a.incarnation
            .cmp(&b.incarnation)
            .then_with(|| a.instance.cmp(&b.instance))
            .then_with(|| a.first_record_at_ms.total_cmp(&b.first_record_at_ms))
            .then_with(|| match (a.hb_first_at_ms, b.hb_first_at_ms) {
                (Some(x), Some(y)) => x.total_cmp(&y),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
    });
    segments
}

#[cfg(test)]
mod tests {
    use super::{model_inputs_of, silences_of};
    use analyzer_lake::ledger::SegmentArrival;

    /// A segment of run `r` on `i0`, heartbeats at `hb` with `dropped` stated at each.
    fn segment(seq: i64, hb: (f64, f64), dropped: (u64, u64)) -> SegmentArrival {
        SegmentArrival {
            incarnation: "r".into(),
            segment_seq: seq,
            instance: Some("i0".into()),
            first_record_at_ms: hb.0,
            last_record_at_ms: hb.1,
            heartbeat_secs: 60,
            max_segment_secs: 300,
            hb_count: 2,
            hb_first_at_ms: Some(hb.0),
            hb_last_at_ms: Some(hb.1),
            stop_at_ms: None,
            msgs_seen: 1,
            dropped: dropped.1,
            oversized: 0,
            dropped_at_hb_first: dropped.0,
            oversized_at_hb_first: 0,
            publisher_restarts: 0,
            agent_version: Some("t".into()),
            topic_model: None,
            topic_pod: None,
            declared_model: None,
            declared_pod: None,
            engine_version: None,
            attributed: true,
            canary: None,
        }
    }

    #[test]
    fn an_instance_is_silent_from_the_bound_past_its_last_heartbeat_across_its_runs() {
        // The bound is the 30-minute staleness grace: longer than the run's declared 300 s.
        let bound = 1_800_000.0;
        let early = segment(0, (0.0, 600_000.0), (0, 0));
        let near = segment(1, (1_000_000.0, 1_600_000.0), (0, 0));
        let late = segment(2, (4_000_000.0, 4_600_000.0), (0, 0));
        assert_eq!(
            silences_of(&[early.clone(), near.clone(), late.clone()], 6_000_000.0),
            vec![("i0".to_string(), 1_600_000.0 + bound)],
            "the gap past the bound is silence, the shorter one is not"
        );
        assert_eq!(
            silences_of(std::slice::from_ref(&late), 4_600_000.0 + bound),
            vec![("i0".to_string(), 4_600_000.0 + bound)],
            "past the last heartbeat, once the read reaches the bound"
        );
        assert!(
            silences_of(std::slice::from_ref(&late), 4_600_000.0 + bound - 1.0).is_empty(),
            "not before the read reaches it"
        );
        // A restarted producer's next run heard within the bound: no silence between.
        let mut restarted = segment(0, (2_500_000.0, 3_000_000.0), (0, 0));
        restarted.incarnation = "r2".into();
        assert!(
            silences_of(&[early, near, restarted, late], 6_000_000.0).is_empty(),
            "the instance across its runs"
        );
    }

    fn drop_windows(rows: &[SegmentArrival]) -> Vec<(f64, f64)> {
        model_inputs_of(rows)
            .expect("rows")
            .spans
            .iter()
            .filter(|s| s.is_drop_window())
            .map(|s| (s.start_at_ms, s.end_at_ms))
            .collect()
    }

    #[test]
    fn a_drop_inside_a_segment_after_a_gap_leaves_the_gap_complete() {
        // Hours apart; the counter moves only between the second segment's own beats.
        let rows = [
            segment(0, (0.0, 300_000.0), (0, 0)),
            segment(1, (18_000_000.0, 18_300_000.0), (0, 3)),
        ];
        assert_eq!(drop_windows(&rows), vec![(18_000_000.0, 18_300_000.0)]);
    }

    #[test]
    fn a_drop_in_the_gap_marks_the_gap_and_not_the_segment() {
        let rows = [
            segment(0, (0.0, 300_000.0), (0, 0)),
            segment(1, (18_000_000.0, 18_300_000.0), (3, 3)),
        ];
        assert_eq!(drop_windows(&rows), vec![(300_000.0, 18_000_000.0)]);
    }

    #[test]
    fn a_one_beat_segment_carries_its_movement_over_the_whole_gap() {
        let mut one = segment(1, (18_000_000.0, 18_000_000.0), (3, 3));
        one.hb_count = 1;
        let rows = [segment(0, (0.0, 300_000.0), (0, 0)), one];
        assert_eq!(drop_windows(&rows), vec![(300_000.0, 18_000_000.0)]);
    }
}
