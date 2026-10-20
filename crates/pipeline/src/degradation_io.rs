// SPDX-License-Identifier: AGPL-3.0-only

//! The degradation ledger's I/O half: assembly over the `EventSource`, and the two scope
//! expansions.
//!
//! **Five of the six causes are folds** (`analysis::degradation`). What stays here is assembly —
//! running each cause over the slice it needs — plus spec 5.1's expansion: resolving an incident's
//! incarnation to the instances that run covered takes the union of its DECLARED coverage and
//! the instances its surviving cache records mention.
//!
//! The expansion is bounded by an INCARNATION SET rather than by time, which is a scope no seam
//! method offers — so it reads the horizon-wide lifecycle slice and the window's cache slice and
//! filters ABOVE the seam, per the seam's own law (a source must not filter). It runs only when
//! an incident exists, so a healthy fleet's ledger costs sparse lifecycle scans and nothing else.

use crate::Error;
use analyzer_analysis::completeness::LIFECYCLE_CLIP_MARGIN_MS;
use analyzer_analysis::degradation::AbsenceRegister;
use analyzer_analysis::degradation::{
    Cause, Degradation, Incident, Ledger, ProducerBracket, Scope,
};
use analyzer_analysis::{Params, Window};

use analyzer_analysis::degradation::{
    compact, declared_drop_incidents_from_events, drop_window_entries_from_model,
    incarnation_boundaries_from_events, recovered_truncation_incidents_from_events,
    refusal_entries_from_events, sequence_holes_from_events,
};
use analyzer_analysis::events::EventSource;
use std::collections::{BTreeMap, BTreeSet};

/// The spec 5.6 absence register for a producer-clock span: [`AbsenceRegister::Exact`]
/// exactly when the ledger holds nothing against the span AND some run's counted
/// bracket — an observed `agent_start` through an observed `agent_stop` — contains it.
///
/// The two conditions split the rule's two halves: the bracket is the "observed,
/// counted sequence" (a run with either endpoint unobserved cannot vouch for its own
/// edges), and the empty ledger is "counted, with nothing lost" (a nonzero heartbeat
/// delta is a drop-window entry, a sequence hole is a gap entry, so any loss the
/// counters or the delivery layer can see already stands in the ledger). Everything
/// else — before the first start, past the last observation, across a blind spot —
/// falls to the lower bound, the direction that never over-claims.
///
/// # Errors
/// Source failures or malformed input.
pub fn absence_register(
    source: &dyn EventSource,
    fleet_id: &str,
    span: (f64, f64),
    params: &Params,
) -> Result<AbsenceRegister, Error> {
    let w = Window {
        start_ms: span.0,
        end_ms: span.1,
    };
    // The one-shot/operator path derives its model from the source at its own scale;
    // the daemon's cycle builds it from banked arrivals and calls [`ledger`] directly.
    let model = crate::read::uncertainty_model(source, fleet_id, &w, params)?;
    let opens = source.segment_opens_at_retention_horizon(fleet_id)?;
    if !ledger(source, fleet_id, &w, &model, &opens)?
        .in_window(&w)
        .is_empty()
    {
        return Ok(AbsenceRegister::LowerBound);
    }
    for (start_at, stop_at) in counted_brackets(source, fleet_id, &w)? {
        if start_at <= span.0 && span.1 <= stop_at {
            return Ok(AbsenceRegister::Exact);
        }
    }
    Ok(AbsenceRegister::LowerBound)
}

/// Counted brackets: runs whose `agent_start` AND `agent_stop` were both observed
/// (inventory Q15), each as `(first start, last stop)` — producer domain throughout,
/// clipped like every lifecycle fetch. A bracket that began before the clip margin is
/// missed and the span falls to the lower bound: the conservative direction, never the
/// over-claiming one.
///
/// # Errors
/// Source failures or malformed input.
pub(crate) fn counted_brackets(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
) -> Result<Vec<(f64, f64)>, Error> {
    let lifecycle = source.lifecycle_events(
        fleet_id,
        w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
        w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
    )?;
    let mut runs: BTreeMap<String, (Option<f64>, Option<f64>)> = BTreeMap::new();
    for e in &lifecycle {
        let entry = runs.entry(e.incarnation.clone()).or_default();
        match e.kind.as_str() {
            "agent_start" => entry.0 = Some(entry.0.map_or(e.at_ms, |t| t.min(e.at_ms))),
            "agent_stop" => entry.1 = Some(entry.1.map_or(e.at_ms, |t| t.max(e.at_ms))),
            _ => {}
        }
    }
    Ok(runs
        .into_values()
        .filter_map(|(start, stop)| Some((start?, stop?)))
        .collect())
}

/// Assemble the ledger for one fleet and window.
///
/// **The model arrives as a parameter**, so the ledger never materializes the window's cache
/// events. A caller with an input log builds it from the arrivals
/// (`arrivals::window_model_from_arrivals`); callers without one (corpus tests) build it from
/// the source at their own scale.
///
/// **And the stage announces itself**, entry and completion: a long fetch with no log line reads
/// as absence.
///
/// # Errors
/// Source failures or malformed input.
pub fn ledger(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
    model: &analyzer_analysis::completeness::UncertaintyModel,
    opens: &[analyzer_analysis::events::LifecycleEvent],
) -> Result<Ledger, Error> {
    let started = std::time::Instant::now();
    log::info!(
        "degradation ledger: entering window \
         [{:.0}..{:.0}]",
        w.start_ms,
        w.end_ms
    );
    // ONE lifecycle fetch for the three folds that read it, fetched at the WIDER of the two
    // margins the ledger's consumers use; the window folds clip back to their own margin from
    // the wide slice, which preserves their exact semantics. `opens` arrives from the CALLER
    // because the input log answers it without a fetch (`arrivals::opens_of`).
    let wide =
        LIFECYCLE_CLIP_MARGIN_MS.max(analyzer_analysis::completeness::EVIDENCE_CLIP_MARGIN_MS);
    let fetch_started = std::time::Instant::now();
    let lifecycle_wide = source.lifecycle_events(fleet_id, w.start_ms - wide, w.end_ms + wide)?;
    log::info!(
        "degradation ledger: lifecycle slice: {} record(s) in \
         {:.1}s, folded four ways",
        lifecycle_wide.len(),
        fetch_started.elapsed().as_secs_f64()
    );
    let (clip_lo, clip_hi) = (
        w.start_ms - LIFECYCLE_CLIP_MARGIN_MS,
        w.end_ms + LIFECYCLE_CLIP_MARGIN_MS,
    );
    let lifecycle: Vec<analyzer_analysis::events::LifecycleEvent> = lifecycle_wide
        .iter()
        .filter(|e| e.at_ms >= clip_lo && e.at_ms < clip_hi)
        .cloned()
        .collect();
    let incidents = run_scoped_incidents(&lifecycle, opens);
    let mut entries = incarnation_boundaries_from_events(&lifecycle);
    entries.extend(refusal_entries_from_events(&lifecycle, w));
    entries.extend(drop_window_entries_from_model(model));

    let ledger = if incidents.is_empty() && entries.is_empty() {
        Ledger::default() // the healthy fast path: sparse scans only, no expansion
    } else {
        if !incidents.is_empty() {
            // The one remaining cache walk on this path, bounded by incidents being
            // present at all — named here so it cannot hide (the family's tenth
            // suspect, held rather than fixed: expansion needs the window's cache
            // slice to scope a run's degradation to the instances it touched).
            log::info!(
                "degradation ledger: expanding {} incident(s) — \
                 walking the window's cache slice",
                incidents.len()
            );
            entries.extend(expand_incidents(
                source,
                fleet_id,
                w,
                incidents,
                &lifecycle_wide,
            )?);
        }
        Ledger {
            entries: compact(entries),
        }
    };
    log::info!(
        "degradation ledger: {} entr(ies) in {:.1}s",
        ledger.entries.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(ledger)
}

/// The three run-scoped incident causes: sequence holes, declared drops, and an attributed
/// recovery's declared truncation — the loss continuity accounting cannot see, because the
/// recovered segment keeps its `segment_seq` and leaves no hole.
fn run_scoped_incidents(
    lifecycle: &[analyzer_analysis::events::LifecycleEvent],
    // RETENTION-scoped, not window-clipped: a producer may declare a loss long after
    // it happened, and a clipped load would silently narrow the degraded span.
    opens: &[analyzer_analysis::events::LifecycleEvent],
) -> Vec<Incident> {
    let mut incidents: Vec<Incident> = sequence_holes_from_events(lifecycle)
        .into_iter()
        .map(|h| Incident {
            incarnation: h.incarnation,
            cause: Cause::SequenceGap {
                first_missing: h.missing_from,
                last_missing: h.missing_to,
            },
            bracket: ProducerBracket {
                start_ms: Some(h.bracket_start_ms),
                end_ms: Some(h.bracket_end_ms),
            },
        })
        .collect();
    incidents.extend(
        declared_drop_incidents_from_events(lifecycle, opens)
            .into_iter()
            .map(|d| Incident {
                incarnation: d.incarnation,
                cause: Cause::DeclaredDrop {
                    first_seq: d.first_seq,
                    last_seq: d.last_seq,
                },
                bracket: ProducerBracket {
                    start_ms: d.bracket_start_ms,
                    end_ms: d.bracket_end_ms,
                },
            }),
    );
    incidents.extend(recovered_truncation_incidents_from_events(lifecycle, opens));
    incidents
}

/// Spec 5.1's expansion: resolve each incident's incarnation to the instance set it
/// covered, and emit the run-scoped entries.
///
/// **Two halves, each with the scope the mapping demands.** The DECLARED half reads
/// `endpoints[].source` off the horizon-wide lifecycle slice — deliberately not
/// window-scoped, like every lifecycle-identity fetch (inventory Q11): coverage declared
/// before the window still names who the loss touches. The FALLBACK half reads the
/// instances an incarnation's SURVIVING cache records mention (inventory Q12),
/// window-bounded. Both filter by the incident's incarnation set ABOVE the seam, because
/// an incarnation-set scope is no seam method's to offer.
fn expand_incidents(
    source: &dyn EventSource,
    fleet_id: &str,
    w: &Window,
    incidents: Vec<Incident>,
    // The ledger's own wide slice (evidence margin) — the second per-expansion fetch
    // this parameter replaces cost 377 seconds for ONE incident.
    lifecycle_wide: &[analyzer_analysis::events::LifecycleEvent],
) -> Result<Vec<Degradation>, Error> {
    let runs: BTreeSet<String> = incidents.iter().map(|i| i.incarnation.clone()).collect();
    let mut covered: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    // The declared half. Only heartbeats carry endpoints, so the other lifecycle kinds
    // contribute nothing and need no kind filter.
    for e in lifecycle_wide {
        if !runs.contains(&e.incarnation) {
            continue;
        }
        for endpoint in &e.endpoints {
            covered
                .entry(e.incarnation.clone())
                .or_default()
                .insert(endpoint.source.clone());
        }
    }
    // The fallback half.
    for e in source.cache_events(fleet_id, w)?.iter() {
        if runs.contains(&e.incarnation) {
            covered
                .entry(e.incarnation.clone())
                .or_default()
                .insert(e.instance.clone());
        }
    }
    Ok(incidents
        .into_iter()
        .map(|i| {
            let instances: Vec<String> = covered
                .get(&i.incarnation)
                .map(|set| set.iter().cloned().collect())
                .unwrap_or_default();
            Degradation {
                scope: Scope::Run {
                    incarnation: i.incarnation,
                    instances,
                },
                cause: i.cause,
                producer_bracket: i.bracket,
                engine_extent: None,
                incidents: 1,
            }
        })
        .collect())
}
