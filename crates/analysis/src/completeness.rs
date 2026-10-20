// SPDX-License-Identifier: AGPL-3.0-only

//! Completeness clock, drop windows, coverage (join-semantics), and the
//! indeterminate machinery.
//!
//! The completeness clock: heartbeat cumulative-counter deltas define per-span
//! completeness per source. A span whose deltas show no drops/oversized is **complete**;
//! a positive delta makes the whole span a **drop window** (loss cannot be localized
//! inside it). `agent_start` is the zero-counter origin (the span before the first
//! heartbeat is attested when an `agent_start` precedes it); `agent_stop` closes the
//! tail. Per-endpoint `dropped` sub-stats are complete by tap construction (the tap's
//! `dropped_total` is the sum over its per-source counters), so per-source deltas capture
//! every drop; `oversized` is agent-level only and is replicated onto every source's span —
//! the conservative direction.
//!
//! Clock domains: heartbeat `at_ms` is the tap process's
//! clock and cache `at_ms` is the engine's — two clocks on ONE host, compared directly
//! and bounded by `δ_skew`, the same allowance every cross-instance
//! comparison carries. A drop window's engine-domain extent is therefore the window's
//! own producer-time bracket widened by the skew, and classification taint is a record's
//! `at_ms` falling inside it — a function of the bytes, identical for every reader of
//! them. The arrival-stamp mapping this replaces made both figures functions of the
//! OBSERVER's poll timing: two analyzers over one bucket disagreed about identical
//! bytes, a restart changed a daemon's own answers, and archive input took a second
//! semantics. The conservative direction is preserved: skew widens windows, so excess
//! skew over-taints toward `indeterminate` and never manufactures a false `cold`.
//!
//! Tail-uncertainty and before-first-heartbeat spans reduce the COVERAGE figure only —
//! The classification taint consults drop windows, never tail spans.
//!
//! **v1 agent-identity reading (multi-agent fleets, stated precisely):** lifecycle
//! records carry no agent identifier, so `agent_start`/`agent_stop` apply fleet-wide.
//! For drop evidence this is conservative. For tail closure and pre-first-heartbeat
//! attestation it is OPTIMISTIC in multi-agent fleets: one agent's graceful stop closes
//! every source's tail, so another agent's crash could read as expected departure. All
//! current deployments are single-agent; the tap-side fix is queued (`agent_start` carries
//! its endpoint list, `agent_stop` carries per-endpoint sub-stats), after which start/stop
//! attribute per-source exactly like heartbeats.

use crate::module::run_window;
use crate::observed_coverage::{CoverageCtx, ObservedCoverage};
use crate::Error;
use crate::{Params, Window, CONTEXT_MARGIN_DAYS, DAY_MS};
use std::collections::{BTreeMap, BTreeSet};

/// Wall-side clip for lifecycle fetches: a generous pre-filter (window ± this margin)
/// bounding heartbeat scans over long-lived deployments. A pre-filter with days of
/// slack, not a cross-domain comparison — labeled analytic constant, re-derivation list.
///
/// **Scope narrowed: this is the clip for reads whose output is SNAPSHOT-SCOPED** —
/// the roster, fleet health, the degradation ledger. Its seven days are wall-vs-engine clock
/// slack: lifecycle stamps are wall-domain and the window is engine-domain, the two do not
/// convert, and a generous pre-filter is what lets the scan be bounded without a comparison.
/// Nothing about determinism argues that number down, because none of those reads is banked.
///
/// The coverage path took its evidence through this same constant and must not: see
/// [`EVIDENCE_CLIP_MARGIN_MS`].
pub const LIFECYCLE_CLIP_MARGIN_MS: f64 = 7.0 * 86_400_000.0;

/// **The clip for evidence that reaches EMITTED rows, and the last step of day-determinism**.
///
/// Coverage is banked — `coverage_daily`, which every figure read weights by at read time — so a day's
/// coverage figure is keyed history and must be a function of `(day, parameters)` like every
/// other emitted number. Under [`LIFECYCLE_CLIP_MARGIN_MS`] it was not: the coverage path is
/// handed the analysis window `[D − examined_history, D + 1 day)` and then reached SEVEN MORE
/// DAYS behind it for heartbeats, so a heartbeat older than the window's own horizon could anchor
/// a span reaching into day D — and whether that heartbeat was still retained depended on how
/// far back raw happened to hold. The same day, re-passed later, could bank a different coverage
/// figure under the same discriminator.
///
/// So the evidence clip reaches exactly one labeled margin past the analysis window, which puts
/// its total backward reach from an emitted day at
/// `EXAMINED_HISTORY_DAYS + CONTEXT_MARGIN_DAYS` — **the same quantity as the input horizon's
/// floor**, the least input a deployment keeps. Everything this fetch can see is therefore
/// still there on a re-derivation.
///
/// **It is a coverage-semantics change, and it errs the conservative way.** A span whose
/// anchoring heartbeat now falls outside the clip cannot be constructed, so that stretch reads
/// UNCERTAIN rather than covered — coverage falls, never rises, and an unobserved stretch is
/// never claimed as observed.
///
/// **The consequence, stated because it bounds who is affected: attestation requires
/// heartbeat spacing inside the examined history.** At the tap's 60-second cadence the gap
/// would have to exceed the examined history (`Params::examined_history_ms`, an hour by
/// default), which means the agent was silent for an hour — and a silent agent's stretch
/// SHOULD read uncertain rather than covered by a span stitched across its silence.
pub const EVIDENCE_CLIP_MARGIN_MS: f64 = CONTEXT_MARGIN_DAYS as f64 * DAY_MS;

/// One inter-heartbeat span for a source, with its counter deltas.
#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatSpan {
    /// The source the heartbeat's endpoint sub-stats name — the same string as the cache
    /// records' `instance`, which is how taint finds a record's own spans.
    pub source: String,
    /// The earlier heartbeat's `at_ms` — the tap process's wall clock, never engine time.
    pub start_at_ms: f64,
    /// The later heartbeat's `at_ms`, same clock; the span is the stretch between the two beats.
    pub end_at_ms: f64,
    /// Movement of this source's cumulative `dropped` counter across the span; positive makes
    /// it a drop window.
    pub dropped_delta: i64,
    /// Movement of the agent-level `oversized` counter, replicated onto every source's span —
    /// the conservative direction.
    pub oversized_delta: i64,
    /// The stretch inside the span no heartbeat attests: from the liveness bound
    /// ([`silence_after_ms`]) past one heartbeat to the next, where they came further apart.
    pub silence: Option<(f64, f64)>,
}

/// How long a gap between two heartbeats may run before the rest of it is silence: the larger
/// of the run's declared `heartbeat_secs` and `max_segment_secs` (spec 5.8), or the staleness
/// grace where that is longer — the measure §9 judges a producer stale by. A producer that
/// emitted no heartbeat for longer attested nothing over the rest of the gap, whatever its
/// counters say when it resumes.
#[must_use]
pub fn silence_after_ms(heartbeat_secs: u64, max_segment_secs: u64) -> f64 {
    #[allow(clippy::cast_precision_loss)] // declared seconds are far below 2^53
    let declared = heartbeat_secs.max(max_segment_secs) as f64 * 1000.0;
    declared.max(crate::staleness::STALENESS_GRACE_MS)
}

/// The silence between heartbeats at `from_ms` and `to_ms` under a liveness bound of
/// `bound_ms`: from the bound past the first to the second, or `None` when they came within it.
#[must_use]
pub fn silence_between(from_ms: f64, to_ms: f64, bound_ms: f64) -> Option<(f64, f64)> {
    let silent_from = from_ms + bound_ms;
    (silent_from < to_ms).then_some((silent_from, to_ms))
}

impl HeartbeatSpan {
    /// The completeness clock's verdict for this span.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.dropped_delta <= 0 && self.oversized_delta <= 0
    }

    /// A positive counter delta makes the whole span a drop window: the loss cannot
    /// be localized inside it (spec 5.4's second detected-gap class).
    #[must_use]
    pub fn is_drop_window(&self) -> bool {
        !self.is_complete()
    }
}

/// Fleet-level lifecycle boundaries (v1 agent-identity reading, module docs).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgentBoundaries {
    /// The earliest `agent_start`, wall clock: the zero-counter origin that attests the stretch
    /// before a source's first heartbeat. `None` = none observed.
    pub start_at_ms: Option<f64>,
    /// The latest `agent_stop`, wall clock: closes every source's tail. `None` = no stop
    /// observed, so the tail reads uncertain.
    pub stop_at_ms: Option<f64>,
    /// Counter movement between the last heartbeat and the stop (top-level counters;
    /// the stop's endpoint sub-stats are empty by contract).
    pub stop_span_dropped: bool,
}

/// An uncertainty span in engine-domain time (post-mapping), clipped to the window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UncertaintySpan {
    /// Where the uncertainty begins, engine-domain ms, no earlier than the window's start.
    pub start_ms: f64,
    /// Where it ends, engine-domain ms, no later than the window's end; always past `start_ms`.
    pub end_ms: f64,
}

/// One UTC day's observed coverage for one source.
///
/// **The figure a 90-day trend point needs and did not have**:
/// `per_source_covered_ms` is one number for the whole window, so a trend drawn over it can
/// state only the window's coverage, not each point's — the naked number `Covered` exists to
/// prevent, one axis over.
#[derive(Debug, Clone, PartialEq)]
pub struct DailyCoverage {
    /// UTC midnight opening the day, engine-domain ms.
    pub day_ms: f64,
    /// The source the row covers — one row per `(day, source)`.
    pub source: String,
    /// Engine-domain ms of the day the source was observed complete: `window_ms` less the
    /// union of its uncertainty spans cut to the day.
    pub covered_ms: f64,
    /// The day's intersection with the window, which is the denominator this day's fraction is
    /// stated over. An open day is stated through the covered edge, so its denominator is the
    /// part of the day stated: carried rather than assumed, so a partial day never reads as a
    /// full one.
    pub window_ms: f64,
}

/// The observed-coverage figure: instance-time weighted (design law: a number without its
/// coverage figure does not appear).
#[derive(Debug, Clone, PartialEq)]
pub struct Coverage {
    /// Covered time summed over the sources, over the window's length times the source count;
    /// `0.0` with no sources.
    pub fraction: f64,
    /// Per source, in source order: engine-domain ms of the window observed complete.
    pub per_source_covered_ms: Vec<(String, f64)>,
    /// Per source, the stretches not covered — before the first heartbeat, each drop window
    /// widened by the skew, after the last heartbeat absent a stop — clipped to the window.
    pub uncertain: Vec<(String, UncertaintySpan)>,
    /// The same coverage, sliced at UTC day boundaries — one row per (day, source), ordered by
    /// day then source. Derived from the SAME span set as the figures above rather than
    /// recomputed, which is what makes the days sum back to `per_source_covered_ms` exactly.
    pub per_day: Vec<DailyCoverage>,
}

/// The taint overlap predicate over CLOSED ranges. Closed, not half-open: E2/E7 pin
/// point-extent drop windows (a single affected record) that must still taint a
/// deciding range touching them — the conservative direction. Consulted only by
/// absence-based decisions; positive evidence never asks.
#[must_use]
pub(crate) fn ranges_intersect(a_start: f64, a_end: f64, b_start: f64, b_end: f64) -> bool {
    a_start <= b_end && b_start <= a_end
}

/// The per-window uncertainty model: heartbeat spans, boundaries, evidence — built
/// once, consulted for both the coverage figure and classification taint.
#[derive(Debug)]
pub struct UncertaintyModel {
    window: Window,
    delta_skew_ms: f64,
    spans: Vec<HeartbeatSpan>,
    boundaries: AgentBoundaries,
    evidence_sources: BTreeSet<String>,
    /// Where each instance's heartbeats stopped past its liveness bound, ascending.
    silences: Vec<(String, f64)>,
}

/// Build the model from the two Ordered Event IR slices — the store-free path.
///
/// **Two slices and two windows, because there are two clocks.** `cache_ir` is engine-domain and
/// arrives clipped to the analysis window; `lifecycle_ir` is the tap's wall clock and arrives
/// clipped to `window ± EVIDENCE_CLIP_MARGIN_MS`, a reach inside the least input a deployment
/// keeps. Handing them in separately is what keeps the two domains from meeting: there is
/// no predicate here that compares one against the other.
///
/// # Errors
/// None today; the `Result` is where a future validation reports.
#[allow(clippy::unnecessary_wraps)]
pub fn uncertainty_model_from_events(
    cache_ir: &[crate::events::Event],
    lifecycle_ir: &[crate::events::LifecycleEvent],
    w: &Window,
    params: &Params,
) -> Result<UncertaintyModel, Error> {
    Ok(UncertaintyModel {
        window: *w,
        delta_skew_ms: params.delta_skew_ms,
        spans: heartbeat_spans_from_events(lifecycle_ir),
        boundaries: agent_boundaries_from_events(lifecycle_ir),
        evidence_sources: cache_ir
            .iter()
            .map(|e| e.instance.clone())
            .collect::<BTreeSet<_>>(),
        silences: Vec::new(),
    })
}

/// The model from pre-derived parts — the STORE-backed path. The spans
/// and boundaries arrive reconstructed at SEGMENT grain from the input log's segment arrivals
/// (consecutive per-source rows, cumulative-counter deltas between their heartbeat
/// marks), where the coarsening direction is the allowance law's own: a drop delta
/// that occurred inside one segment taints that segment's whole span — wider, never
/// narrower. The event-grain constructor above remains the source-fed fallback.
#[must_use]
pub fn uncertainty_model_from_parts(
    spans: Vec<HeartbeatSpan>,
    boundaries: AgentBoundaries,
    evidence_sources: BTreeSet<String>,
    w: &Window,
    params: &Params,
) -> UncertaintyModel {
    UncertaintyModel {
        window: *w,
        delta_skew_ms: params.delta_skew_ms,
        spans,
        boundaries,
        evidence_sources,
        silences: Vec::new(),
    }
}

/// Per-source completeness spans, folded from consecutive heartbeats.
///
/// Each heartbeat carries a cumulative `dropped` per endpoint source; a span runs between two
/// consecutive heartbeats of one source, and its delta is the difference. The first heartbeat
/// of a source opens no span, because a counter with no predecessor states a total, not a change.
///
/// **`oversized` is agent-level and is replicated onto every source's span**: one agent-level
/// counter fanned across the sources it covers.
/// The direction is conservative — a source that dropped nothing still inherits the agent's
/// oversized delta and its span reads as a drop window.
fn heartbeat_spans_from_events(ir: &[crate::events::LifecycleEvent]) -> Vec<HeartbeatSpan> {
    // Each run's declared liveness bounds, from whichever of its records state them.
    let mut bounds: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    for ev in ir {
        let b = bounds.entry(ev.incarnation.as_str()).or_default();
        b.0 = b.0.max(ev.heartbeat_secs.unwrap_or(0));
        b.1 = b.1.max(ev.max_segment_secs.unwrap_or(0));
    }
    // (source, at_ms, dropped, oversized, silence bound), grouped by source and ordered in time.
    let mut by_source: BTreeMap<&str, Vec<(f64, i64, i64, f64)>> = BTreeMap::new();
    for ev in ir.iter().filter(|e| e.kind == "heartbeat") {
        let oversized = ev.oversized.unwrap_or(0);
        let (hb, seg) = bounds
            .get(ev.incarnation.as_str())
            .copied()
            .unwrap_or_default();
        for ep in &ev.endpoints {
            by_source.entry(ep.source.as_str()).or_default().push((
                ev.at_ms,
                ep.dropped,
                oversized,
                silence_after_ms(hb, seg),
            ));
        }
    }

    let mut spans = Vec::new();
    for (source, mut beats) in by_source {
        // Total, so two runs over identical bytes produce identical spans. The query's
        // `ORDER BY at_ms` inside the window is not total and the engine breaks ties itself.
        beats.sort_by(|a, b| {
            a.0.total_cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
        });
        for pair in beats.windows(2) {
            let (prev, cur) = (pair[0], pair[1]);
            spans.push(HeartbeatSpan {
                source: source.to_string(),
                start_at_ms: prev.0,
                end_at_ms: cur.0,
                dropped_delta: cur.1 - prev.1,
                oversized_delta: cur.2 - prev.2,
                silence: silence_between(prev.0, cur.0, cur.3),
            });
        }
    }
    spans.sort_by(|a, b| {
        a.source
            .cmp(&b.source)
            .then_with(|| a.end_at_ms.total_cmp(&b.end_at_ms))
    });
    spans
}

/// The agent-level brackets: the earliest start, the latest stop, and whether the stop's own span
/// showed loss.
fn agent_boundaries_from_events(ir: &[crate::events::LifecycleEvent]) -> AgentBoundaries {
    let mut ordered: Vec<&crate::events::LifecycleEvent> = ir.iter().collect();
    ordered.sort_by(|a, b| {
        a.at_ms
            .total_cmp(&b.at_ms)
            .then_with(|| a.kind.cmp(&b.kind))
    });

    let mut b = AgentBoundaries::default();
    let mut last_hb: Option<(f64, i64, i64)> = None;
    for ev in ordered {
        let (dropped, oversized) = (ev.dropped.unwrap_or(0), ev.oversized.unwrap_or(0));
        match ev.kind.as_str() {
            "agent_start" => {
                b.start_at_ms = Some(b.start_at_ms.map_or(ev.at_ms, |v| v.min(ev.at_ms)));
            }
            "heartbeat" => last_hb = Some((ev.at_ms, dropped, oversized)),
            "agent_stop" => {
                b.stop_at_ms = Some(b.stop_at_ms.map_or(ev.at_ms, |v| v.max(ev.at_ms)));
                if let Some((hb_at, hb_d, hb_o)) = last_hb {
                    if ev.at_ms >= hb_at && (dropped > hb_d || oversized > hb_o) {
                        b.stop_span_dropped = true;
                    }
                }
            }
            // Unknown lifecycle kinds land here and are ignored rather than fatal — the reader's
            // own rule, and this fold has no bracket to build from one it does not model.
            _ => {}
        }
    }
    b
}

impl UncertaintyModel {
    /// Engine-domain extents of a source's drop windows — the NARROW affected-records
    /// extents classification taint consults (coverage uses the wide bracket instead).
    fn mapped_drop_spans(&self, source: &str) -> Vec<UncertaintySpan> {
        self.spans
            .iter()
            .filter(|s| s.source == source && s.is_drop_window())
            .map(|span| UncertaintySpan {
                start_ms: span.start_at_ms - self.delta_skew_ms,
                end_ms: span.end_at_ms + self.delta_skew_ms,
            })
            .collect()
    }

    /// Classification taint for a record of `instance`: membership of
    /// the record's own `at_ms` in a drop window's skew-widened bracket. A function of
    /// the bytes — identical for every reader of them — and conservative at the edges:
    /// the skew widens the window, so clock disagreement over-taints toward
    /// `indeterminate` and never manufactures a false `cold`.
    #[must_use]
    pub fn is_record_tainted(&self, instance: &str, at_ms: f64) -> bool {
        self.spans
            .iter()
            .filter(|s| s.source == instance && s.is_drop_window())
            .any(|s| {
                at_ms >= s.start_at_ms - self.delta_skew_ms
                    && at_ms <= s.end_at_ms + self.delta_skew_ms
            })
    }

    /// This model with `silences`: each `(instance, at)` where the instance's heartbeats stopped
    /// past its liveness bound, which closes every residency on it there.
    #[must_use]
    pub fn with_silences(mut self, mut silences: Vec<(String, f64)>) -> Self {
        silences.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        self.silences = silences;
        self
    }

    /// The silences starting in `[from, to)`.
    pub fn silences_in(&self, from: f64, to: f64) -> impl Iterator<Item = (String, f64)> + '_ {
        self.silences
            .iter()
            .filter(move |(_, at)| *at >= from && *at < to)
            .cloned()
    }

    /// Own-instance lookback taint (class 5's first clause): does one of THIS
    /// source's drop windows intersect the given engine-domain range (closed overlap)?
    /// The absence claim "no prior close within the examined history" is about that entire
    /// range — a drop window anywhere in it could have swallowed the deciding evict.
    #[must_use]
    pub fn own_instance_range_taint(&self, instance: &str, start_ms: f64, end_ms: f64) -> bool {
        self.mapped_drop_spans(instance)
            .iter()
            .any(|m| ranges_intersect(start_ms, end_ms, m.start_ms, m.end_ms))
    }

    /// Cross-instance taint (class 5's second clause): does any OTHER source's drop
    /// window intersect the given engine-domain range (closed overlap)?
    #[must_use]
    pub fn cross_instance_taint(&self, exclude: &str, start_ms: f64, end_ms: f64) -> bool {
        let sources: BTreeSet<&str> = self
            .spans
            .iter()
            .filter(|s| s.is_drop_window() && s.source != exclude)
            .map(|s| s.source.as_str())
            .collect();
        sources.into_iter().any(|source| {
            self.mapped_drop_spans(source)
                .iter()
                .any(|m| ranges_intersect(start_ms, end_ms, m.start_ms, m.end_ms))
        })
    }

    /// The spans the completeness clock grades as drop windows, for the degradation
    /// ledger to cite (spec 5.4's second detected-gap class lives here; the ledger is the
    /// single surface figures consult).
    pub fn drop_windows(&self) -> impl Iterator<Item = &HeartbeatSpan> {
        self.spans.iter().filter(|s| s.is_drop_window())
    }

    /// The observation spans themselves, read-only — the capacity register's
    /// pair-coverage basis intersects them per instance pair. The field
    /// stays private because construction owns the two-clock discipline; reading the
    /// finished spans carries no such hazard.
    #[must_use]
    pub fn observed_spans(&self) -> &[HeartbeatSpan] {
        &self.spans
    }

    /// The observed-coverage figure over the model's window for `sources` (default: every
    /// source with heartbeat or cache evidence), sorted and deduplicated.
    ///
    /// The per-source arithmetic is the coverage module's fold
    /// ([`crate::observed_coverage::ObservedCoverage`]), driven for every source at once
    /// through the framework's one driver; the window's figure is assembled from the merged
    /// cells ([`Coverage::of_window`]).
    #[must_use]
    #[allow(clippy::zero_sized_map_values)] // a stateless fact's carried map is exactly that
    pub fn coverage(&self, sources: Option<&[String]>) -> Coverage {
        let source_set: BTreeSet<String> = if let Some(s) = sources {
            s.iter().cloned().collect()
        } else {
            let mut set = self.evidence_sources.clone();
            set.extend(self.spans.iter().map(|s| s.source.clone()));
            set
        };
        let mut grouped: BTreeMap<String, Vec<HeartbeatSpan>> =
            source_set.into_iter().map(|s| (s, Vec::new())).collect();
        for span in &self.spans {
            if let Some(group) = grouped.get_mut(&span.source) {
                group.push(span.clone());
            }
        }
        let ctx = CoverageCtx {
            delta_skew_ms: self.delta_skew_ms,
            boundaries: &self.boundaries,
        };
        let out = run_window(
            &ObservedCoverage,
            grouped,
            &BTreeMap::new(),
            self.window,
            &ctx,
        );
        Coverage::of_window(out.cells, &self.window)
    }
}

impl UncertaintyModel {
    /// Each source the model knows, with its heartbeat spans in the model's order.
    fn spans_by_source(&self) -> BTreeMap<String, Vec<HeartbeatSpan>> {
        let mut grouped: BTreeMap<String, Vec<HeartbeatSpan>> = self
            .evidence_sources
            .iter()
            .map(|s| (s.clone(), Vec::new()))
            .collect();
        for span in &self.spans {
            grouped
                .entry(span.source.clone())
                .or_default()
                .push(span.clone());
        }
        grouped
    }

    /// The instant through which every source's coverage is settled at the model's window end:
    /// a source's tail after its last heartbeat is uncertain until a later heartbeat attests
    /// it, so the edge is the earliest tail among the sources heard from within `grace_ms` of
    /// the window's end. A source a stop closes, one silent past the grace and one never heard
    /// from hold nothing back: what is uncertain of them stays so.
    #[must_use]
    pub fn coverage_settled_to(&self, grace_ms: f64) -> f64 {
        let end = self.window.end_ms;
        self.spans_by_source()
            .values()
            .filter_map(|spans| spans.iter().map(|s| s.end_at_ms).reduce(f64::max))
            .filter(|last| {
                let closed = self.boundaries.stop_at_ms.is_some_and(|s| s >= *last)
                    && !self.boundaries.stop_span_dropped;
                !closed && end - last <= grace_ms
            })
            .map(|last| last - self.delta_skew_ms)
            .fold(end, f64::min)
    }

    /// Every source's coverage of `window` as cells that sum, each source judged against its
    /// whole span history in the model, so a window cut in two sums to the window whole. The
    /// caller keeps `window` inside [`Self::coverage_settled_to`].
    #[must_use]
    pub fn coverage_sums(&self, window: Window) -> crate::coverage_sums::CoverageSums {
        let mut sums = crate::coverage_sums::CoverageSums::default();
        for (source, spans) in self.spans_by_source() {
            crate::module::Mergeable::merge(&mut sums, self.fold_source(&source, &spans, window));
        }
        sums
    }

    /// [`Self::coverage_sums`] for `source` alone; nothing for a source the model does not
    /// hold.
    #[must_use]
    pub fn source_coverage_sums(
        &self,
        source: &str,
        window: Window,
    ) -> crate::coverage_sums::CoverageSums {
        let spans: Vec<HeartbeatSpan> = self
            .spans
            .iter()
            .filter(|s| s.source == source)
            .cloned()
            .collect();
        if spans.is_empty() && !self.evidence_sources.contains(source) {
            return crate::coverage_sums::CoverageSums::default();
        }
        self.fold_source(source, &spans, window)
    }

    /// One source's coverage of `window` as cells that sum, from its spans.
    fn fold_source(
        &self,
        source: &str,
        spans: &[HeartbeatSpan],
        window: Window,
    ) -> crate::coverage_sums::CoverageSums {
        let ctx = crate::observed_coverage::CoverageCtx {
            delta_skew_ms: self.delta_skew_ms,
            boundaries: &self.boundaries,
        };
        let folded = crate::module::DerivationModule::fold(
            &crate::observed_coverage::ObservedCoverage,
            &source.to_string(),
            spans,
            (),
            window,
            &ctx,
        );
        let uncertain = folded
            .cells
            .uncertain
            .get(source)
            .cloned()
            .unwrap_or_default();
        crate::coverage_sums::CoverageSums::of_source(source, window, &uncertain)
    }
}

/// The same coverage arithmetic, over one UTC day's interval instead of the window's.
///
/// **Exact, not apportioned.** The uncertainty spans have real time extents, so a span that
/// straddles midnight is CUT at the boundary and each day charged for the part inside it —
/// never given a share of the whole in proportion to anything. Slicing the union and unioning
/// the slices give the same total length, which is why the days sum back to
/// `per_source_covered_ms` exactly rather than approximately, and why the assertion in the
/// tests below can be an equality.
///
/// The denominator is the day's own intersection with the window, so a caller that hands in a
/// window straddling a day boundary gets an honest partial day rather than a full day's worth
/// of denominator it never observed. That is how an open day is stated: through the covered
/// edge, its cells restated as the edge moves on.
pub(crate) fn per_day_coverage(
    w: &Window,
    source_set: &[String],
    uncertain: &[(String, UncertaintySpan)],
) -> Vec<DailyCoverage> {
    let mut out = Vec::new();
    let mut day = (w.start_ms / DAY_MS).floor();
    while day * DAY_MS < w.end_ms {
        let day_ms = day * DAY_MS;
        day += 1.0;
        let clipped = Window {
            start_ms: day_ms.max(w.start_ms),
            end_ms: (day_ms + DAY_MS).min(w.end_ms),
        };
        let window_ms = clipped.length_ms();
        if window_ms <= 0.0 {
            continue;
        }
        for source in source_set {
            let spans: Vec<UncertaintySpan> = uncertain
                .iter()
                .filter(|(s, _)| s == source)
                .filter_map(|(_, span)| clip(span.start_ms, span.end_ms, &clipped))
                .collect();
            out.push(DailyCoverage {
                day_ms,
                source: source.clone(),
                covered_ms: (window_ms - union_length(spans)).max(0.0),
                window_ms,
            });
        }
    }
    out
}

pub(crate) fn clip(start: f64, end: f64, w: &Window) -> Option<UncertaintySpan> {
    let s = start.max(w.start_ms);
    let e = end.min(w.end_ms);
    (s < e).then_some(UncertaintySpan {
        start_ms: s,
        end_ms: e,
    })
}

pub(crate) fn union_length(mut spans: Vec<UncertaintySpan>) -> f64 {
    spans.sort_by(|a, b| a.start_ms.total_cmp(&b.start_ms));
    let mut total = 0.0;
    let mut cur: Option<(f64, f64)> = None;
    for s in spans {
        match &mut cur {
            Some((_, e)) if s.start_ms <= *e => *e = e.max(s.end_ms),
            _ => {
                if let Some((cs, ce)) = cur {
                    total += ce - cs;
                }
                cur = Some((s.start_ms, s.end_ms));
            }
        }
    }
    if let Some((cs, ce)) = cur {
        total += ce - cs;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::{per_day_coverage, ranges_intersect, union_length, UncertaintySpan};
    use crate::{Window, DAY_MS};

    fn span(start_ms: f64, end_ms: f64) -> UncertaintySpan {
        UncertaintySpan { start_ms, end_ms }
    }

    #[test]
    fn the_taint_overlap_is_closed_so_point_extents_still_taint() {
        // E2/E7 pin single-record drop extents (point spans): a deciding range touching
        // one must taint — closed overlap, the conservative direction.
        assert!(ranges_intersect(0.0, 100.0, 100.0, 100.0)); // touch at end
        assert!(ranges_intersect(100.0, 100.0, 50.0, 150.0)); // point inside
        assert!(!ranges_intersect(0.0, 99.0, 100.0, 100.0));
        assert!(!ranges_intersect(101.0, 200.0, 100.0, 100.0));
    }

    #[test]
    fn a_span_straddling_midnight_is_cut_at_it_and_the_days_sum_to_the_window_figure() {
        // The claim the daily figure rests on: uncertainty spans have real time extents, so
        // each day is charged for the part of the span INSIDE it. An apportioned reading —
        // splitting the whole span's length across the days it touches — would put uncertainty
        // on a day that was fully observed.
        let w = Window {
            start_ms: 0.0,
            end_ms: 2.0 * DAY_MS,
        };
        let sources = vec!["i1".to_string(), "quiet".to_string()];
        let uncertain = vec![
            // straddles midnight: 1000 ms in day 1, 3000 ms in day 2
            ("i1".to_string(), span(DAY_MS - 1000.0, DAY_MS + 3000.0)),
            // overlaps the tail of the first inside day 2 — the union must count the
            // overlap once, per day, exactly as the window figure does
            ("i1".to_string(), span(DAY_MS + 2000.0, DAY_MS + 5000.0)),
        ];
        let rows = per_day_coverage(&w, &sources, &uncertain);
        assert_eq!(rows.len(), 4, "two days x two sources: {rows:?}");

        let get = |day: f64, source: &str| {
            rows.iter()
                .find(|r| (r.day_ms - day).abs() < f64::EPSILON && r.source == source)
                .unwrap_or_else(|| panic!("no row for {day} {source}"))
        };
        assert!((get(0.0, "i1").covered_ms - (DAY_MS - 1000.0)).abs() < 1e-9);
        assert!((get(DAY_MS, "i1").covered_ms - (DAY_MS - 5000.0)).abs() < 1e-9);
        // a source with no uncertainty at all is fully covered on both days, and still gets a
        // row per day: a trend point with no coverage row would read as a gap
        assert!((get(0.0, "quiet").covered_ms - DAY_MS).abs() < 1e-9);
        assert!((get(DAY_MS, "quiet").covered_ms - DAY_MS).abs() < 1e-9);

        // and the exactness claim, stated as an equality because it is one: slicing the union
        // and unioning the slices produce the same total length
        let whole = w.length_ms()
            - union_length(
                uncertain
                    .iter()
                    .filter(|(s, _)| s == "i1")
                    .map(|(_, sp)| *sp)
                    .collect(),
            );
        let summed: f64 = rows
            .iter()
            .filter(|r| r.source == "i1")
            .map(|r| r.covered_ms)
            .sum();
        assert!(
            (summed - whole).abs() < 1e-9,
            "the days must account for the window figure exactly: {summed} vs {whole}"
        );
    }

    #[test]
    fn a_window_that_is_not_day_aligned_states_the_partial_days_denominator() {
        // An open day stated through the covered edge: the figure is honest about the part of
        // the day it states rather than claiming a full day's denominator it never observed.
        let w = Window {
            start_ms: DAY_MS - 1000.0,
            end_ms: DAY_MS + 2000.0,
        };
        let rows = per_day_coverage(&w, &["i1".to_string()], &[]);
        assert_eq!(rows.len(), 2, "two days touched: {rows:?}");
        assert!((rows[0].window_ms - 1000.0).abs() < 1e-9);
        assert!((rows[1].window_ms - 2000.0).abs() < 1e-9);
    }

    #[test]
    fn union_length_merges_overlaps() {
        let spans = vec![
            UncertaintySpan {
                start_ms: 0.0,
                end_ms: 10.0,
            },
            UncertaintySpan {
                start_ms: 5.0,
                end_ms: 20.0,
            },
            UncertaintySpan {
                start_ms: 30.0,
                end_ms: 40.0,
            },
        ];
        let total = union_length(spans);
        assert!((total - 30.0).abs() < 1e-9);
    }
}
