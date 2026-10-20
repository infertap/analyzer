// SPDX-License-Identifier: AGPL-3.0-only

//! Staleness and anomaly (spec 5.8) — the fleet-health join.
//! **Wall domain only**: computed exclusively from lifecycle facts,
//! per-endpoint heartbeat sub-stats, and their timestamps; no engine-domain figure
//! enters.
//!
//! Staleness is judged **per producer run** (`incarnation` — one tap run has one
//! declaration set for its whole life), and the threshold rests on the DECLARED bounds:
//! spec 5.8 makes `heartbeat_secs`/`max_segment_secs` from every `segment_open` the
//! entire basis on which a reader may call a producer stale, sets the line at the LARGER of
//! the two, and leaves only the allowance beyond it to the implementation. This
//! reader's allowance is the platform constant [`STALENESS_GRACE_MS`]: the threshold is
//! `max(larger declared bound, STALENESS_GRACE_MS)` — the grace may WIDEN the threshold
//! (a producer on the shipped 5-minute cadence gets six missed beats before an alarm),
//! never tighten it below the declared bound, so a producer legally declaring a slower
//! cadence is still never mis-graded. One platform number every operator can state beats a
//! per-run allowance derived from observed spacing, which nobody could predict; a producer
//! heartbeating slower than it declared is covered by the constant being far above the declared
//! cadence.
//!
//! Four verdicts per run ([`RunStaleness`]):
//! - **live** — silence within the threshold at the horizon.
//! - **departed** — the run's last record is an `agent_stop`: an announced departure,
//!   at any later horizon (spec 2.4).
//! - **unexplained** — silence past the threshold with no stop; the absence of the
//!   stop record is the signal (spec 2.4).
//! - **no basis** — no declaration observed for the run: unobserved declarations are
//!   outside the stream's knowledge, so no staleness verdict can be reached, however
//!   long the silence. Inventing a threshold from observed cadence here would grade a
//!   coverage gap as an anomaly.
//!
//! Beside the per-run verdicts, **observed-idle-or-stalled** stays per SOURCE:
//! heartbeats current but the endpoint's `last_msg_at_ms` silent beyond the cadence
//! threshold (or absent entirely: nothing seen yet). Never "healthy": the tap cannot
//! distinguish a healthy idle engine from a wedged one, and the report says exactly
//! that. This one KEEPS its observed-cadence basis (`IDLE_CADENCE_K` × median
//! spacing): it compares an endpoint's silence against the same run's own heartbeat
//! rhythm, a ratio between two observed streams — a wall constant has no meaning there.
//!
//! Batch-posture reference point: the **observation horizon** — the latest wall
//! timestamp in the fetched lifecycle stream. Staleness is relative to observed
//! knowledge: a wholly-stale archive shows no anomaly (correct — there is no evidence
//! of missing time); the server posture substitutes real now at its tier. The sharpest
//! instance of that rule: a single-agent crash at end-of-stream (agent dies without a
//! stop; horizon equals its own last heartbeat) is **structurally invisible to batch
//! health — deliberately**: end-of-file is indistinguishable from end-of-capture (the
//! file may simply have been copied mid-run). The accounting is three-layered and never
//! silently fine: invisible here by the horizon rule; carried as **tail-uncertainty in
//! coverage** (completeness marks last-heartbeat-to-window-end uncertain absent a stop,
//! so the crash costs coverage on every number); caught outright at the server
//! posture's real now.

use std::collections::BTreeMap;

/// The platform staleness grace: the implementation-defined allowance beyond the larger
/// declared bound (spec 5.8) before silence becomes `Unexplained`. 30 minutes — six
/// missed beats of the shipped 5-minute heartbeat cadence. One number, stated in the
/// contract and in `docs/conformance.md`, judged at read time against the serve
/// posture's real now (batch posture keeps the observation horizon).
pub const STALENESS_GRACE_MS: f64 = 1_800_000.0;

/// Missed-cadence multiplier for the per-source idle-or-stalled report — the one
/// judgment that keeps an observed-cadence basis, because it is a ratio between two
/// streams of the same run rather than a wall-clock silence.
const IDLE_CADENCE_K: f64 = 3.0;

/// The threshold in force for a run with the given declared bound: the larger declared
/// bound widened by the platform grace, or `None` where nothing was declared —
/// [`RunStaleness::NoBasis`], never an invented line. Public so the roster can state
/// the number it judged by next to the verdict.
#[must_use]
pub fn run_threshold_ms(larger_bound_ms: Option<f64>) -> Option<f64> {
    larger_bound_ms.map(|b| b.max(STALENESS_GRACE_MS))
}

/// The staleness verdict for one producer run, judged at the observation horizon.
///
/// The vocabulary is the reader corpus's (`conformance/reader/staleness_*`): these four
/// are what a conforming reader may conclude from spec 5.8 and spec 2.4, and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStaleness {
    /// Within the threshold at the horizon.
    Live,
    /// The run's last record is an `agent_stop`: an announced, expected departure.
    Departed,
    /// Silence past the larger declared bound (plus the cadence allowance) with no
    /// stop — the absence of the stop record is the signal.
    Unexplained,
    /// No declaration observed: the declared bounds are the entire basis for the staleness
    /// verdict, so none can be reached.
    NoBasis,
}

/// The fleet's liveness over a window: departures, idle-or-stalled producers, agent and
/// instance counts.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FleetHealth {
    /// Runs judged [`RunStaleness::Departed`]: the last record is an `agent_stop`.
    pub expected_departures: u64,
    /// Runs judged [`RunStaleness::Unexplained`]: silent past the threshold with no stop.
    pub unexplained_departures: u64,
    /// Sources whose heartbeats are current but whose endpoint's `last_msg_at_ms` is silent
    /// past `IDLE_CADENCE_K` × the median heartbeat spacing, or absent entirely. Never "healthy".
    pub observed_idle_or_stalled: Vec<String>,
    /// Fleet-level reading: 1 when any lifecycle record was observed, else 0 — a deliberate
    /// floor, not a count. Runs are graded individually by [`run_verdicts_from_events`]; this
    /// is the coarse "was any producer heard from at all" bit.
    pub agents: u64,
    /// Distinct sources any heartbeat's endpoint sub-stats named, flagged or not.
    pub instances: u64,
}

struct SourceObs {
    hb_at: Vec<f64>,
    last_msg: Option<f64>,
}

#[derive(Default)]
struct RunObs {
    last_record: f64,
    stop_at: Option<f64>,
    /// The larger declared bound in milliseconds, from the run's `agent_start` —
    /// `None` when no declaration was observed.
    larger_bound_ms: Option<f64>,
}

fn median_spacing(sorted_at: &[f64]) -> Option<f64> {
    if sorted_at.len() < 2 {
        return None;
    }
    let mut gaps: Vec<f64> = sorted_at.windows(2).map(|p| p[1] - p[0]).collect();
    gaps.sort_by(f64::total_cmp);
    Some(gaps[gaps.len() / 2])
}

/// Per-run staleness verdicts, folded from the lifecycle IR — the store-free path.
///
/// **The threshold rests on the DECLARED bounds, widened by the platform grace and by nothing
/// else.** Spec 5.8 makes `heartbeat_secs`/`max_segment_secs` from `agent_start` the entire
/// basis on which a reader may call a producer stale; the allowance beyond it is this reader's,
/// and it is the constant [`STALENESS_GRACE_MS`]. A grace that would TIGHTEN the line below the
/// declaration is impossible by construction — the threshold is the max of the two.
///
/// **Unknown lifecycle kinds are skipped rather than fatal.** The IR carries the delivery
/// families beside the liveness ones, and skipping is the reader's own rule for a kind it does
/// not model.
#[must_use]
pub fn run_verdicts_from_events(
    ir: &[crate::events::LifecycleEvent],
) -> BTreeMap<String, RunStaleness> {
    let mut runs: BTreeMap<String, RunObs> = BTreeMap::new();
    let mut horizon = f64::NEG_INFINITY;

    for ev in ir
        .iter()
        .filter(|e| matches!(e.kind.as_str(), "agent_start" | "heartbeat" | "agent_stop"))
    {
        horizon = horizon.max(ev.at_ms);
        let obs = runs.entry(ev.incarnation.clone()).or_default();
        obs.last_record = obs.last_record.max(ev.at_ms);
        if ev.kind == "agent_stop" {
            obs.stop_at = Some(obs.stop_at.map_or(ev.at_ms, |v: f64| v.max(ev.at_ms)));
        }
    }

    // **The declared bounds, read separately from the observations.** They ride every
    // `segment_open` (spec 5.8), so a run is judgeable from any segment of it rather than only
    // from the one that happened to carry its start record.
    //
    // This scan is deliberately not folded into the loop above. Whether a `segment_open` also
    // counts as EVIDENCE the producer was alive is a real question — it is a record, so it is —
    // but answering it here would change what `last_record` and the horizon mean while the
    // subject of this pass is only where a declaration comes from. Separate question, separate
    // change.
    for ev in ir.iter().filter(|e| e.kind == "segment_open") {
        // Both bounds or neither: a declaration missing half of itself states no line.
        if let (Some(hb), Some(seg)) = (ev.heartbeat_secs, ev.max_segment_secs) {
            let obs = runs.entry(ev.incarnation.clone()).or_default();
            #[allow(clippy::cast_precision_loss)] // declared seconds are small integers
            let larger = (hb.max(seg) as f64) * 1000.0;
            obs.larger_bound_ms = Some(obs.larger_bound_ms.map_or(larger, |v: f64| v.max(larger)));
        }
    }

    runs.into_iter()
        .map(|(run, obs)| {
            (
                run,
                verdict(obs.last_record, obs.stop_at, obs.larger_bound_ms, horizon),
            )
        })
        .collect()
}

/// The spec 5.8 decision itself, shared by the raw path and the arrivals path so the two can
/// never drift into two readers: departed beats everything where the stop closes the record
/// stream; a declared bound grades against the reference instant widened by the platform
/// grace; no declaration is NO BASIS, never a default to Live. `reference_ms` is the
/// observation horizon at batch posture and real now at serve posture.
fn verdict(
    last_record: f64,
    stop_at: Option<f64>,
    larger_bound_ms: Option<f64>,
    reference_ms: f64,
) -> RunStaleness {
    if stop_at.is_some_and(|s| s >= last_record) {
        RunStaleness::Departed
    } else if let Some(threshold_ms) = run_threshold_ms(larger_bound_ms) {
        if reference_ms - last_record > threshold_ms {
            RunStaleness::Unexplained
        } else {
            RunStaleness::Live
        }
    } else {
        RunStaleness::NoBasis
    }
}

/// One run's arrival-grain liveness facts — the ingest-derived summary of what
/// [`run_verdicts_from_events`] folds from the raw stream (facts at
/// ingest, verdicts at read). The threshold is the declared bound widened by
/// [`STALENESS_GRACE_MS`], and both facts ride here.
#[derive(Debug, Clone, PartialEq)]
pub struct RunFacts {
    /// The producer run these facts describe: one tap run, one declaration set for its life.
    pub incarnation: String,
    /// Producer clock, liveness kinds only — the same vocabulary as the raw fold.
    pub last_record_ms: f64,
    /// The run's latest `agent_stop`, producer clock; `None` where no stop was observed.
    pub stop_at_ms: Option<f64>,
    /// The larger declared bound in milliseconds; `None` where no header declared both halves.
    pub larger_bound_ms: Option<f64>,
}

/// Per-run staleness verdicts from arrival-grain facts — the read-time path.
///
/// **`now_ms` is the posture switch** (module doc: "the server posture substitutes real
/// now at its tier"). `Some(now)` is the SERVE posture: silence is judged against the
/// wall clock, so a fleet that went dark reads as dark — the answer an operator's
/// console needs. `None` is the BATCH posture: the reference falls back to the fleet's
/// own knowledge horizon (max last record across runs), exactly as the raw fold computes
/// it, so an archive read later reaches the same verdicts.
#[must_use]
pub fn run_verdicts_from_facts(
    facts: &[RunFacts],
    now_ms: Option<f64>,
) -> BTreeMap<String, RunStaleness> {
    let reference = now_ms.unwrap_or_else(|| {
        facts
            .iter()
            .map(|f| f.last_record_ms)
            .fold(f64::NEG_INFINITY, f64::max)
    });
    facts
        .iter()
        .map(|f| {
            (
                f.incarnation.clone(),
                verdict(f.last_record_ms, f.stop_at_ms, f.larger_bound_ms, reference),
            )
        })
        .collect()
}

/// Fleet health, folded from the lifecycle IR — the store-free path.
///
/// **Graded against the OBSERVATION horizon rather than real now**, exactly as the engine path is:
/// staleness is relative to observed knowledge, so a wholly-stale archive shows no anomaly. The
/// server posture substitutes real now at its own tier.
///
/// **`observed_idle_or_stalled` is never "healthy".** Heartbeats current but the endpoint silent
/// means the tap cannot distinguish a healthy idle engine from a wedged one, and neither can this.
#[must_use]
pub fn fleet_health_from_events(ir: &[crate::events::LifecycleEvent]) -> FleetHealth {
    // Per SOURCE, not per run: the ARRAY JOIN this replaces fans one heartbeat across every
    // endpoint it carries, so a heartbeat covering three sources is an observation of each.
    let mut sources: BTreeMap<String, SourceObs> = BTreeMap::new();
    for ev in ir.iter().filter(|e| e.kind == "heartbeat") {
        for ep in &ev.endpoints {
            let entry = sources.entry(ep.source.clone()).or_insert(SourceObs {
                hb_at: Vec::new(),
                last_msg: None,
            });
            entry.hb_at.push(ev.at_ms);
            if let Some(stamp) = ep.last_msg_at_ms {
                entry.last_msg = Some(entry.last_msg.map_or(stamp, |v: f64| v.max(stamp)));
            }
        }
    }
    for obs in sources.values_mut() {
        obs.hb_at.sort_by(f64::total_cmp);
    }

    let verdicts = run_verdicts_from_events(ir);
    let horizon = sources
        .values()
        .flat_map(|o| o.hb_at.iter().copied())
        .fold(f64::NEG_INFINITY, f64::max);

    let mut health = FleetHealth {
        agents: u64::from(!verdicts.is_empty()),
        instances: sources.len() as u64,
        ..FleetHealth::default()
    };
    for verdict in verdicts.values() {
        match verdict {
            RunStaleness::Departed => health.expected_departures += 1,
            RunStaleness::Unexplained => health.unexplained_departures += 1,
            RunStaleness::Live | RunStaleness::NoBasis => {}
        }
    }
    for (source, obs) in &sources {
        let last_hb = obs.hb_at.last().copied().unwrap_or(f64::NEG_INFINITY);
        let Some(cadence) = median_spacing(&obs.hb_at) else {
            continue; // cadence unobservable: counted, never flagged
        };
        let threshold = IDLE_CADENCE_K * cadence;
        if horizon - last_hb <= threshold && obs.last_msg.is_none_or(|m| last_hb - m > threshold) {
            health.observed_idle_or_stalled.push(source.clone());
        }
    }
    health
}

#[cfg(test)]
mod tests {
    use super::median_spacing;

    #[test]
    fn cadence_is_the_median_observed_spacing() {
        assert_eq!(median_spacing(&[0.0]), None);
        assert_eq!(median_spacing(&[0.0, 60.0]), Some(60.0));
        // one long outlier gap does not drag the cadence (median, not mean)
        assert_eq!(median_spacing(&[0.0, 60.0, 120.0, 500.0]), Some(60.0));
    }
}

#[cfg(test)]
mod facts_tests {
    use super::{run_verdicts_from_facts, RunFacts, RunStaleness, STALENESS_GRACE_MS};

    fn facts(name: &str, last: f64, stop: Option<f64>, bound_ms: Option<f64>) -> RunFacts {
        RunFacts {
            incarnation: name.into(),
            last_record_ms: last,
            stop_at_ms: stop,
            larger_bound_ms: bound_ms,
        }
    }

    /// The four spec 5.8 verdicts from arrival facts, each placed decisively past or inside
    /// the grace — the same shared decision the raw path runs, so this pins the facts
    /// path's plumbing, not new law.
    #[test]
    fn the_four_verdicts_from_facts() {
        let now = 10_000_000.0;
        let rows = [
            // Last heard 10 min ago with a 60 s bound: inside the grace, live.
            facts("live", now - 600_000.0, None, Some(60_000.0)),
            // Silent twice the grace with no stop: unexplained.
            facts("gone", now - 2.0 * STALENESS_GRACE_MS, None, Some(60_000.0)),
            // Same silence, but the stop record closes the stream: departed.
            let_stop(now),
            // No declared bounds at all: NO BASIS — never a default to Live.
            facts("unbounded", now - 2.0 * STALENESS_GRACE_MS, None, None),
        ];
        let v = run_verdicts_from_facts(&rows, Some(now));
        assert_eq!(v["live"], RunStaleness::Live);
        assert_eq!(v["gone"], RunStaleness::Unexplained);
        assert_eq!(v["stopped"], RunStaleness::Departed);
        assert_eq!(v["unbounded"], RunStaleness::NoBasis);
    }

    fn let_stop(now: f64) -> RunFacts {
        let at = now - 2.0 * STALENESS_GRACE_MS;
        facts("stopped", at, Some(at), Some(60_000.0))
    }

    /// The grace may only WIDEN the declared bound, never tighten it: a bound larger
    /// than the grace stays the line, and a bound smaller than the grace inherits it.
    #[test]
    fn the_grace_widens_and_never_tightens() {
        let now = 100_000_000.0;
        // Declared bound 2 h, silent 1 h: past the grace, inside the declaration — live.
        let wide = facts("wide", now - 3_600_000.0, None, Some(7_200_000.0));
        // Declared bound 60 s, silent 29 min: past the declaration, inside the grace — live.
        let graced = facts("graced", now - 1_740_000.0, None, Some(60_000.0));
        // Declared bound 60 s, silent 31 min: past both — unexplained.
        let gone = facts("gone", now - 1_860_000.0, None, Some(60_000.0));
        let v = run_verdicts_from_facts(&[wide, graced, gone], Some(now));
        assert_eq!(
            v["wide"],
            RunStaleness::Live,
            "the declaration is the floor"
        );
        assert_eq!(v["graced"], RunStaleness::Live, "widened by the grace");
        assert_eq!(v["gone"], RunStaleness::Unexplained);
    }

    /// Batch posture: no `now` means the fleet's own knowledge horizon is the reference,
    /// so a wholly-stale archive read later reaches the same verdicts it always did.
    #[test]
    fn without_now_the_horizon_is_the_reference() {
        // Everything ancient, but the horizon-setter is only 10 min ahead: live.
        let old = facts("old", 1_000_000.0, None, Some(60_000.0));
        let horizon = facts("edge", 1_600_000.0, None, Some(60_000.0));
        let v = run_verdicts_from_facts(&[old, horizon], None);
        assert_eq!(v["old"], RunStaleness::Live);
        assert_eq!(v["edge"], RunStaleness::Live);
    }
}
