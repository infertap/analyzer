// SPDX-License-Identifier: AGPL-3.0-only

//! The disk-pressure ladder.
//!
//! A pure function from a self-health sample to a decision, so every rung is testable
//! without a server, a disk, or a fixture. The loop only executes what this returns.
//!
//! **The rule this file exists to hold: backpressure applies to intake, never to
//! reclamation.** Merges, TTL enforcement, mutation drain and health emission run at every
//! rung. A daemon that throttled the only mechanism that frees space would convert a full
//! disk into a permanent one.

use crate::memory::GIB;
use analyzer_serving_clickhouse::selfhealth::{
    metric, SelfHealth, DISK_FREE_FRACTION_CRITICAL, DISK_FREE_FRACTION_WARN,
};

/// What the loop may do this cycle.
///
/// Ordered by severity, but deliberately NOT `Ord`: `Unmeasured` is not a point on the
/// same axis as the others, and letting it compare would invite `>= Reclaim` style tests that
/// silently include it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pressure {
    /// The sample could not measure the volume — the engine did not answer. **No intake
    /// decision is possible and none is invented.** The loop has nothing to ingest INTO
    /// either, so this is a skipped cycle rather than a refusal, and the sample already
    /// carries `engine_reachable` as CRITICAL.
    Unmeasured,
    /// Free space above every line. Intake continues; reclamation is the engine's own, unprompted.
    Normal,
    /// Escalate reclamation. Intake continues.
    Reclaim,
    /// Refuse intake. Everything that frees space keeps running.
    Refuse,
}

impl Pressure {
    /// Whether this cycle may ingest. The one question the loop asks.
    #[must_use]
    pub fn allows_ingest(self) -> bool {
        matches!(self, Pressure::Normal | Pressure::Reclaim)
    }

    /// Whether reclamation should be escalated beyond what the engine does unprompted.
    #[must_use]
    pub fn wants_reclaim(self) -> bool {
        matches!(self, Pressure::Reclaim | Pressure::Refuse)
    }

    /// The rung's lowercase name, for text that must state which rung was in force.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Pressure::Unmeasured => "unmeasured",
            Pressure::Normal => "normal",
            Pressure::Reclaim => "reclaim",
            Pressure::Refuse => "refuse",
        }
    }
}

impl std::fmt::Display for Pressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A decision with the numbers that produced it, so a refusal can state its reason rather
/// than assert itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Assessment {
    /// The rung.
    pub pressure: Pressure,
    /// The sentence that states why, with the figures.
    pub reason: String,
}

/// Assess disk pressure from a sample.
///
/// `previous` carries the hysteresis: once intake is refused it stays refused until free
/// space reaches the WARN line, not merely until it crosses back over the refusal line.
/// Without it the loop would oscillate across a threshold that its own ingest moves.
#[must_use]
pub(crate) fn assess(vitals: &SelfHealth, previous: Pressure) -> Assessment {
    let (Some(free), Some(total)) = (
        vitals.value(metric::DISK_FREE_BYTES),
        vitals.value(metric::DISK_TOTAL_BYTES),
    ) else {
        return Assessment {
            pressure: Pressure::Unmeasured,
            reason: "the volume could not be measured this sample; no intake decision is \
                     invented and the cycle is skipped"
                .to_string(),
        };
    };

    // The fraction floor is the whole rule: no event grain reaches the local disk, so what
    // grows here is bounded caches, the working state and the rollups.
    let refuse_line = DISK_FREE_FRACTION_CRITICAL * total;
    let warn_line = DISK_FREE_FRACTION_WARN * total;

    let gib = |b: f64| b / GIB;

    // Hysteresis: leaving Refuse requires the WARN line, not the refusal line.
    if previous == Pressure::Refuse && free < warn_line {
        return Assessment {
            pressure: Pressure::Refuse,
            reason: format!(
                "intake still refused: {:.1} GiB free has not yet reached the {:.1} GiB \
                 resumption line. Resumption is deliberately higher than the refusal line \
                 so the loop cannot oscillate across a threshold its own ingest moves.",
                gib(free),
                gib(warn_line)
            ),
        };
    }

    if free < refuse_line {
        return Assessment {
            pressure: Pressure::Refuse,
            reason: format!(
                "INTAKE REFUSED: {:.1} GiB free is below the {:.1} GiB line ({:.0}% of the \
                 volume). \
                 Reclamation, TTL enforcement and health emission continue — only intake \
                 stops. Source files are left in place and ingest resumes where it left \
                 off IF the source keeps them: its rotation policy is the customer's, not \
                 ours, and source_files_lost_total counts any that do not survive.",
                gib(free),
                gib(refuse_line),
                DISK_FREE_FRACTION_CRITICAL * 100.0
            ),
        };
    }
    if free < warn_line {
        return Assessment {
            pressure: Pressure::Reclaim,
            reason: format!(
                "escalating reclamation: {:.1} GiB free is below the {:.1} GiB warn line. \
                 Intake is unaffected.",
                gib(free),
                gib(warn_line)
            ),
        };
    }
    Assessment {
        pressure: Pressure::Normal,
        reason: format!("{:.1} GiB free of {:.1} GiB.", gib(free), gib(total)),
    }
}

#[cfg(test)]
mod tests {
    use super::{assess, Pressure};
    use analyzer_serving_clickhouse::selfhealth::{metric, Metric, MetricKind, SelfHealth};

    const TIB: f64 = 1_099_511_627_776.0;

    fn sample(free: f64, total: f64) -> SelfHealth {
        let gauge = |name, value| Metric {
            name,
            help: "h",
            value,
            kind: MetricKind::Gauge,
        };
        let metrics = vec![
            gauge(metric::DISK_FREE_BYTES, Some(free)),
            gauge(metric::DISK_TOTAL_BYTES, Some(total)),
        ];
        SelfHealth {
            analyzer_id: None,
            fleet: "f".to_string(),
            meaning: None,
            metrics,
            checks: vec![],
        }
    }

    #[test]
    fn an_unmeasurable_volume_is_a_skipped_cycle_not_a_refusal() {
        // The engine did not answer. There is nothing to ingest INTO, so inventing a
        // refusal would be a decision made on no evidence.
        let blind = SelfHealth {
            analyzer_id: None,
            fleet: "f".to_string(),
            meaning: None,
            metrics: vec![],
            checks: vec![],
        };
        let a = assess(&blind, Pressure::Normal);
        assert_eq!(a.pressure, Pressure::Unmeasured);
        assert!(!a.pressure.allows_ingest());
    }

    #[test]
    fn the_ladder_climbs_in_order_on_a_quiet_fleet() {
        // The fraction floor is the whole rule: refuse below 10%, reclaim below the 20% warn
        // line, normal above.
        let at = |frac: f64| assess(&sample(TIB * frac, TIB), Pressure::Normal).pressure;
        assert_eq!(at(0.50), Pressure::Normal);
        assert_eq!(at(0.17), Pressure::Reclaim);
        assert_eq!(at(0.12), Pressure::Reclaim);
        assert_eq!(at(0.05), Pressure::Refuse);
        // and intake survives every rung except the last
        assert!(at(0.50).allows_ingest());
        assert!(at(0.12).allows_ingest());
        assert!(!at(0.05).allows_ingest());
    }

    #[test]
    fn every_rung_of_the_ladder_is_reachable() {
        // Asserting the rungs in order cannot catch a band collapsed to zero width: it still
        // climbs in order, it just has no interior. So this walks the whole range and requires
        // every rung to appear.
        let mut seen: Vec<Pressure> = Vec::new();
        for step in 0..=100 {
            let frac = f64::from(step) / 100.0;
            let p = assess(&sample(TIB * frac, TIB), Pressure::Normal).pressure;
            if !seen.contains(&p) {
                seen.push(p);
            }
        }
        for rung in [Pressure::Normal, Pressure::Reclaim, Pressure::Refuse] {
            assert!(
                seen.contains(&rung),
                "{rung} is unreachable — a band collapsed to zero width, so a lever the ladder \
                 declares never actually runs"
            );
        }
    }

    #[test]
    fn reclamation_is_never_throttled_at_any_rung() {
        // The one invariant this whole file exists to hold. A daemon that stopped merging
        // to save space would convert a full disk into a permanent one.
        for frac in [0.50, 0.19, 0.15, 0.05, 0.001] {
            let p = assess(&sample(TIB * frac, TIB), Pressure::Normal).pressure;
            assert_ne!(p, Pressure::Unmeasured);
            assert!(
                p == Pressure::Normal || p.wants_reclaim(),
                "reclamation must never be suppressed at {frac}"
            );
        }
    }

    #[test]
    fn refusal_resumes_at_the_warn_line_not_at_the_refusal_line() {
        // Without hysteresis the loop oscillates across a threshold its own ingest moves:
        // refuse, free a little, resume, fill it back, refuse.
        let just_above_refuse = sample(TIB * 0.12, TIB);
        // coming from Normal, 12% is above the 10% refusal line and below the 20% warn line
        assert_eq!(
            assess(&just_above_refuse, Pressure::Normal).pressure,
            Pressure::Reclaim
        );
        // coming from Refuse, the same reading STAYS refused — 12% has not reached 20%
        let held = assess(&just_above_refuse, Pressure::Refuse);
        assert_eq!(held.pressure, Pressure::Refuse);
        assert!(held.reason.contains("resumption line"), "{}", held.reason);
        // and at the warn line it releases
        assert_ne!(
            assess(&sample(TIB * 0.25, TIB), Pressure::Refuse).pressure,
            Pressure::Refuse
        );
    }
}
