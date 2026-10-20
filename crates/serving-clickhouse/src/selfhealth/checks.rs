// SPDX-License-Identifier: AGPL-3.0-only

//! The checks: each a measured figure against a labelled threshold, and the status it earns.

use super::{Check, DirStats, LoopStatus, ServerStatus, Status};

/// Dead (merged-away) bytes as a fraction of live bytes, above which reclamation is not
/// keeping up. Derived from posture, not taste: under a live server the cleanup task
/// holds this near zero, so anything approaching parity with live data means the
/// background work is not running — the batch posture's verified condition.
pub const DEAD_PART_RATIO_WARN: f64 = 0.5;

/// The WARN free-disk fraction: at or under it the disk check reads `Warning`, which still
/// leaves room to act inside one retention cycle (the CRITICAL line is below).
pub const DISK_FREE_FRACTION_WARN: f64 = 0.20;

/// The CRITICAL free-disk fraction: at or under it the disk check reads `Critical` — the
/// "refuse rather than wedge" line, since a full disk takes recovery out of the system's
/// own hands.
pub const DISK_FREE_FRACTION_CRITICAL: f64 = 0.10;

/// Parts per partition at which the check reads `Warning`. NOT invented: it is the engine's
/// own `parts_to_delay_insert` (inserts start throttling), verified at the pin; the CRITICAL line below is its `parts_to_throw_insert`. Alarming at the engine's
/// own thresholds is the point.
pub const PARTS_PER_PARTITION_WARN: f64 = 1000.0;

/// The engine's `parts_to_throw_insert`: at this many parts in one partition, inserts FAIL.
pub const PARTS_PER_PARTITION_CRITICAL: f64 = 3000.0;

/// How far past its horizon the oldest raw row may sit before TTL enforcement is
/// considered broken. Slack for merge scheduling, not for a stopped TTL: engine TTLs
/// apply during merges, so a modest overshoot is normal and a growing one is not.
pub const RAW_TTL_OVERDUE_FACTOR: f64 = 1.25;

/// Temp files older than this are orphans from a crashed run, not work in flight.
pub const ORPHAN_TEMP_AGE_WARN_SECONDS: f64 = 86_400.0;

/// Tick lag, in multiples of the deployment's own declared tick cadence, above which the
/// loop is not keeping up. Derived rather than chosen: a tick that runs longer than its
/// cadence can never catch up, so lag past ONE cadence means a cycle was missed and lag
/// past TWO means the miss was not a single slow run. The comparison is against the
/// declared cadence rather than an absolute duration because the cadence is a deployment
/// input (daemon) — an absolute threshold would alarm on a fleet that ingests hourly and
/// stay silent on one that ingests every minute.
pub const TICK_LAG_WARN_CADENCES: f64 = 2.0;

/// How many ingest cycles the envelope averages over before it will judge.
///
/// **Derived from the property that one saturated cycle must not, by itself, cross the warn
/// line.** A single cycle at 100% raises an average of `b` over `N` cycles to `b + (1 - b)/N`.
/// For that to stay under [`ENVELOPE_UTILISATION_WARN`] from a 20% baseline needs
/// `0.8/N < 0.1`, so `N > 8`. Ten is the next round number above it, and it holds the property
/// for every baseline at or below 20% — which is where a healthy deployment sits.
///
/// **Cycles rather than minutes**, so the window follows a deployment that has changed its ingest
/// cadence instead of silently becoming a different number of samples. At the default one-minute
/// cadence this is ten minutes.
///
/// Re-derived if [`ENVELOPE_UTILISATION_WARN`] moves, since the relation above is between the two.
pub const ENVELOPE_WINDOW_CYCLES: usize = 10;

/// The fraction of its own ingest capacity a deployment may consume before the envelope warns.
///
/// The fraction is of the machine's own measured ingest duty cycle.
///
/// **Dimensionless, and that is why it is portable.** A rows-per-second ceiling is a property of
/// a VM — its cores, its disk, whether the ingest path parallelises — so a constant holding one
/// would be right on the box it was measured on and wrong everywhere else. A ratio of time spent
/// to time available is the same question on every box, and needs no re-measurement when the
/// hardware or the path changes.
pub const ENVELOPE_UTILISATION_WARN: f64 = 0.30;

/// Is the fleet consuming more than ~30% of this analyzer's ingest capacity?
///
/// **Observed, never declared.** The fleet is who spoke, so the envelope watches the machine's
/// own duty cycle rather than asking an operator to declare a fleet size. A GPU count would not
/// convert honestly anyway: holders are `(instance, dp_rank, group_idx)` triples, so a 500-GPU
/// fleet has more than 500 of them, while tensor parallelism gives fewer. And a rows-per-second
/// ceiling is a property of one machine; time spent over time available asks the same question on
/// any hardware and corrects itself when the path gets faster.
///
/// **WARN, never Critical, and that is what makes an observed fence safe.** A declared one could
/// refuse at install, before anything ran. An observed one fires against a deployment that is
/// already working, and refusing there would take a customer down for the crime of growing.
pub(super) fn check_envelope(status: LoopStatus) -> Check {
    let Some(duty) = status.ingest_duty_cycle else {
        return Check {
            name: "within_envelope",
            status: Status::Unknown,
            message: "no ingest cycle has run yet in this process: utilisation cannot be \
                      observed."
                .to_string(),
        };
    };
    let percent = duty * 100.0;
    // **The figure is shown from the first steady cycle; the VERDICT waits for a full window.**
    // Averaging fewer would let one sample carry a large share of a judgement, which is the
    // flapping the window exists to prevent.
    if status.ingest_duty_samples < ENVELOPE_WINDOW_CYCLES {
        return Check {
            name: "within_envelope",
            status: Status::Unknown,
            message: format!(
                "ingest is using {percent:.0}% of this analyzer's cycle so far, averaged over \
                 {} of {ENVELOPE_WINDOW_CYCLES} cycles — provisional, and not yet judged. The \
                 first cycles after a start carry whatever accumulated while it was down.",
                status.ingest_duty_samples
            ),
        };
    }
    let limit = ENVELOPE_UTILISATION_WARN * 100.0;
    let rate = status
        .ingest_rows_per_second
        .map_or_else(String::new, |r| format!(" ({r:.0} rows/s arriving)"));
    Check {
        name: "within_envelope",
        status: if duty > ENVELOPE_UTILISATION_WARN {
            Status::Warn
        } else {
            Status::Ok
        },
        message: if duty > ENVELOPE_UTILISATION_WARN {
            format!(
                "ingest is using {percent:.0}% of this analyzer's cycle{rate} averaged over the \
                 last {ENVELOPE_WINDOW_CYCLES} cycles, past the {limit:.0}% the envelope holds. \
                 It is still keeping up; the margin for a burst is what is going. Split the fleet \
                 across analyzers, one per cluster."
            )
        } else {
            format!(
                "ingest is using {percent:.0}% of this analyzer's cycle{rate}, averaged over the \
                 last {ENVELOPE_WINDOW_CYCLES} cycles — within the envelope."
            )
        },
    }
}

/// The sampled audit agrees: every block it re-derived stated what its tick stated. A
/// disagreement is a defect in the keyed path and stays critical until the derived state is
/// derived afresh.
pub(super) fn check_audit(status: LoopStatus) -> Check {
    let (state, message) = match (status.audit_disagreed_at_ms, status.audit_blocks_total) {
        (Some(at), _) => (
            Status::Critical,
            format!(
                "a sampled block's re-derivation disagreed with what its tick stated, in the \
                 window from {at:.0} ms; the audit edge holds there. The daemon log names the \
                 block and the store. Report it, then derive afresh with reset-derived."
            ),
        ),
        (None, 0) => (
            Status::Unknown,
            "no block has been audited yet in this process.".to_string(),
        ),
        (None, n) => (
            Status::Ok,
            format!("{n} sampled block(s) re-derived, each as its tick stated it."),
        ),
    };
    Check {
        name: "classification_audit",
        status: state,
        message,
    }
}

/// The producer resolves evicts as it resolved their stores: on the sampled engine ids, an evict
/// naming another identity than its store is the silent-wrong direction and critical; one naming
/// none where the same run's store named one is a lost promise and a warning.
pub(super) fn check_evict_resolution(status: LoopStatus) -> Check {
    let (audited, mismatched, under) = (
        status.evict_audited_total,
        status.evict_mismatched_total,
        status.evict_underresolved_total,
    );
    let state = if mismatched > 0 {
        Status::Critical
    } else if under > 0 {
        Status::Warn
    } else if audited == 0 {
        Status::Unknown
    } else {
        Status::Ok
    };
    Check {
        name: "evict_resolution",
        status: state,
        message: format!(
            "{audited} sampled evict(s) graded since this process began: {mismatched} named \
             another identity than their store, {under} named none where the same run's store \
             named one. The daemon log names the instances."
        ),
    }
}

/// The loop is not keeping up if lag has passed [`TICK_LAG_WARN_CADENCES`] of its own
/// declared cadence — one missed cycle is a slow run, two is a trend.
pub(super) fn check_tick_lag(status: LoopStatus) -> Check {
    let Some(lag) = status.tick_lag_seconds else {
        return Check {
            name: "tick_cadence_kept",
            status: Status::Unknown,
            message: "no tick has completed yet in this process: cadence cannot be \
                      observed."
                .to_string(),
        };
    };
    let limit = status.tick_interval_seconds * TICK_LAG_WARN_CADENCES;
    Check {
        name: "tick_cadence_kept",
        status: if lag > limit {
            Status::Warn
        } else {
            Status::Ok
        },
        message: format!(
            "last successful tick finished {:.1} min ago against a {:.1} min cadence \
             (warn past {:.1} min). A tick that runs longer than its cadence can never \
             catch up, so a growing lag is the deployment outgrowing its schedule.",
            lag / 60.0,
            status.tick_interval_seconds / 60.0,
            limit / 60.0
        ),
    }
}

#[allow(clippy::cast_precision_loss)] // counts and byte totals; f64 is exact to 2^53
pub(super) fn f64_from(v: u64) -> f64 {
    v as f64
}

pub(super) fn check_dead_parts(ratio: f64, unreclaimed_bytes: f64) -> Check {
    let status = if ratio >= DEAD_PART_RATIO_WARN {
        Status::Warn
    } else {
        Status::Ok
    };
    Check {
        name: "dead_parts_reclaimed",
        status,
        message: format!(
            "{:.0} MiB held by merged-away parts, {ratio:.2}x the active parts (warn at \
             {DEAD_PART_RATIO_WARN:.2}x). Read from the engine's own inactive-part view, \
             which a living server keeps whole. Reclamation is background work; a ratio \
             that does not fall means it is not running.",
            unreclaimed_bytes / 1_048_576.0
        ),
    }
}

pub(super) fn check_disk(fraction: f64, free_bytes: f64) -> Check {
    let status = if fraction <= DISK_FREE_FRACTION_CRITICAL {
        Status::Critical
    } else if fraction <= DISK_FREE_FRACTION_WARN {
        Status::Warn
    } else {
        Status::Ok
    };
    Check {
        name: "disk_headroom",
        status,
        message: format!(
            "{:.1} GiB free, {:.1}% of the volume (warn at {:.0}%, critical at {:.0}%).",
            free_bytes / 1_073_741_824.0,
            fraction * 100.0,
            DISK_FREE_FRACTION_WARN * 100.0,
            DISK_FREE_FRACTION_CRITICAL * 100.0
        ),
    }
}

pub(super) fn check_parts(max_parts: f64) -> Check {
    let status = if max_parts >= PARTS_PER_PARTITION_CRITICAL {
        Status::Critical
    } else if max_parts >= PARTS_PER_PARTITION_WARN {
        Status::Warn
    } else {
        Status::Ok
    };
    Check {
        name: "parts_per_partition",
        status,
        message: format!(
            "{max_parts:.0} parts in the busiest partition (engine throttles inserts at \
             {PARTS_PER_PARTITION_WARN:.0}, refuses them at \
             {PARTS_PER_PARTITION_CRITICAL:.0} — both verified at the pin)."
        ),
    }
}

pub(super) fn check_mutations(unfinished: f64) -> Check {
    Check {
        name: "mutation_debt",
        status: if unfinished > 0.0 {
            Status::Warn
        } else {
            Status::Ok
        },
        message: format!(
            "{unfinished:.0} unfinished mutations. Mutations apply during merges, so \
             debt that does not drain means the same missing background work."
        ),
    }
}

pub(super) fn check_retention(oldest_days: Option<f64>, horizon_days: f64) -> Check {
    let Some(oldest_days) = oldest_days else {
        return Check {
            name: "raw_retention_enforced",
            status: Status::Unknown,
            message: "no residency deltas: retention enforcement cannot be observed yet."
                .to_string(),
        };
    };
    let limit = horizon_days * RAW_TTL_OVERDUE_FACTOR;
    Check {
        name: "raw_retention_enforced",
        status: if oldest_days > limit {
            Status::Warn
        } else {
            Status::Ok
        },
        message: format!(
            "oldest residency delta is {oldest_days:.2} d against a {horizon_days:.0} d \
             horizon (warn past {limit:.1} d). Engine TTLs apply during merges — an \
             overshoot that grows means they are not running."
        ),
    }
}

/// At server posture a dead engine is silent in a way a failed CLI invocation is not.
///
/// A batch invocation that fails announces itself — a non-zero exit in front of whoever
/// ran it. A daemon whose engine died keeps its own process alive, keeps answering
/// `health`, and stops ingesting: the failure mode of an airgapped deployment is not an
/// error message, it is silence. So the sensor states it, and it states it as CRITICAL
/// rather than a warning, because nothing downstream of a dead engine is true.
pub(super) fn check_server(server: ServerStatus) -> Check {
    Check {
        name: "server_alive",
        status: if server.up {
            Status::Ok
        } else {
            Status::Critical
        },
        message: if server.up {
            match server.uptime_seconds {
                Some(up) => format!("engine server up, {:.1} h since start.", up / 3600.0),
                None => "engine server up.".to_string(),
            }
        } else {
            "the engine server this analyzer owns is NOT running: ingest and analysis \
             have stopped, and every figure in this report predates the failure."
                .to_string()
        },
    }
}

pub(super) fn check_orphans(orphans: DirStats) -> Check {
    Check {
        name: "temp_hygiene",
        status: if orphans.files > 0 {
            Status::Warn
        } else {
            Status::Ok
        },
        message: format!(
            "{} orphaned temp files ({:.1} MiB) older than {:.0} h — residue from \
             interrupted runs.",
            orphans.files,
            f64_from(orphans.bytes) / 1_048_576.0,
            ORPHAN_TEMP_AGE_WARN_SECONDS / 3600.0
        ),
    }
}

#[cfg(test)]
mod envelope_tests {
    use super::{check_envelope, LoopStatus, Status, ENVELOPE_UTILISATION_WARN};

    /// A settled window by default: these tests are about the VERDICT, and a partial window has
    /// its own test below.
    fn at(duty: Option<f64>, rate: Option<f64>) -> LoopStatus {
        with_samples(duty, rate, super::ENVELOPE_WINDOW_CYCLES)
    }

    trait Envelope {
        fn status_of_envelope(self) -> Status;
        fn envelope_message(self) -> String;
    }
    impl Envelope for LoopStatus {
        fn status_of_envelope(self) -> Status {
            check_envelope(self).status
        }
        fn envelope_message(self) -> String {
            check_envelope(self).message
        }
    }

    fn with_samples(duty: Option<f64>, rate: Option<f64>, samples: usize) -> LoopStatus {
        LoopStatus {
            ticks_total: 1,
            tick_failures_total: 0,
            files_ingested_total: 0,
            bytes_ingested_total: 0,
            records_ingested_total: 0,
            ingest_backlog_events_estimate: None,
            files_projected_total: 0,
            reclaimed_bytes_total: 0,
            engine_queries_total: 0,
            last_tick_duration_seconds: None,
            tick_lag_seconds: None,
            tick_interval_seconds: 3_600.0,
            effective_retention_days: 14.0,
            ingest_rows_per_second: rate,
            ingest_duty_cycle: duty,
            ingest_duty_samples: samples,
            ingest_catchup_cycles: 0,
            ingest_backlog_files: 0,
            ingest_backlog_bytes: 0,
            ingest_refused_total: 0,
            ingest_refusing: false,
            audit_blocks_total: 0,
            audit_disagreed_at_ms: None,
            evict_audited_total: 0,
            evict_mismatched_total: 0,
            evict_underresolved_total: 0,
        }
    }

    #[test]
    fn the_verdict_follows_the_ratified_ratio_and_needs_no_ceiling_constant() {
        // **The property that makes this portable.** An earlier cut compared an arrival rate
        // against a rows/sec ceiling measured on ONE machine, which was wrong on every other box
        // and stale on that one after the ingest path was rebuilt. A ratio of time-spent to
        // time-available asks the same question on any hardware, so there is no number here that
        // a faster VM or a parallelised path would invalidate.
        assert_eq!(
            check_envelope(at(Some(ENVELOPE_UTILISATION_WARN - 0.01), None)).status,
            Status::Ok
        );
        assert_eq!(
            check_envelope(at(Some(ENVELOPE_UTILISATION_WARN + 0.01), None)).status,
            Status::Warn
        );
    }

    #[test]
    fn the_message_states_the_utilisation_and_the_arrival_rate_when_known() {
        // Two figures because they are two claims: how much of THIS machine the fleet consumes,
        // and how fast the fleet is speaking. An operator deciding whether to split a fleet needs
        // both — the first says whether to act, the second says what they are acting on.
        let warned = check_envelope(at(Some(0.62), Some(84_000.0)));
        assert!(warned.message.contains("62%"), "{}", warned.message);
        assert!(
            warned.message.contains("84000 rows/s"),
            "{}",
            warned.message
        );

        // The rate is optional and its absence must not produce a broken sentence.
        let quiet = check_envelope(at(Some(0.05), None));
        assert!(quiet.message.contains("5%"), "{}", quiet.message);
        assert!(!quiet.message.contains("()"), "{}", quiet.message);
    }

    #[test]
    fn one_saturated_cycle_cannot_trip_the_warn_from_a_healthy_baseline() {
        // **The property `ENVELOPE_WINDOW_CYCLES` is derived from, asserted rather than trusted.**
        // A backfill, a slow bucket moment or a stalled listing can saturate a single cycle. If
        // the envelope judged on that alone it would warn, clear, and warn again — and a surface
        // that flaps is one operators learn to ignore, which costs more than the signal is worth.
        //
        // The arithmetic the constant encodes: one cycle at 100% lifts a mean of `b` over `N`
        // cycles to `b + (1 - b)/N`. This walks that for the baselines a healthy deployment sits
        // at, and fails if the window is ever shortened enough to break it.
        #[allow(clippy::cast_precision_loss)] // a window of ten; exact
        let n = super::ENVELOPE_WINDOW_CYCLES as f64;
        for baseline in [0.05, 0.10, 0.15, 0.20] {
            let spiked = baseline + (1.0 - baseline) / n;
            assert_eq!(
                check_envelope(at(Some(spiked), None)).status,
                Status::Ok,
                "a single saturated cycle took a {baseline:.0?} baseline to {spiked:.3} and \
                 warned; the window is too short to absorb one spike"
            );
        }

        // And the thing it must NOT absorb: sustained load. Every cycle at 60% is a capacity
        // fact, not a blip, and has to be said.
        assert_eq!(check_envelope(at(Some(0.60), None)).status, Status::Warn);
    }

    #[test]
    fn a_partial_window_shows_the_figure_and_withholds_the_verdict() {
        // **The figure exists from the first cycle; the verdict waits.** A fresh deployment should
        // show what it is doing rather than a blank, but the first cycle after a start ingests
        // everything that accumulated while the daemon was down — the single most likely saturated
        // cycle a deployment ever has. Judging on it would warn on essentially every restart.
        let early = with_samples(Some(0.95), None, 2);
        assert_eq!(
            early.status_of_envelope(),
            Status::Unknown,
            "a 95% average over two cycles must not be a verdict"
        );
        assert!(
            early.envelope_message().contains("95%"),
            "the figure is still shown"
        );
        assert!(
            early.envelope_message().contains("2 of 10"),
            "and it says how provisional it is: {}",
            early.envelope_message()
        );

        // One short of the window is still provisional; the window is the whole point.
        assert_eq!(
            with_samples(Some(0.95), None, super::ENVELOPE_WINDOW_CYCLES - 1).status_of_envelope(),
            Status::Unknown
        );
        // And at a full window the same figure is finally a warning.
        assert_eq!(
            with_samples(Some(0.95), None, super::ENVELOPE_WINDOW_CYCLES).status_of_envelope(),
            Status::Warn
        );
    }

    #[test]
    fn a_fleet_that_has_not_spoken_yet_is_unknown_rather_than_within_the_envelope() {
        // Before the window fills there is no sustained utilisation to report. Reporting Ok would
        // claim a measurement nobody took — the naked-number failure the Figure type exists to
        // stop, one surface over — and averaging two observations would let one saturated cycle
        // carry half the verdict, which is the flapping the window exists to prevent.
        assert_eq!(check_envelope(at(None, None)).status, Status::Unknown);
    }

    #[test]
    fn the_envelope_warns_and_never_refuses() {
        // A declared fence could refuse at install, before anything ran. An OBSERVED one fires
        // against a deployment that is already working, and refusing there would take a customer
        // down for the crime of growing. Even a loop falling behind (duty > 1) is a warning here;
        // `tick_cadence_kept` is what speaks to that.
        for duty in [0.0, 0.5, 1.0, 4.0] {
            assert_ne!(
                check_envelope(at(Some(duty), None)).status,
                Status::Critical,
                "no observed utilisation may be Critical (duty {duty})"
            );
        }
    }
}
