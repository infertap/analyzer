// SPDX-License-Identifier: AGPL-3.0-only

//! Preflight: validate a deployment BEFORE the pilot.
//!
//! The environmental checks whose failure modes are cheapest to hit before any data exists:
//! directory permissions, the engine child's health, clock sanity, disk headroom, and the
//! bucket's reachability. Every check reports; refusal is the worst-wins verdict, in the same
//! vocabulary the dashboard uses ([`Status`]), so "preflight green" and "health green" are the
//! same claim made at two times.
//!
//! The directory and clock checks are shared with `Daemon::start`, so the install-time and
//! start-time answers come from one implementation. What the daemon deliberately does not
//! inherit is the disk headroom check: preflight validates a deployment that does not exist
//! yet and may demand a clean slate, but a running daemon must not refuse to start at 15%
//! free, since the pressure ladder exists to survive exactly that. Conditions the running
//! system is built to survive belong to it, not to the door.
//!
//! No check asks an operator to declare a fleet size. The envelope is observed from the
//! analyzer's own ingest utilisation once it runs (`selfhealth::ENVELOPE_UTILISATION_WARN`),
//! so preflight cannot refuse an oversized fleet; before ingest there is nothing to observe.

use std::path::Path;

use crate::memory::GIB;
use crate::BucketConfig;
use analyzer_serving_clickhouse::engine::server::{self, ServerConfig, ServerHandle};
use analyzer_serving_clickhouse::selfhealth::{Check, Status, DISK_FREE_FRACTION_WARN};

/// Bound on |engine clock − host clock| before timestamps stop being comparable enough to
/// trust. Derived from what the product tolerates rather than taste: cross-instance joins
/// carry a 1000 ms skew allowance (`Params::delta_skew_ms`), and a host whose own engine
/// disagrees with it by minutes will misfile day-partitioned rows and mis-age TTLs.
/// Sixty seconds is far past any healthy same-host difference and far below the hours that
/// break day attribution.
pub const CLOCK_SKEW_MAX_SECONDS: f64 = 60.0;

/// One preflight run: every check, and the worst-wins verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preflight {
    /// Every check that ran, in run order; [`Preflight::refused`] is worst-wins over them.
    pub checks: Vec<Check>,
}

impl Preflight {
    /// The verdict: refuse on any Critical. Warn passes — preflight's job is to stop
    /// deployments that cannot work, not to demand perfection before day one.
    #[must_use]
    pub fn refused(&self) -> bool {
        self.checks.iter().any(|c| c.status == Status::Critical)
    }
}

/// A check with its verdict: `Ok` carries the sentence for a pass, `Err` the sentence for
/// a refusal.
fn check(name: &'static str, verdict: Result<String, String>) -> Check {
    match verdict {
        Ok(message) => Check {
            name,
            status: Status::Ok,
            message,
        },
        Err(message) => Check {
            name,
            status: Status::Critical,
            message,
        },
    }
}

/// A check whose figure was not measured.
fn unmeasured(name: &'static str, message: &str) -> Check {
    Check {
        name,
        status: Status::Unknown,
        message: message.to_string(),
    }
}

/// Writable, verified by writing and removing a probe file rather than by reading
/// permission bits.
fn check_writable(name: &'static str, dir: &Path) -> Check {
    let probe = dir.join(".preflight-write-probe");
    let writable = std::fs::create_dir_all(dir).is_ok()
        && std::fs::write(&probe, b"probe").is_ok()
        && std::fs::remove_file(&probe).is_ok();
    check(
        name,
        if writable {
            Ok(format!("{} is writable.", dir.display()))
        } else {
            Err(format!(
                "cannot write beneath {}: verified by writing and removing a probe file, \
                 not by reading permission bits. Fix ownership or mount options.",
                dir.display()
            ))
        },
    )
}

/// Start the engine child on the real data directory, prove it answers, read its clock and
/// its disk, and shut it down in order. One start exercises the whole chain a pilot needs:
/// binary resolution, config rendering, credentials, loopback transport, and schema
/// migration on the volume that will hold the data.
fn engine_checks(bin: &Path, data_dir: &Path, checks: &mut Vec<Check>) {
    if let server::Owner::Live(pid) = server::owner_of(data_dir) {
        checks.push(check(
            "engine_child",
            Err(format!(
                "a live process (pid {pid}) already owns {}. Preflight validates a \
                 deployment before it starts, and one analyzer per cluster is the envelope, \
                 so a second one here is a misdeployment, not a race to win.",
                data_dir.display()
            )),
        ));
        return;
    }
    let config = ServerConfig::new(data_dir);
    let handle = match ServerHandle::start(bin, &config) {
        Ok(h) => h,
        Err(e) => {
            checks.push(check(
                "engine_child",
                Err(format!(
                    "the engine child did not start on {}: {e}. Nothing downstream can be \
                     checked until it does.",
                    data_dir.display()
                )),
            ));
            return;
        }
    };
    let answer = handle.client_query(
        "SELECT version(), toFloat64(now64(3)), \
         (SELECT toFloat64(min(free_space)) FROM system.disks), \
         (SELECT toFloat64(min(total_space)) FROM system.disks)",
    );
    match answer {
        Err(e) => checks.push(check(
            "engine_child",
            Err(format!("the engine child started but did not answer: {e}")),
        )),
        Ok(row) => {
            let cols: Vec<&str> = row.trim().split('\t').collect();
            let num = |i: usize| cols.get(i).and_then(|c| c.trim().parse::<f64>().ok());
            checks.push(check(
                "engine_child",
                Ok(format!(
                    "engine child started, answered, and reports version {}.",
                    cols.first().unwrap_or(&"?")
                )),
            ));
            checks.push(clock_check(num(1)));
            checks.push(disk_check(num(2), num(3)));
        }
    }
    if let Err(e) = handle.shutdown() {
        checks.push(check(
            "engine_shutdown",
            Err(format!(
                "the engine child would not stop in order: {e}. A deployment whose engine \
                 cannot be stopped cleanly cannot be upgraded or recovered cleanly either."
            )),
        ));
    }
}

/// Write-then-read through the store, under the customer's own prefix and credential;
/// unconditional, no mode skips it. The probe key ends `.txt`, a suffix ingest's glob never
/// matches, so the probe can never enter the ledger; its cleanup belongs to the customer's
/// lifecycle policy, the same authority that owns every other object's disposition.
fn bucket_probe(bucket: &BucketConfig) -> Check {
    use analyzer_objectstore::ObjectStore as _;
    let key = format!(
        "{}{}probe-{}.txt",
        bucket.key_prefix,
        analyzer_lake::dated::PREFLIGHT_PREFIX,
        std::process::id()
    );
    let stamp = format!("infertap-preflight-{}", std::process::id());
    let store = match analyzer_objectstore::S3Store::open(&bucket.url, bucket.credentials.as_ref())
    {
        Ok(s) => s,
        Err(e) => {
            return check(
                "bucket_probe",
                Err(format!("cannot reach {}: {e}", bucket.url)),
            )
        }
    };
    let round_trip = store
        .put(&key, stamp.as_bytes())
        .and_then(|()| store.get(&key));
    check(
        "bucket_probe",
        match round_trip {
            Ok(back) if String::from_utf8_lossy(&back).trim() == stamp => {
                Ok(format!("wrote and read back {key}."))
            }
            Ok(back) => Err(format!(
                "the bucket returned different bytes than were written to {key}: {:?}. \
                 A transport that mutates objects cannot carry telemetry.",
                String::from_utf8_lossy(&back)
            )),
            Err(e) => Err(format!(
                "cannot write and read {key}: {e}. Fix the endpoint, credential, or bucket \
                 policy before the pilot; ingest cannot work where this probe cannot."
            )),
        },
    )
}

/// The bucket keeps conditional writes: a create of an existing key and an update at a
/// superseded version are both refused (`analyzer_objectstore::check_conditional_writes`).
/// The lake's heads and the writer's lease rest on that refusal; a store that ignores
/// `If-None-Match` or `If-Match` lets two writers both commit and nothing downstream can tell,
/// so a store that fails here cannot hold the analyzer's state. The probe key is removed after.
fn bucket_fences_writers(bucket: &BucketConfig) -> Check {
    use analyzer_objectstore::ObjectStore as _;
    let key = format!(
        "{}{}fence-{}-{}.txt",
        bucket.key_prefix,
        analyzer_lake::dated::PREFLIGHT_PREFIX,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    let store = match analyzer_objectstore::S3Store::open(&bucket.url, bucket.credentials.as_ref())
    {
        Ok(s) => s,
        Err(e) => {
            return check(
                "bucket_fences_writers",
                Err(format!("cannot reach {}: {e}", bucket.url)),
            )
        }
    };
    let verdict = analyzer_objectstore::check_conditional_writes(&store, &key);
    let removed = store.delete(&key);
    check(
        "bucket_fences_writers",
        match (verdict, removed) {
            (Ok(()), Ok(())) => Ok(format!(
                "a conflicting create and a stale update were both refused at {key}."
            )),
            (Ok(()), Err(e)) => Ok(format!(
                "a conflicting create and a stale update were both refused at {key}; removing \
                 the probe failed ({e}), so the customer's lifecycle policy owns it."
            )),
            (Err(e), _) => Err(format!(
                "{e}. The analyzer's state needs a store that refuses conflicting conditional \
                 writes (S3, or a compatible store that honours If-None-Match and If-Match)."
            )),
        },
    )
}

/// Engine clock against host clock. Same host, so the comparison crosses no clock domain:
/// it measures whether this box agrees with itself. That is what makes it safe to treat as
/// fatal at daemon start, not only at preflight: an NTP outage moves both readings
/// together and cannot trip this; only something genuinely pathological can (a clock
/// namespace, a shifted container, a sandbox).
#[must_use]
pub(crate) fn clock_check(engine_now_epoch: Option<f64>) -> Check {
    let Some(engine_now) = engine_now_epoch else {
        return unmeasured(
            "clock_sane",
            "the engine returned no readable clock; skew is unmeasured.",
        );
    };
    let host_now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    let skew = (engine_now - host_now).abs();
    check(
        "clock_sane",
        if skew <= CLOCK_SKEW_MAX_SECONDS {
            Ok(format!("engine and host clocks agree within {skew:.1} s."))
        } else {
            Err(format!(
                "engine and host clocks disagree by {skew:.0} s (bound \
                 {CLOCK_SKEW_MAX_SECONDS:.0} s). Day partitioning, TTL aging, and staleness \
                 all read the clock; fix time sync before trusting any of them."
            ))
        },
    )
}

/// Disk headroom on the data volume, judged by the same fraction the running system warns
/// at — a deployment that STARTS past its own alarm line reaches it by design.
fn disk_check(free: Option<f64>, total: Option<f64>) -> Check {
    let (Some(free), Some(total)) = (free, total) else {
        return unmeasured(
            "disk_headroom",
            "the engine reported no disk figures; headroom is unmeasured.",
        );
    };
    if total <= 0.0 {
        return unmeasured(
            "disk_headroom",
            "the engine reported a zero-size volume; headroom is unmeasured.",
        );
    }
    let fraction = free / total;
    let (free_gib, total_gib) = (free / GIB, total / GIB);
    check(
        "disk_headroom",
        if fraction > DISK_FREE_FRACTION_WARN {
            Ok(format!(
                "{free_gib:.1} GiB free of {total_gib:.1} GiB ({:.0}% — above the {:.0}% warn line).",
                fraction * 100.0,
                DISK_FREE_FRACTION_WARN * 100.0
            ))
        } else {
            Err(format!(
                "only {free_gib:.1} GiB free of {total_gib:.1} GiB ({:.0}%), at or below the \
                 {:.0}% line the running system alarms on. A deployment that starts past its \
                 own alarm threshold will reclaim and refuse from day one; free space or use a \
                 larger volume.",
                fraction * 100.0,
                DISK_FREE_FRACTION_WARN * 100.0
            ))
        },
    )
}

/// The environmental checks that need no engine: can we write everywhere we must. Shared
/// by `preflight` and by `Daemon::start`: an unwritable `out_dir` produces a daemon that
/// starts cleanly, runs its loop, and fails every health emission, which is a silent daemon.
#[must_use]
pub(crate) fn directory_checks(data_dir: &Path, out_dir: &Path) -> Vec<Check> {
    vec![
        check_writable("data_dir_writable", data_dir),
        check_writable("out_dir_writable", out_dir),
    ]
}

/// What preflight needs to know about the deployment it is validating.
#[derive(Debug, Clone)]
pub struct Deployment<'a> {
    /// The records bucket, or `None` for an embedder that validates no transport here.
    /// The CLI always passes one: a deployment without a bucket is not a deployment.
    pub bucket: Option<&'a BucketConfig>,
    /// The engine's data directory: probed writable, then the engine child is started on it.
    pub data_dir: &'a Path,
    /// Where health artifacts and the bootstrap token go: probed writable.
    pub out_dir: &'a Path,
    /// The engine binary the child is started from.
    pub clickhouse_bin: &'a Path,
}

/// Run every check. Environmental failures accumulate rather than short-circuit, so one
/// run reports everything wrong with a box instead of one thing per attempt, except past
/// the engine, whose downstream checks (clock, disk) cannot exist without it. The bucket
/// probe does not wait on the engine: one run reports a broken engine and a broken bucket.
#[must_use]
pub fn run(d: &Deployment) -> Preflight {
    let mut checks = directory_checks(d.data_dir, d.out_dir);
    // Only reach for the engine on a box whose directories work: an engine started on an
    // unwritable data dir would fail with its own error and bury the actionable one.
    if checks.iter().all(|c| c.status != Status::Critical) {
        engine_checks(d.clickhouse_bin, d.data_dir, &mut checks);
    }
    if let Some(bucket) = d.bucket {
        let probe = bucket_probe(bucket);
        let reachable = probe.status != Status::Critical;
        checks.push(probe);
        if reachable {
            checks.push(bucket_fences_writers(bucket));
        }
    }
    Preflight { checks }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_and_disk_judge_against_their_stated_bounds() {
        let host = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_secs_f64();
        assert_eq!(clock_check(Some(host)).status, Status::Ok);
        assert_eq!(clock_check(Some(host + 3600.0)).status, Status::Critical);
        assert_eq!(clock_check(None).status, Status::Unknown);
        assert_eq!(disk_check(Some(50.0), Some(100.0)).status, Status::Ok);
        assert_eq!(disk_check(Some(10.0), Some(100.0)).status, Status::Critical);
        assert_eq!(disk_check(None, None).status, Status::Unknown);
    }

    #[test]
    fn directory_checks_probe_rather_than_trust_permission_bits() {
        let dir = std::env::temp_dir().join(format!("preflight-probe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch");
        let verdict = check_writable("data_dir_writable", &dir).status;
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(verdict, Status::Ok);
    }

    #[test]
    fn refusal_is_worst_wins_over_all_checks() {
        let verdict = |status| Preflight {
            checks: vec![Check {
                name: "x",
                status,
                message: String::new(),
            }],
        };
        assert!(
            !verdict(Status::Warn).refused(),
            "warns pass; preflight stops what cannot work"
        );
        assert!(verdict(Status::Critical).refused());
    }
}
