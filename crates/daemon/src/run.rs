// SPDX-License-Identifier: AGPL-3.0-only

//! The run loop: `Daemon::run` and one `cycle` of it.
//!
//! The cadences the loop owns, the SIGTERM it honours, the readiness it notifies, and the
//! order of one cycle — assess pressure, reclaim, ingest, tick, maintain, publish.

use super::sample::sample_with;
use super::{CycleOutcome, Daemon, Due, Shutdown, STALE_SAMPLE_INTERVALS, TICK};
use crate::metrics::{self, Published};
use crate::pressure::{self, Pressure};
use crate::Error;
use analyzer_serving_clickhouse::selfhealth::ServerStatus;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;

/// The systemd notify socket, spoken natively: one datagram per message, no library.
/// `NOTIFY_SOCKET` names a path or (with a leading `@`) an abstract name; absent, every
/// call is a no-op — a dev host or an embedder simply has no watchdog.
///
/// The watchdog pet rides the sampler's successful publication, the exact signal
/// `/metrics` serves, so "systemd restarts the daemon" and "the metrics endpoint would be
/// serving 503" are the same condition, observed from outside the process. The daemon's
/// own liveness checks are in-process, and a wedge that parks the loop parks them with it.
fn sd_notify(state: &str) {
    let Ok(socket) = std::env::var("NOTIFY_SOCKET") else {
        return;
    };
    let Ok(sock) = std::os::unix::net::UnixDatagram::unbound() else {
        return;
    };
    #[cfg(target_os = "linux")]
    if let Some(name) = socket.strip_prefix('@') {
        use std::os::linux::net::SocketAddrExt;
        if let Ok(addr) = std::os::unix::net::SocketAddr::from_abstract_name(name) {
            let _ = sock.send_to_addr(state.as_bytes(), &addr);
        }
        return;
    }
    let _ = sock.send_to(state.as_bytes(), socket);
}

/// A due-by-cadence timer that never drifts into a busy loop and never fires twice for one
/// interval.
#[derive(Debug)]
pub(super) struct Cadence {
    interval: std::time::Duration,
    last: Option<Instant>,
}

impl Cadence {
    pub(super) fn new(interval: std::time::Duration) -> Self {
        Self {
            interval,
            last: None,
        }
    }

    /// Due if it has never run, or if the interval has elapsed. First-cycle-due is
    /// deliberate: a daemon that waited a whole interval before its first tick would leave a
    /// fresh install with nothing to say for that long.
    fn due(&self) -> bool {
        self.last.is_none_or(|last| last.elapsed() >= self.interval)
    }

    pub(super) fn ran(&mut self) {
        self.last = Some(Instant::now());
    }

    /// How long until this is due, or `None` when it is due NOW — which covers both "the
    /// interval has elapsed" and "it has never run". Both are the present rather than a time in
    /// the future, and a serving layer renders them the same way.
    pub(super) fn remaining(&self) -> Option<std::time::Duration> {
        self.last
            .and_then(|last| self.interval.checked_sub(last.elapsed()))
    }
}

impl Daemon {
    /// Run one cycle: assess pressure, then do what it permits.
    ///
    /// Exposed so tests drive cycles directly instead of sleeping through cadences. A failing
    /// tick is counted and reported, never propagated: the loop's whole purpose is to survive
    /// a bad cycle and try the next one.
    pub fn cycle(&mut self, due: Due) -> CycleOutcome {
        // Sampled on the loop's own engine, never the sampler's: taking the sampler's mutex
        // here would let a wedged engine park the loop while holding the lock the sampler
        // needs, making both liveness checks unreachable at once.
        self.publish_inputs();
        let vitals = sample_with(
            &self.engine,
            &self.spine.lake(),
            &self.config,
            &self.shared,
            &self.counters,
            self.sampler_queries.load(Ordering::Relaxed) + self.serving_queries.total(),
        );
        let assessment = pressure::assess(&vitals, self.last_pressure);
        self.last_pressure = assessment.pressure;

        let mut outcome = CycleOutcome {
            pressure: assessment.pressure,
            ingested_files: 0,
            ticked: false,
            reclaimed: false,
        };

        if assessment.pressure == Pressure::Refuse {
            self.counters.ingest_refused();
            log::warn!("{}", assessment.reason);
        }
        if assessment.pressure.wants_reclaim() {
            outcome.reclaimed = self.reclaim();
        }
        self.hold_horizon_for_unread_input();

        if due.ingest && assessment.pressure.allows_ingest() {
            self.ingest_step(&mut outcome);
            self.publish_input_tables();
        }
        // The tick rides every ingest cycle, refused intake or not: it derives what the log
        // has already committed, and a cycle that ingested nothing still moves the edges a
        // stalled run held back.
        if due.ingest && assessment.pressure != Pressure::Unmeasured {
            outcome.ticked = self.tick_and_report();
        }
        if due.maintain {
            self.maintain_input_log();
        }

        self.publish_inputs();
        outcome
    }

    /// The ONE write path for the effective horizon: the loop's field and the shared cell move
    /// together, so a source constructed after this call discovers at the new horizon while the
    /// loop keeps its working value in days.
    pub(super) fn set_effective_horizon_days(&mut self, days: u32) {
        self.effective_horizon_days = days;
        self.horizon.set(i64::from(days) * super::HOURS_PER_DAY);
    }

    /// Keep discovery wide enough to read what the driver has not read past, recomputed each
    /// cycle from the declared horizon.
    fn hold_horizon_for_unread_input(&mut self) {
        let declared = self.config.declared_horizon_days();
        // An unreadable edge must not widen retention forever: the next cycle retries.
        let target = declared.max(self.oldest_unread_hold_days().unwrap_or(0));
        if self.effective_horizon_days == target {
            return;
        }
        log::info!("input retention target set to {target} days (declared {declared})");
        self.set_effective_horizon_days(target);
    }

    /// The ingest arm of one cycle: the walk, the counters and the run-state gauges.
    ///
    /// Timed, because the duration is the envelope's measurement: how long a cycle spends
    /// ingesting, over the cadence it runs on, is the fraction of this machine's capacity the
    /// fleet consumes, and unlike a rows-per-second ceiling it needs no constant.
    fn ingest_step(&mut self, outcome: &mut CycleOutcome) {
        let ingest_started = Instant::now();
        let report = match self.discover_widen_and_ingest() {
            Ok(report) => report,
            Err(e) => {
                log::warn!("ingest failed, will retry: {e}");
                return;
            }
        };
        // The projected share is a cycle-end fact: a cycle's composition is settled when
        // the cycle is.
        self.counters.files_projected(report.projected_files);
        outcome.ingested_files = report.files_ingested;
        self.record_ingest_counters(&report, ingest_started);
        // The run-state gauges refresh at ingest cadence, the freshness of the facts they
        // are computed from, through the same roster read the console serves. Every cycle,
        // not only fetching ones: a run ages into silence while nothing new arrives from it.
        match self.derive_run_states() {
            Ok(runs) => self.latest_runs = Some(runs),
            Err(e) => log::warn!("run-state gauges not refreshed, will retry next cycle: {e}"),
        }
        // Only when work happened: this loop wakes every minute and usually has nothing to
        // fetch, and a line each time buries the ones that matter.
        if report.files_ingested > 0 {
            log::info!(
                "ingested {} file(s), {} records in {:.1}s ({} via the projected walk)",
                report.files_ingested,
                report.records_inserted,
                ingest_started.elapsed().as_secs_f64(),
                report.projected_files,
            );
        }
    }

    /// The sampler thread: on its own engine, publishing to `/metrics` and the health
    /// artifacts on the health cadence, petting the watchdog on each publication.
    fn spawn_sampler(
        &self,
        shutdown: &Shutdown,
        published: Published,
    ) -> Result<JoinHandle<()>, Error> {
        let engine = Arc::clone(&self.sampler_engine);
        let lake = self.spine.lake();
        let config = self.config.clone();
        let shared = Arc::clone(&self.shared);
        let counters = Arc::clone(&self.counters);
        // The loop engine's statement count as a live handle: the sampler must never touch
        // the loop's engine, and this is how its half of the exported sum stays current
        // mid-cycle instead of frozen at the last publication.
        let loop_queries = self.engine.queries_shared();
        let serving_queries = Arc::clone(&self.serving_queries);
        let stop = shutdown.clone();
        std::thread::Builder::new()
            .name("sampler".into())
            .spawn(move || {
                let mut health = Cadence::new(config.health_interval);
                while !stop.requested() {
                    if health.due() {
                        let vitals = {
                            let engine = super::lock(&engine);
                            sample_with(
                                &engine,
                                &lake,
                                &config,
                                &shared,
                                &counters,
                                loop_queries.load(Ordering::Relaxed) + serving_queries.total(),
                            )
                        };
                        published.publish(vitals.to_prometheus());
                        // The pet rides the publication: a wedged engine blocks the sample,
                        // the publication goes stale, the petting stops, and systemd's
                        // WatchdogSec restarts a process whose checking machinery is itself
                        // parked.
                        sd_notify("WATCHDOG=1");
                        if let Err(e) =
                            analyzer_serving_clickhouse::selfhealth::write_health_artifacts(
                                &config.out_dir,
                                &vitals,
                            )
                        {
                            log::warn!("health emission failed: {e}");
                        }
                        health.ran();
                    }
                    std::thread::sleep(TICK);
                }
            })
            .map_err(|source| Error::Thread {
                name: "sampler",
                source,
            })
    }

    /// The loop's two exits of its own: a dead engine child, and a sampler whose
    /// publication has gone stale.
    ///
    /// A dead emitter is a dead daemon, and it is checked by freshness rather than by the
    /// thread's liveness. The sampler's whole purpose is that observation never queues
    /// behind work, which is exactly what lets it fail while this loop stays healthy; the
    /// sample is the only channel, so a loop that kept ingesting while nothing could see
    /// it is worse than one that stops. `JoinHandle::is_finished` would catch only a
    /// panic; a sampler wedged on a query that never returns is alive and reports
    /// nothing. The publication's age catches both.
    fn liveness_failure(&mut self, published: &Published) -> Option<Error> {
        if !self.handle.is_alive() {
            if let Ok(mut shared) = self.shared.write() {
                shared.server = ServerStatus {
                    up: false,
                    uptime_seconds: None,
                };
            }
            return Some(Error::EngineDied);
        }
        if published.is_stale() {
            return Some(Error::SamplerStale {
                age: published.age(),
                intervals: STALE_SAMPLE_INTERVALS,
            });
        }
        None
    }

    /// Run until shutdown is requested, or until the engine child dies.
    ///
    /// Emission runs on its own thread, so observation never queues behind work: the health
    /// artifacts and `/metrics` are what an operator watches during a long tick, which is
    /// exactly when an emission sharing the loop's thread would freeze. Because the sampler
    /// can fail independently it is supervised independently, and a stale publication is
    /// treated exactly like a dead engine child: an ordered exit, handing the restart to
    /// the init system.
    ///
    /// A dead engine child is an ordered exit, not a nursing attempt. The failures a
    /// server-only restart fixes (a wedged thread, a leaked descriptor) cannot be told
    /// apart from the ones it cannot (a full disk, a corrupt part), and the second class
    /// recurs immediately on a retry. So the loop exits and the init system's restart runs
    /// the full claim-and-recover path. This is only correct if the init system always
    /// takes the handoff, which makes `Restart=always` with `StartLimitIntervalSec=0` a
    /// requirement of this decision, enforced in `packaging/systemd/infertap-analyzer.service`.
    ///
    /// A tick in flight is not interrupted: SIGTERM between cycles stops within a second,
    /// SIGTERM mid-tick stops when the tick ends. A unit file's `TimeoutStopSec` should exceed
    /// the deployment's observed tick duration.
    ///
    /// # Errors
    /// A dead engine child, a sampler that stopped publishing, a sampler or listener that
    /// cannot start, or a failure to stop the engine in order. Everything else inside the
    /// loop is survivable by construction.
    pub fn run(mut self, shutdown: &Shutdown) -> Result<(), Error> {
        self.cancel = Some(shutdown.flag());
        // The staleness bound is shared by the listener (which stops serving past it) and by
        // this loop (which exits past it). Derived from what the sampler can achieve, not from
        // the configured cadence alone: it wakes on `TICK`, so a `health_interval` below one
        // tick cannot be met and a bound computed from it would declare every healthy sampler
        // dead. A cadence faster than the tick is a slower sample rather than an immediate exit.
        let published =
            Published::new(self.config.health_interval.max(TICK) * STALE_SAMPLE_INTERVALS);
        if let Some(addr) = self.config.metrics_addr {
            metrics::serve(addr, published.clone())?;
            log::info!("serving /metrics on {addr}");
        }
        // Type=notify: the unit is "started" once the engine is owned and the loop is
        // to spin, which is when the claim, the migration and the horizon re-assertion have
        // all already held.
        sd_notify("READY=1");
        let sampler = self.spawn_sampler(shutdown, published.clone())?;

        let mut ingest = Cadence::new(self.config.ingest_interval);
        let mut maintain = Cadence::new(self.config.maintain_interval);
        let exit = loop {
            if shutdown.requested() {
                break Ok(());
            }
            if let Some(failure) = self.liveness_failure(&published) {
                break Err(failure);
            }
            // The kick is consumed unconditionally, not short-circuited behind `due`:
            // `due || swap` would leave a kick set through a scheduled cycle and run a second,
            // unasked one after it.
            let kicked = self.kick.swap(false, Ordering::Relaxed);
            let due = Due {
                ingest: ingest.due() || kicked,
                maintain: maintain.due(),
            };
            if due.any() {
                self.cycle(due);
                if due.ingest {
                    ingest.ran();
                }
                if due.maintain {
                    maintain.ran();
                }
            }
            self.publish_snapshot(&ingest);
            std::thread::sleep(TICK);
        };
        // One stop path for every exit: the sampler ends with the loop, then the engine
        // stops in order — never an emission racing a dying server.
        shutdown.request();
        let _ = sampler.join();
        // The working state closed cleanly before the engine stops: the restart's point.
        self.close_working_state();
        match (self.handle.shutdown(), exit) {
            (Err(e), Ok(())) => Err(Error::from(e)),
            (_, exit) => exit,
        }
    }
}
