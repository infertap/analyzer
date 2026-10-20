// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed tick on every ingest cycle, and its report.
//!
//! One tick reads what the log committed, classifies, places, covers and publishes it in one
//! exactly-once batch (`analyzer_pipeline::Executor::advance`), states the days it settled,
//! and applies. What follows it — the ledger over the days it settled, the fleet figures — is
//! [`super::settle`]'s.

use super::Daemon;
use std::time::Instant;

impl Daemon {
    /// One keyed tick and its report. Returns whether it was applied. A failure is counted and
    /// journaled, never propagated: the intent stays unapplied, and the next cycle publishes
    /// its batch again before it ticks.
    pub(super) fn tick_and_report(&mut self) -> bool {
        self.counters.tick_started();
        let started = Instant::now();
        let requests = analyzer_objectstore::S3Store::requests();
        let source = self.tick_source();
        // Taken, not peeked: a failed tick puts them back, so counts never describe a
        // tick that was never applied and never vanish because one failed.
        let pending = self.counters.take_ingest();
        let advanced = self
            .executor()
            .and_then(|executor| executor.with_lent(&self.lent).advance(&source));
        match advanced {
            Ok(tick) => {
                let sent = analyzer_objectstore::S3Store::requests().since(requests);
                log::info!(
                    "tick {} applied in {:.1}s: {} store(s) classified, classified to {}, \
                     emitted to {}, covered to {}; {} late record(s) corrected, {} too late; \
                     {} placement late, {} corrected; {} coverage source(s) restated; \
                     {} block(s) audited, {} store(s) compared; \
                     {} batch(es) pending; bucket {} GET, {} PUT, {} LIST, {} DELETE{}",
                    tick.publication,
                    started.elapsed().as_secs_f64(),
                    tick.stores,
                    edge(tick.classified_to_ms),
                    edge(tick.emitted_to_ms),
                    edge(tick.covered_to_ms),
                    tick.late.corrected,
                    tick.late.too_late,
                    tick.placement_late.records,
                    tick.placement_late.corrected,
                    tick.coverage_restated,
                    tick.audit.blocks,
                    tick.audit.stores,
                    tick.pending,
                    sent.gets,
                    sent.puts,
                    sent.lists,
                    sent.deletes,
                    if tick.republished {
                        "; the previous tick's batch was published again first"
                    } else {
                        ""
                    },
                );
                for d in &tick.audit.disagreements {
                    log::error!("the sampled audit disagreed: {d}");
                }
                if let Some(at) = tick.audit_edge.disagreed_at_ms {
                    log::error!(
                        "the audit edge holds at {:.0}: a sampled block disagreed in the window \
                         from {at:.0}; derive afresh with reset-derived once it is reported",
                        tick.audit_edge.audited_to_ms.unwrap_or(f64::NAN)
                    );
                }
                self.counters
                    .audited(tick.audit.blocks, tick.audit_edge.disagreed_at_ms);
                if tick.evicts.mismatched() > 0 {
                    log::error!(
                        "evicts named another identity than their store: {}",
                        tick.evicts.naming(|i| i.mismatched)
                    );
                }
                if tick.evicts.underresolved() > 0 {
                    log::warn!(
                        "evicts named no identity where the same run's store named one: {}",
                        tick.evicts.naming(|i| i.underresolved)
                    );
                }
                self.counters.evicts_audited(&tick.evicts);
                if !tick.settled_days.is_empty() {
                    log::info!("days settled or restated: {:?}", tick.settled_days);
                }
                self.settle_tick(&source, &tick);
                self.counters.tick_succeeded(started.elapsed());
                true
            }
            Err(e) => {
                self.counters.tick_failed();
                self.counters.restore_ingest(pending);
                log::warn!("tick failed, its batch is published again next cycle: {e}");
                false
            }
        }
    }
}

/// An edge for the journal: its instant, or that none is stated yet.
fn edge(at_ms: Option<f64>) -> String {
    at_ms.map_or_else(|| "none yet".to_string(), |ms| format!("{ms:.0}"))
}
