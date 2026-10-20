// SPDX-License-Identifier: AGPL-3.0-only

//! The ingest step: discover, widen and ingest the bucket's new segments on the ingest
//! cadence, walk the annotations, and keep the counters that describe the walk.

use super::{days_back_to, Daemon, INGEST_PROGRESS_INTERVAL};
use crate::counters;
use crate::Error;
use std::sync::Arc;
use std::time::Instant;

impl Daemon {
    /// The annotations walk, on the ingest cadence — what replays declarations on a rebuild:
    /// objects in, recorded in the input log, deduped
    /// by the log's head; the input publication states their markers.
    /// Resilient rather than fatal: one bad listing must not wedge the loop, and the next
    /// cycle retries.
    fn walk_annotations(&self) {
        match analyzer_ingest::annotations::ingest_annotations(
            &self.spine().lake(),
            self.store.as_ref(),
        ) {
            Ok(walk) if walk.ingested > 0 || walk.refused > 0 => {
                log::info!(
                    "annotations: {} ingested, {} refused",
                    walk.ingested,
                    walk.refused
                );
            }
            Ok(_) => {}
            Err(e) => log::info!("annotations walk: {e}"),
        }
    }

    /// The slice deadline's journal line: a deferred backlog is a declared fact, not a
    /// silent queue.
    fn note_deferred(report: &analyzer_ingest::IngestReport) {
        if report.files_deferred > 0 {
            log::info!(
                "ingest slice deadline: {} file(s) / {} bytes deferred to the next cycle",
                report.files_deferred,
                report.bytes_deferred
            );
        }
    }

    /// Widen source discovery before landing a backlog so the next tick's source discovers the
    /// backlog's days and the input log's upkeep does not compact them as past the horizon.
    fn widen_horizon_for_backlog(&mut self, fetch: &[analyzer_ingest::bucket::Discovered]) {
        let Some(day) = analyzer_ingest::bucket::oldest_key_day(fetch) else {
            return;
        };
        let needed = days_back_to(day);
        if needed > self.effective_horizon_days {
            log::warn!(
                "event horizon widened to {needed} days BEFORE ingest — the backlog reaches \
                 back beyond the declared source-discovery horizon"
            );
            self.set_effective_horizon_days(needed);
        }
    }

    /// Discovery, the horizon widened for the backlog, then the walk.
    ///
    /// Progress is accounted per file, live: the counter moves as each commit lands, and a
    /// cycle that outlives [`INGEST_PROGRESS_INTERVAL`] says so in the journal at that
    /// cadence. The cycle-end summary line remains the completion record.
    pub(super) fn discover_widen_and_ingest(
        &mut self,
    ) -> Result<analyzer_ingest::IngestReport, Error> {
        // Declarations are inputs like segments, and this is also what replays them on a
        // rebuild.
        self.walk_annotations();
        let fetch = analyzer_ingest::bucket::discover(
            &self.spine().lake(),
            self.store.as_ref(),
            &self.config.bucket.key_prefix,
            self.config.admit_corpus_forms,
        )?;
        self.widen_horizon_for_backlog(&fetch);
        let counters = Arc::clone(&self.counters);
        let mut files: u64 = 0;
        let mut records: u64 = 0;
        let mut last_note = Instant::now();
        let report = analyzer_ingest::bucket::ingest_discovered(
            &self.spine().lake(),
            self.store.as_ref(),
            &self.config.fleet_id,
            fetch,
            analyzer_ingest::bucket::Slice {
                // One cadence interval is the slice: a cycle that ingests longer than its own
                // cadence has consumed the next cycle's turn and starved the tick.
                deadline: Some(Instant::now() + self.config.ingest_interval),
                workers: self.config.ingest_workers,
            },
            &mut |file_records| {
                counters.files_ingested(1);
                files += 1;
                records += file_records;
                if last_note.elapsed() >= INGEST_PROGRESS_INTERVAL {
                    log::info!(
                        "ingest in progress: {files} file(s), {records} records committed \
                         so far this cycle"
                    );
                    last_note = Instant::now();
                }
            },
        )?;
        Ok(report)
    }

    /// One ingest cycle's counter bookkeeping: pending figures carry to the next snapshot
    /// (a tick that fails puts them back), bytes ride beside files, and the
    /// cycle row records what was waiting before any of it was fetched.
    pub(super) fn record_ingest_counters(
        &mut self,
        report: &analyzer_ingest::IngestReport,
        ingest_started: Instant,
    ) {
        self.counters.record_ingest(counters::PendingIngest {
            files_seen: report.files_seen,
            records: report.records_inserted,
            unknown_kinds: report.unknown_kinds,
            duplicates_skipped: report.duplicates_skipped,
        });
        self.counters.bytes_ingested(report.bytes_seen);
        self.counters.record_ingest_cycle(counters::IngestCycle {
            elapsed: ingest_started.elapsed(),
            interval: self.config.ingest_interval,
            files_deferred: report.files_deferred,
            bytes_deferred: report.bytes_deferred,
        });
        Self::note_deferred(report);
    }
}
