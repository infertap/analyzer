// SPDX-License-Identifier: AGPL-3.0-only

//! The response to disk pressure: what the ladder in [`crate::pressure`] permits, the loop does
//! here, the escalated reclamation. Beside it, the unread input the effective horizon must
//! hold.

use super::{days_back_to, Daemon, RECLAIM_COOLDOWN};
use analyzer_serving_clickhouse::selfhealth;
use std::time::Instant;

impl Daemon {
    /// Escalated reclamation: force the merges the engine's selector will not start on its
    /// own, on the table whose merged-away parts hold the most bytes, which is where
    /// replacement accumulates; the selector waits for pressure, and this is the lever the
    /// loop has, and the reason `wants_reclaim` is true at every pressured rung.
    ///
    /// Under a cooldown ([`RECLAIM_COOLDOWN`]): an `OPTIMIZE ... FINAL` is a full-partition
    /// rewrite issued while the disk is already tight, and the engine needs minutes to
    /// return the freed bytes.
    pub(super) fn reclaim(&mut self) -> bool {
        if self
            .last_reclaim
            .is_some_and(|at| at.elapsed() < RECLAIM_COOLDOWN)
        {
            return false;
        }
        self.last_reclaim = Some(Instant::now());
        let before = selfhealth::collect_unreclaimed(&self.engine).unwrap_or(0.0);
        let heaviest = self.engine.run(
            "SELECT table FROM system.parts \
             WHERE NOT active AND database = currentDatabase() \
             GROUP BY table ORDER BY sum(bytes_on_disk) DESC LIMIT 1",
        );
        let ok = match heaviest.as_deref().map(str::trim) {
            Ok("") => true,
            Ok(table) => self
                .engine
                .run(&format!(
                    "OPTIMIZE TABLE {} FINAL",
                    analyzer_serving_clickhouse::engine::quote_ident(table)
                ))
                .is_ok(),
            Err(_) => false,
        };
        if ok {
            let after = selfhealth::collect_unreclaimed(&self.engine).unwrap_or(0.0);
            // Only a genuine fall counts. A merge that produced MORE dead bytes than it
            // reclaimed is the normal mid-merge state, not negative reclamation, and a
            // counter may not go backwards.
            if before > after {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                self.counters.reclaimed((before - after) as u64);
            }
        }
        ok
    }

    /// How many days back the driver's unread input reaches, plus one day of margin: the floor
    /// discovery and the input log must hold so the driver can read what it has not read past.
    /// Before a tick has stated the edges (a first start, or a fresh derivation after
    /// `reset-derived`), every day the input log holds is unread. `None` when the edges or the
    /// log did not read.
    pub(super) fn oldest_unread_hold_days(&self) -> Option<u32> {
        let edges = match self.dao().edges() {
            Ok(edges) => edges,
            Err(e) => {
                log::info!("the edges did not read: {e}");
                return None;
            }
        };
        if let Some((classified, emitted)) = edges.classified_to_ms.zip(edges.emitted_to_ms) {
            return Some(days_back_to(analyzer_analysis::emitted::day_index(
                classified.min(emitted),
            )));
        }
        let oldest = self
            .spine()
            .lake()
            .ledger(analyzer_pipeline::ledger::SHARD)
            .and_then(|log| log.head());
        match oldest {
            Ok(head) => Some(
                head.days
                    .iter()
                    .find(|(_, index)| index.batches > 0)
                    .map_or(0, |(day, _)| days_back_to(*day)),
            ),
            Err(e) => {
                log::info!("the input log did not read: {e}");
                None
            }
        }
    }
}
