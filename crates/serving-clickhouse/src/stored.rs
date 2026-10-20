// SPDX-License-Identifier: AGPL-3.0-only

//! The stored-mass denominator: the "of N stored" beside the recompute fraction, read from
//! `facts_rollup_daily`.
//!
//! The table is the time-axis INVENTORY: counted at ingest, per batch, and published from the
//! input log, never computed by the derivation. That is why these rows carry no coverage and only
//! the input meaning — they count what arrived, exactly, and a store row means the same thing
//! under every parameter generation. The recompute mass beside this denominator carries
//! both; the two are different kinds of fact and render as such.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Dao;
use crate::Error;
use analyzer_analysis::residual::ZeroBasis;
use analyzer_analysis::Window;

impl Dao<'_> {
    /// Token mass STORED per UTC day over `window` — the denominator the anchor line states
    /// its fraction over.
    ///
    /// `store` rows only: an evict carries no tokens and a clear is not mass arriving. The
    /// table holds one row per batch, so the read sums over batches.
    ///
    /// # Errors
    /// Engine failure, or a row the engine returned that does not parse as declared.
    pub fn stored_mass(&self, window: &Window) -> Result<Vec<StoredMassRow>, Error> {
        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let sql = format!(
            "SELECT toString(day), toUInt64(sum(records)), toUInt64(sum(tokens)) \
             FROM facts_rollup_daily FINAL \
             WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0 AND kind = 'store' \
               AND day >= toDate(toDateTime({s}, 'UTC')) \
               AND day <= toDate(toDateTime({e}, 'UTC')) \
             GROUP BY day ORDER BY day",
            s = crate::epoch_seconds(window.start_ms),
            e = crate::epoch_seconds((window.end_ms - 1.0).max(window.start_ms)),
        );
        let out = self.engine.run(&sql)?;
        Row::each("stored_mass", &out)
            .map(|mut row| {
                let mass = StoredMassRow {
                    day: row.text("day")?.to_string(),
                    records: row.parse("records")?,
                    tokens: row.parse("tokens")?,
                };
                row.end()?;
                Ok(mass)
            })
            .collect()
    }

    /// What `window`'s stores establish about a cross-instance zero (spec 5.7), over the
    /// window's UTC days: the instances that stored into their own cache, their own and
    /// comparable stores, and the key spaces of the runs that made them. A run with no
    /// declared canary joins as `''`, so the runs that declared none count as one unkeyed
    /// space.
    ///
    /// # Errors
    /// Engine failure, or a row the engine returned that does not parse as declared.
    pub fn zero_basis(&self, window: &Window) -> Result<ZeroBasis, Error> {
        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let sql = format!(
            "SELECT s.instance_id, s.incarnation, s.own, s.comparable, k.canary \
             FROM ( \
               SELECT instance_id, incarnation, toUInt64(sum(own)) AS own, \
                      toUInt64(sum(comparable)) AS comparable \
               FROM facts_rollup_daily FINAL \
               WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' \
                 AND gone = 0 AND kind = 'store' \
                 AND day >= toDate(toDateTime({s}, 'UTC')) \
                 AND day <= toDate(toDateTime({e}, 'UTC')) \
               GROUP BY instance_id, incarnation HAVING own > 0 \
             ) AS s \
             LEFT JOIN ( \
               SELECT incarnation, canary FROM input_key_spaces FINAL \
               WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' \
                 AND gone = 0 \
             ) AS k ON s.incarnation = k.incarnation",
            s = crate::epoch_seconds(window.start_ms),
            e = crate::epoch_seconds((window.end_ms - 1.0).max(window.start_ms)),
        );
        let mut runs = std::collections::BTreeMap::new();
        let mut spaces = std::collections::BTreeSet::new();
        let out = self.engine.run(&sql)?;
        for mut row in Row::each("zero_basis", &out) {
            let run = (
                row.text("instance")?.to_string(),
                row.text("incarnation")?.to_string(),
            );
            let own: u64 = row.parse("own_stores")?;
            let comparable: u64 = row.parse("comparable_stores")?;
            let canary = row.text("canary")?.to_string();
            row.end()?;
            // A run declaring two canaries joins twice; its stores count once.
            runs.insert(run, (own, comparable));
            spaces.insert(canary);
        }
        let instances: std::collections::BTreeSet<&str> =
            runs.keys().map(|(instance, _)| instance.as_str()).collect();
        Ok(ZeroBasis {
            instances: instances.len() as u64,
            stores: runs.values().map(|(own, _)| own).sum(),
            comparable_stores: runs.values().map(|(_, comparable)| comparable).sum(),
            key_spaces: spaces.len() as u64,
        })
    }
}

/// One day of the stored-mass inventory. Plain counts, deliberately: counted at insert,
/// exact for what arrived — not a measurement over a coverage, so [`crate::Covered`] does
/// not apply and wrapping it would claim a basis the number does not have.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredMassRow {
    /// The UTC day, as the engine rendered it.
    pub day: String,
    /// Store events counted into this day.
    pub records: u64,
    /// Token mass stored this day.
    pub tokens: u64,
}
