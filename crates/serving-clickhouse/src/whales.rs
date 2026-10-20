// SPDX-License-Identifier: AGPL-3.0-only

//! The whale panel — top-N identities by duplicated token-time.
//!
//! Reads the whale register (`analyzer_analysis::whale_days`): each settled day's identities
//! cut at the analyzer's `WHALE_CUT` by duplicated token-time (`replication_whale_day`), and the
//! day's totals over every identity (`replication_whale_day_total`). A window sums each
//! identity's days. The totals are exact; a row is exact for the days it was in the cut and
//! carries the most it can be short by: the floor of each stated day it was left out of, the
//! most an identity outside that day's cut duplicated.
//!
//! A day is stated once it settles, so the window reads its settled days, and says how many.
//! These rows carry no coverage figure: the register's integrals are exact for what placement
//! emitted, so the honest uncertainty is which days are stated, not a fraction.

use crate::engine::{quote_literal, sql_i64};
use crate::row::Row;
use crate::Dao;
use crate::Error;
use analyzer_analysis::Window;

/// One identity's window totals, fleet scope.
#[derive(Debug, Clone, PartialEq)]
pub struct WhaleRow {
    /// The identity the tree joins on: the tap's `content_id`, else the engine `block_id`.
    pub identity: String,
    /// The identity's token mass — max across its stores, the tree's node figure.
    pub n_tokens: i64,
    /// Token-milliseconds held at two or more simultaneous copies, over the days it was in the
    /// cut.
    pub dup_token_ms: f64,
    /// The most `dup_token_ms` is short by: the floors of the stated days it was not in the
    /// cut. Zero when it was in every day's cut, or a day's cut held every identity.
    pub dup_token_ms_bound: f64,
    /// Token-milliseconds held at any copy count, over the days it was in the cut.
    pub held_token_ms: f64,
    /// The stated days it was in the cut.
    pub days_in_cut: u32,
}

/// The whale panel: the heaviest duplicated identities, and the totals their share is
/// honest against.
#[derive(Debug, Clone, PartialEq)]
pub struct Whales {
    /// Top `limit` identities by `dup_token_ms`, descending, ties by identity.
    pub rows: Vec<WhaleRow>,
    /// The window's days the register has stated: the settled ones.
    pub days_stated: u32,
    /// Each day's cut.
    pub cut: u32,
    /// Σ over the stated days of the identities with duplicated time that day: identity-days,
    /// since an identity duplicated on two days counts on each.
    pub duplicated_identity_days: u64,
    /// Σ `dup_token_ms` over every identity on the stated days, returned or not.
    pub window_dup_token_ms: f64,
    /// Σ `held_token_ms` over every identity on the stated days — the share's denominator.
    pub window_held_token_ms: f64,
}

impl Dao<'_> {
    /// The heaviest duplicated identities over `window`'s stated days, fleet scope, ranked by
    /// duplicated token-time.
    ///
    /// # Errors
    /// Engine failure, or a row the engine returned that does not parse as declared.
    pub fn whales(&self, window: &Window, limit: usize) -> Result<Whales, Error> {
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let (day_lo, day_hi) = day_bounds(window);
        let (day_lo, day_hi) = (sql_i64(day_lo), sql_i64(day_hi));
        let totals = self.engine.run(&format!(
            "SELECT count(), toUInt32(max(cut)), toUInt64(sum(duplicated_identities)), \
                    sum(dup_token_ms), sum(held_token_ms), sum(floor_dup_token_ms) \
             FROM replication_whale_day_total FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} \
               AND toInt64(day) >= {day_lo} AND toInt64(day) < {day_hi}"
        ))?;
        let mut totals = Row::single("whale totals", &totals);
        let days_stated: u32 = totals.parse("days")?;
        let cut: u32 = totals.parse("cut")?;
        let duplicated_identity_days: u64 = totals.parse("duplicated_identities")?;
        let window_dup_token_ms: f64 = totals.parse("dup_token_ms")?;
        let window_held_token_ms: f64 = totals.parse("held_token_ms")?;
        let floors: f64 = totals.parse("floor_dup_token_ms")?;
        totals.end()?;

        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let out = self.engine.run(&format!(
            "SELECT w.identity, max(w.n_tokens), sum(w.dup_token_ms), sum(w.held_token_ms), \
                    toUInt32(count()), sum(t.floor_dup_token_ms) \
             FROM replication_whale_day AS w FINAL \
             INNER JOIN (SELECT day, floor_dup_token_ms FROM replication_whale_day_total FINAL \
                         WHERE fleet_id = {fleet} AND gone = 0 AND {meaning}) AS t \
               ON w.day = t.day \
             WHERE w.fleet_id = {fleet} AND w.gone = 0 AND {meaning} \
               AND toInt64(w.day) >= {day_lo} AND toInt64(w.day) < {day_hi} \
             GROUP BY w.identity \
             ORDER BY sum(w.dup_token_ms) DESC, w.identity ASC \
             LIMIT {limit}"
        ))?;
        let mut rows = Vec::new();
        for mut row in Row::each("whales", &out) {
            let identity = row.text("identity")?.to_string();
            let n_tokens = row.parse("n_tokens")?;
            let dup_token_ms: f64 = row.parse("dup_token_ms")?;
            let held_token_ms: f64 = row.parse("held_token_ms")?;
            let days_in_cut = row.parse("days_in_cut")?;
            let floors_in_cut: f64 = row.parse("floor_dup_token_ms")?;
            row.end()?;
            rows.push(WhaleRow {
                identity,
                n_tokens,
                dup_token_ms,
                dup_token_ms_bound: (floors - floors_in_cut).max(0.0),
                held_token_ms,
                days_in_cut,
            });
        }
        Ok(Whales {
            rows,
            days_stated,
            cut,
            duplicated_identity_days,
            window_dup_token_ms,
            window_held_token_ms,
        })
    }
}

/// The half-open epoch-day range covering `window` — the ledger's own grain.
fn day_bounds(window: &Window) -> (i64, i64) {
    use analyzer_analysis::DAY_MS;
    #[allow(clippy::cast_possible_truncation)] // day indices are small integers
    let lo = (window.start_ms / DAY_MS).floor() as i64;
    #[allow(clippy::cast_possible_truncation)]
    let hi = (window.end_ms / DAY_MS).ceil() as i64;
    (lo, hi.max(lo + 1))
}
