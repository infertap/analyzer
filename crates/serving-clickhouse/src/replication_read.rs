// SPDX-License-Identifier: AGPL-3.0-only

//! The copies histogram — the replication panel's read (the contract #1, stated;
//! suggested route `GET /v1/residual/replication`).
//!
//! Reads `replication_minute`, the streamed producer's serving table, and speaks the
//! console's vocabulary: duplicated token-time per bucket per FIXED copy band — `2`,
//! `3-4`, `5-8`, `9+` — which nests losslessly over the emitted bands (`3-4` sums `3`
//! and `4`; `9+` sums `9-16` and `17+`; band `1` is excluded from the histogram rows and
//! included in the held denominator). The panel's only cut is WORKLOAD (or fleet, the
//! `""` scope); no class dim, no locality dim on the rows — locality serves the remedy
//! split scalars instead.
//!
//! **No coverage figure, by statement**: the producer's integrals are exact through its
//! emission frontier, and `emitted_to_ms` is the honest edge — same pattern as whales.

use crate::engine::{quote_ident, quote_literal, sql_i64};
use crate::row::Row;
use crate::Error;
use crate::{Dao, Grain};
use analyzer_analysis::Window;

/// The console's fixed histogram bands, in render order, with the emitted bands each
/// one sums. Band `1` deliberately absent: the histogram states DUPLICATED mass.
pub const HISTOGRAM_BANDS: [(&str, &[&str]); 4] = [
    ("k2", &["2"]),
    ("k34", &["3", "4"]),
    ("k58", &["5-8"]),
    ("k9", &["9-16", "17+"]),
];

// The served grain is the workspace's one `Grain`, so the copies chart and the recompute chart
// draw one bar width for one window. Minute cells serve every coarser bucket by summation, so
// one measured reach (`replication_reach_ms`) bounds every tier.

/// One bucket's duplicated token-time by histogram band, in band order (k2/k34/k58/k9).
#[derive(Debug, Clone, PartialEq)]
pub struct CopiesBucket {
    /// Bucket start, engine-domain ms.
    pub bucket_ms: i64,
    /// Duplicated token-milliseconds per band, indexed as [`HISTOGRAM_BANDS`] orders them.
    pub bands_token_ms: [f64; 4],
}

/// One workload's membership row: its share of the fleet's duplicated mass, and the
/// band profile of its own duplicated mass.
#[derive(Debug, Clone, PartialEq)]
pub struct CopiesMember {
    /// The workload class, as declared (spec 2.7) — never `""`, which is the fleet scope.
    pub workload: String,
    /// Σ duplicated token-ms over the window, this member's own.
    pub dup_token_ms: f64,
    /// This member's share of the fleet's duplicated mass (`dup_token_ms / dup_total`); zero
    /// where the fleet has none.
    pub share: f64,
    /// Band shares of the member's own duplicated mass, summing to 1 (all zero when the
    /// member has no duplicated mass).
    pub profile: [f64; 4],
}

/// The copies histogram, one read: buckets for the selected cut, members for the table,
/// and the fleet scalars the anchor renders.
#[derive(Debug, Clone, PartialEq)]
pub struct CopiesData {
    /// The bucket width `rows` are stated at — the caller's grain, served from its own tier table.
    pub grain: Grain,
    /// One bucket per grain step holding duplicated mass, in time order, for the selected cut.
    pub rows: Vec<CopiesBucket>,
    /// Every workload with duplicated mass in the window, plus the fleet is implicit
    /// (its totals ride the scalars below).
    pub members: Vec<CopiesMember>,
    /// The selected cut's Σ duplicated token-ms over the window.
    pub window_dup_token_ms: f64,
    /// Fleet scalars, always fleet-scope whatever the cut: the remedy split (disjoint
    /// partition of duplicated mass) and the mean-copies pair.
    ///
    /// **The split is THREE-WAY and the three sum to `dup_total_token_ms`**
    /// (`analysis::residual::locality`). A two-way read would drop
    /// the third silently — the mass would still be in the total and in neither remedy,
    /// which is the failure the reconciliation pin below exists to make impossible.
    pub cross_engine_token_ms: f64,
    /// Duplicated mass across one engine's data-parallel ranks — the DP-affinity remedy's share.
    pub within_engine_token_ms: f64,
    /// Duplicated mass that exists only because the model splits its layers into
    /// separate KV cache groups — one prefix resident in each group of ONE rank.
    /// **Architecture, not waste**: no routing or placement change removes it, and it
    /// must never be read as the engine placing redundantly.
    pub cache_group_token_ms: f64,
    /// Σ duplicated token-ms at fleet scope — the denominator the three remedies
    /// partition. Carried so a reader can CHECK the partition rather than trust it.
    pub dup_total_token_ms: f64,
    /// Σ copies × token-time over ALL cached mass — mean = this / held.
    pub copies_token_ms: f64,
    /// Σ token-time held at any copy count, band `1` included — the mean-copies denominator.
    pub window_held_token_ms: f64,
    /// Band shares of the FLEET's own duplicated mass (the members table's fleet row).
    pub fleet_profile: [f64; 4],
    /// The producer's emission frontier; `None` before its first applied tick.
    pub emitted_to_ms: Option<f64>,
}

/// One node's INTERNAL duplication over a window: mass it held more than once inside
/// itself, split by how many copies and by which of the two internal causes.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceDuplication {
    /// `within` — across this node's data-parallel ranks. The lever: vLLM's own DP
    /// balancer scores by load with no prefix term, so a gateway that sets
    /// `data_parallel_rank` by prefix affinity is what removes this mass.
    pub within_token_ms: f64,
    /// `group` — across one rank's KV cache groups. A hybrid model's architecture; no
    /// policy removes it, and it is carried beside the other so the node does not appear
    /// to duplicate less than it does.
    pub group_token_ms: f64,
    /// Per `(bucket, band)` rows, worst-first by mass — the shape a sparkline reads.
    pub rows: Vec<InstanceDupRow>,
    /// Σ copies × token-time over this node's internal duplication.
    pub copies_token_ms: f64,
    /// **The denominator**: all token-time this node held, band `1` included — held once is
    /// still held. Without it the duplication figures are numerators a reader cannot weigh,
    /// and the honest ratio ("how much of what this node holds is held twice inside it")
    /// cannot be formed at all. Added after the console could only render the
    /// duplication's own COMPOSITION — which states its character and never its size.
    pub held_token_ms: f64,
}

/// One bucket of one node's internal duplication.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceDupRow {
    /// Bucket start, engine-domain ms — minute or hour wide, whichever tier the grain served.
    pub bucket_ms: i64,
    /// The emitted copy-count band (`2`, `3`, `4`, `5-8`, `9-16`, `17+`); band `1` is never a row.
    pub copies_band: String,
    /// The internal cause: `within` (across DP ranks) or `group` (across one rank's cache groups).
    pub locality: String,
    /// Duplicated token-milliseconds in this `(bucket, band, locality)` cell.
    pub token_ms: f64,
}

impl Dao<'_> {
    /// One instance's internal duplication, at `grain`: the minute table for
    /// [`Grain::Minute`] (the minute knob's reach), the hourly table otherwise — one
    /// sweep's cells bucketed two ways by the producer, so the tiers cannot disagree.
    ///
    /// **Internal only, and the absence is the point.** Copies spanning engines belong to
    /// no single node — attributing them to each participant would double the mass on any
    /// sum across instances — so the producer never writes them here and this read cannot
    /// accidentally report them. The cross-engine half stays a fleet figure.
    ///
    /// # Errors
    /// Engine failure, or a row that does not parse as declared.
    pub fn instance_duplication(
        &self,
        window: &Window,
        instance: &str,
        grain: Grain,
    ) -> Result<InstanceDuplication, Error> {
        let table = instance_tier(grain);
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let inst = quote_literal(instance);
        let lo = sql_i64(to_ms(window.start_ms) / 1000);
        let hi = sql_i64(to_ms(window.end_ms) / 1000);
        let out = self.engine.run(&format!(
            "SELECT toUnixTimestamp(bucket), copies_band, locality, \
                    sum(token_ms), sum(copies_token_ms) \
             FROM {into} FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND instance_id = {inst} \
               AND copies_band != '1' \
               AND bucket >= toDateTime({lo}, 'UTC') AND bucket < toDateTime({hi}, 'UTC') \
             GROUP BY bucket, copies_band, locality \
             ORDER BY bucket, copies_band, locality"
        ))?;
        // The denominator, over EVERY band including `1` — a separate aggregate rather than
        // a widened one, because the rows above are deliberately duplicated-mass only and
        // folding held-once into them would put a non-duplicate in the duplication rows.
        let held = self.instance_held_token_ms(window, instance, table)?;
        let mut data = InstanceDuplication {
            within_token_ms: 0.0,
            group_token_ms: 0.0,
            rows: Vec::new(),
            copies_token_ms: 0.0,
            held_token_ms: held,
        };
        for mut row in Row::each("instance_duplication", &out) {
            let bucket: i64 = row.parse("bucket")?;
            let band = row.text("copies_band")?.to_string();
            let loc = row.text("locality")?;
            let token_ms: f64 = row.parse("token_ms")?;
            data.copies_token_ms += row.parse::<f64>("copies_token_ms")?;
            row.end()?;
            match loc {
                "within" => data.within_token_ms += token_ms,
                "group" => data.group_token_ms += token_ms,
                // A locality this read has not been taught is a fact it will not fold into
                // one it knows: counted nowhere rather than silently added to `within`.
                _ => {}
            }
            data.rows.push(InstanceDupRow {
                bucket_ms: bucket * 1000,
                copies_band: band,
                locality: loc.to_string(),
                token_ms,
            });
        }
        Ok(data)
    }

    /// One node's total held token-time over `window` — every band, `1` included.
    ///
    /// # Errors
    /// Engine failure.
    fn instance_held_token_ms(
        &self,
        window: &Window,
        instance: &str,
        table: &str,
    ) -> Result<f64, Error> {
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let inst = quote_literal(instance);
        let lo = sql_i64(to_ms(window.start_ms) / 1000);
        let hi = sql_i64(to_ms(window.end_ms) / 1000);
        let out = self.engine.run(&format!(
            "SELECT sum(token_ms) FROM {into} FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND instance_id = {inst} \
               AND bucket >= toDateTime({lo}, 'UTC') AND bucket < toDateTime({hi}, 'UTC')"
        ))?;
        Ok(out.trim().parse().unwrap_or(0.0))
    }

    /// The copies histogram over `window` at `grain`, cut to `workload` (`None` = fleet).
    /// The grain is the caller's, resolved against [`Dao::replication_reach_ms`]; every read
    /// below serves from that grain's own table, so a 14-day stop sums
    /// daily cells, not minute ones.
    ///
    /// # Errors
    /// Engine failure, or a row the engine returned that does not parse as declared.
    pub fn copies(
        &self,
        window: &Window,
        grain: Grain,
        workload: Option<&str>,
    ) -> Result<CopiesData, Error> {
        let scope = workload.unwrap_or("");
        let rows = self.copies_buckets(window, grain, scope)?;
        let mut members = self.copies_members(window, grain)?;
        let window_dup_token_ms = if scope.is_empty() {
            self.copies_scope_dup(window, grain, "")?
        } else {
            members
                .iter()
                .find(|m| m.workload == scope)
                .map_or(0.0, |m| m.dup_token_ms)
        };
        let scalars = self.copies_fleet_scalars(window, grain)?;
        // MEASURED, never derived from the remedy buckets: a sum of `cross` and `within` would
        // leave the third remedy out of every member's denominator, and the shares would sum
        // past 1 with no error anywhere.
        for member in &mut members {
            member.share = if scalars.dup_total > 0.0 {
                member.dup_token_ms / scalars.dup_total
            } else {
                0.0
            };
        }
        let fleet_profile = self.copies_profile(window, grain)?;
        Ok(CopiesData {
            grain,
            rows,
            members,
            window_dup_token_ms,
            cross_engine_token_ms: scalars.cross,
            within_engine_token_ms: scalars.within,
            cache_group_token_ms: scalars.group,
            dup_total_token_ms: scalars.dup_total,
            copies_token_ms: scalars.copies,
            window_held_token_ms: scalars.held,
            fleet_profile,
            emitted_to_ms: self.emitted_frontier_ms()?,
        })
    }

    /// The fleet's own band profile — shares of ITS duplicated mass, the members
    /// table's first row.
    fn copies_profile(&self, window: &Window, grain: Grain) -> Result<[f64; 4], Error> {
        let (table, col) = copies_tier(grain);
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let lo = sql_i64(to_ms(window.start_ms));
        let hi = sql_i64(to_ms(window.end_ms));
        let out = self.engine.run(&format!(
            "SELECT copies_band, sum(token_ms) FROM {into} FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND workload_class = '' \
               AND copies_band != '1' AND {col} >= {lo} AND {col} < {hi} \
             GROUP BY copies_band FORMAT TSV"
        ))?;
        let mut bands = [0.0f64; 4];
        for mut row in Row::each("copies profile", &out) {
            let band = row.text("copies_band")?;
            let sum: f64 = row.parse("token_ms")?;
            row.end()?;
            if let Some(slot) = histogram_slot(band) {
                bands[slot] += sum;
            }
        }
        let dup: f64 = bands.iter().sum();
        if dup > 0.0 {
            Ok([
                bands[0] / dup,
                bands[1] / dup,
                bands[2] / dup,
                bands[3] / dup,
            ])
        } else {
            Ok([0.0; 4])
        }
    }

    /// The oldest bucket one tier's table holds under this meaning, INSIDE that tier's
    /// horizon — the tier's measured reach.
    ///
    /// **Bounded on purpose**. The tables expire by bucket age at the next merge, so a
    /// cell past the horizon can linger, and the repair writes a whole day's difference onto
    /// that day's first bucket — an island exactly where an unbounded `min()` would find it.
    /// The bound and the TTL read one constant per tier, so they agree.
    ///
    /// # Errors
    /// Engine failure.
    pub fn replication_reach_ms(&self, grain: Grain) -> Result<Option<f64>, Error> {
        let (table, col) = copies_tier(grain);
        let horizon_days = i64::from(match grain {
            Grain::Minute => self.retention.minute_retention_days,
            Grain::Hour => self.retention.hour_retention_days,
            Grain::Day => self.retention.rollup_retention_years * 366,
        });
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let horizon_ms = sql_i64(horizon_days * 86_400_000);
        let out = self.engine.run(&format!(
            "SELECT min({col}) FROM {into} \
             WHERE fleet_id = {fleet} AND {meaning} \
               AND {col} >= toInt64(toUnixTimestamp(now())) * 1000 - {horizon_ms}"
        ))?;
        Ok(out.trim().parse::<f64>().ok().filter(|v| *v > 0.0))
    }

    fn copies_buckets(
        &self,
        window: &Window,
        grain: Grain,
        scope: &str,
    ) -> Result<Vec<CopiesBucket>, Error> {
        // The served grain's own table: its rows ARE the buckets, so nothing re-buckets
        // minute cells at hour or day any more.
        let (table, col) = copies_tier(grain);
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let cut = quote_literal(scope);
        let lo = sql_i64(to_ms(window.start_ms));
        let hi = sql_i64(to_ms(window.end_ms));
        let out = self.engine.run(&format!(
            "SELECT {col} AS b, copies_band, sum(token_ms) \
             FROM {into} FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND workload_class = {cut} \
               AND copies_band != '1' AND {col} >= {lo} AND {col} < {hi} \
             GROUP BY b, copies_band ORDER BY b FORMAT TSV"
        ))?;
        let mut buckets: std::collections::BTreeMap<i64, [f64; 4]> =
            std::collections::BTreeMap::new();
        for mut row in Row::each("copies buckets", &out) {
            let b: i64 = row.parse("bucket")?;
            let band = row.text("copies_band")?;
            let sum: f64 = row.parse("token_ms")?;
            row.end()?;
            if let Some(slot) = histogram_slot(band) {
                buckets.entry(b).or_default()[slot] += sum;
            }
        }
        Ok(buckets
            .into_iter()
            .map(|(bucket_ms, bands_token_ms)| CopiesBucket {
                bucket_ms,
                bands_token_ms,
            })
            .collect())
    }

    fn copies_members(&self, window: &Window, grain: Grain) -> Result<Vec<CopiesMember>, Error> {
        let (table, col) = copies_tier(grain);
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let lo = sql_i64(to_ms(window.start_ms));
        let hi = sql_i64(to_ms(window.end_ms));
        let out = self.engine.run(&format!(
            "SELECT workload_class, copies_band, sum(token_ms) \
             FROM {into} FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND workload_class != '' \
               AND copies_band != '1' AND {col} >= {lo} AND {col} < {hi} \
             GROUP BY workload_class, copies_band ORDER BY workload_class FORMAT TSV"
        ))?;
        let mut by_member: std::collections::BTreeMap<String, [f64; 4]> =
            std::collections::BTreeMap::new();
        for mut row in Row::each("copies members", &out) {
            let w = row.text("workload_class")?;
            let band = row.text("copies_band")?;
            let sum: f64 = row.parse("token_ms")?;
            row.end()?;
            if let Some(slot) = histogram_slot(band) {
                by_member.entry(w.to_string()).or_default()[slot] += sum;
            }
        }
        Ok(by_member
            .into_iter()
            .map(|(workload, bands)| {
                let dup: f64 = bands.iter().sum();
                let profile = if dup > 0.0 {
                    [
                        bands[0] / dup,
                        bands[1] / dup,
                        bands[2] / dup,
                        bands[3] / dup,
                    ]
                } else {
                    [0.0; 4]
                };
                CopiesMember {
                    workload,
                    dup_token_ms: dup,
                    share: 0.0,
                    profile,
                }
            })
            .collect())
    }

    fn copies_scope_dup(&self, window: &Window, grain: Grain, scope: &str) -> Result<f64, Error> {
        let (table, col) = copies_tier(grain);
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let cut = quote_literal(scope);
        let lo = sql_i64(to_ms(window.start_ms));
        let hi = sql_i64(to_ms(window.end_ms));
        let out = self.engine.run(&format!(
            "SELECT sum(token_ms) FROM {into} FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND workload_class = {cut} \
               AND copies_band != '1' AND {col} >= {lo} AND {col} < {hi}"
        ))?;
        Ok(out.trim().parse().unwrap_or(0.0))
    }

    /// The anchor's fleet scalars in one aggregate: the remedy split over duplicated
    /// mass, the mean-copies numerator over ALL mass, and the held denominator. One read of
    /// one row, so the six cannot drift from each other.
    fn copies_fleet_scalars(&self, window: &Window, grain: Grain) -> Result<CopiesScalars, Error> {
        let (table, col) = copies_tier(grain);
        let into = quote_ident(table);
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let lo = sql_i64(to_ms(window.start_ms));
        let hi = sql_i64(to_ms(window.end_ms));
        let out = self.engine.run(&format!(
            "SELECT sumIf(token_ms, locality = 'cross' AND copies_band != '1'), \
                    sumIf(token_ms, locality = 'within' AND copies_band != '1'), \
                    sumIf(token_ms, locality = 'group' AND copies_band != '1'), \
                    sumIf(token_ms, copies_band != '1'), \
                    sum(copies_token_ms), sum(token_ms) \
             FROM {into} FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND workload_class = '' \
               AND {col} >= {lo} AND {col} < {hi}"
        ))?;
        let mut row = Row::single("copies totals", &out);
        let totals = CopiesScalars {
            cross: row.parse("cross_token_ms")?,
            within: row.parse("within_token_ms")?,
            group: row.parse("group_token_ms")?,
            dup_total: row.parse("duplicated_token_ms")?,
            copies: row.parse("copies_token_ms")?,
            held: row.parse("token_ms")?,
        };
        row.end()?;
        Ok(totals)
    }

    /// The producer's applied emission frontier — shared provenance with the whale
    /// read, and the anchor the panel's relative stops resolve against.
    ///
    /// # Errors
    /// Engine failures.
    pub fn emitted_frontier_ms(&self) -> Result<Option<f64>, Error> {
        Ok(self.status_fact("emitted_to_ms")?.filter(|v| *v > 0.0))
    }
}

/// The fleet's copies scalars over one window, token-time (milliseconds × tokens) each: the
/// three remedies' duplicated mass, their total, the mean-copies numerator over all mass, and
/// the held denominator.
struct CopiesScalars {
    cross: f64,
    within: f64,
    group: f64,
    dup_total: f64,
    copies: f64,
    held: f64,
}

/// Which histogram slot an emitted band sums into; band `1` and unknown spellings sum
/// nowhere (the caller filtered band `1` already; an unknown spelling is a generation
/// mismatch the meaning pair already fences).
fn histogram_slot(band: &str) -> Option<usize> {
    HISTOGRAM_BANDS
        .iter()
        .position(|(_, sources)| sources.contains(&band))
}

#[allow(clippy::cast_possible_truncation)] // engine-domain instants fit i64 ms
fn to_ms(v: f64) -> i64 {
    v as i64
}

/// The per-instance table for `grain`: the minute cut for [`Grain::Minute`], the hourly cut
/// for every wider grain (a day ask reads hour buckets; the instance page states the width
/// it was served).
fn instance_tier(grain: Grain) -> &'static str {
    match grain {
        Grain::Minute => "replication_instance_minute",
        Grain::Hour | Grain::Day => "replication_instance_hourly",
    }
}

/// The table and bucket column one grain of the copies read serves from: the minute cells, or the hourly or daily rollup
/// the tick writes beside them. One key, one measure pair, three widths.
fn copies_tier(grain: Grain) -> (&'static str, &'static str) {
    match grain {
        Grain::Minute => ("replication_minute", "minute_ms"),
        Grain::Hour => ("replication_hourly", "bucket_ms"),
        Grain::Day => ("replication_daily", "bucket_ms"),
    }
}

#[cfg(test)]
mod tests {
    use super::{histogram_slot, HISTOGRAM_BANDS};
    use crate::Grain;
    use analyzer_analysis::Window;

    /// Every emitted band above 1 lands in exactly one histogram slot, and band 1 in
    /// none — the nesting that makes the console's vocabulary a lossless sum of ours.
    #[test]
    fn the_histogram_bands_nest_losslessly_over_the_emitted_ones() {
        for (band, slot) in [
            ("2", Some(0)),
            ("3", Some(1)),
            ("4", Some(1)),
            ("5-8", Some(2)),
            ("9-16", Some(3)),
            ("17+", Some(3)),
            ("1", None),
        ] {
            assert_eq!(histogram_slot(band), slot, "band {band}");
        }
        assert_eq!(HISTOGRAM_BANDS.len(), 4);
    }

    /// One resolver for this read: the workspace's auto chooser, fed this table's own reach
    /// for BOTH tiers, since its coarser buckets are sums of its minute cells. Minute while
    /// the span fits the minute budget and the reach covers the start; hour while the hour
    /// budget fits; day past that — and day whatever the span once the reach is behind it.
    #[test]
    fn the_copies_grain_ladders_by_span_against_one_reach() {
        const MIN: f64 = 60_000.0;
        let w = |start: f64, minutes: f64| Window {
            start_ms: start,
            end_ms: start + minutes * MIN,
        };
        let pick = |window: &Window, reach: Option<f64>| Grain::auto(window, reach, reach);
        assert_eq!(pick(&w(1_000_000.0, 180.0), Some(0.0)), Grain::Minute);
        assert_eq!(pick(&w(1_000_000.0, 240.0), Some(0.0)), Grain::Minute);
        assert_eq!(pick(&w(1_000_000.0, 241.0), Some(0.0)), Grain::Hour);
        assert_eq!(pick(&w(1_000_000.0, 200.0 * 60.0), Some(0.0)), Grain::Hour);
        assert_eq!(
            pick(&w(1_000_000.0, 200.0 * 60.0 + 1.0), Some(0.0)),
            Grain::Day
        );
        // Reach clamps: an uncovered start coarsens to day whatever the span.
        assert_eq!(pick(&w(1_000_000.0, 60.0), Some(2_000_000.0)), Grain::Day);
        assert_eq!(pick(&w(1_000_000.0, 60.0), None), Grain::Day);
    }
}
