// SPDX-License-Identifier: AGPL-3.0-only

//! The thrash lens — shape 1 with the band dimension.
//!
//! **One method, three sources, one response shape** (the minute source
//! joined under the banded-minute ruling). Day grain reads `band_rollup_daily`, hour grain
//! `band_rollup_hourly`, and minute grain `band_minute`, the 48-hour banded cells repeat
//! compute emits with the band stamped per event,
//! with
//! reach measured against that table's own span. The caller passes a window and a grain; the
//! shape of what comes back does not depend on which source served it — which is what let
//! this grain join without an API change.
//!
//! **The band boundary has ONE spelling by construction**: `Band::of` at classify emission,
//! walking `Band::UPPER_EDGES_MS`. Every grain is banked, so no read computes a band in SQL. A
//! boundary spelled twice is correct in types and broken at the moment it becomes a SQL
//! string; the safest version of that rule is having nowhere to break.

use crate::drill::RawReach;
use crate::engine::quote_literal;
use crate::row::Row;
use crate::shape1::dim_values;
use crate::Error;
use crate::{Covered, Dao, Dim, Grain};
use analyzer_analysis::Window;

impl Dao<'_> {
    /// The recompute register banded by time-since-eviction, over `window`, bucketed by
    /// `grain`, additionally grouped by `dims`.
    ///
    /// The band is always in the grouping — it is what this method is for; [`Dao::waste_by`]
    /// is the same shape without it. `band` values are the closed set [`analyzer_analysis::emitted::Band::ALL`]
    /// enumerates. The `none` band is mass with no observed prior close — first stored in the
    /// window, or gone longer than the examined history reaches, indistinguishable by
    /// construction — and renders beside the observed bands, never stacked among them.
    ///
    /// # Errors
    /// Engine failure, or a row the engine returned that does not parse as declared.
    pub fn bands_by(
        &self,
        window: &Window,
        grain: Grain,
        dims: &[Dim],
        scope: &crate::Scope,
    ) -> Result<BandRead, Error> {
        let mut wanted: Vec<Dim> = dims.to_vec();
        wanted.sort_unstable();
        wanted.dedup();
        let cols: Vec<&str> = wanted
            .iter()
            .flat_map(|d| d.columns().iter().copied())
            .collect();
        let group = if cols.is_empty() {
            String::new()
        } else {
            format!(", {}", cols.join(", "))
        };

        let (sql, evidence) = self.bands_sql(window, grain, &wanted, &group, scope)?;

        let out = self.engine.run(&sql)?;
        let mut rows = Vec::new();
        for mut row in Row::each("bands_by", &out) {
            let bucket = row.text("bucket")?.to_string();
            let band = row.text("copies_band")?.to_string();
            let dims = dim_values(&wanted, &row.texts(cols.len(), "dimension")?);
            let events = row.parse("events")?;
            let tokens: f64 = row.parse("tokens")?;
            let wsum: f64 = row.parse("weighted_tokens")?;
            row.end()?;
            rows.push(BandRow {
                bucket,
                band,
                dims,
                events,
                tokens: Covered {
                    value: tokens,
                    register: ">=",
                    coverage: if tokens > 0.0 { wsum / tokens } else { 0.0 },
                    coverage_kind: "token-weighted",
                },
            });
        }
        Ok(BandRead { rows, evidence })
    }
}

impl Dao<'_> {
    /// The three sources' statements, one per grain — split from [`Dao::bands_by`] for
    /// length alone; the response shape they feed is the one thing this module promises.
    fn bands_sql(
        &self,
        window: &Window,
        grain: Grain,
        wanted: &[Dim],
        group: &str,
        scope: &crate::Scope,
    ) -> Result<(String, RawReach), Error> {
        Ok(match grain {
            // The banded minute cells (the banded-minute ruling): the band is
            // an event fact stamped at emission, so this is a grouped read over finished
            // cells (keyed replace since pipeline 1: FINAL, not gone) — never an event scan. Dims are the cells' own key: workload and model
            // ride; a holder cut at this grain is refused by name — the
            // 0014 cardinality line, and the incident's WHERE belongs to the drills.
            Grain::Minute => {
                // A holder cut has no minute table: the rank-grain
                // cells keep the instance and the rank and roll the group
                // up. A holder-shaped SCOPE or the instance cut is served from them; the
                // fleet cut from the fleet cells. Scoping and grouping are two doors into
                // one table choice, and the refusal names what has no table.
                if let Some(d) = wanted.iter().find(|d| matches!(d, Dim::Holder)) {
                    return Err(Error::GrainUnavailable(format!(
                        "the minute grain keeps cells per rank with cache groups and tiers \
                         rolled up; a {d:?} cut has no minute table — ask at `hour`, or \
                         scope to the instance or the holder",
                    )));
                }
                let rank = scope.is_holder_shaped() || wanted.contains(&Dim::Instance);
                let table = if rank {
                    "band_rank_minute"
                } else {
                    "band_minute"
                };
                let into = crate::engine::quote_ident(table);
                let fleet = quote_literal(&self.fleet_id);
                let meaning = self.meaning_pair();
                (
                    format!(
                        "SELECT toString(bucket), band{group}, \
                                toUInt64(sum(events)), toUInt64(sum(tokens)), \
                                sum(tokens * ifNull(cov.fraction, 0)) \
                         FROM {into} FINAL{cov} \
                         WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND {when}{scoped} \
                         GROUP BY bucket, band{group} HAVING sum(events) > 0 ORDER BY bucket, band{group}",
                        when = grain.window_predicate(window),
                        cov = self.coverage_join(grain),
                        scoped = scope.predicate_on(rank),
                    ),
                    // Measured against THIS table's own span, never the class cells' — each
                    // table keeps its own retention, and inferring reach from a sibling would
                    // render the difference as "no churn" (the adjacent-claim disease).
                    self.banked_band_reach(table, window)?,
                )
            }
            // The banked table: complete from the first tick, and the only source once the
            // events age out — so the durable grain carries no reach caveat. Fleet and the
            // meaning pair bind immediately above the statement.
            Grain::Day => {
                let fleet = quote_literal(&self.fleet_id);
                let meaning = self.meaning_pair();
                (
                    format!(
                        "SELECT toString(bucket), band{group}, \
                                toUInt64(sum(events)), toUInt64(sum(tokens)), \
                                sum(tokens * ifNull(cov.fraction, 0)) \
                         FROM band_rollup_daily FINAL{cov} \
                         WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND {when}{scoped} \
                         GROUP BY bucket, band{group} HAVING sum(events) > 0 ORDER BY bucket, band{group}",
                        when = grain.window_predicate(window),
                        cov = self.coverage_join(grain),
                        scoped = scope.predicate(),
                    ),
                    RawReach::Covers,
                )
            }
            // The banked hourly rollup, not an event scan: a scan is INPUT-priced, so the same
            // panel would cost milliseconds or tens of seconds with the customer's traffic, and
            // in a shared cell it lands on every other tenant's dashboard. The rollup is
            // output-priced like the day arm, at ~1.5-2x `class_rollup_hourly`'s row count.
            Grain::Hour => {
                let fleet = quote_literal(&self.fleet_id);
                let meaning = self.meaning_pair();
                (
                    format!(
                        "SELECT toString(bucket), band{group}, \
                                toUInt64(sum(events)), toUInt64(sum(tokens)), \
                                sum(tokens * ifNull(cov.fraction, 0)) \
                         FROM band_rollup_hourly FINAL{cov} \
                         WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND {when}{scoped} \
                         GROUP BY bucket, band{group} HAVING sum(events) > 0 ORDER BY bucket, band{group}",
                        when = grain.window_predicate(window),
                        cov = self.coverage_join(grain),
                        scoped = scope.predicate(),
                    ),
                    // This table's OWN span, never the raw horizon's — each keeps its own
                    // retention, and borrowing raw's reach would render the difference as
                    // "no churn" (the same rule the minute arm states).
                    self.banked_band_reach("band_rollup_hourly", window)?,
                )
            }
        })
    }

    /// Whether a banked band table reaches `window`'s start — that table's OWN measured
    /// span, with the same "no rows is a different sentence" rule the raw probe states.
    ///
    /// `table` is a static string from this module's own call sites and never a caller's
    /// value, which is why it interpolates without an identifier quote: there is no input
    /// here to quote.
    fn banked_band_reach(&self, table: &str, window: &Window) -> Result<RawReach, Error> {
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        // The minute cells expire by bucket age at the next merge, so a row past the
        // horizon can linger; bounding the probe by the same horizon the TTL reads keeps a
        // re-derived old day's cells from reading as reach.
        // The hourly table needs no bound: a re-derivation reaches back only the input log's
        // horizon (`ledger_retention_days`), which the defaults keep far inside
        // `hour_retention_days`.
        let bound = if table.ends_with("_minute") {
            format!(
                " AND bucket >= now() - INTERVAL {} DAY",
                crate::engine::sql_i64(i64::from(self.retention.minute_retention_days))
            )
        } else {
            String::new()
        };
        let out = self.engine.run(&format!(
            "SELECT toUnixTimestamp(min(bucket)) FROM {table} \
             WHERE fleet_id = {fleet} AND {meaning}{bound}"
        ))?;
        let from_ms = out
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|v| *v > 0.0)
            .map(|secs| secs * 1000.0);
        Ok(match from_ms {
            Some(from) if from <= window.start_ms => RawReach::Covers,
            _ => RawReach::Outside {
                raw_from_ms: from_ms,
            },
        })
    }
}

/// The thrash lens as a consumer reads it: the observed bands' rows, the gap-unobserved mass
/// beside them, and the holder cut's population where there is one.
#[derive(Debug, Clone, PartialEq)]
pub struct BandLens {
    /// The observed bands' rows, in bucket then band order; under a holder bound, only the
    /// kept holders'.
    pub rows: Vec<BandRow>,
    /// The `none` band's mass over the window: stores with no observed prior close, which
    /// first-seen and gone-past-lookback share by construction. A real zero at full coverage
    /// where the read looked and found none.
    pub gap_unobserved: Covered<f64>,
    /// The `none` band's events.
    pub gap_unobserved_events: u64,
    /// Under a holder cut: how many distinct holders had banded mass in the window before
    /// the bound cut them, and how many it kept. `None` without one.
    pub holders: Option<HolderBound>,
    /// Whether the source could see the window; see [`BandRead::evidence`].
    pub evidence: RawReach,
}

/// A holder cut's population and what the bound kept of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HolderBound {
    /// Distinct holders with banded mass in the window.
    pub population: u64,
    /// Holders the bound kept: the heaviest by window mass, each whole.
    pub returned: u64,
}

impl BandRead {
    /// The lens over these rows: the `none` band folded into one figure beside the observed
    /// bands, and, under a holder cut, the rows of the `holder_limit` heaviest holders by
    /// window mass (ties by name), with the population they were cut from.
    ///
    /// `holder_limit` is `None` when the request carries no holder dim; a limit applies only
    /// to rows that carry a holder.
    #[must_use]
    pub fn lens(self, holder_limit: Option<usize>) -> BandLens {
        let holder_of = |row: &BandRow| {
            row.dims
                .iter()
                .find(|(d, _)| *d == Dim::Holder)
                .map(|(_, h)| h.clone())
        };
        let kept = holder_limit.map(|limit| {
            let mut totals: std::collections::BTreeMap<String, f64> =
                std::collections::BTreeMap::new();
            for row in self.rows.iter().filter(|r| r.band != "none") {
                if let Some(holder) = holder_of(row) {
                    *totals.entry(holder).or_insert(0.0) += row.tokens.value;
                }
            }
            let population = totals.len() as u64;
            let mut ranked: Vec<(String, f64)> = totals.into_iter().collect();
            ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            ranked.truncate(limit);
            let kept: std::collections::BTreeSet<String> =
                ranked.into_iter().map(|(h, _)| h).collect();
            let bound = HolderBound {
                population,
                returned: kept.len() as u64,
            };
            (kept, bound)
        });

        let mut rows = Vec::new();
        let mut gap_events = 0;
        let mut gap: Option<Covered<f64>> = None;
        for row in self.rows {
            if row.band == "none" {
                gap_events += row.events;
                gap = Some(match gap.take() {
                    None => row.tokens,
                    Some(mut held) => {
                        held.value += row.tokens.value;
                        held.coverage = held.coverage.min(row.tokens.coverage);
                        held
                    }
                });
                continue;
            }
            if let (Some((kept, _)), Some(holder)) = (&kept, holder_of(&row)) {
                if !kept.contains(&holder) {
                    continue;
                }
            }
            rows.push(row);
        }
        BandLens {
            rows,
            gap_unobserved: gap.unwrap_or(Covered {
                value: 0.0,
                register: "recompute",
                coverage: 1.0,
                coverage_kind: "gap-unobserved share, event basis",
            }),
            gap_unobserved_events: gap_events,
            holders: kept.map(|(_, bound)| bound),
            evidence: self.evidence,
        }
    }
}

/// The thrash lens's rows, plus whether the source could actually see the window.
#[derive(Debug, Clone, PartialEq)]
pub struct BandRead {
    /// One row per bucket × band × dims with events, in bucket then band order.
    pub rows: Vec<BandRow>,
    /// [`RawReach::Covers`] at day grain always (the banked table outlives the events); at
    /// hour grain, whether the event horizon still reaches the window's start. **Carried
    /// rather than inferred from an empty result**, which is the whole point — see
    /// `crate::drill`'s module docs.
    pub evidence: RawReach,
}

/// One banded row of the recompute register — [`crate::WasteRow`] one dimension wider.
#[derive(Debug, Clone, PartialEq)]
pub struct BandRow {
    /// The time bucket, as the engine rendered it.
    pub bucket: String,
    /// A [`analyzer_analysis::emitted::Band::as_str`] value.
    pub band: String,
    /// The dimension values this row was grouped by, in the order [`Dim`] sorts.
    pub dims: Vec<(Dim, String)>,
    /// Store events summed into this row — the count behind `tokens`.
    pub events: u64,
    /// Banded recompute mass, with the coverage it was measured at.
    pub tokens: Covered<f64>,
}
