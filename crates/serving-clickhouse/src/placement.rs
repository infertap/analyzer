// SPDX-License-Identifier: AGPL-3.0-only

//! Shape 10 — where the duplication sits, as co-residency TOKEN-TIME.
//!
//! **This register sums and the other one does not, which is the whole reason it exists.**
//! Shape 6's mass is an order statistic — a MAX simultaneous holder count — so it has no window
//! total at any grain. Token-time is an integral (Σ tokens × overlap duration), so it sums
//! across spans, days, pairs and identities, and a window figure, a trend and a matrix all
//! become expressible. The two are never added: `never_summed_with` rides the wire for exactly
//! that boundary.
//!
//! **`rows` and `within_engine` are disjoint partitions of the window total**, not two views of
//! it: a pair's two instances are equal or they are not, and there is no third case. The caller
//! splits them on instance equality; this layer emits one cell per pair and the arithmetic is
//! checkable rather than promised.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Dao;
use crate::Error;
use crate::{Covered, Holder};
use analyzer_analysis::Window;
use std::collections::{BTreeMap, BTreeSet};

/// One workload × holder-pair × day cell. Canonical `a < b` under the holder order, banked that
/// way, so one pair is one key whichever order its spans arrived in.
#[derive(Debug, Clone, PartialEq)]
pub struct PairRow {
    /// The UTC day, as `YYYY-MM-DD` — the cell's day index, rendered by the engine.
    pub day: String,
    /// The producer's declared class, or `undeclared` where it declared none (spec 2.7).
    pub workload: String,
    /// The pair's first holder — the lesser under the holder order.
    pub a: Holder,
    /// The pair's second holder — the greater, so `a < b` and one pair is one key.
    pub b: Holder,
    /// Token-milliseconds of co-residency. Coverage is the pair's OWN observed intersection.
    pub token_ms: Covered<f64>,
}

/// The window's placement figures, with the total the two partitions sum to.
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    /// One cell per workload × pair × day, for the `limit` heaviest pairs, each pair whole.
    pub rows: Vec<PairRow>,
    /// Σ over every cell **including any this response did not return** — the quantity `rows`
    /// and `within_engine` partition, and the denominator [`Truncation`] is a share of.
    pub window_total: Covered<f64>,
    /// What the limit left out, or `None` when nothing was left out.
    ///
    /// **A cap is a fact, not a horizon.** Pairs grow with the SQUARE of holders — 300 instances
    /// is 44,850 of them — so a response has to be bounded, and a bound nobody states reads as
    /// "this is all of it". Absent means the response is complete BECAUSE the count was taken,
    /// never because nobody looked.
    pub truncation: Option<Truncation>,
}

/// The pairs a limit excluded, and the mass they carried.
///
/// **The mass matters more than the count.** Fifty dropped pairs holding 0.1 % of the window are
/// a rounding error; three holding 40 % mean the reader is looking at the wrong picture. Reporting
/// only how many were dropped would leave that unanswerable.
#[derive(Debug, Clone, PartialEq)]
pub struct Truncation {
    /// Distinct pairs `rows` carries, every cell of each.
    pub pairs_returned: usize,
    /// Distinct pairs the window holds, counted — what `pairs_returned` was cut from.
    pub pairs_total: usize,
    /// Σ token-milliseconds over the excluded pairs, at the weakest coverage among them.
    pub token_ms_excluded: Covered<f64>,
}

/// **The pair's own basis, not the fleet's.** A cell measured while only one of its two engines
/// was observed is a weaker claim than one measured while both were, and a heatmap cell is
/// exactly where that difference would hide. Instance-grain: a per-rank gap inside one engine is
/// not visible here, which is why the name says so rather than saying "observed".
const PAIR_BASIS: &str = "pair-observed time, instance grain";

use analyzer_analysis::DAY_MS;

impl Dao<'_> {
    /// Co-residency token-time over `window`, one row per workload × pair × day.
    ///
    /// **Coverage on the window total is the WEAKEST cell it contains, never an average.** An
    /// average lets one thin pair vanish into twenty good ones, and a total reading 96 % while
    /// holding a 51 % cell is a naked number wearing a coverage figure. Any rollup added later
    /// inherits this rule.
    ///
    /// **Bounded by `limit` PAIRS, worst first — never by rows.** Truncating rows would return a
    /// pair's Tuesday and drop its Wednesday, so the console's per-pair sum would silently
    /// understate a pair it appears to have in full. A pair is whole or absent, and what is
    /// absent is counted and priced on [`Truncation`].
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn placement(&self, window: &Window, limit: usize) -> Result<Placement, Error> {
        // The producer's pairs: token-time per pair per day from
        // `replication_pairs_day`, keyed replace since pipeline 2 (FINAL, not gone); the basis
        // from each instance's spans that day, intersected here with placement's own formula.
        // `day` is a day index on both tables; the window's edges become indices the same
        // way the producer splits its windows.
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let lo = crate::engine::sql_i64(day_index(window.start_ms));
        let hi = crate::engine::sql_i64(day_index((window.end_ms - 1.0).max(window.start_ms)));
        let spans = self.instance_spans(&lo, &hi)?;
        let sql = format!(
            "SELECT toString(toDate(toDateTime(day * 86400, 'UTC'))), day, workload_class, \
                    a_instance, a_dp_rank, a_group_idx, \
                    b_instance, b_dp_rank, b_group_idx, \
                    sum(token_ms) \
             FROM replication_pairs_day FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} \
               AND day >= {lo} AND day <= {hi} \
             GROUP BY day, workload_class, a_instance, a_dp_rank, a_group_idx, \
                      b_instance, b_dp_rank, b_group_idx \
             HAVING sum(token_ms) > 0 \
             ORDER BY day, workload_class, a_instance, a_dp_rank, a_group_idx, \
                      b_instance, b_dp_rank, b_group_idx",
        );

        let out = self.engine.run(&sql)?;
        let mut rows = Vec::new();
        let mut total = 0.0f64;
        // The weakest cell, seeded high so an empty read states nothing rather than zero.
        let mut weakest = f64::INFINITY;
        for mut row in Row::each("placement", &out) {
            let day = row.text("day")?.to_string();
            let d: i64 = row.parse("day index")?;
            let workload = row.text("workload_class")?.to_string();
            let mut holder = |side: &str| -> Result<Holder, Error> {
                Ok(Holder {
                    instance: row.text(side)?.to_string(),
                    dp_rank: row.parse(side)?,
                    group_idx: row.parse(side)?,
                })
            };
            let a = holder("a")?;
            let b = holder("b")?;
            let value: f64 = row.parse("token_ms")?;
            row.end()?;
            let empty: Vec<(f64, f64)> = Vec::new();
            let observed = analyzer_analysis::instance_spans::pair_observed_ms(
                spans.get(&(d, a.instance.clone())).unwrap_or(&empty),
                spans.get(&(d, b.instance.clone())).unwrap_or(&empty),
            );
            // Clamped, never extrapolated: a pair observed longer than the day it is attributed
            // to would be a coverage above 1.0, which is a claim no basis supports.
            let coverage = (observed / DAY_MS).clamp(0.0, 1.0);

            total += value;
            weakest = weakest.min(coverage);
            rows.push(PairRow {
                day,
                workload,
                a,
                b,
                token_ms: Covered {
                    value,
                    register: "placement",
                    coverage,
                    coverage_kind: PAIR_BASIS,
                },
            });
        }

        let truncation = keep_worst_pairs(&mut rows, limit);

        Ok(Placement {
            window_total: Covered {
                value: total,
                register: "placement",
                // No cells, no coverage claim: an empty window has nothing to be confident
                // about, and 1.0 would read as "fully observed and genuinely zero".
                coverage: if rows.is_empty() && truncation.is_none() {
                    0.0
                } else {
                    weakest
                },
                coverage_kind: PAIR_BASIS,
            },
            truncation,
            rows,
        })
    }
}

/// Keep the `limit` worst PAIRS by window mass, dropping the rest from `rows`, and report what
/// went — or `None` when everything fits.
///
/// **Bounded by pairs, never by rows, and the distinction is the whole point.** Rows are
/// (workload × pair × day): a row cap would return a pair's Tuesday and drop its Wednesday, so a
/// consumer summing per pair would silently understate a pair it appears to hold in full. A pair
/// is whole or absent.
fn keep_worst_pairs(rows: &mut Vec<PairRow>, limit: usize) -> Option<Truncation> {
    let mut per_pair: BTreeMap<(String, Holder, Holder), f64> = BTreeMap::new();
    for r in rows.iter() {
        *per_pair
            .entry((r.workload.clone(), r.a.clone(), r.b.clone()))
            .or_insert(0.0) += r.token_ms.value;
    }
    let pairs_total = per_pair.len();
    if pairs_total <= limit {
        return None;
    }

    let mut ranked: Vec<((String, Holder, Holder), f64)> = per_pair.into_iter().collect();
    // Mass descending, then the key ascending — a TOTAL order, so the same window returns the
    // same pairs rather than a coin flip between two of equal mass.
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let kept: BTreeSet<(String, Holder, Holder)> =
        ranked.into_iter().take(limit).map(|(k, _)| k).collect();

    let mut excluded_mass = 0.0;
    // Seeded high so "nothing excluded" is distinguishable from "excluded at zero coverage".
    let mut excluded_weakest = f64::INFINITY;
    rows.retain(|r| {
        if kept.contains(&(r.workload.clone(), r.a.clone(), r.b.clone())) {
            return true;
        }
        excluded_mass += r.token_ms.value;
        excluded_weakest = excluded_weakest.min(r.token_ms.coverage);
        false
    });

    Some(Truncation {
        pairs_returned: kept.len(),
        pairs_total,
        token_ms_excluded: Covered {
            value: excluded_mass,
            register: "placement",
            coverage: if excluded_weakest.is_finite() {
                excluded_weakest
            } else {
                0.0
            },
            coverage_kind: PAIR_BASIS,
        },
    })
}

/// The day index of an instant, the producer's own split (`floor(ms / day)`).
fn day_index(ms: f64) -> i64 {
    #[allow(clippy::cast_possible_truncation)] // in-window instants fit i64 ms
    let day = (ms / DAY_MS).floor() as i64;
    day
}

/// `(day, instance) → merged observed spans`: the pair basis's substrate as the read holds it.
type InstanceSpans = std::collections::BTreeMap<(i64, String), Vec<(f64, f64)>>;

impl Dao<'_> {
    /// Each instance's observed spans per day over `[lo, hi]`, merged — the pieces the
    /// producer appended tick by tick, joined back into the day's span set.
    fn instance_spans(&self, lo: &str, hi: &str) -> Result<InstanceSpans, Error> {
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let out = self.engine.run(&format!(
            "SELECT day, instance_id, start_ms, end_ms FROM replication_instance_spans_day FINAL \
             WHERE fleet_id = {fleet} AND {meaning} AND gone = 0 AND day >= {lo} AND day <= {hi} \
             ORDER BY day, instance_id, start_ms"
        ))?;
        let mut per: InstanceSpans = InstanceSpans::new();
        for mut row in Row::each("instance spans", &out) {
            let day: i64 = row.parse("day")?;
            let instance = row.text("instance_id")?.to_string();
            let lo: f64 = row.parse("start_ms")?;
            let hi: f64 = row.parse("end_ms")?;
            row.end()?;
            let spans = per.entry((day, instance)).or_default();
            match spans.last_mut() {
                Some((_, mhi)) if lo <= *mhi => *mhi = mhi.max(hi),
                _ => spans.push((lo, hi)),
            }
        }
        Ok(per)
    }
}
