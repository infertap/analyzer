// SPDX-License-Identifier: AGPL-3.0-only

//! Shape 3 — observed coverage per UTC day, for the fleet or per instance.
//!
//! **Instance-time weighted**, matching `completeness::Coverage::fraction`. The other coverage
//! basis — token-weighted — rides shape 1's figures, and the two are not interchangeable.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Error;
use crate::{day_literal, Dao};
use analyzer_analysis::Window;

impl Dao<'_> {
    /// Observed coverage per UTC day — shape 3, the trust tile and the meter health.
    ///
    /// **One method with a grouping parameter, for the same reason [`Dao::waste_by`] is one
    /// method.** S1 wants the fleet figure per day; S5 wants it per instance. Written as two
    /// methods they drift on the weighting, which is exactly the thing that must not vary.
    ///
    /// **Instance-time weighted**, matching `completeness::Coverage::fraction`: covered
    /// milliseconds over observed milliseconds, never a mean of per-instance fractions. An
    /// unweighted average lets one briefly-seen instance carry the same weight as one seen all
    /// day.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn coverage(&self, window: &Window, by: CoverageBy) -> Result<Vec<CoverageRow>, Error> {
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let (select, group) = match by {
            CoverageBy::Fleet => ("''", ""),
            CoverageBy::Instance => ("instance_id", ", instance_id"),
        };
        let sql = format!(
            "SELECT toString(day), {select}, sum(covered_ms), sum(window_ms), \
                    toUInt64(uniqExact(instance_id)) \
             FROM coverage_daily FINAL \
             WHERE fleet_id = {fleet} AND {meaning} AND gone = 0 \
               AND day >= {lo} AND day <= {hi} \
             GROUP BY day{group} ORDER BY day{group}",
            lo = day_literal(window.start_ms),
            // the bucket containing the last instant the window covers — a day is atomic, the
            // same reading `Grain::window_predicate` applies to the rollups
            hi = day_literal((window.end_ms - 1.0).max(window.start_ms)),
        );
        let token_basis = self.token_basis(window, by)?;
        let out = self.engine.run(&sql)?;
        Row::each("coverage", &out)
            .map(|mut row| {
                let day = row.text("day")?;
                let instance = row.text("instance")?;
                let covered_ms: f64 = row.parse("covered_ms")?;
                let window_ms: f64 = row.parse("window_ms")?;
                let instances = row.parse("instances")?;
                row.end()?;
                let tokens = token_basis
                    .get(&(day.to_string(), instance.to_string()))
                    .copied();
                Ok(CoverageRow {
                    day: day.to_string(),
                    instance: (!instance.is_empty()).then(|| instance.to_string()),
                    covered_ms,
                    window_ms,
                    // Zero observed time carries no coverage claim at all — absent, not 100%,
                    // the same reading every other figure here applies to an empty denominator.
                    fraction: if window_ms > 0.0 {
                        covered_ms / window_ms
                    } else {
                        0.0
                    },
                    instances,
                    // The SECOND basis: at what coverage this row's
                    // classified mass was observed — Σ(coverage × tokens)/Σ tokens, with
                    // both sums behind the fraction so a consumer merging days merges
                    // honestly (fractions do not average; the sums do). Absent where the
                    // row has no classified mass: an engine seen but never classified
                    // states nothing on this basis, and absent beats zero.
                    token_wsum: tokens.map(|(w, _)| w),
                    token_mass: tokens.map(|(_, t)| t),
                    token_weighted_fraction: tokens.and_then(|(w, t)| (t > 0.0).then(|| w / t)),
                })
            })
            .collect()
    }

    /// The token basis per (day, grouping): Σ coverage-weighted tokens and Σ tokens from
    /// the classified rollup, the coverage × waste join aggregated to the coverage
    /// read's own grain.
    fn token_basis(&self, window: &Window, by: CoverageBy) -> Result<TokenBasis, Error> {
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let select = match by {
            CoverageBy::Fleet => "''",
            CoverageBy::Instance => "instance_id",
        };
        let out = self.engine.run(&format!(
            "SELECT toString(bucket), {select}, sum(tokens * ifNull(cov.fraction, 0)), toFloat64(sum(tokens)) \
             FROM class_rollup_daily FINAL{cov} \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} \
               AND bucket >= {lo} AND bucket <= {hi} \
             GROUP BY bucket, {select}",
            lo = day_literal(window.start_ms),
            hi = day_literal((window.end_ms - 1.0).max(window.start_ms)),
            cov = self.coverage_join(crate::Grain::Day),
        ))?;
        let mut basis = std::collections::BTreeMap::new();
        for mut row in Row::each("token basis", &out) {
            let key = (
                row.text("day")?.to_string(),
                row.text("instance")?.to_string(),
            );
            let wsum = row.parse("weighted_tokens")?;
            let tokens = row.parse("tokens")?;
            row.end()?;
            basis.insert(key, (wsum, tokens));
        }
        Ok(basis)
    }
}

/// The token basis per (day, grouping): (Σ coverage-weighted tokens, Σ tokens).
type TokenBasis = std::collections::BTreeMap<(String, String), (f64, f64)>;

/// How [`Dao::coverage`] groups its rows.
///
/// Two callers, one shape: the trust tile wants the fleet's figure per day, the meter health
/// wants it per instance. The grouping is a parameter for the same reason [`crate::Dim`] is one — two
/// methods would drift on the weighting, and the weighting is the part that must not vary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoverageBy {
    /// One row per day, every instance summed; `instance` is `None` on the row.
    Fleet,
    /// One row per `(day, instance)`.
    Instance,
}

/// One day's observed coverage, for the fleet or for one instance.
#[derive(Debug, Clone, PartialEq)]
pub struct CoverageRow {
    /// The UTC day, as the engine rendered it.
    pub day: String,
    /// `None` when the row is the fleet's figure for that day.
    pub instance: Option<String>,
    /// Observed milliseconds summed over the row's instances: `window_ms` less the uncertain spans.
    pub covered_ms: f64,
    /// The observed denominator: the part of the day stated, through the covered edge while the day
    /// is open and the whole day once it settles.
    pub window_ms: f64,
    /// `covered_ms / window_ms`, instance-time weighted. A zero denominator carries no claim.
    pub fraction: f64,
    /// How many instances the row aggregates — the denominator a fleet figure is read against:
    /// "94% covered" over two instances is a different statement than over eighty.
    pub instances: u64,
    /// The SECOND basis: Σ(coverage × tokens) over this row's classified
    /// mass — half of the token-weighted fraction, carried so day-merging stays honest.
    pub token_wsum: Option<f64>,
    /// The other half: Σ tokens of the row's classified mass.
    pub token_mass: Option<f64>,
    /// `token_wsum / token_mass`. Absent where the row has no classified mass — an engine
    /// observed but never classified states nothing on this basis, and absent beats zero.
    pub token_weighted_fraction: Option<f64>,
}
