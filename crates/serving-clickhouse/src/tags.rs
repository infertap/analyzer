// SPDX-License-Identifier: AGPL-3.0-only

//! The tag grouped read: the declared tag intervals and their cells (`analyzer_analysis::tags`)
//! grouped by tag over a window, with each tag's exposure, its two figures and the window's
//! coverage beside them. A tag is grouped byte for byte and never interpreted.
//!
//! Exposure is read from the declared intervals, clipped to the classified edge: past it no store
//! has been classified, so a minute there would count exposure with no chance of refetch. Labels
//! and identities are opaque strings, so the rows come back as JSON rather than as tab-separated
//! text a label could break.

use crate::engine::{quote_literal, sql_i64};
use crate::{Dao, Error};
use analyzer_analysis::tags::{exposure_minutes, Exposed, TagFigures, TagSums};
use analyzer_analysis::Window;
use std::collections::BTreeMap;

/// The read's answer.
#[derive(Debug, Clone, PartialEq)]
pub struct TagRead {
    /// Where exposure stops: the window's end, or the classified edge where that is earlier.
    /// `None` when nothing has been classified, and then no tag is read.
    pub through_ms: Option<f64>,
    /// Observed milliseconds over the window's days, summed across the fleet's instances.
    pub covered_ms: f64,
    /// The coverage denominator over the same days.
    pub window_ms: f64,
    /// Each tag with its figures, in the labels' byte order.
    pub tags: Vec<(String, TagFigures)>,
}

/// One tag's identities, each with its intervals and its summed cells.
type Grouped = BTreeMap<String, BTreeMap<String, (Vec<(f64, Option<f64>)>, TagSums)>>;

impl Dao<'_> {
    /// The tags declared over `window`, each with its exposure and figures.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn tags(&self, window: &Window) -> Result<TagRead, Error> {
        let (covered_ms, window_ms) = self.window_coverage(window)?;
        let through_ms = self
            .edges()?
            .classified_to_ms
            .map(|edge| edge.min(window.end_ms))
            .filter(|&to| to > window.start_ms);
        let Some(to) = through_ms else {
            return Ok(TagRead {
                through_ms: None,
                covered_ms,
                window_ms,
                tags: Vec::new(),
            });
        };
        let mut grouped = Grouped::new();
        // The bounds widened to whole milliseconds: exposure is clipped exactly below.
        for row in self.json_rows(&format!(
            "SELECT tag, identity, from_ms, until_ms FROM tag_intervals FINAL \
             WHERE fleet_id = {fleet} AND {meaning} AND gone = 0 \
               AND from_ms < {to} AND (until_ms IS NULL OR until_ms > {from})",
            fleet = quote_literal(&self.fleet_id),
            meaning = self.meaning_pair(),
            from = sql_i64(whole_ms(window.start_ms)),
            to = sql_i64(whole_ms(to.ceil())),
        ))? {
            let until = row.get("until_ms").and_then(serde_json::Value::as_f64);
            grouped
                .entry(text(&row, "tag")?)
                .or_default()
                .entry(text(&row, "identity")?)
                .or_default()
                .0
                .push((number(&row, "from_ms")?, until));
        }
        for row in self.json_rows(&format!(
            "SELECT tag, identity, sum(stores) AS stores, sum(prefill_tokens) AS prefill, \
                    sum(refetch_tokens) AS refetch, sum(indeterminate_tokens) AS indeterminate, \
                    sum(ambiguous_stores) AS ambiguous \
             FROM tag_minute FINAL \
             WHERE fleet_id = {fleet} AND {meaning} AND gone = 0 \
               AND bucket >= toDateTime(intDiv({from}, 1000)) \
               AND bucket < toDateTime(intDiv({to}, 1000)) \
             GROUP BY tag, identity",
            fleet = quote_literal(&self.fleet_id),
            meaning = self.meaning_pair(),
            from = sql_i64(whole_ms(window.start_ms)),
            to = sql_i64(whole_ms(to)),
        ))? {
            let sums = TagSums {
                stores: integer(&row, "stores")?,
                prefill_tokens: integer(&row, "prefill")?,
                refetch_tokens: integer(&row, "refetch")?,
                indeterminate_tokens: integer(&row, "indeterminate")?,
                ambiguous_stores: integer(&row, "ambiguous")?,
            };
            // A cell is only ever attributed inside an interval, so its identity has one; one
            // without is a declaration replaced since, and its cells are counted all the same.
            grouped
                .entry(text(&row, "tag")?)
                .or_default()
                .entry(text(&row, "identity")?)
                .or_default()
                .1 = sums;
        }
        let tags = grouped
            .into_iter()
            .map(|(tag, identities)| {
                let exposed: Vec<Exposed> = identities
                    .into_values()
                    .map(|(intervals, sums)| Exposed {
                        minutes: exposure_minutes(&intervals, window.start_ms, to),
                        sums,
                    })
                    .collect();
                (tag, TagFigures::of(&exposed))
            })
            .collect();
        Ok(TagRead {
            through_ms,
            covered_ms,
            window_ms,
            tags,
        })
    }

    /// Covered and observed milliseconds over `window`'s days, summed across the fleet.
    fn window_coverage(&self, window: &Window) -> Result<(f64, f64), Error> {
        let rows = self.coverage(window, crate::coverage::CoverageBy::Fleet)?;
        Ok(rows
            .iter()
            .fold((0.0, 0.0), |(c, w), r| (c + r.covered_ms, w + r.window_ms)))
    }

    /// `sql`'s rows as JSON objects, 64-bit integers unquoted.
    fn json_rows(&self, sql: &str) -> Result<Vec<serde_json::Value>, Error> {
        self.engine
            .run(&format!(
                "{sql} SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow"
            ))?
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).map_err(|e| Error::Row(format!("tags: {e}: {l}"))))
            .collect()
    }
}

#[allow(clippy::cast_possible_truncation)] // instants of the read's window fit i64
fn whole_ms(ms: f64) -> i64 {
    ms.floor() as i64
}

fn text(row: &serde_json::Value, column: &str) -> Result<String, Error> {
    row.get(column)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Error::Row(format!("tags: no {column} in {row}")))
}

fn number(row: &serde_json::Value, column: &str) -> Result<f64, Error> {
    row.get(column)
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| Error::Row(format!("tags: no {column} in {row}")))
}

fn integer(row: &serde_json::Value, column: &str) -> Result<i64, Error> {
    row.get(column)
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| Error::Row(format!("tags: no {column} in {row}")))
}
