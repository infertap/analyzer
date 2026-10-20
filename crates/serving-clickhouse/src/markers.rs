// SPDX-License-Identifier: AGPL-3.0-only

//! Shape 7 — discontinuity markers. One concept, two provenances.
//!
//! Observed transitions and recorded annotations render identically and mean the same thing, so
//! they are one table with a `provenance` column rather than two mechanisms.
//! [`Marker::breaks`] is where the one distinction that matters is decided.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Dao;
use crate::Error;
use analyzer_analysis::Window;

impl Dao<'_> {
    /// Discontinuity markers touching `window` — shape 7, the "did the number's meaning change".
    ///
    /// **CLOCK DOMAIN: engine `at_ms`, so this is an exact window and not a clip.** `markers.at_ms`
    /// is engine domain by contract (migration 0001) — `param_change` stamps the first bucket its
    /// generation is keyed under, which is the position the seam actually occupies on a trend.
    /// That is what lets the predicate here be the window itself: same domain both sides, no
    /// mapping, no slack.
    ///
    /// **Which generation was in force during a window is a different question and this is not
    /// it.** This returns markers INSIDE the window, which is what an overlay needs; the last
    /// marker at or before the window start is a separate read nothing asks for yet.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn markers(&self, window: &Window) -> Result<Vec<Marker>, Error> {
        // Declared deploys under the input meaning, generation seams under the generations
        // meaning: neither belongs to one generation.
        let (fleet, input, generations) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
            quote_literal(analyzer_analysis::emitted::GENERATIONS),
        );
        let sql = format!(
            "SELECT at_ms, kind, provenance, label, from_value, to_value, source, \
                    declared_at_ms \
             FROM markers FINAL \
             WHERE fleet_id = {fleet} AND gone = 0 \
               AND semantics_version IN ({input}, {generations}) AND discriminator = '' \
               AND at_ms >= {lo} AND at_ms < {hi} \
             ORDER BY at_ms, kind, label",
            lo = window.start_ms,
            hi = window.end_ms,
        );
        let out = self.engine.run(&sql)?;
        Row::each("markers", &out)
            .map(|mut row| {
                let marker = Marker {
                    at_ms: row.parse("at_ms")?,
                    kind: row.text("kind")?.to_string(),
                    provenance: row.text("provenance")?.to_string(),
                    label: row.text("label")?.to_string(),
                    from_value: row.text_or_null("from_value")?.map(str::to_string),
                    to_value: row.text_or_null("to_value")?.map(str::to_string),
                    source: row.text("source")?.to_string(),
                    declared_at_ms: row.nullable("declared_at_ms")?,
                };
                row.end()?;
                Ok(marker)
            })
            .collect()
    }
}

/// One discontinuity marker.
#[derive(Debug, Clone, PartialEq)]
pub struct Marker {
    /// See [`Dao::markers`] on which clock this is — it is not the trend's.
    pub at_ms: f64,
    /// `param_change` today; observed transitions when they ship.
    pub kind: String,
    /// `recorded` or `observed` — the two provenances one concept has.
    pub provenance: String,
    /// The marker's own sentence: a deploy's text, or the analyzer's wording of a generation seam.
    pub label: String,
    /// Both sides of a parameter change, so a discontinuity's cause is legible from the marker
    /// alone rather than by diffing two rollup reads.
    pub from_value: Option<String>,
    /// The value in force after the marker. `None` on deploy annotations, which name no generation.
    pub to_value: Option<String>,
    /// Who wrote it: `analyzer` for derived seams; `cli`, `api` or `pipeline` for a deploy.
    pub source: String,
    /// When a DECLARED marker's statement was made, distinct from [`Marker::at_ms`] — the
    /// instant it is about. The lateness of a declaration is provenance a
    /// renderer may state. `None` on derived markers.
    pub declared_at_ms: Option<f64>,
}

impl Marker {
    /// Does this marker break the series, or does the series continue across it?
    ///
    /// **One concept, two provenances, and only one of them breaks anything**.
    /// A parameter change or a semantics change means the rows either
    /// side are not one quantity — the series stops and is never drawn across it. A deploy
    /// annotation means the FLEET changed, which is a thing the series exists to show: it
    /// continues, with the marker drawn on it.
    ///
    /// **Decided here rather than by whoever renders**, because it is the difference between an
    /// annotation and a refusal to sum, and a consumer that got it wrong would either blend two
    /// generations or refuse to compare across a deploy. Both are wrong in ways nobody notices
    /// from the output.
    /// **A BASELINE is not a seam, and this is the correction the API goldens forced.**
    ///
    /// The first settled day states a baseline marker on each axis, so the earliest
    /// trend point has a stated basis. Those rows carry `from_value: None` — they name no prior
    /// generation, because there was none. Treating them as breaking split every window at the
    /// fleet's own beginning and produced an `Unlinked` segment covering the span BEFORE any
    /// data existed, with the real figures pushed into a second segment.
    ///
    /// Read plainly: a discontinuity is a transition, and a transition needs two sides. A marker
    /// that names only where a generation STARTED is an origin, not a break.
    #[must_use]
    pub fn breaks(&self) -> bool {
        matches!(self.kind.as_str(), "param_change" | "semantics_change")
            && self.from_value.is_some()
    }
}

// The annotation contract moved DOWN to `analyzer_ingest::annotations` when the bucket-object
// form landed: ingest is the crate
// that owns inbound formats and their row writers, and the prefix walk needs the same spelling
// this crate's callers use. Re-exported here so the CLI and the API keep one surface.
pub use analyzer_ingest::annotations::{DeployAnnotation, DeploySource};
