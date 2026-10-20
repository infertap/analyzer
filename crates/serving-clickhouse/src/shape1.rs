// SPDX-License-Identifier: AGPL-3.0-only

//! Shape 1 — aggregate the recompute register over a window, bucketed and grouped.
//!
//! **Seven of the nineteen panels are this one shape**, differing only in `(grain, dims,
//! window)`: the headline and trend, the deltas and top movers, and all three S3 splits.
//! Written as seven methods they drift within a month.
//!
//! [`Dao::waste_by_segmented`] is the same shape over a range that crosses a meaning seam. It
//! lives here because it IS shape 1 — one read per generation rather than one read.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Error;
use crate::{Covered, Dao, Dim, Discriminator, Grain, Marker};
use analyzer_analysis::Window;

impl Dao<'_> {
    /// Aggregate the recompute register over `window`, bucketed by `grain`, grouped by `dims`.
    ///
    /// **One method because it is one shape.** The headline and trend, the deltas and top
    /// movers, and all three S3 splits are this call with a different `(grain, dims, window)`.
    /// Written as seven methods they drift; there is one place to change.
    ///
    /// Coverage is computed, never assumed: `sum(tokens × day fraction) / sum(tokens)` is the
    /// token-weighted coverage of the grouped rows, the day's fraction joined at read time
    /// (`coverage_join`) — a weighted pair summed once, never an average a `GROUP BY` would
    /// silently re-average.
    ///
    /// # Errors
    /// Engine failure, or a row the engine returned that does not parse as declared.
    pub fn waste_by(
        &self,
        window: &Window,
        grain: Grain,
        dims: &[Dim],
        scope: &crate::Scope,
    ) -> Result<Vec<WasteRow>, Error> {
        // Deduplicated and ordered, so two callers asking for the same dimensions in a
        // different order get the same SQL — and therefore the same query cache entry and the
        // same column order back.
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
        // `FINAL` and `gone = 0`: the cell tables are keyed replace, written whole by the
        // keyed driver — the latest version per key is the cell, a tombstone hides a
        // key the driver stopped producing, and the sum here is the read's own cut over
        // finished cells, never a derivation of them. The HAVING stays as the belt to the
        // braces: every served cell carries at least one event.
        //
        // The quoted values are bound immediately above the statement, which is what keeps the
        // quoting inside `static_audit::sql_construction_*`'s detection window (the shape
        // `pass.rs` documents).
        // **Never sum across a meaning generation, and there are TWO halves of one.** Without
        // the pair this method would add a day's rows under `js-5` to the same day's rows under
        // `js-6` and report the sum as one number. The values are the running binary's, because
        // that is the only meaning this process can explain.
        // At minute grain a holder-shaped cut or scope is served from the rank-grain cells,
        // which keep the instance and the rank and roll the group up; a holder
        // or tier cut has no minute table and is refused by name rather than answered
        // by a column that is not there.
        let rank =
            grain == Grain::Minute && (scope.is_holder_shaped() || wanted.contains(&Dim::Instance));
        if grain == Grain::Minute {
            if let Some(d) = wanted.iter().find(|d| matches!(d, Dim::Holder)) {
                return Err(Error::GrainUnavailable(format!(
                    "the minute grain keeps cells per rank with cache groups and tiers \
                     rolled up; a {d:?} cut has no minute table — ask at `hour`, or scope \
                     to the instance or the holder"
                )));
            }
        }
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let sql = format!(
            "SELECT toString(bucket){group}, \
                    toUInt64(sum(events)), toUInt64(sum(tokens)), sum(tokens * ifNull(cov.fraction, 0)) \
             FROM {table} FINAL{cov} \
             WHERE fleet_id = {fleet} AND gone = 0 AND {meaning} AND {when}{scoped} \
             GROUP BY bucket{group} HAVING sum(events) > 0 ORDER BY bucket{group}",
            table = grain.class_table(rank),
            when = grain.window_predicate(window),
            cov = self.coverage_join(grain),
            // Bound immediately above the statement like every other interpolation here;
            // `Scope::predicate_on` routes each value through `quote_literal` itself.
            scoped = scope.predicate_on(rank),
        );

        let out = self.engine.run(&sql)?;
        let mut rows = Vec::new();
        for mut row in Row::each("waste_by", &out) {
            let bucket = row.text("bucket")?.to_string();
            // One field per COLUMN, one entry per DIMENSION: `Dim::Holder` expands to three
            // columns, so the dimension values are cut by column count, never zipped.
            let dims = dim_values(&wanted, &row.texts(cols.len(), "dimension")?);
            // An integer, never through f64: a count is exact only below 2^53 as a float.
            let events: u64 = row.parse("events")?;
            let tokens: f64 = row.parse("tokens")?;
            let wsum: f64 = row.parse("weighted_tokens")?;
            row.end()?;
            rows.push(WasteRow {
                bucket,
                dims,
                events,
                tokens: Covered {
                    value: tokens,
                    register: ">=",
                    // Token-weighted, so a group of rows at mixed coverage reports the coverage
                    // its MASS was observed at rather than an unweighted average of fractions.
                    // Zero tokens carries no coverage claim at all — absent, not 100%.
                    coverage: if tokens > 0.0 { wsum / tokens } else { 0.0 },
                    // **this said `window-time` while computing the
                    // token-weighted figure**, which is the one thing `coverage_kind` exists to
                    // prevent — two figures at "80% coverage" on different bases are not
                    // comparable, and a wrong label is worse than a missing one. The basis rule
                    // is general: a coverage figure rendered beside numbers states THOSE
                    // numbers' basis, and this one qualifies a token mass.
                    coverage_kind: "token-weighted",
                },
            });
        }
        Ok(rows)
    }

    /// [`Dao::waste_by`] over a window that may cross a meaning seam, returned as one segment per
    /// generation and **never as one blended series**.
    ///
    /// **Why this is a method and not the API's job.** A [`Dao`] is bound to one meaning pair —
    /// that is what makes never-sum structural rather than remembered. So a range crossing a
    /// parameter change cannot be served by one binding, and the only alternative to this method
    /// is a caller constructing a second `Dao` from a marker it read itself: composition, in the
    /// layer that is supposed to be a pass-through, over a rule that exists to be unforgettable.
    ///
    /// **The two halves of the meaning pair are NOT symmetric, and the asymmetry is the
    /// interesting part.**
    ///
    /// - A **discriminator** seam separates rows this binary can still read. Old and new coexist
    ///   by key — the entire point of the discriminator — so both segments carry figures, read
    ///   under their own generation.
    /// - A **`semantics_version`** seam does not. `SEMANTICS_VERSION` is a compile-time constant,
    ///   so a `js-6` binary has no `js-5` join semantics to explain a `js-5` row with. Those
    ///   segments come back with [`Segment::figures`] `None` and a stated reason.
    ///
    /// Refusing to render is the honest move rather than a lossy one: the alternative is a
    /// binary claiming numbers it cannot account for. A consumer hatches the span the way it
    /// hatches an unexamined day. **What it must never do is render it as zero.**
    ///
    /// **A broken chain is stated, never guessed.** Segments are derived by walking the window's
    /// markers newest-first and linking each `to_value` to the generation after it. Where a
    /// marker does not link — a lineage this binary was not part of, a marker written by a
    /// version whose token grammar differs — the segment is [`SegmentBasis::Unlinked`] and
    /// carries no figures. Inferring the missing generation is exactly the guess that would put
    /// one generation's rows under another's name.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row from either read.
    pub fn waste_by_segmented(
        &self,
        window: &Window,
        grain: Grain,
        dims: &[Dim],
        scope: &crate::Scope,
    ) -> Result<Vec<Segment>, Error> {
        self.generation_spans(window)?
            .iter()
            .map(|spec| self.segment(window, grain, dims, scope, spec))
            .collect()
    }

    /// The meaning generation in force over each span of `window`, **oldest first**.
    ///
    /// **and the extraction IS the fix for a real defect.**
    /// `waste_by_segmented` walked the markers to learn which generation each span belonged to;
    /// [`Dao::changed`] split at the SAME markers and then read every span through the running
    /// binary's own discriminator. Where the span it picked belonged to a prior generation the
    /// read matched nothing, `Side::default()` supplied zeros, and a fully populated comparison
    /// came back describing a window it could not read — with a named marker and a days-summed
    /// count that made it look considered.
    ///
    /// One walk, two callers, so "which generation is this?" has exactly one answer.
    ///
    /// # Errors
    /// Engine failure, or an unparseable marker row.
    pub(crate) fn generation_spans(&self, window: &Window) -> Result<Vec<SegmentSpec>, Error> {
        let mut breaking: Vec<Marker> = self
            .markers(window)?
            .into_iter()
            .filter(Marker::breaks)
            .collect();
        // newest first: the walk starts from the generation this process is in and works back
        breaking.sort_by(|a, b| b.at_ms.total_cmp(&a.at_ms));

        let mut basis = SegmentBasis::Current;
        let mut discriminator = self.discriminator.clone();
        let mut semantics = analyzer_analysis::SEMANTICS_VERSION.to_string();
        let mut end_ms = window.end_ms;

        let mut spans = Vec::new();
        for marker in breaking {
            spans.push(SegmentSpec {
                basis,
                discriminator: discriminator.clone(),
                semantics: semantics.clone(),
                start_ms: marker.at_ms,
                end_ms,
            });

            // Step back across the marker. `to_value` must name the generation we just described
            // or the chain does not link, and a link we cannot verify is one we do not make.
            let links = marker.to_value.as_deref()
                == Some(match marker.kind.as_str() {
                    "semantics_change" => semantics.as_str(),
                    _ => discriminator.as_str(),
                });
            match (links, marker.from_value.as_deref()) {
                (true, Some(from)) if marker.kind == "semantics_change" => {
                    semantics = from.to_string();
                    basis = SegmentBasis::SupersededSemantics;
                }
                (true, Some(from)) => {
                    discriminator = Discriminator::observed(from);
                    basis = SegmentBasis::PriorParameters;
                }
                _ => basis = SegmentBasis::Unlinked,
            }
            end_ms = marker.at_ms;
        }
        spans.push(SegmentSpec {
            basis,
            discriminator,
            semantics,
            start_ms: window.start_ms,
            end_ms,
        });

        spans.reverse();
        Ok(spans)
    }

    /// Read shape 1 under a NAMED generation rather than the running binary's own.
    ///
    /// A prior parameter generation's rows coexist by key — that is what the discriminator is
    /// for — so reading them is a matter of asking under the right token rather than a refusal.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub(crate) fn waste_by_under(
        &self,
        discriminator: &Discriminator,
        window: &Window,
        grain: Grain,
        dims: &[Dim],
        scope: &crate::Scope,
    ) -> Result<Vec<WasteRow>, Error> {
        Self {
            engine: self.engine,
            retention: self.retention,
            spine: self.spine.clone(),
            serving: self.serving.clone(),
            sources: self.sources.clone(),
            fleet_id: self.fleet_id.clone(),
            discriminator: discriminator.clone(),
            params: self.params.clone(),
        }
        .waste_by(window, grain, dims, scope)
    }

    /// One generation's slice of a segmented read.
    fn segment(
        &self,
        window: &Window,
        grain: Grain,
        dims: &[Dim],
        scope: &crate::Scope,
        spec: &SegmentSpec,
    ) -> Result<Segment, Error> {
        let slice = Window {
            start_ms: spec.start_ms.max(window.start_ms),
            end_ms: spec.end_ms.min(window.end_ms),
        };
        // A generation this process cannot explain gets no read at all — not an empty one. The
        // distinction survives to the wire, where `None` and `[]` are different claims.
        let figures = match spec.basis {
            SegmentBasis::SupersededSemantics | SegmentBasis::Unlinked => None,
            SegmentBasis::Current | SegmentBasis::PriorParameters => {
                Some(self.waste_by_under(&spec.discriminator, &slice, grain, dims, scope)?)
            }
        };
        Ok(Segment {
            discriminator: spec.discriminator.as_str().to_string(),
            semantics_version: spec.semantics.clone(),
            basis: spec.basis,
            window: slice,
            figures,
        })
    }
}

/// One grouped row of the recompute register.
#[derive(Debug, Clone, PartialEq)]
pub struct WasteRow {
    /// The time bucket, as the engine rendered it.
    pub bucket: String,
    /// The dimension values this row was grouped by, in the order [`Dim`] sorts.
    pub dims: Vec<(Dim, String)>,
    /// Store events summed into this row — the count behind `tokens`, filters applied.
    pub events: u64,
    /// Recompute mass. Carries its coverage, because a figure that could not is the thing
    /// [`Covered`] exists to prevent.
    pub tokens: Covered<f64>,
}

/// What a segment's generation is, and therefore whether this process can state its figures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentBasis {
    /// The running binary's own meaning pair. Figures are read directly.
    Current,
    /// A prior parameter generation, named by a marker that links. Its rows coexist by key, so
    /// they are read under that discriminator — the coexistence the discriminator exists for.
    PriorParameters,
    /// A prior join-semantics generation. **No figures**: `SEMANTICS_VERSION` is compile-time, so
    /// this binary has no semantics to explain those rows with, and stating them anyway would be
    /// a claim it cannot account for.
    SupersededSemantics,
    /// A marker whose `to_value` does not name the generation after it, so the chain does not
    /// link. **No figures**, because the only alternative is guessing which generation this span
    /// belonged to — and a wrong guess files one generation's rows under another's name, which is
    /// the exact failure the discriminator was introduced to end.
    Unlinked,
}

/// Why a segment carries no figures — the closed set the API maps onto its wire causes.
/// A `match` at that boundary makes adding one a compile error until somebody maps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalCause {
    /// A prior join-semantics generation, which this binary has no semantics to explain.
    SupersededSemantics,
    /// A marker chain that does not link, so which parameters produced a span is unknown.
    UnlinkedGeneration,
}

impl SegmentBasis {
    /// Why a segment carries no figures, for a consumer that must say so on a surface.
    ///
    /// Returns the closed [`RefusalCause`] rather than a token, for the reason stated
    /// there: a string crossing this seam is a second vocabulary nothing checks.
    #[must_use]
    pub fn refusal(self) -> Option<RefusalCause> {
        match self {
            Self::Current | Self::PriorParameters => None,
            Self::SupersededSemantics => Some(RefusalCause::SupersededSemantics),
            Self::Unlinked => Some(RefusalCause::UnlinkedGeneration),
        }
    }
}

/// One generation's span of a window: which meaning was in force, and over what range.
///
/// `pub(crate)` so `changed` can ask the same question `waste_by_segmented`
/// asks, rather than splitting at the same markers and assuming the answer.
pub(crate) struct SegmentSpec {
    pub(crate) basis: SegmentBasis,
    pub(crate) discriminator: Discriminator,
    pub(crate) semantics: String,
    pub(crate) start_ms: f64,
    pub(crate) end_ms: f64,
}

/// One meaning generation's slice of a range that crosses a seam.
#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    /// The parameter generation's token the segment's rows are keyed under.
    pub discriminator: String,
    /// The join-semantics version the segment's rows carry; `SEMANTICS_VERSION` on the current one.
    pub semantics_version: String,
    /// Whether this binary can state the segment's figures — see [`SegmentBasis`].
    pub basis: SegmentBasis,
    /// The sub-window this generation was in force over, clipped to the requested window.
    pub window: Window,
    /// `None` where this process cannot state figures for the generation — never an empty vector,
    /// which would read as "looked and found nothing". See [`SegmentBasis`].
    pub figures: Option<Vec<WasteRow>>,
}

/// Pair each dimension with its own field(s), consuming as many as it has columns.
///
/// A multi-column dimension arrives joined by a tab, so the triple stays ONE value, which is what [`Dim::Holder`] being one
/// dimension rather than three means at the type level. Whoever needs the parts splits them.
pub(crate) fn dim_values(wanted: &[Dim], fields: &[&str]) -> Vec<(Dim, String)> {
    let mut out = Vec::with_capacity(wanted.len());
    let mut at = 0;
    for dim in wanted {
        let width = dim.columns().len();
        // Defensive: the caller built `fields` from the same `wanted`, so a short slice means the
        // engine returned fewer columns than the SELECT named. Truncating is wrong and panicking
        // is worse; the row simply carries what arrived and the arity check above catches it.
        let end = fields.len().min(at + width);
        out.push((*dim, fields[at..end].join("\t")));
        at = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::dim_values;
    use crate::Dim;

    #[test]
    fn a_multi_column_dimension_consumes_its_own_fields_and_not_its_neighbours() {
        // The defect, latent from the day `waste_by` was written and found the moment a second
        // caller asked for more than one dimension: `Dim::Holder` is ONE dimension over THREE
        // columns, so zipping dimensions against fields positionally handed `Workload` the value
        // of `dp_rank`. Nothing errored — every field is a string, and the row looked fine.
        let wanted = [Dim::Class, Dim::Holder, Dim::Workload, Dim::Model];
        let fields = ["ETR", "i-001", "3", "1", "chat-serve", "llama-70b"];

        let out = dim_values(&wanted, &fields);

        assert_eq!(
            out,
            vec![
                (Dim::Class, "ETR".to_string()),
                // the triple stays one value, tab-joined as the engine delivered it
                (Dim::Holder, "i-001\t3\t1".to_string()),
                (Dim::Workload, "chat-serve".to_string()),
                (Dim::Model, "llama-70b".to_string()),
            ],
            "each dimension must consume exactly as many fields as it has columns"
        );
    }

    #[test]
    fn the_single_dimension_case_every_existing_caller_uses_is_unchanged() {
        // Why the bug survived: this is the only shape anyone exercised.
        assert_eq!(
            dim_values(&[Dim::Class], &["post_clear"]),
            vec![(Dim::Class, "post_clear".to_string())]
        );
    }
}
