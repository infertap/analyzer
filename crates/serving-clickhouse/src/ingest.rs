// SPDX-License-Identifier: AGPL-3.0-only

//! The ingest ledger — what arrived, when, and how much of it.
//!
//! **the ingest section, and the hydration source for the console's status strip.** SSE carries
//! DELTAS (`ingest_batch`), so a console loading at t₀ has nothing to render the dark strip from
//! until something happens to arrive. This is what it reads on load.
//!
//! **Wall-clock facts, and they stay wall-clock facts**. The
//! dark chrome carries these; the light canvas carries tick-clock accounting; a number never
//! crosses. Nothing here is a [`crate::Covered`] figure, because none of it is a measurement OF
//! THE FLEET — an odometer counts what we received, not what the fleet did.

use crate::engine::quote_literal;
use crate::row::Row;
use crate::Dao;
use crate::Error;

/// What the ledger says about intake.
#[derive(Debug, Clone, PartialEq)]
pub struct IngestStatus {
    /// The newest batch, or `None` when nothing has ever been ingested — which is a real state
    /// on day one and renders as its own sentence, never as zeroes.
    pub last: Option<Batch>,
    /// Batches ingested since install. The ledger is forever-sized and tiny.
    pub batches_total: u64,
    /// **The odometer.** Records received since install, across every batch.
    pub records_total: u64,
    /// Records whose `kind` the reader did not recognize. Carried because a rising count is
    /// the shape a producer-side change takes here, and it is invisible in the totals.
    pub unknown_kinds_total: u64,
}

/// One ingested batch.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    /// The batch's content hash — the ledger's key, and the `delivery_id` its events carry.
    pub batch_id: String,
    /// The segment file it came from — what the strip renders as "last segment".
    pub source: String,
    /// **OUR wall clock, at insert.** Not the producer's, and never compared against one.
    pub ingested_at_ms: f64,
    /// Records the batch carried — the ledger row's `record_count`, as ingest decoded them.
    pub records: u64,
    /// The engine-domain extent of the CACHE FACTS in this batch, where it carried any —
    /// read from the ledger row's own `first_at_ms`/`last_at_ms`, which ingest stamped at
    /// commit from the records it had decoded.
    ///
    /// **Cache facts only, and the scope is the clock-domain rule rather than a nicety.** A
    /// batch carries lifecycle records too, whose `at_ms` is the producer's WALL clock; a
    /// min/max across both would produce a span whose endpoints live in different domains — the
    /// cross-domain join design forbids outright. `None` where the batch carried no cache
    /// facts at all, which is the ordinary case for a lifecycle-only segment.
    pub cache_span_ms: Option<(f64, f64)>,
}

impl Dao<'_> {
    /// What the ingest ledger says right now.
    ///
    /// **Two statements, not one**, because the transport refuses multi-statement bodies
    /// (pinned in `tests/transport.rs`). They are separate reads of a ledger that
    /// only grows, so a batch landing between them adds a row the totals miss and the `last`
    /// row does not — which reads as a batch that arrived a moment ago, and is true.
    ///
    /// **No clock is read here.** The caller computes lag against its own clock from
    /// [`Batch::ingested_at_ms`]: no wall-clock function enters query text on an asserted path,
    /// so a test injects rather than races a UTC midnight.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn ingest_status(&self) -> Result<IngestStatus, Error> {
        // Newest by ingest time, then by id so two batches sharing a second are still ordered.
        // `FINAL`: `input_batches` is a `ReplacingMergeTree` keyed on the batch, so a
        // republished batch must read as one row, not two.
        //
        // The binding sits IMMEDIATELY above its statement: a comment between `quote_literal`
        // and the `format!` pushes the binding out of the static audit's lookback and the site
        // reads as unquoted.
        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let sql = format!(
            "SELECT batch_id, source, ingested_at_ms, \
                    toUInt64(record_count), first_at_ms, last_at_ms \
             FROM input_batches FINAL WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0 \
             ORDER BY ingested_at_ms DESC, batch_id DESC LIMIT 1"
        );
        let newest = self.engine.run(&sql)?;
        let last = Row::each("ingest_status", &newest)
            .next()
            .map(|mut row| -> Result<Batch, Error> {
                let batch_id = row.text("batch_id")?.to_string();
                let source = row.text("source")?.to_string();
                let ingested_at_ms = row.parse("ingested_at_ms")?;
                let records = row.parse("record_count")?;
                // The span rides the ledger row itself: ingest stamped `first_at_ms` and
                // `last_at_ms` at commit from the records it had decoded. Null on either side
                // is a lifecycle-only batch — a real state, `None`.
                let first_at: Option<f64> = row.nullable("first_at_ms")?;
                let last_at: Option<f64> = row.nullable("last_at_ms")?;
                row.end()?;
                Ok(Batch {
                    batch_id,
                    source,
                    ingested_at_ms,
                    records,
                    cache_span_ms: first_at.zip(last_at),
                })
            })
            .transpose()?;

        let (fleet, input) = (
            quote_literal(&self.fleet_id),
            quote_literal(analyzer_analysis::emitted::INPUT_FORMAT),
        );
        let totals_sql = format!(
            "SELECT toUInt64(count()), toUInt64(sum(record_count)), \
                    toUInt64(sum(unknown_kinds)) \
             FROM input_batches FINAL WHERE fleet_id = {fleet} AND semantics_version = {input} AND discriminator = '' AND gone = 0"
        );
        let totals = self.engine.run(&totals_sql)?;
        let mut totals = Row::single("ingest_status totals", &totals);
        let status = IngestStatus {
            last,
            batches_total: totals.parse("batches_total")?,
            records_total: totals.parse("records_total")?,
            unknown_kinds_total: totals.parse("unknown_kinds_total")?,
        };
        totals.end()?;
        Ok(status)
    }
}
