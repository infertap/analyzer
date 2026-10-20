// SPDX-License-Identifier: AGPL-3.0-only

//! The derivation's input seam beyond the pure one: [`FillSource`] extends
//! [`analyzer_analysis::events::EventSource`] with the bounded, ledger-aware streaming read the
//! fill uses. The dependency-free crate keeps only what a derivation needs — events for a
//! window, lifecycle for a range — and the fill's mechanics (object keys, exclusions,
//! per-segment sinks) live here, beside the readers that use them.

use analyzer_analysis::events::{Event, EventSource};
use analyzer_analysis::Window;

/// The streaming read's sink: one segment's events in bounded batches, plus that
/// segment's OBJECT KEY ('' where the source has none) — per-segment consumers (the
/// fill's ledger) need the key with the rows, not at stream end.
pub type SegmentSink<'a> = dyn FnMut(Vec<Event>, &str) -> Result<(), analyzer_analysis::Error> + 'a;

/// What a fill already holds, for a streaming read to skip before it fetches or decodes —
/// the input log's batches, as the source seam reads them. An empty exclusion streams everything.
#[derive(Default)]
pub struct FillExclusion {
    /// Segments already folded, by OBJECT KEY — skipped before any fetch, the cheap exclusion.
    pub object_keys: std::collections::HashSet<String>,
    /// Deliveries already folded, by BATCH ID (content hash) — skipped before any decode: the
    /// same bytes under a new key.
    pub batch_ids: std::collections::HashSet<String>,
    /// When set, the fill's UNIVERSE: keys outside it are skipped before any fetch.
    /// The keyed reader states it: the committed batches it was delivered, because the
    /// input log already knows which segments carry
    /// which days, and the horizon walk that fetched every day to fill one moved
    /// 62 GiB of S3 for zero rows. `None` = no scoping (the
    /// deep audit's whole-span fill).
    pub only_keys: Option<std::collections::HashSet<String>>,
}

/// Selection of events from source carriers, separate from the output interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InputSelection {
    /// Every event in each selected carrier; no sentinel timestamp bounds.
    WholeBatches,
    /// Events in a finite half-open interval.
    Window(Window),
}

impl InputSelection {
    /// Validate any interval before source I/O.
    ///
    /// # Errors
    /// Invalid interval bounds.
    pub fn validate(self) -> Result<(), analyzer_analysis::Error> {
        match self {
            Self::WholeBatches => Ok(()),
            Self::Window(window) => window.validate(),
        }
    }
    /// Optional bounds for columnar row-group pruning.
    #[must_use]
    pub fn bounds(self) -> Option<(f64, f64)> {
        match self {
            Self::WholeBatches => None,
            Self::Window(w) => Some((w.start_ms, w.end_ms)),
        }
    }
    /// Whether an event is selected, preserving fractional milliseconds.
    #[must_use]
    pub fn contains(self, at_ms: f64) -> bool {
        at_ms.is_finite()
            && self
                .bounds()
                .is_none_or(|(lo, hi)| at_ms >= lo && at_ms < hi)
    }
}

/// An [`EventSource`] the derivation can fill from: the window's cache facts streamed in bounded
/// batches, with the fill ledger's exclusions applied before any fetch.
pub trait FillSource: EventSource {
    /// The window's cache facts, streamed INTO a sink in bounded batches — the grouped
    /// keyed reader's fill path. The flat load above materializes the window,
    /// and a tick's input is the thing that must never be materialized whole; this
    /// hands the caller one delivery's events at a time, so peak is one segment,
    /// whatever the window.
    ///
    /// Sources decode one carrier at a time. Batches carry no ordering promise
    /// between carriers; the consumer orders the events it uses.
    ///
    /// # Errors
    /// Transport failures, undecodable bytes, or the sink's own error.
    /// Returns the `(batch_id, object_key)` pairs this call actually folded — the fill
    /// ledger's rows — so an incremental consumer can record them and exclude them next
    /// time. A source with no object keys reports the batch id in both positions.
    /// The sink's second argument is the segment's OBJECT KEY ('' where the source has
    /// none): per-segment consumers (the fill's ledger) need it the moment the
    /// segment's rows are down, not at stream end — a resumed fill skips a ledgered
    /// segment by KEY, before any fetch, and a key learned only at the end is a key
    /// every restart re-fetches the day at 48 MiB/s to relearn.
    /// The returned content identities attest completely decoded carriers, including
    /// carriers with no selected events. Events must carry their content identity in
    /// `delivery_id`. Recovery compares this evidence with committed input membership;
    /// an absent or unreadable carrier must never be reported as completed.
    fn stream_cache_events_into(
        &self,
        fleet_id: &str,
        selection: InputSelection,
        exclude: &FillExclusion,
        sink: &mut SegmentSink<'_>,
    ) -> Result<Vec<(String, String)>, analyzer_analysis::Error>;
}
