// SPDX-License-Identifier: AGPL-3.0-only

//! The second source: the Ordered Event IR built from sealed segments, with no store in the path.
//!
//! **This is what converts "store-agnostic" from an assertion into a fact.** The bright line —
//! `analyzer-analysis` depending on nothing — is a property of a manifest, and one implementation
//! makes any interface look sufficient because it cannot disagree with anything. Two sources over
//! one corpus is the proof, and `tests/two_source_differential.rs` is where it is taken.
//!
//! **LIST, not a manifest.** Over a corpus, "everything retained" is a listing of the segments
//! under a prefix: cheap, exact, and needing nothing that does not exist. This LIST does not
//! scale — no time predicate, three sweeps per pass, everything materialised, a flat prefix —
//! and it does not have to: nothing in production reads it. It exists to be the naive second
//! implementation the production source (`pipeline::parquet_source` — dated discovery, the
//! content cache, the pruned columnar decode) can disagree with.
//!
//! **It decodes with `analyzer-segments` and reads rows with the PRODUCTION row suite**
//! (`parquet_source::cache_event_of` / `lifecycle_event_of`). The row readers are deliberately
//! shared rather than twinned: a reading defect both copies carried — `dp_rank: -1` silently
//! reading as absent — was invisible to the differential precisely because the twin restated
//! the production reading verbatim. What this twin still holds independently, and what the
//! differential therefore still exercises, is everything operational: discovery, the cache,
//! pruning, projection, and the per-load scoping. A conversion error here fails the LOAD —
//! the naive reader has no quarantine, which is itself a difference the corrupt-corpus tests
//! lean on.
//!
//! **What this does and does not exercise.** It reads a prefix on the filesystem, which is the
//! shape the conformance corpora already take, and it decodes with the shipped decoder. It does
//! not exercise object-store transport — listing, pagination, retries, etag identity — which
//! `ingest::bucket` covers on its own path. The claim taken here is about the DERIVATION running
//! off segments, not about S3.

use analyzer_analysis::events::{Event, EventSource, LifecycleEvent};
use analyzer_analysis::Error;
use analyzer_analysis::Window;
use analyzer_pipeline::parquet_source::{cache_event_of, lifecycle_event_of, sort_lifecycle};
use serde_json::Value;
use std::path::PathBuf;

/// Sealed segments under a prefix, read directly.
pub struct BucketSource {
    prefix: PathBuf,
}

/// One decoded row, carrying the fact that is a property of its SEGMENT rather than of it.
struct Shaped {
    row: Value,
    /// The content hash of the segment — the same delivery identity the records path links on,
    /// because it is a function of the bytes alone.
    delivery: String,
}

impl BucketSource {
    /// A source over the dated tree under `prefix`.
    #[must_use]
    pub fn new(prefix: impl Into<PathBuf>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    fn keys(&self) -> Result<Vec<PathBuf>, Error> {
        let mut keys: Vec<PathBuf> = std::fs::read_dir(&self.prefix)
            .map_err(|e| Error::Source(format!("list {}: {e}", self.prefix.display())))?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().is_some_and(|x| x == "jsonl"))
            .collect();
        keys.sort();
        Ok(keys)
    }

    /// LIST the prefix, GET each object, decode it.
    ///
    /// **Sorted by key**, so the set is deterministic before [`analyzer_analysis::events::sort`]
    /// runs. Ordering is this crate's property rather than a store's, which is what lets a source
    /// reading segments skip reproducing an `ORDER BY`.
    fn decoded(&self, fleet_id: &str) -> Result<Vec<Shaped>, analyzer_analysis::Error> {
        let keys = self.keys()?;

        let mut out = Vec::new();
        for key in keys {
            let bytes = std::fs::read(&key)
                .map_err(|e| Error::Source(format!("get {}: {e}", key.display())))?;
            let name = key.to_string_lossy();
            let seg = analyzer_segments::decode_segment(&name, fleet_id, &bytes)
                .map_err(|e| analyzer_analysis::Error::Source(e.to_string()))?;
            for line in seg.shaped.lines().filter(|l| !l.trim().is_empty()) {
                let row: Value = serde_json::from_str(line)
                    .map_err(|e| Error::Source(format!("{name}: shaped row: {e}")))?;
                out.push(Shaped {
                    row,
                    delivery: seg.batch_id.clone(),
                });
            }
        }
        Ok(out)
    }
}

impl EventSource for BucketSource {
    /// The window's cache facts.
    ///
    /// **The predicate is kind and a HALF-OPEN window and nothing else.** No semantic rule is
    /// applied here — not the own-copy rule, not the `block_id` condition — because those
    /// belong to the derivations that disagree about them.
    fn cache_events(
        &self,
        fleet_id: &str,
        w: &Window,
    ) -> Result<std::sync::Arc<Vec<Event>>, analyzer_analysis::Error> {
        let mut events = Vec::new();
        for r in self.decoded(fleet_id)? {
            let delivery = Some(r.delivery.clone()).filter(|d| !d.is_empty());
            let Some(ev) = cache_event_of(&r.row, delivery)? else {
                continue;
            };
            if ev.at_ms < w.start_ms || ev.at_ms >= w.end_ms {
                continue;
            }
            events.push(ev);
        }
        analyzer_analysis::events::sort(&mut events);
        Ok(std::sync::Arc::new(events))
    }

    fn lifecycle_events(
        &self,
        fleet_id: &str,
        lo_ms: f64,
        hi_ms: f64,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        let mut out = Vec::new();
        for r in self.decoded(fleet_id)? {
            let Some(ev) = lifecycle_event_of(&r.row)? else {
                continue;
            };
            if ev.at_ms < lo_ms || ev.at_ms >= hi_ms {
                continue;
            }
            out.push(ev);
        }
        sort_lifecycle(&mut out);
        Ok(out)
    }

    /// Every `segment_open` under the prefix — the corpus's whole retention horizon.
    ///
    /// **No time predicate, and that is the correct scope rather than a shortcut.** A declared
    /// drop's producer-clock bracket comes from the nearest observed opens on either side of the
    /// reclaimed range, which may sit arbitrarily far from any analysis window.
    fn segment_opens_at_retention_horizon(
        &self,
        fleet_id: &str,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        let mut out = Vec::new();
        for r in self.decoded(fleet_id)? {
            let Some(ev) = lifecycle_event_of(&r.row)? else {
                continue;
            };
            if ev.kind == "segment_open" && ev.segment_seq.is_some() {
                out.push(ev);
            }
        }
        // (incarnation, segment_seq) — the sequence the continuity fold reads.
        out.sort_by(|a, b| {
            a.incarnation
                .cmp(&b.incarnation)
                .then(a.segment_seq.cmp(&b.segment_seq))
        });
        Ok(out)
    }
}

impl analyzer_pipeline::source::FillSource for BucketSource {
    fn stream_cache_events_into(
        &self,
        fleet_id: &str,
        selection: analyzer_pipeline::source::InputSelection,
        exclude: &analyzer_pipeline::source::FillExclusion,
        sink: &mut analyzer_pipeline::source::SegmentSink<'_>,
    ) -> Result<Vec<(String, String)>, analyzer_analysis::Error> {
        selection.validate()?;
        let mut completed = Vec::new();
        for path in self.keys()? {
            let key = path.to_string_lossy().into_owned();
            if exclude.object_keys.contains(&key)
                || exclude
                    .only_keys
                    .as_ref()
                    .is_some_and(|only| !only.contains(&key))
            {
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|e| Error::Source(e.to_string()))?;
            let segment = analyzer_segments::decode_segment(&key, fleet_id, &bytes)
                .map_err(|e| Error::Source(e.to_string()))?;
            if exclude.batch_ids.contains(&segment.batch_id) {
                continue;
            }
            let mut events = Vec::new();
            for line in segment
                .shaped
                .lines()
                .filter(|line| !line.trim().is_empty())
            {
                let row: Value =
                    serde_json::from_str(line).map_err(|e| Error::Source(e.to_string()))?;
                if let Some(event) = cache_event_of(&row, Some(segment.batch_id.clone()))? {
                    if selection.contains(event.at_ms) {
                        events.push(event);
                    }
                }
            }
            analyzer_analysis::events::sort(&mut events);
            sink(events, &key)?;
            completed.push((segment.batch_id, key));
        }
        Ok(completed)
    }
}
