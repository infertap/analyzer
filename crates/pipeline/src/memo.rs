// SPDX-License-Identifier: AGPL-3.0-only

//! The per-derivation load memo: one `derive()`/pass runs its reads over ONE window through
//! ~23 adapter calls, and each call re-ran discovery and decode for a load shape an earlier
//! call had already paid for. This wrapper remembers each distinct load — per
//! `(fleet, bounds)` per seam method, which IS the projection class, since every method binds
//! its projection — for exactly the life of one wrapper.
//!
//! **A cache of RESULTS, not of bytes** — the byte cache is `parquet_source`'s and holds
//! across processes; this holds only within one derivation, where re-asking an immutable
//! question is pure waste. It is a plain `HashMap` behind the `Mutex` the `&self` seam forces
//! (the same shape the API's engine lock uses), never a global: the caller constructs it,
//! threads it, and drops it, so nothing outlives the derivation that asked.
//!
//! Errors are never memoized: a failed load reports and the next call retries, because a
//! remembered failure would turn one transient fault into a whole derivation's answer.

use analyzer_analysis::events::{Event, EventSource, LifecycleEvent};
use analyzer_analysis::Window;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The seam's shared cache stream: one allocation, reference-counted.
type SharedEvents = Arc<Vec<Event>>;

/// One derivation's memoizing view of an [`EventSource`]: the flat loads remembered per
/// `(fleet, bounds)` per seam method, streams forwarded, errors never kept. Construct at
/// the top of a derive or a pass, hand it everywhere the inner source went, drop it with
/// the derivation.
///
/// The cache stream is held behind the seam's own `Arc`: the map and
/// every caller share ONE copy of the retained span, where storing it owned meant `insert` and
/// the returned value were two complete copies of the largest load in the process.
pub struct MemoSource<'a, S: ?Sized + EventSource = dyn crate::source::FillSource> {
    inner: &'a S,
    cache: Mutex<HashMap<(String, u64, u64), SharedEvents>>,
    lifecycle: Mutex<HashMap<(String, u64, u64), Vec<LifecycleEvent>>>,
    opens: Mutex<HashMap<String, Vec<LifecycleEvent>>>,
    declarations: Mutex<HashMap<(String, u64, u64), Vec<LifecycleEvent>>>,
}

impl<'a, S: ?Sized + EventSource> MemoSource<'a, S> {
    /// An empty memo over `inner`: nothing remembered until the first load.
    #[must_use]
    pub fn new(inner: &'a S) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
            lifecycle: Mutex::new(HashMap::new()),
            opens: Mutex::new(HashMap::new()),
            declarations: Mutex::new(HashMap::new()),
        }
    }
}

/// Look up or load one shape. A poisoned lock falls through to the inner source: the memo is
/// a cost lever, and a lever that could fail a load would be a second failure mode for free.
fn memoized<K: std::hash::Hash + Eq + Clone, V: Clone, E>(
    map: &Mutex<HashMap<K, V>>,
    key: &K,
    load: impl FnOnce() -> Result<V, E>,
) -> Result<V, E> {
    if let Ok(held) = map.lock() {
        if let Some(hit) = held.get(key) {
            return Ok(hit.clone());
        }
    }
    let loaded = load()?;
    if let Ok(mut held) = map.lock() {
        held.insert(key.clone(), loaded.clone());
    }
    Ok(loaded)
}

impl<S: ?Sized + EventSource> EventSource for MemoSource<'_, S> {
    fn cache_events(
        &self,
        fleet_id: &str,
        w: &Window,
    ) -> Result<Arc<Vec<Event>>, analyzer_analysis::Error> {
        let key = (
            fleet_id.to_string(),
            w.start_ms.to_bits(),
            w.end_ms.to_bits(),
        );
        memoized(&self.cache, &key, || self.inner.cache_events(fleet_id, w))
    }

    fn lifecycle_events(
        &self,
        fleet_id: &str,
        lo_ms: f64,
        hi_ms: f64,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        let key = (fleet_id.to_string(), lo_ms.to_bits(), hi_ms.to_bits());
        memoized(&self.lifecycle, &key, || {
            self.inner.lifecycle_events(fleet_id, lo_ms, hi_ms)
        })
    }

    fn segment_opens_at_retention_horizon(
        &self,
        fleet_id: &str,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        memoized(&self.opens, &fleet_id.to_string(), || {
            self.inner.segment_opens_at_retention_horizon(fleet_id)
        })
    }

    fn lifecycle_declarations(
        &self,
        fleet_id: &str,
        w: &Window,
    ) -> Result<Vec<LifecycleEvent>, analyzer_analysis::Error> {
        let key = (
            fleet_id.to_string(),
            w.start_ms.to_bits(),
            w.end_ms.to_bits(),
        );
        memoized(&self.declarations, &key, || {
            self.inner.lifecycle_declarations(fleet_id, w)
        })
    }
}

impl<S: ?Sized + crate::source::FillSource> crate::source::FillSource for MemoSource<'_, S> {
    /// Streams are FORWARDED, never memoized: a memo of a stream is the materialization
    /// the stream exists to avoid, and the grouped pass drives one stream per window by
    /// construction. The flat loads below stay memoized for the derivations that re-ask
    /// them.
    fn stream_cache_events_into(
        &self,
        fleet_id: &str,
        selection: crate::source::InputSelection,
        exclude: &crate::source::FillExclusion,
        sink: &mut crate::source::SegmentSink<'_>,
    ) -> Result<Vec<(String, String)>, analyzer_analysis::Error> {
        self.inner
            .stream_cache_events_into(fleet_id, selection, exclude, sink)
    }
}
