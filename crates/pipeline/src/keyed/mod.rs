// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed streaming pipeline (`docs/pipeline.md`): a durable log delivered in
//! order per producer run, lanes keyed by block over their own state, and summed cells as
//! output.
//!
//! - `source` and `intake`: the input log delivered in each run's order, and where the driver
//!   stands in it.
//! - `repeat`, `placement`, `coverage`, `roster`, `tags`, `blocks`: the operators, each over its
//!   lanes or its feed.
//! - `driver`: one tick over them all, the correction path for late records, and settling.
//! - `held`, `store`, `state`: the held cells and the emission batch, and the working state's
//!   relations.
//! - `spine`, `refold`, `ring`: the log partitioned by block, the re-derivation of one block or
//!   one identity from it, and each holder's ring of recent stores.
//! - `settled`: the hook that states a settled day's day-grain outputs.

mod audit;
mod blocks;
mod coverage;
mod day_records;
mod driver;
mod frontiers;
mod held;
mod intake;
mod placement;
mod refold;
mod repeat;
mod ring;
mod roster;
mod settled;
mod source;
mod spine;
mod state;
mod store;
mod tags;
mod whales;

pub use audit::{
    audit, sample, Audit, AuditEdge, Disagreement, Statement, EVICT_SAMPLE_MODULUS,
    SAMPLES_PER_TICK,
};
pub(crate) use blocks::KEYED_BLOCKS;
pub use coverage::{CoverageFeed, StatedSource};
pub use driver::{Driver, Late, TickRun, Watermark, BLOCK_CAPACITY, BLOCK_CUT};
pub use held::{Batch, Held};
pub(crate) use held::{KEYED_CELLS, KEYED_OUTBOX};
pub use intake::{Intake, Read};
pub use placement::{PlacementCompute, PlacementTick};
pub use refold::{placement_from, refold_block, refold_placement, Models, PlacementRefold, Refold};
pub use repeat::{
    Changes, Emission, EmissionPolicy, IdentityKey, RepeatCompute, RepeatContext, Tick,
};
pub(crate) use ring::KEYED_RING;
pub use ring::{ring_of, HolderRing, RING_CAPACITY};
pub use roster::{RosterChanges, RosterFeed};
pub use settled::{Hook, SettledDay, SourceDay};
pub use source::{Delivery, MissingSegment, Order, Position, SegmentReader, SegmentSource, Source};
pub(crate) use spine::KEYED_SPINE;
pub use spine::{
    block_history, collect_held_garbage, compact, held_from_hour, hour_of, hours_with_ticks,
    retire_before, SpineLayout, Written,
};
pub use state::{KeyedState, RamState};
pub use store::{Generation, Restored, Seeds, Store};
pub(crate) use store::{KEYED_FRONTIER, KEYED_HEAD, KEYED_ROSTER, KEYED_SEEDS};
pub(crate) use tags::KEYED_TAGS;
pub use tags::{Restated, TagFeed};
pub(crate) use whales::KEYED_WHALES;

use crate::Error;
use analyzer_analysis::events::Event;

/// A [`SegmentReader`] over a [`crate::source::FillSource`]: each batch's records decoded from
/// the object it was committed from, as the source's fill reads them.
pub struct FillReader<'a> {
    source: &'a dyn crate::source::FillSource,
    fleet: &'a str,
}

impl<'a> FillReader<'a> {
    /// A reader of `fleet`'s batches through `source`.
    #[must_use]
    pub fn new(source: &'a dyn crate::source::FillSource, fleet: &'a str) -> Self {
        Self { source, fleet }
    }
}

impl SegmentReader for FillReader<'_> {
    fn read(&self, delivery: &Delivery) -> Result<Vec<Event>, Error> {
        Ok(self.read_all(&[delivery])?.pop().unwrap_or_default())
    }

    /// One pass over the source for every batch asked: scoped to their objects when each has
    /// one, the source's whole listing otherwise. A batch the source does not report as
    /// decoded whole is missing history, never an empty batch.
    fn read_all(&self, deliveries: &[&Delivery]) -> Result<Vec<Vec<Event>>, Error> {
        use crate::source::{FillExclusion, InputSelection};
        use std::collections::{HashMap, HashSet};
        let carrying: Vec<&Delivery> = deliveries
            .iter()
            .copied()
            .filter(|d| d.first_at_ms.is_some())
            .collect();
        let mut by_batch: HashMap<&str, Vec<Event>> = carrying
            .iter()
            .map(|d| (d.batch_id.as_str(), Vec::new()))
            .collect();
        if !carrying.is_empty() {
            let keyed = carrying.iter().all(|d| !d.object_key.is_empty());
            let exclude = FillExclusion {
                only_keys: keyed.then(|| carrying.iter().map(|d| d.object_key.clone()).collect()),
                ..FillExclusion::default()
            };
            let completed = self.source.stream_cache_events_into(
                self.fleet,
                InputSelection::WholeBatches,
                &exclude,
                &mut |rows, _key| {
                    for event in rows {
                        let batch = event.delivery_id.as_deref().ok_or_else(|| {
                            analyzer_analysis::Error::Source(
                                "a cache fact has no content identity".into(),
                            )
                        })?;
                        if let Some(records) = by_batch.get_mut(batch) {
                            records.push(event);
                        }
                    }
                    Ok(())
                },
            )?;
            let completed: HashSet<String> = completed.into_iter().map(|(b, _)| b).collect();
            if let Some(missing) = carrying.iter().find(|d| !completed.contains(&d.batch_id)) {
                return Err(Error::UnavailableHistory {
                    day: analyzer_lake::ledger::day_of(missing.first_at_ms.unwrap_or_default()),
                });
            }
        }
        Ok(deliveries
            .iter()
            .map(|d| by_batch.remove(d.batch_id.as_str()).unwrap_or_default())
            .collect())
    }
}
