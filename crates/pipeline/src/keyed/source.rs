// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed pipeline's input (`docs/pipeline.md`).
//!
//! A [`Source`] delivers records in the order an operator may fold them: each producer run's
//! segments in its own sequence, so each holder's records in the engine's order. It keeps a
//! [`Position`] a checkpoint can record and a restart resume from, and it owns the policy for a
//! segment that has not arrived ([`MissingSegment`]); no operator decides what a gap means.
//!
//! [`SegmentSource`] reads the input log. A run's first segment the source sees anchors the
//! run; a segment behind that anchor arrived after its successors were folded and is delivered
//! as [`Order::Late`], for the correction path. A batch that carries no sequence claim (an
//! annotation, a declared drop, a segment from a producer that numbers nothing) is delivered as
//! [`Order::Unsequenced`], the moment it is committed.

use crate::Error;
use analyzer_analysis::events::Event;
use analyzer_lake::ledger::BatchRecord;
use std::collections::{BTreeMap, BTreeSet};

/// Where a source stands: what it has delivered, and what it holds back.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Position {
    /// Every batch committed at or below this input position has been delivered or is held.
    committed: u64,
    /// Each run's delivery state, by the run's incarnation.
    runs: BTreeMap<String, Run>,
    /// Batches held behind a gap, by input position and batch id: a commit cycle gives every
    /// batch it commits one position.
    held: BTreeSet<(u64, String)>,
}

/// One run's delivery state.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Run {
    /// Whether the run's first seen segment has set `next`; a run known only by a declared
    /// loss is not.
    anchored: bool,
    /// The next sequence number to deliver.
    next: i64,
    /// The newest liveness instant its delivered, contiguous segments state.
    live_ms: f64,
    /// Whether a delivered segment observed the run's stop: a drained run proves no instant.
    stopped: bool,
    /// Sequence ranges the run declared lost, not yet passed.
    dropped: Vec<(i64, i64)>,
}

impl Position {
    /// The input position everything at or below has been considered.
    #[must_use]
    pub fn committed(&self) -> u64 {
        self.committed
    }

    /// How many batches wait behind a gap.
    #[must_use]
    pub fn held(&self) -> usize {
        self.held.len()
    }
}

/// How a delivery stands against its run's order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Order {
    /// The run's next segment: everything before it in the run has been delivered.
    InOrder,
    /// A segment behind its run's anchor, delivered after its successors.
    Late,
    /// A batch with no sequence claim.
    Unsequenced,
}

/// One committed batch, delivered once: what it is and the span of its cache facts, not its
/// records. An operator reads the records through [`Source::records`] when it folds them, so
/// what waits past the watermark is a reference, never the records themselves.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Delivery {
    /// The batch.
    pub batch_id: String,
    /// The object its records were committed from, empty for a carrier with no object.
    pub object_key: String,
    /// Its input position.
    pub input_position: u64,
    /// How it stands against its run's order.
    pub order: Order,
    /// Its earliest cache fact's instant, or `None` when it carries none.
    pub first_at_ms: Option<f64>,
    /// Its latest cache fact's instant.
    pub last_at_ms: Option<f64>,
}

/// What a source does with a run whose next segment has not been committed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MissingSegment {
    /// Hold every later segment of the run until the missing one arrives or is declared lost.
    Wait,
    /// As [`Self::Wait`], until the earliest held segment has waited `grace_ms` since it was
    /// ingested; then the run moves past the gap, and the missing segment, if it arrives,
    /// is delivered as [`Order::Late`].
    ReleaseAfter {
        /// How long a gap may hold a run, in milliseconds.
        grace_ms: f64,
    },
}

/// The pipeline's input: ordered per-run delivery with sequence numbers.
pub trait Source {
    /// Deliver what follows `from`, each run's segments in sequence order, and return the
    /// position after them. Order is promised within a run; across runs, deliveries follow
    /// commit order. Delivering from the returned position again delivers nothing until more
    /// is committed or a held gap is released.
    ///
    /// # Errors
    /// The input could not be read, or the sink failed.
    fn deliver(
        &self,
        from: &Position,
        sink: &mut dyn FnMut(Delivery) -> Result<(), Error>,
    ) -> Result<Position, Error>;

    /// The instant every live run's records are complete through at `at`: the minimum over
    /// runs not yet stopped, the maximum over stopped ones when none remain, `None` when no
    /// run has delivered a sequenced segment.
    fn watermark(&self, at: &Position) -> Option<f64>;

    /// A delivered batch's cache facts, in the order the segment carries them.
    ///
    /// # Errors
    /// The batch could not be fetched or decoded.
    fn records(&self, delivery: &Delivery) -> Result<Vec<Event>, Error>;

    /// Several delivered batches' cache facts, one list per delivery in the order given: for a
    /// source that reads a set of batches more cheaply than each alone.
    ///
    /// # Errors
    /// A batch could not be fetched or decoded.
    fn records_of(&self, deliveries: &[&Delivery]) -> Result<Vec<Vec<Event>>, Error> {
        deliveries.iter().map(|d| self.records(d)).collect()
    }
}

/// Reads one delivered batch's records.
pub trait SegmentReader {
    /// The cache facts `delivery` carries.
    ///
    /// # Errors
    /// The batch could not be fetched or decoded.
    fn read(&self, delivery: &Delivery) -> Result<Vec<Event>, Error>;

    /// Several batches' cache facts, one list per delivery in the order given.
    ///
    /// # Errors
    /// A batch could not be fetched or decoded.
    fn read_all(&self, deliveries: &[&Delivery]) -> Result<Vec<Vec<Event>>, Error> {
        deliveries.iter().map(|d| self.read(d)).collect()
    }
}

/// A [`Source`] over the input log.
pub struct SegmentSource<'a> {
    lake: &'a analyzer_lake::Lake,
    reader: &'a dyn SegmentReader,
    missing: MissingSegment,
    now_ms: f64,
}

impl<'a> SegmentSource<'a> {
    /// A source over `lake`'s input log, reading records through `reader`, waiting on a
    /// missing segment until it arrives or is declared lost.
    #[must_use]
    pub fn new(lake: &'a analyzer_lake::Lake, reader: &'a dyn SegmentReader) -> Self {
        Self {
            lake,
            reader,
            missing: MissingSegment::Wait,
            now_ms: 0.0,
        }
    }

    /// The policy for a missing segment, judged at `now_ms` (epoch milliseconds).
    #[must_use]
    pub fn with_missing(mut self, missing: MissingSegment, now_ms: f64) -> Self {
        self.missing = missing;
        self.now_ms = now_ms;
        self
    }
}

/// A batch's sequence claims: the (run, sequence) of each segment it carries.
fn claims(batch: &BatchRecord) -> Vec<(&str, i64)> {
    batch
        .segments
        .iter()
        .filter(|a| a.segment_seq >= 0 && !a.incarnation.is_empty())
        .map(|a| (a.incarnation.as_str(), a.segment_seq))
        .collect()
}

impl Run {
    /// Move `next` past every declared-lost range it has reached.
    fn skip_dropped(&mut self) {
        loop {
            let next = self.next;
            let Some(&(_, last)) = self
                .dropped
                .iter()
                .find(|(first, last)| *first <= next && next <= *last)
            else {
                break;
            };
            self.next = last + 1;
        }
        let next = self.next;
        self.dropped.retain(|(_, last)| *last >= next);
    }
}

impl Position {
    /// Take in `pending`, the batches read from the log after `self`: the committed mark
    /// moves past them, their declared losses are noted (a drop committed after the segment
    /// it excuses still excuses it), and a run first seen among them anchors at its lowest
    /// sequence there.
    fn admit(&mut self, pending: &[BatchRecord]) {
        let before = self.committed;
        self.committed = pending
            .iter()
            .map(|b| b.input_position)
            .fold(before, u64::max);
        self.held.clear();
        for batch in pending.iter().filter(|b| b.input_position > before) {
            for d in &batch.drops {
                let run = self.runs.entry(d.incarnation.clone()).or_default();
                run.dropped.push((d.first_seq, d.last_seq));
            }
        }
        let mut anchors: BTreeMap<String, i64> = BTreeMap::new();
        for (run, seq) in pending.iter().flat_map(claims) {
            if self.runs.get(run).is_none_or(|r| !r.anchored) {
                anchors
                    .entry(run.to_string())
                    .and_modify(|lowest| *lowest = (*lowest).min(seq))
                    .or_insert(seq);
            }
        }
        for (run, seq) in anchors {
            let state = self.runs.entry(run).or_default();
            state.anchored = true;
            state.next = seq;
        }
        for run in self.runs.values_mut() {
            run.skip_dropped();
        }
    }

    /// How `batch` stands against its runs, or `None` while a gap holds it.
    fn order_of(&self, batch: &BatchRecord) -> Option<Order> {
        let claims = claims(batch);
        let before = |pass: fn(i64, i64) -> bool| {
            claims
                .iter()
                .all(|(run, seq)| self.runs.get(*run).is_some_and(|r| pass(*seq, r.next)))
        };
        if claims.is_empty() {
            Some(Order::Unsequenced)
        } else if before(|seq, next| seq < next) {
            Some(Order::Late)
        } else if before(|seq, next| seq <= next) {
            Some(Order::InOrder)
        } else {
            None
        }
    }

    /// Move each run `batch` continues past it, taking its liveness and its stop.
    fn advance(&mut self, batch: &BatchRecord) {
        for a in &batch.segments {
            if a.segment_seq < 0 || a.incarnation.is_empty() {
                continue;
            }
            let run = self.runs.entry(a.incarnation.clone()).or_default();
            if a.segment_seq == run.next {
                run.next += 1;
                run.skip_dropped();
                let live = a
                    .hb_last_at_ms
                    .unwrap_or(0.0)
                    .max(a.last_record_at_ms)
                    .max(a.first_record_at_ms);
                run.live_ms = run.live_ms.max(live);
                run.stopped |= a.stop_at_ms.is_some_and(|stop| stop > 0.0);
            }
        }
    }

    /// Move each run whose held segments have waited `grace_ms` since ingest at `now_ms` to
    /// the lowest of them, so none is skipped. Returns whether any run moved.
    fn release(&mut self, pending: &[BatchRecord], now_ms: f64, grace_ms: f64) -> bool {
        let mut released: BTreeMap<&str, i64> = BTreeMap::new();
        for batch in pending {
            #[allow(clippy::cast_precision_loss)] // epoch milliseconds are exact in f64
            if now_ms - (batch.ingested_at_ms as f64) < grace_ms {
                continue;
            }
            for (run, seq) in claims(batch) {
                if self.runs.get(run).is_some_and(|state| seq > state.next) {
                    released
                        .entry(run)
                        .and_modify(|lowest| *lowest = (*lowest).min(seq))
                        .or_insert(seq);
                }
            }
        }
        for (run, seq) in &released {
            if let Some(state) = self.runs.get_mut(*run) {
                state.next = *seq;
                state.skip_dropped();
            }
        }
        !released.is_empty()
    }
}

impl Source for SegmentSource<'_> {
    fn deliver(
        &self,
        from: &Position,
        sink: &mut dyn FnMut(Delivery) -> Result<(), Error>,
    ) -> Result<Position, Error> {
        // Re-read from the earliest held batch, so what a gap held is considered again.
        let since = from.held.first().map_or(from.committed, |(first, _)| {
            first.saturating_sub(1).min(from.committed)
        });
        let mut pending: Vec<BatchRecord> = crate::inputs::log(self.lake)?
            .committed_since(since)?
            .into_iter()
            .filter(|b| {
                b.input_position > from.committed
                    || from.held.contains(&(b.input_position, b.batch_id.clone()))
            })
            .collect();
        let mut at = from.clone();
        at.admit(&pending);
        loop {
            let mut progressed = false;
            let mut waiting = Vec::new();
            for batch in pending.drain(..) {
                let Some(order) = at.order_of(&batch) else {
                    waiting.push(batch);
                    continue;
                };
                at.advance(&batch);
                sink(Delivery {
                    batch_id: batch.batch_id,
                    object_key: batch.object_key,
                    input_position: batch.input_position,
                    order,
                    first_at_ms: batch.first_at_ms,
                    last_at_ms: batch.last_at_ms,
                })?;
                progressed = true;
            }
            pending = waiting;
            let released = match self.missing {
                MissingSegment::Wait => false,
                MissingSegment::ReleaseAfter { grace_ms } => {
                    !progressed && at.release(&pending, self.now_ms, grace_ms)
                }
            };
            if !progressed && !released {
                break;
            }
        }
        for batch in pending {
            at.held.insert((batch.input_position, batch.batch_id));
        }
        Ok(at)
    }

    fn records(&self, delivery: &Delivery) -> Result<Vec<Event>, Error> {
        self.reader.read(delivery)
    }

    fn records_of(&self, deliveries: &[&Delivery]) -> Result<Vec<Vec<Event>>, Error> {
        self.reader.read_all(deliveries)
    }

    fn watermark(&self, at: &Position) -> Option<f64> {
        let proven = at.runs.values().filter(|r| r.live_ms > 0.0);
        let live = proven
            .clone()
            .filter(|r| !r.stopped)
            .map(|r| r.live_ms)
            .reduce(f64::min);
        live.or_else(|| proven.map(|r| r.live_ms).reduce(f64::max))
    }
}
