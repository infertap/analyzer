// SPDX-License-Identifier: AGPL-3.0-only

//! An operator's intake from a [`Source`]: where the source stands, and the batches delivered
//! with records the operator has not yet folded.
//!
//! **What waits is a reference.** A delivered batch whose records reach past the operator's
//! bound stays pending as its [`Delivery`], and its records are read again from the source on
//! the tick that folds them. The watermark trails the newest record by at least a segment's
//! seal, so holding the records themselves would hold minutes of the fleet's events in memory
//! all the time; the references are a few per producer run.

use super::source::{Delivery, Position, Source};
use crate::Error;
use analyzer_analysis::events::Event;

/// The records of one read, and those that arrived behind it.
#[derive(Debug, Default)]
pub struct Read {
    /// The records in the window read, each batch's in its own order.
    pub records: Vec<Event>,
    /// Records behind the window's start in batches read for the first time: records that
    /// arrived after the operator had folded past their instant, the correction path's.
    pub behind: Vec<Event>,
}

/// Where a source stands for one operator, and the batches it has yet to finish.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Intake {
    position: Position,
    /// Each pending batch, and whether a read has taken records from it before.
    pending: Vec<(Delivery, bool)>,
    /// Every record of the batches read for the first time since the last take: what the
    /// log partitioned by block keeps.
    #[serde(skip)]
    fresh: Vec<Event>,
}

impl Intake {
    /// Where the source stands.
    #[must_use]
    pub fn position(&self) -> &Position {
        &self.position
    }

    /// The records of every batch read for the first time since the last call.
    pub fn take_fresh(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.fresh)
    }

    /// The earliest record instant among the pending batches, if any.
    #[must_use]
    pub fn first_at_ms(&self) -> Option<f64> {
        self.pending
            .iter()
            .filter_map(|(d, _)| d.first_at_ms)
            .reduce(f64::min)
    }

    /// Delivered batches with records not yet read past.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Take what `source` delivers after the intake's position. A batch that carries no cache
    /// fact is passed over: it has no records to read.
    ///
    /// # Errors
    /// The source could not deliver.
    pub fn take(&mut self, source: &dyn Source) -> Result<(), Error> {
        let mut delivered = Vec::new();
        self.position = source.deliver(&self.position, &mut |delivery| {
            if delivery.first_at_ms.is_some() {
                delivered.push((delivery, false));
            }
            Ok(())
        })?;
        self.pending.extend(delivered);
        Ok(())
    }

    /// The instant every live run's records are complete through, at the intake's position.
    #[must_use]
    pub fn watermark(&self, source: &dyn Source) -> Option<f64> {
        source.watermark(&self.position)
    }

    /// Drop, unread, every pending batch whose records all lie before `from_ms`: an operator
    /// resumed from a checkpoint at `from_ms` has folded them, or leaves them to the
    /// correction path.
    pub fn forget_before(&mut self, from_ms: f64) {
        self.pending
            .retain(|(d, _)| d.last_at_ms.is_some_and(|last| last >= from_ms));
    }

    /// The pending records in `[from_ms, to_ms)`: [`Self::read_windows`] for one window.
    ///
    /// # Errors
    /// A batch could not be read.
    pub fn read(&mut self, source: &dyn Source, from_ms: f64, to_ms: f64) -> Result<Read, Error> {
        Ok(self
            .read_windows(source, &[(from_ms, to_ms)])?
            .pop()
            .unwrap_or_default())
    }

    /// The pending records of each window `[from, to)`, one read of each batch for all of them:
    /// a batch whose records reach below the latest window's end is read, and kept only while
    /// its records reach the earliest window's end or later. A batch read for the first time
    /// counts its records before a window's start as behind that window.
    ///
    /// Each consumer reads its next window from where its last ended, so a record a kept batch
    /// holds behind a window's start was taken by that window before.
    ///
    /// # Errors
    /// A batch could not be read.
    pub fn read_windows(
        &mut self,
        source: &dyn Source,
        windows: &[(f64, f64)],
    ) -> Result<Vec<Read>, Error> {
        let mut reads: Vec<Read> = windows.iter().map(|_| Read::default()).collect();
        let (Some(latest), Some(earliest)) = (
            windows.iter().map(|w| w.1).reduce(f64::max),
            windows.iter().map(|w| w.1).reduce(f64::min),
        ) else {
            return Ok(reads);
        };
        let (due, mut keep): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|(d, _)| d.first_at_ms.is_some_and(|first| first < latest));
        let deliveries: Vec<&Delivery> = due.iter().map(|(d, _)| d).collect();
        let batches = source.records_of(&deliveries)?;
        for ((delivery, before), records) in due.into_iter().zip(batches) {
            if !before {
                self.fresh.extend(records.iter().cloned());
            }
            for event in records {
                for (read, (from, to)) in reads.iter_mut().zip(windows) {
                    if event.at_ms < *from {
                        if !before {
                            read.behind.push(event.clone());
                        }
                    } else if event.at_ms < *to {
                        read.records.push(event.clone());
                    }
                }
            }
            if delivery.last_at_ms.is_some_and(|last| last >= earliest) {
                keep.push((delivery, true));
            }
        }
        self.pending = keep;
        Ok(reads)
    }
}

#[cfg(test)]
mod tests {
    use super::{Delivery, Intake, Position, Source};
    use crate::keyed::Order;
    use analyzer_analysis::events::{Event, Kind};

    /// One batch of stores at the given instants, delivered once.
    struct OneBatch(Vec<f64>, std::cell::Cell<bool>);

    impl OneBatch {
        fn delivery(&self) -> Delivery {
            Delivery {
                batch_id: "b".into(),
                object_key: String::new(),
                input_position: 1,
                order: Order::Unsequenced,
                first_at_ms: self.0.first().copied(),
                last_at_ms: self.0.last().copied(),
            }
        }
    }

    impl Source for OneBatch {
        fn deliver(
            &self,
            from: &Position,
            sink: &mut dyn FnMut(Delivery) -> Result<(), crate::Error>,
        ) -> Result<Position, crate::Error> {
            if !self.1.replace(true) {
                sink(self.delivery())?;
            }
            Ok(from.clone())
        }

        fn watermark(&self, _: &Position) -> Option<f64> {
            None
        }

        fn records(&self, _: &Delivery) -> Result<Vec<Event>, crate::Error> {
            Ok(self
                .0
                .iter()
                .map(|at| Event::new(Kind::Store, "i", *at))
                .collect())
        }
    }

    fn instants(read: &super::Read) -> Vec<f64> {
        read.records.iter().map(|e| e.at_ms).collect()
    }

    #[test]
    fn a_batch_stays_until_the_earliest_window_has_read_past_it() {
        let source = OneBatch(vec![10.0, 20.0, 30.0], std::cell::Cell::new(false));
        let mut intake = Intake::default();
        intake.take(&source).expect("take");
        // The earlier window ends before the batch's last record, the later one after it.
        let reads = intake
            .read_windows(&source, &[(0.0, 25.0), (0.0, 40.0)])
            .expect("read");
        assert_eq!(instants(&reads[0]), vec![10.0, 20.0]);
        assert_eq!(instants(&reads[1]), vec![10.0, 20.0, 30.0]);
        assert_eq!(intake.pending(), 1, "the earlier window still owes 30");
        let reads = intake
            .read_windows(&source, &[(25.0, 35.0), (40.0, 50.0)])
            .expect("read again");
        assert_eq!(instants(&reads[0]), vec![30.0]);
        assert!(
            reads[1].records.is_empty(),
            "the later window read it already"
        );
        assert!(
            reads[1].behind.is_empty(),
            "a record read before is not behind"
        );
        assert_eq!(intake.pending(), 0);
    }
}
