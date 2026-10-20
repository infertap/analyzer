// SPDX-License-Identifier: AGPL-3.0-only

//! The newest stores per holder (`docs/pipeline.md`): what the rank
//! timeline and the evidence by holder read, held beside the operators' state.
//!
//! Each holder keeps its newest `capacity` stores as repeat compute classified them, in
//! instant order. A correction that restates a block's stores restates them here too, so the
//! ring never shows a class the cells no longer hold. The log is partitioned by block, not by
//! holder, so a holder's stores older than its ring are not read back: the ring is the
//! evidence by holder, and it says how far back it reaches.

use crate::rowbinary::{Incomplete, Reader, Writer};
use crate::Error;
use analyzer_analysis::classify::Class;
use analyzer_analysis::emitted::{ClassifiedFact, Meaning};
use analyzer_analysis::intervals::Holder;
use analyzer_lake::keyed::Record;
use analyzer_lake::working::{Relation, Scope};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Each holder's ring.
pub(crate) const KEYED_RING: &str = "keyed_ring";

/// Stores a holder keeps, unless set: the rank timeline's newest five with room to page.
pub const RING_CAPACITY: usize = 100;

/// The newest stores of every holder.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HolderRing {
    capacity: usize,
    holders: BTreeMap<Holder, VecDeque<ClassifiedFact>>,
    changed: BTreeSet<Holder>,
}

impl HolderRing {
    /// Rings of `capacity` stores (at least one).
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            ..Self::default()
        }
    }

    /// Take classified stores into their holders' rings, each kept in instant order and
    /// trimmed to the newest `capacity`.
    pub fn add(&mut self, facts: &[ClassifiedFact]) {
        for fact in facts {
            let ring = self.holders.entry(fact.holder.clone()).or_default();
            let at = ring.partition_point(|f| order(f) <= order(fact));
            ring.insert(at, fact.clone());
            while ring.len() > self.capacity {
                ring.pop_front();
            }
            self.changed.insert(fact.holder.clone());
        }
    }

    /// Restate a correction: every store of `old` a ring holds leaves it, and `new` enters as
    /// [`Self::add`] takes stores.
    pub fn restate(&mut self, old: &[ClassifiedFact], new: &[ClassifiedFact]) {
        for fact in old {
            if let Some(ring) = self.holders.get_mut(&fact.holder) {
                let before = ring.len();
                ring.retain(|f| order(f) != order(fact) || f.block != fact.block);
                if ring.len() != before {
                    self.changed.insert(fact.holder.clone());
                }
            }
        }
        self.add(new);
    }

    /// Every holder's ring, oldest store first.
    pub fn holders(&self) -> impl Iterator<Item = (&Holder, &VecDeque<ClassifiedFact>)> {
        self.holders.iter()
    }

    /// A holder's ring, oldest first.
    #[must_use]
    pub fn of(&self, holder: &Holder) -> Option<&VecDeque<ClassifiedFact>> {
        self.holders.get(holder)
    }

    /// The rings that changed since the last call, as they stand.
    pub fn drain(&mut self) -> Vec<(Holder, Vec<ClassifiedFact>)> {
        std::mem::take(&mut self.changed)
            .into_iter()
            .map(|h| {
                let now = self
                    .holders
                    .get(&h)
                    .map(|r| r.iter().cloned().collect())
                    .unwrap_or_default();
                (h, now)
            })
            .collect()
    }

    /// Rings a store kept, with nothing changed.
    #[must_use]
    pub fn restored(
        capacity: usize,
        rings: impl IntoIterator<Item = (Holder, Vec<ClassifiedFact>)>,
    ) -> Self {
        Self {
            capacity: capacity.max(1),
            holders: rings.into_iter().map(|(h, r)| (h, r.into())).collect(),
            changed: BTreeSet::new(),
        }
    }
}

/// A store's place in its holder's ring: its instant, then its block.
fn order(f: &ClassifiedFact) -> (u64, &str) {
    (f.at_ms.to_bits(), f.block.as_str())
}

fn relation() -> Relation<'static> {
    Relation {
        name: KEYED_RING,
        scope: Scope::Lineage,
    }
}

fn holder_key(h: &Holder) -> Vec<u8> {
    let mut w = Writer::default();
    w.string(&h.instance);
    w.u32(h.dp_rank);
    w.u32(h.group_idx);
    w.bytes
}

/// A ring as bytes: the meaning its stores were classified under, then each store's own
/// fields; the holder is the record's key.
fn encode(ring: &[ClassifiedFact]) -> Vec<u8> {
    let mut w = Writer::default();
    let meaning = ring.first().map(|f| &f.meaning);
    w.string(meaning.map_or("", |m| m.semantics_version));
    w.string(meaning.map_or("", |m| m.discriminator.as_str()));
    w.varint(ring.len() as u64);
    for f in ring {
        w.string(&f.block);
        w.nullable(f.key_epoch, Writer::u32);
        w.f64(f.at_ms);
        w.i64(f.n_tokens);
        w.string(f.class.as_str());
        w.nullable(f.gap_ms, Writer::f64);
        w.f64(f.coverage);
        w.string(&f.workload_class);
        w.nullable(f.model.as_deref(), Writer::string);
    }
    w.bytes
}

/// A ring back from [`encode`], or nothing for a ring classified under a semantics version
/// this build does not compute: its classes are another generation's.
fn decode(holder: &Holder, bytes: &[u8]) -> Result<Vec<ClassifiedFact>, Error> {
    let mut r = Reader::new(bytes);
    let owned = |r: &mut Reader<'_>| r.string().map(str::to_string);
    let bad = || Error::Row("a keyed ring record does not decode".into());
    let decoded = (|| -> Result<Result<Vec<ClassifiedFact>, Error>, Incomplete> {
        let semantics = r.string()?;
        if semantics != analyzer_analysis::SEMANTICS_VERSION {
            return Ok(Ok(Vec::new()));
        }
        let meaning = Meaning {
            semantics_version: analyzer_analysis::SEMANTICS_VERSION,
            discriminator: owned(&mut r)?,
        };
        let mut out = Vec::new();
        for _ in 0..r.varint()? {
            let block = owned(&mut r)?;
            let key_epoch = r.nullable(Reader::u32)?;
            let at_ms = r.f64()?;
            let n_tokens = r.i64()?;
            let Some(class) = Class::parse(r.string()?) else {
                return Ok(Err(bad()));
            };
            out.push(ClassifiedFact {
                holder: holder.clone(),
                block,
                key_epoch,
                at_ms,
                n_tokens,
                class,
                gap_ms: r.nullable(Reader::f64)?,
                coverage: r.f64()?,
                workload_class: owned(&mut r)?,
                model: r.nullable(owned)?,
                meaning: meaning.clone(),
            });
        }
        Ok(Ok(out))
    })()
    .map_err(|_| bad())??;
    if !decoded.is_empty() && !r.at_end() {
        return Err(bad());
    }
    Ok(decoded)
}

/// Write the changed rings under `version`.
///
/// # Errors
/// The working state failed.
pub(crate) fn write(
    lake: &analyzer_lake::Lake,
    rings: &[(Holder, Vec<ClassifiedFact>)],
    version: f64,
) -> Result<(), Error> {
    if rings.is_empty() {
        return Ok(());
    }
    let records = rings
        .iter()
        .map(|(h, ring)| {
            let key = holder_key(h);
            let hash = holder_hash(h);
            (
                analyzer_lake::stripe::of(hash),
                Record {
                    hash,
                    key,
                    version,
                    gone: ring.is_empty(),
                    payload: encode(ring),
                },
            )
        })
        .collect();
    let writer = crate::fold_state::KeyedWriter::open(lake, relation(), false)?;
    writer.put(records)?;
    writer.commit()
}

/// The hash a holder's ring is keyed under.
fn holder_hash(h: &Holder) -> u64 {
    crate::spine::identity_hash(&h.instance) ^ (u64::from(h.dp_rank) << 32) ^ u64::from(h.group_idx)
}

/// One holder's ring as the store holds it: its newest stores, oldest first; empty for a holder
/// it holds none of, or one classified under another semantics version.
///
/// # Errors
/// The working state failed, or the record does not decode.
pub fn ring_of(lake: &analyzer_lake::Lake, holder: &Holder) -> Result<Vec<ClassifiedFact>, Error> {
    let hash = holder_hash(holder);
    let key = holder_key(holder);
    for record in crate::fold_state::read(
        lake,
        &relation(),
        analyzer_lake::stripe::of(hash),
        Some(&[hash]),
    )? {
        if record.key == key {
            return decode(holder, &record.payload);
        }
    }
    Ok(Vec::new())
}

/// Every ring the store holds.
///
/// # Errors
/// The working state failed, or a record does not decode.
pub(crate) fn read_all(
    lake: &analyzer_lake::Lake,
) -> Result<Vec<(Holder, Vec<ClassifiedFact>)>, Error> {
    let mut out = Vec::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(), stripe, None)? {
            let mut r = Reader::new(&record.key);
            let holder = (|| {
                Ok::<_, Incomplete>(Holder {
                    instance: r.string()?.to_string(),
                    dp_rank: r.u32()?,
                    group_idx: r.u32()?,
                })
            })()
            .map_err(|_| Error::Row("a keyed ring key does not decode".into()))?;
            let ring = decode(&holder, &record.payload)?;
            out.push((holder, ring));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{decode, encode, HolderRing};
    use analyzer_analysis::classify::Class;
    use analyzer_analysis::emitted::{ClassifiedFact, Meaning};
    use analyzer_analysis::intervals::Holder;
    use analyzer_analysis::{Discriminator, Params};

    fn holder() -> Holder {
        Holder {
            instance: "i1".into(),
            dp_rank: 0,
            group_idx: 1,
        }
    }

    fn fact(block: &str, at_ms: f64, class: Class) -> ClassifiedFact {
        ClassifiedFact {
            holder: holder(),
            block: block.into(),
            key_epoch: Some(2),
            at_ms,
            n_tokens: 16,
            class,
            gap_ms: Some(1500.0),
            coverage: 0.5,
            workload_class: "agentic".into(),
            model: Some("llama".into()),
            meaning: Meaning::current(&Discriminator::of(&Params::default())),
        }
    }

    #[test]
    fn a_ring_keeps_the_newest_and_a_restatement_replaces_what_it_names() {
        let mut ring = HolderRing::new(3);
        ring.add(&[
            fact("a", 4.0, Class::Cold),
            fact("b", 1.0, Class::Cold),
            fact("c", 3.0, Class::Etr),
            fact("d", 2.0, Class::Cold),
        ]);
        let kept: Vec<&str> = ring
            .of(&holder())
            .expect("ring")
            .iter()
            .map(|f| f.block.as_str())
            .collect();
        assert_eq!(kept, ["d", "c", "a"], "the newest three, oldest first");
        ring.restate(&[fact("c", 3.0, Class::Etr)], &[fact("c", 3.0, Class::Re)]);
        let c = ring
            .of(&holder())
            .expect("ring")
            .iter()
            .find(|f| f.block == "c")
            .cloned();
        assert_eq!(c.map(|f| f.class), Some(Class::Re));
        assert_eq!(ring.drain().len(), 1, "one holder changed");
        assert!(ring.drain().is_empty());
    }

    #[test]
    fn a_ring_reads_back_as_written() {
        let ring = vec![
            fact("a", 1.0, Class::Cold),
            fact("b", 2.5, Class::PostClear),
        ];
        assert_eq!(decode(&holder(), &encode(&ring)).expect("decodes"), ring);
    }
}
