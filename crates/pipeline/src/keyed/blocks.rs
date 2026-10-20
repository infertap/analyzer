// SPDX-License-Identifier: AGPL-3.0-only

//! The blocks by stored mass in the store (`analyzer_analysis::block_mass`): each day's
//! summary and the blocks it last published, a record per day ([`super::day_records`]).

use crate::rowbinary::{Incomplete, Reader, Writer};
use crate::Error;
use analyzer_analysis::block_mass::{Monitored, SpaceSaving};
use analyzer_analysis::intervals::Holder;
use std::collections::BTreeSet;

/// Each day's summary.
pub(crate) const KEYED_BLOCKS: &str = "keyed_blocks";

/// One day's summary and the blocks it last published, as bytes.
pub(crate) fn encode(summary: &SpaceSaving, published: &BTreeSet<String>) -> Vec<u8> {
    let mut w = Writer::default();
    w.u64(summary.capacity() as u64);
    w.i64(summary.total());
    let mut items: Vec<(&String, &Monitored)> = summary.monitored().collect();
    items.sort_by(|a, b| a.0.cmp(b.0));
    w.varint(items.len() as u64);
    for (block, m) in items {
        w.string(block);
        w.i64(m.count);
        w.i64(m.error);
        w.varint(m.holders.len() as u64);
        for h in &m.holders {
            w.string(&h.instance);
            w.u32(h.dp_rank);
            w.u32(h.group_idx);
        }
    }
    w.varint(published.len() as u64);
    for block in published {
        w.string(block);
    }
    w.bytes
}

/// A day back from [`encode`].
pub(crate) fn decode(bytes: &[u8]) -> Result<(SpaceSaving, BTreeSet<String>), Error> {
    let mut r = Reader::new(bytes);
    let owned = |r: &mut Reader<'_>| r.string().map(str::to_string);
    let decoded = (|| -> Result<(SpaceSaving, BTreeSet<String>), Incomplete> {
        let capacity = usize::try_from(r.u64()?).unwrap_or(usize::MAX);
        let total = r.i64()?;
        let mut items = Vec::new();
        for _ in 0..r.varint()? {
            let block = owned(&mut r)?;
            let count = r.i64()?;
            let error = r.i64()?;
            let mut holders = BTreeSet::new();
            for _ in 0..r.varint()? {
                holders.insert(Holder {
                    instance: owned(&mut r)?,
                    dp_rank: r.u32()?,
                    group_idx: r.u32()?,
                });
            }
            items.push((
                block,
                Monitored {
                    count,
                    error,
                    holders,
                },
            ));
        }
        let mut published = BTreeSet::new();
        for _ in 0..r.varint()? {
            published.insert(owned(&mut r)?);
        }
        Ok((SpaceSaving::restored(capacity, total, items), published))
    })()
    .map_err(|_| Error::Row("a keyed blocks record is truncated".into()))?;
    if !r.at_end() {
        return Err(Error::Row(
            "a keyed blocks record has trailing bytes".into(),
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};
    use analyzer_analysis::block_mass::SpaceSaving;
    use analyzer_analysis::intervals::Holder;
    use std::collections::BTreeSet;

    #[test]
    fn a_day_reads_back_as_written() {
        let mut s = SpaceSaving::new(2);
        let h = |i: u32| Holder {
            instance: format!("i{i}"),
            dp_rank: i,
            group_idx: 0,
        };
        s.add("a", 30, &h(0));
        s.add("b", 10, &h(1));
        s.add("c", 5, &h(2));
        let published = BTreeSet::from(["a".to_string(), "c".to_string()]);
        let (back, back_published) = decode(&encode(&s, &published)).expect("decodes");
        assert_eq!(back, s);
        assert_eq!(back_published, published);
    }
}
