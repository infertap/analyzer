// SPDX-License-Identifier: AGPL-3.0-only

//! The whale register in the store (`analyzer_analysis::whale_days`): each stated day and the
//! identities it last published, a record per day ([`super::day_records`]).

use crate::rowbinary::{Incomplete, Reader, Writer};
use crate::Error;
use analyzer_analysis::dup_cells::WhaleSum;
use analyzer_analysis::whale_days::Day;
use std::collections::BTreeSet;

/// Each stated day.
pub(crate) const KEYED_WHALES: &str = "keyed_whales";

/// One stated day and the identities it last published, as bytes.
pub(crate) fn encode(day: &Day, published: &BTreeSet<String>) -> Vec<u8> {
    let mut w = Writer::default();
    w.varint(day.top.len() as u64);
    for (identity, sum) in &day.top {
        w.string(identity);
        w.i64(sum.n_tokens);
        w.f64(sum.dup_token_ms);
        w.f64(sum.held_token_ms);
    }
    w.u64(day.held_identities);
    w.u64(day.duplicated_identities);
    w.f64(day.dup_token_ms);
    w.f64(day.held_token_ms);
    w.u64(day.cut as u64);
    w.f64(day.floor_dup_token_ms);
    w.varint(published.len() as u64);
    for identity in published {
        w.string(identity);
    }
    w.bytes
}

/// A day back from [`encode`].
pub(crate) fn decode(bytes: &[u8]) -> Result<(Day, BTreeSet<String>), Error> {
    let mut r = Reader::new(bytes);
    let owned = |r: &mut Reader<'_>| r.string().map(str::to_string);
    let decoded = (|| -> Result<(Day, BTreeSet<String>), Incomplete> {
        let mut top = Vec::new();
        for _ in 0..r.varint()? {
            let identity = owned(&mut r)?;
            top.push((
                identity,
                WhaleSum {
                    n_tokens: r.i64()?,
                    dup_token_ms: r.f64()?,
                    held_token_ms: r.f64()?,
                },
            ));
        }
        let day = Day {
            top,
            held_identities: r.u64()?,
            duplicated_identities: r.u64()?,
            dup_token_ms: r.f64()?,
            held_token_ms: r.f64()?,
            cut: usize::try_from(r.u64()?).unwrap_or(usize::MAX),
            floor_dup_token_ms: r.f64()?,
        };
        let mut published = BTreeSet::new();
        for _ in 0..r.varint()? {
            published.insert(owned(&mut r)?);
        }
        Ok((day, published))
    })()
    .map_err(|_| Error::Row("a keyed whales record is truncated".into()))?;
    if !r.at_end() {
        return Err(Error::Row(
            "a keyed whales record has trailing bytes".into(),
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};
    use analyzer_analysis::dup_cells::WhaleSum;
    use analyzer_analysis::whale_days::Day;
    use std::collections::BTreeSet;

    #[test]
    fn a_day_reads_back_as_written() {
        let day = Day::of(
            [
                (
                    "a".to_string(),
                    WhaleSum {
                        n_tokens: 16,
                        dup_token_ms: 3.0,
                        held_token_ms: 9.0,
                    },
                ),
                (
                    "b".to_string(),
                    WhaleSum {
                        n_tokens: 8,
                        dup_token_ms: 0.0,
                        held_token_ms: 2.0,
                    },
                ),
            ],
            1,
        );
        let published = BTreeSet::from(["a".to_string()]);
        let (back, back_published) = decode(&encode(&day, &published)).expect("decodes");
        assert_eq!(back, day);
        assert_eq!(back_published, published);
    }
}
