// SPDX-License-Identifier: AGPL-3.0-only

//! Declared tag intervals on their own clock (`analyzer_analysis::tags`, spec §2.8): the
//! statements every commit carries, taken into the index before repeat compute classifies, so
//! a store is attributed against every declaration committed by then.
//!
//! A statement that arrives after repeat compute has classified past part of its interval
//! changes what those stores are attributed to. The driver re-derives that identity's stores
//! over the span the change reaches and moves its tag cells from the old interval to the new
//! one, so a late tag is corrected like a late record.

use crate::rowbinary::{Incomplete, Reader, Writer};
use crate::Error;
use analyzer_analysis::tags::{TagIndex, TagInterval, TagStatement};
use analyzer_lake::keyed::Record;
use analyzer_lake::working::{Relation, Scope};

/// Each declaration's interval.
pub(crate) const KEYED_TAGS: &str = "keyed_tags";

/// A declaration a tick changed: its interval before, if any, and now.
#[derive(Debug, Clone)]
pub struct Restated {
    /// The declaration.
    pub declaration_id: String,
    /// Its interval before the tick.
    pub before: Option<TagInterval>,
    /// Its interval now.
    pub now: TagInterval,
}

/// The tag index and the input position it has read through.
#[derive(Debug, Default)]
pub struct TagFeed {
    index: TagIndex,
    position: u64,
    /// Declarations changed since the last save, for the store.
    unsaved: std::collections::BTreeSet<String>,
}

impl TagFeed {
    /// A feed resumed at `position` over `declarations`.
    #[must_use]
    pub fn restored(
        declarations: impl IntoIterator<Item = (String, TagInterval)>,
        position: u64,
    ) -> Self {
        Self {
            index: TagIndex::restored(declarations),
            position,
            unsaved: std::collections::BTreeSet::new(),
        }
    }

    /// The index.
    #[must_use]
    pub fn index(&self) -> &TagIndex {
        &self.index
    }

    /// The input position read through.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Take every statement committed since the feed's position. Returns the declarations
    /// whose interval changed.
    ///
    /// # Errors
    /// The input log could not be read.
    pub fn tick(&mut self, lake: &analyzer_lake::Lake) -> Result<Vec<Restated>, Error> {
        let mut restated = Vec::new();
        for batch in crate::inputs::log(lake)?.committed_since(self.position)? {
            self.position = self.position.max(batch.input_position);
            for t in &batch.tags {
                let before = self
                    .index
                    .declarations()
                    .find(|(d, _)| **d == t.declaration_id)
                    .map(|(_, i)| i.clone());
                self.index.state(&TagStatement {
                    declaration_id: t.declaration_id.clone(),
                    identity: t.identity.clone(),
                    tag: t.tag.clone(),
                    from_ms: t.from_ms,
                    until_ms: t.until_ms,
                    at_ms: t.at_ms,
                });
                let now = self
                    .index
                    .declarations()
                    .find(|(d, _)| **d == t.declaration_id)
                    .map(|(_, i)| i.clone());
                if let Some(now) = now.filter(|n| before.as_ref() != Some(n)) {
                    self.unsaved.insert(t.declaration_id.clone());
                    restated.push(Restated {
                        declaration_id: t.declaration_id.clone(),
                        before,
                        now,
                    });
                }
            }
        }
        Ok(restated)
    }

    /// The rows of the declarations changed since the last call.
    pub fn drain_rows(&mut self) -> Vec<analyzer_analysis::module::Row> {
        self.index.drain()
    }

    /// The declarations changed since the last save, as they stand.
    pub(crate) fn take_unsaved(&mut self) -> Vec<(String, TagInterval)> {
        std::mem::take(&mut self.unsaved)
            .into_iter()
            .filter_map(|d| {
                let now = self
                    .index
                    .declarations()
                    .find(|(k, _)| **k == d)
                    .map(|(_, i)| i.clone())?;
                Some((d, now))
            })
            .collect()
    }
}

fn relation() -> Relation<'static> {
    Relation {
        name: KEYED_TAGS,
        scope: Scope::Lineage,
    }
}

fn encode(i: &TagInterval) -> Vec<u8> {
    let mut w = Writer::default();
    w.string(&i.identity);
    w.string(&i.tag);
    w.f64(i.from_ms);
    w.nullable(i.until_ms, Writer::f64);
    w.f64(i.stated_at_ms);
    w.bytes
}

fn decode(bytes: &[u8]) -> Result<TagInterval, Error> {
    let mut r = Reader::new(bytes);
    let decoded = (|| -> Result<TagInterval, Incomplete> {
        Ok(TagInterval {
            identity: r.string()?.to_string(),
            tag: r.string()?.to_string(),
            from_ms: r.f64()?,
            until_ms: r.nullable(Reader::f64)?,
            stated_at_ms: r.f64()?,
        })
    })()
    .map_err(|_| Error::Row("a keyed tags record is truncated".into()))?;
    if !r.at_end() {
        return Err(Error::Row("a keyed tags record has trailing bytes".into()));
    }
    Ok(decoded)
}

/// Write `declarations` under `version`.
///
/// # Errors
/// The working state failed.
pub(crate) fn write(
    lake: &analyzer_lake::Lake,
    declarations: &[(String, TagInterval)],
    version: f64,
) -> Result<(), Error> {
    if declarations.is_empty() {
        return Ok(());
    }
    let records = declarations
        .iter()
        .map(|(d, i)| {
            let hash = crate::spine::identity_hash(d);
            (
                analyzer_lake::stripe::of(hash),
                Record {
                    hash,
                    key: d.as_bytes().to_vec(),
                    version,
                    gone: false,
                    payload: encode(i),
                },
            )
        })
        .collect();
    let writer = crate::fold_state::KeyedWriter::open(lake, relation(), false)?;
    writer.put(records)?;
    writer.commit()
}

/// Every declaration the store holds.
///
/// # Errors
/// The working state failed, or a record does not decode.
pub(crate) fn read_all(lake: &analyzer_lake::Lake) -> Result<Vec<(String, TagInterval)>, Error> {
    let mut out = Vec::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(), stripe, None)? {
            let d = String::from_utf8(record.key)
                .map_err(|_| Error::Row("a keyed tags key does not decode".into()))?;
            out.push((d, decode(&record.payload)?));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};
    use analyzer_analysis::tags::TagInterval;

    #[test]
    fn a_declaration_reads_back_as_written() {
        for until_ms in [None, Some(99.5)] {
            let i = TagInterval {
                identity: "cS".into(),
                tag: "rule-3.applied".into(),
                from_ms: 1.0,
                until_ms,
                stated_at_ms: 2.0,
            };
            assert_eq!(decode(&encode(&i)).expect("decodes"), i);
        }
    }
}
