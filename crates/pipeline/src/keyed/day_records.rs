// SPDX-License-Identifier: AGPL-3.0-only

//! A family stated per day in the store: a record per day in its own working-state relation,
//! written inside the operators' unit of work so a restore brings back the day and the rows it
//! stated together. The blocks by stored mass and the whale register keep their days here.

use crate::Error;
use analyzer_lake::keyed::Record;
use analyzer_lake::working::{Relation, Scope};

fn relation(name: &'static str) -> Relation<'static> {
    Relation {
        name,
        scope: Scope::Lineage,
    }
}

fn day_hash(day: i64) -> u64 {
    analyzer_lake::stripe::key_hash(&day.to_be_bytes())
}

fn record(day: i64, payload: Vec<u8>, version: f64, gone: bool) -> (usize, Record) {
    let hash = day_hash(day);
    (
        analyzer_lake::stripe::of(hash),
        Record {
            hash,
            key: day.to_be_bytes().to_vec(),
            version,
            gone,
            payload,
        },
    )
}

/// Write each of `days` to the relation `name` under `version`, and forget its days before
/// `retire_before`.
///
/// # Errors
/// The working state failed.
pub(crate) fn write(
    lake: &analyzer_lake::Lake,
    name: &'static str,
    days: &[(i64, Vec<u8>)],
    retire_before: Option<i64>,
    version: f64,
) -> Result<(), Error> {
    let mut records: Vec<(usize, Record)> = days
        .iter()
        .map(|(day, bytes)| record(*day, bytes.clone(), version, false))
        .collect();
    if let Some(before) = retire_before {
        for (day, _) in read_all(lake, name)? {
            if day < before {
                records.push(record(day, Vec::new(), version, true));
            }
        }
    }
    if records.is_empty() {
        return Ok(());
    }
    let writer = crate::fold_state::KeyedWriter::open(lake, relation(name), false)?;
    writer.put(records)?;
    writer.commit()
}

/// Every day the relation `name` holds, its record's bytes, in day order.
///
/// # Errors
/// The working state failed.
pub(crate) fn read_all(
    lake: &analyzer_lake::Lake,
    name: &'static str,
) -> Result<Vec<(i64, Vec<u8>)>, Error> {
    let mut out = Vec::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(name), stripe, None)? {
            if let Ok(bytes) = <[u8; 8]>::try_from(record.key.as_slice()) {
                out.push((i64::from_be_bytes(bytes), record.payload));
            }
        }
    }
    out.sort_by_key(|(day, _)| *day);
    Ok(out)
}
