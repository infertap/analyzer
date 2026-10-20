// SPDX-License-Identifier: AGPL-3.0-only

//! The event log partitioned by block (`docs/pipeline.md`): the records
//! the keyed driver reads, kept in the bucket so a drill or a correction reads one block's
//! history without the rest.
//!
//! **Written by the driver, from what it reads.** Each batch's records are written once, on
//! the tick that first reads the batch, late ones included: no second decode of the input.
//!
//! **Objects.** A tick writes one object per hour its records fall in, every partition of the
//! hour a compressed section of it and a footer indexing the sections: a drill of the open hour
//! reads its partition's range from each of the hour's tick objects. Once both operators have
//! passed an hour, compaction merges the hour's tick objects into one object per partition, so
//! a sealed hour is one read per block. It reads runs of consecutive sections within a byte
//! budget and keeps the hour under the partition count it was written in. A record's partition is its identity's stable hash over
//! the layout's partitions; the scope closers (`clear`, `holder_reset`), which name no block,
//! are a partition of their own, since every block's classification reads them.
//!
//! **Carried frontiers.** At each UTC day boundary repeat compute writes every key's frontiers
//! as they stand there, each in a carry section of its block's partition (one past the
//! closers' in the section numbering). A re-derivation starts from them and reads the log only
//! from the boundary: a residency open since long before is as visible to it as to the stream.
//! A correction writes a block's carry again in a later object, and the latest object naming a
//! key wins; an empty set says the key left.
//!
//! **The manifest** names each hour's live objects, a record per hour in the working state,
//! written inside the operators' unit of work: an object a rolled-back tick left is named
//! nowhere, is never read, and leaves when the hour compacts.

use super::repeat::IdentityKey;
use crate::rowbinary::{Reader, Writer};
use crate::Error;
use analyzer_analysis::classify::Frontier;
use analyzer_analysis::events::Event;
use analyzer_lake::keyed::Record;
use analyzer_lake::working::{Relation, Scope};
use std::collections::{BTreeMap, BTreeSet};

/// Each hour's live objects.
pub(crate) const KEYED_SPINE: &str = "keyed_spine";

/// An hour in milliseconds.
const HOUR_MS: f64 = 3_600_000.0;
const MAGIC: &[u8; 4] = b"KSP1";

/// How the log is partitioned and kept: knobs, not constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpineLayout {
    /// Partitions per hour: more make a block's read smaller and the hour's objects more.
    pub partitions: u32,
    /// Days of hours kept: the drill and correction horizon.
    pub retention_days: u32,
}

impl Default for SpineLayout {
    /// Sized for a 500-GPU fleet: 4,096 partitions an hour, seven days.
    fn default() -> Self {
        Self {
            partitions: 4_096,
            retention_days: 7,
        }
    }
}

impl SpineLayout {
    /// The partition of a record naming `identity`.
    #[must_use]
    pub fn partition_of(self, identity: &str) -> u32 {
        #[allow(clippy::cast_possible_truncation)] // reduced below `partitions`
        let p = (crate::spine::identity_hash(identity) % u64::from(self.partitions.max(1))) as u32;
        p
    }

    /// The partition of the scope closers: one past the blocks'.
    #[must_use]
    pub fn closers(self) -> u32 {
        self.partitions.max(1)
    }

    /// The section of the frontiers carried for `identity`: past the closers', in its block's
    /// partition.
    #[must_use]
    pub fn carry_of(self, identity: &str) -> u32 {
        self.closers() + 1 + self.partition_of(identity)
    }

    /// Whether `section` holds carried frontiers rather than records.
    fn is_carry(self, section: u32) -> bool {
        section > self.closers()
    }

    fn of(self, event: &Event) -> Option<u32> {
        match event.identity() {
            Some(identity) => Some(self.partition_of(identity)),
            None if event.kind.closes() => Some(self.closers()),
            None => None,
        }
    }
}

/// An object's footer: each partition's section as `(offset, length, rows)`.
type Index = BTreeMap<u32, (u64, u64, u32)>;

/// The hour holding `at_ms`.
#[must_use]
pub fn hour_of(at_ms: f64) -> i64 {
    #[allow(clippy::cast_possible_truncation)] // hour indices of finite instants fit i64
    let hour = (at_ms / HOUR_MS).floor() as i64;
    hour
}

/// One object the manifest names: its key, its size, its footer's length, and the partitions
/// it was cut into, so a change of layout moves new hours and never misreads old ones.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Object {
    key: String,
    size: u64,
    footer: u64,
    partitions: u32,
}

/// An hour's compacted objects: one per section, by the partition count each was cut into, all
/// named by the compaction that wrote them.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Compacted {
    /// Which compaction of the hour wrote them. A compaction writes new names, never over an
    /// object a manifest a rollback can reach still names.
    generation: u32,
    /// The sections written, by the partition count they were cut into. An hour keeps the cut
    /// its records were written under; a later layout cuts later hours.
    cuts: BTreeMap<u32, BTreeSet<u32>>,
}

impl Compacted {
    /// Whether the section `section` of the cut into `partitions` was written.
    fn holds(&self, partitions: u32, section: u32) -> bool {
        self.cuts
            .get(&partitions)
            .is_some_and(|written| written.contains(&section))
    }

    /// The key of every object written.
    fn keys(&self, lake: &analyzer_lake::Lake, hour: i64) -> Vec<String> {
        self.cuts
            .iter()
            .flat_map(|(partitions, written)| {
                written.iter().map(move |section| {
                    compacted_key(lake, hour, (self.generation, *partitions, *section))
                })
            })
            .collect()
    }
}

/// Compaction reads the hour's objects a run of consecutive sections at a time, up to this many
/// compressed bytes across them: the most of the hour it holds at once, whatever the fleet.
const COMPACTION_BATCH_BYTES: u64 = 256 << 20;

/// One hour's live objects: its tick objects, and its compacted ones.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct HourManifest {
    ticks: Vec<Object>,
    compacted: Option<Compacted>,
    /// A compaction's garbage, kept until a checkpoint holds the compaction: the working
    /// state's sequence just before it was written. A rollback returns to the newest
    /// checkpoint, so objects an older manifest names are deleted only once no checkpoint a
    /// rollback can reach still names them.
    #[serde(default)]
    garbage_after: Option<u64>,
}

/// The layout an object was cut into.
fn cut(partitions: u32) -> SpineLayout {
    SpineLayout {
        partitions,
        retention_days: 0,
    }
}

/// The objects a tick wrote, by hour, for the manifest.
#[derive(Debug, Default)]
pub struct Written {
    hours: BTreeMap<i64, Vec<Object>>,
}

/// The oldest hour the log holds anything for, tick objects or compacted ones: how far back a
/// re-derivation reaches.
///
/// # Errors
/// The working state failed, or a manifest does not decode.
pub fn held_from_hour(lake: &analyzer_lake::Lake) -> Result<Option<i64>, Error> {
    let mut oldest: Option<i64> = None;
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(), stripe, None)? {
            let Ok(bytes) = <[u8; 8]>::try_from(record.key.as_slice()) else {
                continue;
            };
            let manifest: HourManifest = serde_json::from_slice(&record.payload)
                .map_err(|e| Error::Row(format!("keyed spine manifest: {e}")))?;
            if !manifest.ticks.is_empty() || manifest.compacted.is_some() {
                let hour = i64::from_be_bytes(bytes);
                oldest = Some(oldest.map_or(hour, |o| o.min(hour)));
            }
        }
    }
    Ok(oldest)
}

impl Written {
    /// Another write's objects beside these.
    pub fn absorb(&mut self, other: Self) {
        for (hour, objects) in other.hours {
            self.hours.entry(hour).or_default().extend(objects);
        }
    }
}

/// A name no other write of any process gives: the instant, the process, and a counter. A
/// rolled-back write's objects keep their names, named by no manifest.
fn unique_name() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        "t{nanos:024}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

fn relation() -> Relation<'static> {
    Relation {
        name: KEYED_SPINE,
        scope: Scope::Lineage,
    }
}

fn prefix(lake: &analyzer_lake::Lake, hour: i64) -> String {
    format!("{}keyed-spine/{hour}/", lake.root())
}

/// `events` as one object: each partition's rows a compressed section in partition order,
/// then a footer of `(partition, offset, length, rows)` per section and the footer's length.
fn encode(sections: &BTreeMap<u32, Vec<&Event>>) -> Result<(Vec<u8>, u64), Error> {
    encode_raw(sections.iter().map(|(partition, events)| {
        let mut w = Writer::default();
        for event in events {
            crate::spine::encode_row(&mut w, event);
        }
        (*partition, w.bytes, events.len())
    }))
}

/// Sections as one object, each `(section, rows' bytes, rows)` compressed in the order given,
/// then the footer.
fn encode_raw(
    sections: impl IntoIterator<Item = (u32, Vec<u8>, usize)>,
) -> Result<(Vec<u8>, u64), Error> {
    let mut out = MAGIC.to_vec();
    let mut index = Vec::new();
    for (partition, raw, rows) in sections {
        let start = out.len() as u64;
        let mut enc = snap::write::FrameEncoder::new(&mut out);
        std::io::Write::write_all(&mut enc, &raw)
            .and_then(|()| std::io::Write::flush(&mut enc))
            .map_err(|e| Error::Spine(format!("keyed spine: compress: {e}")))?;
        drop(enc);
        let rows = u32::try_from(rows)
            .map_err(|_| Error::Spine("keyed spine: a section past four billion rows".into()))?;
        index.push((partition, start, out.len() as u64 - start, rows));
    }
    let mut footer = Writer::default();
    footer.varint(index.len() as u64);
    for (partition, start, len, rows) in index {
        footer.u32(partition);
        footer.u64(start);
        footer.u64(len);
        footer.u32(rows);
    }
    let footer_len = footer.bytes.len() as u64 + 8;
    out.extend_from_slice(&footer.bytes);
    out.extend_from_slice(&footer_len.to_le_bytes());
    Ok((out, footer_len))
}

/// A footer back: `(offset, length, rows)` per partition.
fn footer_of(bytes: &[u8]) -> Result<Index, Error> {
    let body = bytes
        .get(..bytes.len().saturating_sub(8))
        .ok_or_else(|| Error::Spine("keyed spine: a footer too short".into()))?;
    let mut r = Reader::new(body);
    let bad = |_| Error::Spine("keyed spine: a footer does not decode".into());
    let n = r.varint().map_err(bad)?;
    let mut index = BTreeMap::new();
    for _ in 0..n {
        let partition = r.u32().map_err(bad)?;
        let entry = (
            r.u64().map_err(bad)?,
            r.u64().map_err(bad)?,
            r.u32().map_err(bad)?,
        );
        index.insert(partition, entry);
    }
    Ok(index)
}

/// A section's bytes, decompressed.
fn raw_of(section: &[u8]) -> Result<Vec<u8>, Error> {
    let mut raw = Vec::new();
    std::io::Read::read_to_end(&mut snap::read::FrameDecoder::new(section), &mut raw)
        .map_err(|e| Error::Spine(format!("keyed spine: decompress: {e}")))?;
    Ok(raw)
}

/// One key's carried frontiers: an empty set is the key leaving.
pub type Carried = (IdentityKey, Vec<Frontier>);

/// Carried frontiers as a section's rows: each key, then its frontiers.
fn carry_raw(rows: &[Carried]) -> Vec<u8> {
    let mut w = Writer::default();
    for row in rows {
        carry_row(&mut w, row);
    }
    w.bytes
}

/// One key's carried frontiers onto `w`.
fn carry_row(w: &mut Writer, ((block, epoch), frontiers): &Carried) {
    w.string(block);
    w.nullable(*epoch, Writer::u32);
    super::store::write_frontiers(w, frontiers);
}

/// A carry section's rows back.
fn carried_of(section: &[u8], rows: u32) -> Result<Vec<Carried>, Error> {
    let raw = raw_of(section)?;
    let mut r = Reader::new(&raw);
    let decoded = (0..rows)
        .map(|_| {
            let block = r.string()?.to_string();
            let epoch = r.nullable(Reader::u32)?;
            Ok(((block, epoch), super::store::read_frontiers(&mut r)?))
        })
        .collect::<Result<Vec<_>, crate::rowbinary::Incomplete>>()
        .map_err(|_| Error::Spine("keyed spine: a carry section ended mid-row".into()))?;
    if !r.at_end() {
        return Err(Error::Spine(
            "keyed spine: a carry section has trailing bytes".into(),
        ));
    }
    Ok(decoded)
}

/// One section's rows back.
fn rows_of(section: &[u8], rows: u32) -> Result<Vec<Event>, Error> {
    let raw = raw_of(section)?;
    let mut r = Reader::new(&raw);
    let mut out = Vec::with_capacity(rows as usize);
    for _ in 0..rows {
        match crate::spine::decode_row(&mut r) {
            Ok(Ok(event)) => out.push(event),
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(Error::Spine("keyed spine: a section ended mid-row".into())),
        }
    }
    Ok(out)
}

/// Write `events`, one tick's first-read records, as one object per hour they fall in, each
/// under a name no other write gives. Returns what the manifest must name.
///
/// # Errors
/// The store refused a write.
pub fn write_tick(
    lake: &analyzer_lake::Lake,
    layout: SpineLayout,
    events: &[Event],
) -> Result<Written, Error> {
    let name = unique_name();
    let mut by_hour: BTreeMap<i64, BTreeMap<u32, Vec<&Event>>> = BTreeMap::new();
    for event in events {
        if let Some(partition) = layout.of(event) {
            by_hour
                .entry(hour_of(event.at_ms))
                .or_default()
                .entry(partition)
                .or_default()
                .push(event);
        }
    }
    let mut written = Written::default();
    for (hour, sections) in by_hour {
        let (body, footer) = encode(&sections)?;
        let key = format!("{}{name}.bin", prefix(lake, hour));
        lake.store().put(&key, &body)?;
        written.hours.entry(hour).or_default().push(Object {
            key,
            size: body.len() as u64,
            footer,
            partitions: layout.partitions.max(1),
        });
    }
    Ok(written)
}

/// Write `rows`, frontiers carried at the day boundary `at_ms`, as one object of the boundary's
/// hour under a name no other write gives, each key in its block's carry section. Returns what
/// the manifest must name.
///
/// # Errors
/// The store refused a write.
pub fn write_carry(
    lake: &analyzer_lake::Lake,
    layout: SpineLayout,
    at_ms: f64,
    rows: impl IntoIterator<Item = Carried>,
) -> Result<Written, Error> {
    let mut written = Written::default();
    // Each row encoded into its section as it comes: the rows are never all held at once.
    let mut sections: BTreeMap<u32, (Writer, usize)> = BTreeMap::new();
    for row in rows {
        let (w, count) = sections.entry(layout.carry_of(&row.0 .0)).or_default();
        carry_row(w, &row);
        *count += 1;
    }
    if sections.is_empty() {
        return Ok(written);
    }
    let (body, footer) = encode_raw(
        sections
            .into_iter()
            .map(|(section, (w, rows))| (section, w.bytes, rows)),
    )?;
    let hour = hour_of(at_ms);
    let key = format!("{}{}.bin", prefix(lake, hour), unique_name());
    lake.store().put(&key, &body)?;
    written.hours.entry(hour).or_default().push(Object {
        key,
        size: body.len() as u64,
        footer,
        partitions: layout.partitions.max(1),
    });
    Ok(written)
}

/// The frontiers carried at the day boundary `at_ms` for `identity`'s keys: from every object
/// of the boundary's hour, the manifest's and the `unsaved` ones, the latest naming a key
/// winning, and the keys that left dropped.
///
/// # Errors
/// The working state or the store failed, or an object does not decode.
pub fn carried(
    lake: &analyzer_lake::Lake,
    identity: &str,
    at_ms: f64,
    unsaved: &Written,
) -> Result<BTreeMap<IdentityKey, Vec<Frontier>>, Error> {
    let hour = hour_of(at_ms);
    let manifest = read_manifest(lake, hour)?;
    let mut keys: BTreeMap<IdentityKey, Vec<Frontier>> = BTreeMap::new();
    let mut take = |rows: Vec<Carried>| {
        for (key, frontiers) in rows {
            if key.0 == identity {
                keys.insert(key, frontiers);
            }
        }
    };
    if let Some(compacted) = &manifest.compacted {
        for &partitions in compacted.cuts.keys() {
            let section = cut(partitions).carry_of(identity);
            if compacted.holds(partitions, section) {
                let key = compacted_key(lake, hour, (compacted.generation, partitions, section));
                let body = lake.store().get(&key)?;
                let (index, _) = whole_footer(&body)?;
                if let Some((start, len, rows)) = index.get(&section) {
                    take(carried_of(slice(&body, *start, *len)?, *rows)?);
                }
            }
        }
    }
    let unnamed = unsaved.hours.get(&hour).into_iter().flatten();
    for object in manifest.ticks.iter().chain(unnamed) {
        let tail = lake
            .store()
            .get_range(&object.key, object.size - object.footer..object.size)?;
        let index = footer_of(&tail)?;
        if let Some((start, len, rows)) = index.get(&cut(object.partitions).carry_of(identity)) {
            let bytes = lake.store().get_range(&object.key, *start..*start + *len)?;
            take(carried_of(&bytes, *rows)?);
        }
    }
    keys.retain(|_, frontiers| !frontiers.is_empty());
    Ok(keys)
}

fn read_manifest(lake: &analyzer_lake::Lake, hour: i64) -> Result<HourManifest, Error> {
    let key = hour.to_be_bytes().to_vec();
    let hash = hour_hash(hour);
    let records = crate::fold_state::read(
        lake,
        &relation(),
        analyzer_lake::stripe::of(hash),
        Some(&[hash]),
    )?;
    records
        .iter()
        .find(|r| r.key == key)
        .map(|r| {
            serde_json::from_slice(&r.payload)
                .map_err(|e| Error::Row(format!("keyed spine manifest: {e}")))
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn hour_hash(hour: i64) -> u64 {
    analyzer_lake::stripe::key_hash(&hour.to_be_bytes())
}

fn manifest_record(
    hour: i64,
    manifest: &HourManifest,
    version: f64,
) -> Result<(usize, Record), Error> {
    let hash = hour_hash(hour);
    Ok((
        analyzer_lake::stripe::of(hash),
        Record {
            hash,
            key: hour.to_be_bytes().to_vec(),
            version,
            gone: false,
            payload: serde_json::to_vec(manifest)
                .map_err(|e| Error::Row(format!("keyed spine manifest: {e}")))?,
        },
    ))
}

/// Name `written` in the manifest, under `version`: each hour's tick objects added to those it
/// names. The caller holds the operators' unit of work around it.
///
/// # Errors
/// The working state failed.
pub(crate) fn record_written(
    lake: &analyzer_lake::Lake,
    written: &Written,
    version: f64,
) -> Result<(), Error> {
    if written.hours.is_empty() {
        return Ok(());
    }
    let mut records = Vec::with_capacity(written.hours.len());
    for (hour, objects) in &written.hours {
        let mut manifest = read_manifest(lake, *hour)?;
        manifest.ticks.extend(objects.iter().cloned());
        records.push(manifest_record(*hour, &manifest, version)?);
    }
    let writer = crate::fold_state::KeyedWriter::open(lake, relation(), false)?;
    writer.put(records)?;
    writer.commit()
}

/// The records of `hour` naming `identity`, or the scope closers for `None`, from every object
/// the manifest names, each object read at the partition its own layout puts them in.
fn read_hour(
    lake: &analyzer_lake::Lake,
    hour: i64,
    identity: Option<&str>,
    unsaved: &Written,
) -> Result<Vec<Event>, Error> {
    let partition_in = |partitions: u32| {
        let layout = cut(partitions);
        identity.map_or(layout.closers(), |id| layout.partition_of(id))
    };
    let manifest = read_manifest(lake, hour)?;
    let mut events = Vec::new();
    if let Some(compacted) = &manifest.compacted {
        for &partitions in compacted.cuts.keys() {
            let partition = partition_in(partitions);
            if compacted.holds(partitions, partition) {
                let key = compacted_key(lake, hour, (compacted.generation, partitions, partition));
                let body = lake.store().get(&key)?;
                let (index, _) = whole_footer(&body)?;
                if let Some((start, len, rows)) = index.get(&partition) {
                    events.extend(section(&body, *start, *len, *rows)?);
                }
            }
        }
    }
    let unnamed = unsaved.hours.get(&hour).into_iter().flatten();
    for object in manifest.ticks.iter().chain(unnamed) {
        let tail = lake
            .store()
            .get_range(&object.key, object.size - object.footer..object.size)?;
        let index = footer_of(&tail)?;
        if let Some((start, len, rows)) = index.get(&partition_in(object.partitions)) {
            let bytes = lake.store().get_range(&object.key, *start..*start + *len)?;
            events.extend(rows_of(&bytes, *rows)?);
        }
    }
    Ok(events)
}

fn section(body: &[u8], start: u64, len: u64, rows: u32) -> Result<Vec<Event>, Error> {
    rows_of(slice(body, start, len)?, rows)
}

/// The bytes of a section at `start` for `len` within a whole object.
fn slice(body: &[u8], start: u64, len: u64) -> Result<&[u8], Error> {
    let (s, e) = (
        usize::try_from(start).unwrap_or(usize::MAX),
        usize::try_from(start + len).unwrap_or(usize::MAX),
    );
    body.get(s..e)
        .ok_or_else(|| Error::Spine("keyed spine: a section past the object".into()))
}

/// A whole object's footer, and the footer's length.
fn whole_footer(body: &[u8]) -> Result<(Index, u64), Error> {
    if body.len() < MAGIC.len() + 8 || &body[..MAGIC.len()] != MAGIC {
        return Err(Error::Spine("keyed spine: not an object".into()));
    }
    let tail: [u8; 8] = body[body.len() - 8..].try_into().unwrap_or([0; 8]);
    let footer_len = u64::from_le_bytes(tail);
    let from = body
        .len()
        .checked_sub(usize::try_from(footer_len).unwrap_or(usize::MAX))
        .ok_or_else(|| Error::Spine("keyed spine: a footer past the object".into()))?;
    Ok((footer_of(&body[from..])?, footer_len))
}

/// The name of a compacted object: the compaction's generation, the cut, the section.
fn compacted_key(
    lake: &analyzer_lake::Lake,
    hour: i64,
    (generation, partitions, section): (u32, u32, u32),
) -> String {
    format!(
        "{}g{generation}-c{partitions}-p{section:05}.bin",
        prefix(lake, hour)
    )
}

/// One block's history over `window`: the records naming `identity` and the scope closers,
/// every hour's, sorted, from the objects the manifest names and the `unsaved` ones a driver
/// wrote since its last save.
///
/// # Errors
/// The working state or the store failed, or an object does not decode.
pub fn block_history(
    lake: &analyzer_lake::Lake,
    identity: &str,
    window: analyzer_analysis::Window,
    unsaved: &Written,
) -> Result<Vec<Event>, Error> {
    let mut events = Vec::new();
    for hour in hour_of(window.start_ms)..=hour_of(window.end_ms) {
        events.extend(
            read_hour(lake, hour, Some(identity), unsaved)?
                .into_iter()
                .filter(|e| e.identity() == Some(identity)),
        );
        events.extend(read_hour(lake, hour, None, unsaved)?);
    }
    events.retain(|e| e.at_ms >= window.start_ms && e.at_ms < window.end_ms);
    analyzer_analysis::events::sort(&mut events);
    Ok(events)
}

/// Compact `hour`: every section's records from its tick objects and the hour's earlier
/// compaction into one object each, under the cut they were written in, the manifest restated to
/// name them, under `version`. The tick objects are deleted once a checkpoint holds the
/// compaction. It reads a batch of consecutive sections at a time, one ranged read of each tick
/// object a batch, so what it holds is bounded by a fixed budget (256 MiB) and one section
/// decoded. The caller holds a unit of work around it and runs it once both operators have
/// passed the hour.
///
/// # Errors
/// The store or the working state failed, or an object does not decode.
pub fn compact(lake: &analyzer_lake::Lake, hour: i64, version: f64) -> Result<bool, Error> {
    compact_within(lake, hour, version, COMPACTION_BATCH_BYTES)
}

/// [`compact`], reading at most `budget` compressed bytes of the tick objects a batch.
fn compact_within(
    lake: &analyzer_lake::Lake,
    hour: i64,
    version: f64,
    budget: u64,
) -> Result<bool, Error> {
    let manifest = read_manifest(lake, hour)?;
    if manifest.ticks.is_empty() {
        return Ok(false);
    }
    let earlier = manifest.compacted.as_ref();
    let generation = earlier.map_or(0, |c| c.generation + 1);
    let mut ticks = Vec::with_capacity(manifest.ticks.len());
    for object in &manifest.ticks {
        let tail = lake
            .store()
            .get_range(&object.key, object.size - object.footer..object.size)?;
        ticks.push((object, footer_of(&tail)?));
    }
    let cuts: BTreeSet<u32> = ticks
        .iter()
        .map(|(o, _)| o.partitions)
        .chain(earlier.iter().flat_map(|c| c.cuts.keys().copied()))
        .collect();
    let before = lake.working()?.sequence()?;
    let mut written: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
    for partitions in cuts {
        let of_cut: Vec<&(&Object, Index)> = ticks
            .iter()
            .filter(|(o, _)| o.partitions == partitions)
            .collect();
        // Each section's compressed bytes across the cut's tick objects, an earlier compaction's
        // sections among them.
        let mut sizes: BTreeMap<u32, u64> = earlier
            .and_then(|c| c.cuts.get(&partitions))
            .into_iter()
            .flatten()
            .map(|s| (*s, 0))
            .collect();
        for (_, index) in &of_cut {
            for (section, (_, len, _)) in index {
                *sizes.entry(*section).or_default() += len;
            }
        }
        for batch in batches(&sizes, budget) {
            let (lo, hi) = (batch[0], batch[batch.len() - 1]);
            let mut bodies = Vec::new();
            for (object, index) in &of_cut {
                let mut spans = index
                    .range(lo..=hi)
                    .map(|(_, (start, len, _))| (*start, start + len));
                let Some(first) = spans.next() else {
                    continue;
                };
                let (from, to) = spans.fold(first, |(a, b), (c, d)| (a.min(c), b.max(d)));
                bodies.push((from, lake.store().get_range(&object.key, from..to)?, index));
            }
            for section in batch {
                let from_earlier = match earlier {
                    Some(c) if c.holds(partitions, section) => Some(lake.store().get(
                        &compacted_key(lake, hour, (c.generation, partitions, section)),
                    )?),
                    _ => None,
                };
                // The sections in the order written: the earlier compaction, then the ticks.
                let mut parts: Vec<(&[u8], u32)> = Vec::new();
                if let Some(body) = &from_earlier {
                    let (index, _) = whole_footer(body)?;
                    if let Some((start, len, rows)) = index.get(&section) {
                        parts.push((slice(body, *start, *len)?, *rows));
                    }
                }
                for (base, body, index) in &bodies {
                    if let Some((start, len, rows)) = index.get(&section) {
                        parts.push((slice(body, start - base, *len)?, *rows));
                    }
                }
                let Some(body) = merged(section, cut(partitions).is_carry(section), parts)? else {
                    continue;
                };
                lake.store().put(
                    &compacted_key(lake, hour, (generation, partitions, section)),
                    &body,
                )?;
                written.entry(partitions).or_default().insert(section);
            }
        }
    }
    let compacted = HourManifest {
        ticks: Vec::new(),
        compacted: Some(Compacted {
            generation,
            cuts: written,
        }),
        garbage_after: Some(before),
    };
    let writer = crate::fold_state::KeyedWriter::open(lake, relation(), false)?;
    writer.put(vec![manifest_record(hour, &compacted, version)?])?;
    writer.commit()?;
    Ok(true)
}

/// One section's `parts`, in the order written, as one compacted object: a carry section's keys
/// the latest naming each winning, and none where every key left, since nothing older is kept
/// to override; a record section's records sorted.
fn merged(section: u32, carry: bool, parts: Vec<(&[u8], u32)>) -> Result<Option<Vec<u8>>, Error> {
    if carry {
        let mut keys: BTreeMap<IdentityKey, Vec<Frontier>> = BTreeMap::new();
        for (bytes, rows) in parts {
            keys.extend(carried_of(bytes, rows)?);
        }
        let rows: Vec<Carried> = keys.into_iter().filter(|(_, f)| !f.is_empty()).collect();
        if rows.is_empty() {
            return Ok(None);
        }
        return Ok(Some(
            encode_raw([(section, carry_raw(&rows), rows.len())])?.0,
        ));
    }
    let mut events = Vec::new();
    for (bytes, rows) in parts {
        events.extend(rows_of(bytes, rows)?);
    }
    analyzer_analysis::events::sort(&mut events);
    let refs: Vec<&Event> = events.iter().collect();
    Ok(Some(encode(&BTreeMap::from([(section, refs)]))?.0))
}

/// `sizes`' sections in runs of consecutive ones, each run's bytes at most `budget` unless one
/// section alone is larger.
fn batches(sizes: &BTreeMap<u32, u64>, budget: u64) -> Vec<Vec<u32>> {
    let mut out: Vec<Vec<u32>> = Vec::new();
    let mut held = 0;
    for (section, size) in sizes {
        match out.last_mut() {
            Some(run) if held + size <= budget => {
                run.push(*section);
                held += size;
            }
            _ => {
                out.push(vec![*section]);
                held = *size;
            }
        }
    }
    out
}

/// Delete the garbage of every compaction a checkpoint now holds, under `version`: the hours
/// whose compaction was written before the newest checkpoint's sequence. Returns those hours.
///
/// # Errors
/// The store or the working state failed, or a manifest does not decode.
pub fn collect_held_garbage(lake: &analyzer_lake::Lake, version: f64) -> Result<Vec<i64>, Error> {
    let Some(checkpointed) = lake.working()?.checkpointed_sequence(lake)? else {
        return Ok(Vec::new());
    };
    let mut due = Vec::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(), stripe, None)? {
            let Ok(bytes) = <[u8; 8]>::try_from(record.key.as_slice()) else {
                continue;
            };
            let manifest: HourManifest = serde_json::from_slice(&record.payload)
                .map_err(|e| Error::Row(format!("keyed spine manifest: {e}")))?;
            if manifest.garbage_after.is_some_and(|s| s < checkpointed) {
                due.push((i64::from_be_bytes(bytes), manifest));
            }
        }
    }
    let mut records = Vec::with_capacity(due.len());
    let mut hours = Vec::with_capacity(due.len());
    for (hour, mut manifest) in due {
        collect_garbage(lake, hour, &manifest)?;
        manifest.garbage_after = None;
        records.push(manifest_record(hour, &manifest, version)?);
        hours.push(hour);
    }
    if !records.is_empty() {
        let writer = crate::fold_state::KeyedWriter::open(lake, relation(), false)?;
        writer.put(records)?;
        writer.commit()?;
    }
    Ok(hours)
}

/// Delete every object of `hour` the manifest does not name: tick objects compacted away, and
/// the objects of ticks that rolled back.
fn collect_garbage(
    lake: &analyzer_lake::Lake,
    hour: i64,
    manifest: &HourManifest,
) -> Result<(), Error> {
    let named: BTreeSet<String> = manifest
        .ticks
        .iter()
        .map(|o| o.key.clone())
        .chain(manifest.compacted.iter().flat_map(|c| c.keys(lake, hour)))
        .collect();
    for object in lake.store().list(&prefix(lake, hour))? {
        if !named.contains(&object.key) {
            lake.store().delete(&object.key)?;
        }
    }
    Ok(())
}

/// The hours whose manifest names tick objects: what compaction has left to do.
///
/// # Errors
/// The working state failed, or a manifest does not decode.
pub fn hours_with_ticks(lake: &analyzer_lake::Lake) -> Result<Vec<i64>, Error> {
    let mut hours = Vec::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(), stripe, None)? {
            let Ok(bytes) = <[u8; 8]>::try_from(record.key.as_slice()) else {
                continue;
            };
            let manifest: HourManifest = serde_json::from_slice(&record.payload)
                .map_err(|e| Error::Row(format!("keyed spine manifest: {e}")))?;
            if !manifest.ticks.is_empty() {
                hours.push(i64::from_be_bytes(bytes));
            }
        }
    }
    hours.sort_unstable();
    Ok(hours)
}

impl Written {
    /// Whether any object of `hour` is among these.
    #[must_use]
    pub fn holds(&self, hour: i64) -> bool {
        self.hours.contains_key(&hour)
    }
}

/// Forget every hour before `hour`: its objects and its manifest record.
///
/// # Errors
/// The store or the working state failed.
pub fn retire_before(lake: &analyzer_lake::Lake, hour: i64, version: f64) -> Result<(), Error> {
    let mut gone = Vec::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(), stripe, None)? {
            let Ok(bytes) = <[u8; 8]>::try_from(record.key.as_slice()) else {
                continue;
            };
            let held = i64::from_be_bytes(bytes);
            if held < hour {
                for object in lake.store().list(&prefix(lake, held))? {
                    lake.store().delete(&object.key)?;
                }
                gone.push((
                    record_stripe(held),
                    Record {
                        hash: hour_hash(held),
                        key: record.key.clone(),
                        version,
                        gone: true,
                        payload: Vec::new(),
                    },
                ));
            }
        }
    }
    if gone.is_empty() {
        return Ok(());
    }
    let writer = crate::fold_state::KeyedWriter::open(lake, relation(), false)?;
    writer.put(gone)?;
    writer.commit()
}

fn record_stripe(hour: i64) -> usize {
    analyzer_lake::stripe::of(hour_hash(hour))
}

#[cfg(test)]
mod tests {
    use super::{
        block_history, carried, compact, compact_within, hour_of, read_manifest, record_written,
        write_carry, write_tick, SpineLayout, Written,
    };
    use analyzer_analysis::classify::Frontier;
    use analyzer_analysis::events::{Event, IntervalKey, Kind};
    use analyzer_analysis::intervals::CloseReason;

    fn frontier(instance: &str, block: &str) -> Frontier {
        Frontier {
            key: IntervalKey {
                instance: instance.into(),
                dp_rank: 0,
                group_idx: 0,
                identity: block.into(),
                key_epoch: None,
                tier: None,
                backend_id: None,
            },
            last_close_ms: Some(5.0),
            last_close_reason: Some(CloseReason::Evict),
            open: false,
            open_since_ms: None,
            open_readmits: 0,
            open_workload_class: None,
            open_opener_class: None,
        }
    }

    #[test]
    fn the_latest_carry_naming_a_key_wins_before_and_after_compaction() {
        let root = std::env::temp_dir().join(format!("spine-carry-{}", std::process::id()));
        let lake = crate::fold_state::test_lake(&root);
        let working = lake.working().expect("working state");
        let layout = SpineLayout {
            partitions: 4,
            retention_days: 7,
        };
        let at = analyzer_analysis::DAY_MS * 100.0;
        let key = |block: &str| (block.to_string(), None);
        working.begin(&lake).expect("begin");
        let first = write_carry(
            &lake,
            layout,
            at,
            vec![
                (key("x"), vec![frontier("i1", "x")]),
                (key("y"), vec![frontier("i1", "y")]),
            ],
        )
        .expect("write");
        record_written(&lake, &first, 1.0).expect("record");
        // A correction: x carried as it is now, y as leaving.
        let second = write_carry(
            &lake,
            layout,
            at,
            vec![
                (key("x"), vec![frontier("i2", "x")]),
                (key("y"), Vec::new()),
            ],
        )
        .expect("write");
        record_written(&lake, &second, 2.0).expect("record");
        lake.end_unit(working.position()).expect("end");
        let read = |block: &str| carried(&lake, block, at, &Written::default()).expect("read");
        let check = |when: &str| {
            assert_eq!(
                read("x").into_iter().collect::<Vec<_>>(),
                vec![(key("x"), vec![frontier("i2", "x")])],
                "{when}: the latest write of x"
            );
            assert!(read("y").is_empty(), "{when}: y left");
        };
        check("before compaction");
        working.begin(&lake).expect("begin");
        compact(&lake, hour_of(at), 3.0).expect("compact");
        lake.end_unit(working.position()).expect("end");
        check("after compaction");
        std::fs::remove_dir_all(&root).ok();
    }

    /// A lake with one hour of records over `layout`, written as three ticks, each recorded.
    fn hour_of_ticks(
        name: &str,
        layout: SpineLayout,
    ) -> (analyzer_lake::Lake, std::path::PathBuf, f64) {
        let root = std::env::temp_dir().join(format!("spine-{name}-{}", std::process::id()));
        let lake = crate::fold_state::test_lake(&root);
        let working = lake.working().expect("working state");
        let at = analyzer_analysis::DAY_MS * 100.0;
        for tick in 0..3_u32 {
            let events: Vec<Event> = (0..40_u32)
                .map(|k| {
                    let mut e = Event::new(
                        if k % 3 == 0 { Kind::Evict } else { Kind::Store },
                        format!("i{}", k % 2),
                        at + f64::from(tick * 1_000 + k),
                    );
                    e.block_id = Some(format!("b{}", k % 9));
                    e
                })
                .chain(std::iter::once(Event::new(
                    Kind::Clear,
                    "i0",
                    at + f64::from(tick * 1_000 + 500),
                )))
                .collect();
            working.begin(&lake).expect("begin");
            let written = write_tick(&lake, layout, &events).expect("write");
            record_written(&lake, &written, f64::from(tick + 1)).expect("record");
            lake.end_unit(working.position()).expect("end");
        }
        (lake, root, at)
    }

    /// Every block's history over the hour, and the closers', as compared.
    fn histories(lake: &analyzer_lake::Lake, at: f64) -> Vec<Vec<String>> {
        let window = analyzer_analysis::Window {
            start_ms: at,
            end_ms: at + 3_600_000.0,
        };
        (0..9)
            .map(|b| {
                block_history(lake, &format!("b{b}"), window, &Written::default())
                    .expect("history")
                    .iter()
                    .map(|e| format!("{:?} {} {:?} {}", e.kind, e.instance, e.block_id, e.at_ms))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn a_compaction_a_section_at_a_time_holds_what_one_reading_everything_holds() {
        let layout = SpineLayout {
            partitions: 4,
            retention_days: 7,
        };
        let (whole, whole_root, at) = hour_of_ticks("whole", layout);
        let (batched, batched_root, _) = hour_of_ticks("batched", layout);
        let before = histories(&whole, at);
        for (lake, budget) in [(&whole, u64::MAX), (&batched, 1)] {
            let working = lake.working().expect("working state");
            working.begin(lake).expect("begin");
            assert!(compact_within(lake, hour_of(at), 9.0, budget).expect("compact"));
            lake.end_unit(working.position()).expect("end");
        }
        assert_eq!(
            histories(&whole, at),
            before,
            "compaction keeps every block's history"
        );
        assert_eq!(
            histories(&batched, at),
            before,
            "a batch a section keeps it too"
        );
        let sections = |lake: &analyzer_lake::Lake| {
            read_manifest(lake, hour_of(at))
                .expect("manifest")
                .compacted
                .expect("compacted")
                .cuts
        };
        assert_eq!(sections(&whole), sections(&batched));
        assert_eq!(
            sections(&whole).keys().copied().collect::<Vec<_>>(),
            vec![4],
            "the hour keeps the cut it was written under"
        );
        std::fs::remove_dir_all(&whole_root).ok();
        std::fs::remove_dir_all(&batched_root).ok();
    }

    #[test]
    fn a_hour_compacted_again_takes_its_new_ticks_under_new_names() {
        let layout = SpineLayout {
            partitions: 4,
            retention_days: 7,
        };
        let (lake, root, at) = hour_of_ticks("again", layout);
        let working = lake.working().expect("working state");
        working.begin(&lake).expect("begin");
        compact(&lake, hour_of(at), 9.0).expect("compact");
        lake.end_unit(working.position()).expect("end");
        let first = read_manifest(&lake, hour_of(at)).expect("manifest");
        // A late tick into the compacted hour, then the hour compacted again.
        let mut behind = Event::new(Kind::Store, "i1", at + 9_999.0);
        behind.block_id = Some("b4".into());
        working.begin(&lake).expect("begin");
        let written = write_tick(&lake, layout, std::slice::from_ref(&behind)).expect("write");
        record_written(&lake, &written, 10.0).expect("record");
        compact(&lake, hour_of(at), 11.0).expect("compact");
        lake.end_unit(working.position()).expect("end");
        let second = read_manifest(&lake, hour_of(at)).expect("manifest");
        let (first, second) = (
            first.compacted.expect("compacted"),
            second.compacted.expect("compacted"),
        );
        assert_eq!((first.generation, second.generation), (0, 1));
        let first_keys: std::collections::BTreeSet<_> =
            first.keys(&lake, hour_of(at)).into_iter().collect();
        assert!(
            second
                .keys(&lake, hour_of(at))
                .iter()
                .all(|k| !first_keys.contains(k)),
            "no object the first compaction named is written over"
        );
        let b4 = &histories(&lake, at)[4];
        assert!(
            b4.iter().any(|e| e.contains("9999")),
            "the late store is in b4's history"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
