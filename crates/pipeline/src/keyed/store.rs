// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed operators' durable state (`docs/pipeline.md`): repeat
//! compute's frontiers, placement's seeds, and one head, as three relations of the
//! lineage's working state (`analyzer_lake::working`), so its checkpoints and rollback carry
//! them.
//!
//! The operators keep their state in memory; the store writes what each tick changed, as one
//! revision per relation inside one unit of work: the frontiers and seeds of the keys it
//! touched, and the head, which names the tick, each operator's edges
//! and the intake the two share (the source's position and the batches still owed). A unit that
//! does not end is rolled back to the newest checkpoint, and the head read back then names an
//! older tick than the one in memory: the driver reloads from the store and replays the log
//! from the head's position.
//!
//! Records are versioned by the tick that wrote them, so the newest write of a key wins.

use super::held::Batch;
use super::intake::Intake;
use super::placement::PlacementCompute;
use super::repeat::{IdentityKey, RepeatCompute};
use super::state::RamState;
use crate::fold_state::KeyedWriter;
use crate::rowbinary::{Incomplete, Reader, Writer};
use crate::Error;
use analyzer_analysis::classify::{Class, Frontier};
use analyzer_analysis::dup_cells::IdentityState;
use analyzer_analysis::events::IntervalKey;
use analyzer_analysis::intervals::{CloseReason, FoldSeed, IntervalSeed};
use analyzer_lake::keyed::Record;
use analyzer_lake::working::{Relation, Scope};

/// Repeat compute's frontiers, a record per key.
pub(crate) const KEYED_FRONTIER: &str = "keyed_frontier";
/// Placement's seeds, a record per identity.
pub(crate) const KEYED_SEEDS: &str = "keyed_seeds";
/// The roster's runs, a record per incarnation.
pub(crate) const KEYED_ROSTER: &str = "keyed_roster";
/// The head: the tick, each operator's edges, the intake.
pub(crate) const KEYED_HEAD: &str = "keyed_head";

fn relation(name: &'static str) -> Relation<'static> {
    Relation {
        name,
        scope: Scope::Lineage,
    }
}

/// The seed state placement restores into.
pub type Seeds = RamState<String, IdentityState>;

/// What the head record holds.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Head {
    tick: u64,
    publication: u64,
    semantics_version: String,
    discriminator: String,
    settled_through_day: Option<i64>,
    generations: Vec<Generation>,
    /// Each enabled public module's first settled day, from which its output is stated.
    module_spans: std::collections::BTreeMap<String, i64>,
    open_from_day: Option<i64>,
    /// The last batch's days for the settled-day hook, stated again on a replay.
    due: Vec<super::SettledDay>,
    classified_to_ms: Option<f64>,
    placed_to_ms: Option<f64>,
    emitted_from_ms: Option<f64>,
    emitted_to_ms: Option<f64>,
    covered_to_ms: Option<f64>,
    audit: super::audit::AuditEdge,
    roster_position: u64,
    tags_position: u64,
    coverage_position: u64,
    coverage_sources: std::collections::BTreeMap<String, super::StatedSource>,
    intake: Intake,
}

/// One meaning's settled days: the generation it classified them under. The generation seams
/// are stated from these ranges.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Generation {
    /// The semantics version.
    pub semantics_version: String,
    /// The discriminator.
    pub discriminator: String,
    /// The first day it settled.
    pub first_day: i64,
    /// The last day it settled.
    pub last_day: i64,
}

/// The keyed operators' state in the working state: where it last wrote.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Store {
    /// The tick the store last wrote, `0` before the first.
    tick: u64,
    /// The publication its batch goes out under.
    publication: u64,
}

/// The operators and their intake, as the store restored them.
pub struct Restored {
    /// Repeat compute.
    pub repeat: RepeatCompute,
    /// Placement.
    pub placement: PlacementCompute<Seeds>,
    /// Where the source stands for both.
    pub intake: Intake,
    /// Through where coverage is stated.
    pub covered_to_ms: Option<f64>,
    /// Where the sampled audit stands.
    pub audit: super::audit::AuditEdge,
    /// The roster.
    pub roster: super::roster::RosterFeed,
    /// The coverage feed.
    pub coverage: super::CoverageFeed,
    /// Each day of the blocks by stored mass, its summary's bytes.
    pub blocks: Vec<(i64, Vec<u8>)>,
    /// Each stated day of the whale register, its bytes.
    pub whales: Vec<(i64, Vec<u8>)>,
    /// The holders' rings.
    pub rings: Vec<(
        analyzer_analysis::intervals::Holder,
        Vec<analyzer_analysis::emitted::ClassifiedFact>,
    )>,
    /// The tag feed.
    pub tags: super::tags::TagFeed,
    /// The last day settled.
    pub settled_through_day: Option<i64>,
    /// Every meaning's settled days, this one's included.
    pub generations: Vec<Generation>,
    /// Each enabled public module's first settled day.
    pub module_spans: std::collections::BTreeMap<String, i64>,
    /// The first day still open to change: the held cells hold no day before it.
    pub open_from_day: Option<i64>,
}

/// What a save writes beside the operators' own changes.
pub(crate) struct Save<'a> {
    /// Where the source stands for the operators.
    pub(crate) intake: &'a Intake,
    /// The cells that moved, every family.
    pub(crate) batch: &'a Batch,
    /// The publication the batch goes out under.
    pub(crate) publication: u64,
    /// The first day still open to change: the held days before it leave.
    pub(crate) retire_before: Option<i64>,
    /// Through where coverage is stated.
    pub(crate) covered_to_ms: Option<f64>,
    /// Where the sampled audit stands.
    pub(crate) audit: super::audit::AuditEdge,
    /// The roster's changed runs, and the input position it read through.
    pub(crate) roster: (&'a [(String, analyzer_analysis::roster::Run)], u64),
    /// The sources coverage states and the input position its feed read through.
    pub(crate) coverage: &'a super::CoverageFeed,
    /// The declared tag intervals changed since the last save, and the input position the tag
    /// feed read through.
    pub(crate) tags: (&'a [(String, analyzer_analysis::tags::TagInterval)], u64),
    /// The holders' rings that changed, as they stand.
    pub(crate) rings: &'a [(
        analyzer_analysis::intervals::Holder,
        Vec<analyzer_analysis::emitted::ClassifiedFact>,
    )],
    /// Each day of the blocks by stored mass the batch drained, its summary's bytes.
    pub(crate) blocks: &'a [(i64, Vec<u8>)],
    /// Each whale day the batch drained, its bytes.
    pub(crate) whales: &'a [(i64, Vec<u8>)],
    /// The meaning the ticks classified under.
    pub(crate) meaning: &'a analyzer_analysis::emitted::Meaning,
    /// The last day settled.
    pub(crate) settled_through_day: Option<i64>,
    /// Every meaning's settled days.
    pub(crate) generations: &'a [Generation],
    /// Each enabled public module's first settled day.
    pub(crate) module_spans: &'a std::collections::BTreeMap<String, i64>,
    /// The log objects this tick wrote, for the manifest.
    pub(crate) spine: &'a super::spine::Written,
}

impl Store {
    /// Read the store's state back into `repeat` and `placement`, fresh operators configured as
    /// the ones that wrote it, with the intake they share. A store never written restores
    /// nothing.
    ///
    /// # Errors
    /// The working state failed, or a record does not decode.
    pub(crate) fn restore(
        lake: &analyzer_lake::Lake,
        meaning: &analyzer_analysis::emitted::Meaning,
        repeat: RepeatCompute,
        placement: PlacementCompute<Seeds>,
    ) -> Result<(Self, Restored), Error> {
        let head = read_head(lake)?;
        if head.as_ref().is_some_and(|h| {
            h.semantics_version != meaning.semantics_version
                || h.discriminator != meaning.discriminator
        }) {
            return Err(Error::IncompatibleState);
        }
        let Some(head) = head else {
            return Ok((
                Self::default(),
                Restored {
                    repeat,
                    placement,
                    intake: Intake::default(),
                    covered_to_ms: None,
                    audit: super::audit::AuditEdge::default(),
                    // Kept through a reset: read again from the log's start, idempotently.
                    roster: super::roster::RosterFeed::restored(read_runs(lake)?, 0),
                    coverage: super::CoverageFeed::default(),
                    blocks: Vec::new(),
                    whales: Vec::new(),
                    rings: Vec::new(),
                    tags: super::tags::TagFeed::restored(super::tags::read_all(lake)?, 0),
                    settled_through_day: None,
                    generations: read_carried(lake)?,
                    // A reset derives every module again from the log: no span is carried.
                    module_spans: std::collections::BTreeMap::new(),
                    open_from_day: None,
                },
            ));
        };
        let (mut frontiers, mut seeds) = (Vec::new(), Vec::new());
        let runs = read_runs(lake)?;
        for stripe in 0..analyzer_lake::stripe::STRIPES {
            for record in crate::fold_state::read(lake, &relation(KEYED_FRONTIER), stripe, None)? {
                frontiers.push((
                    identity_of(&record.key)?,
                    decode_frontiers(&record.payload)?,
                ));
            }
            for record in crate::fold_state::read(lake, &relation(KEYED_SEEDS), stripe, None)? {
                let identity = std::str::from_utf8(&record.key)
                    .map_err(|_| Error::Row("a keyed seeds key does not decode".into()))?
                    .to_string();
                let carried = decode_seeds(&identity, &record.payload)?;
                seeds.push((identity, carried));
            }
        }
        Ok((
            Self {
                tick: head.tick,
                publication: head.publication,
            },
            Restored {
                repeat: repeat.restored(frontiers, head.classified_to_ms),
                placement: placement.restored(
                    seeds,
                    (head.placed_to_ms, head.emitted_from_ms, head.emitted_to_ms),
                ),
                intake: head.intake,
                covered_to_ms: head.covered_to_ms,
                audit: head.audit,
                roster: super::roster::RosterFeed::restored(runs, head.roster_position),
                coverage: super::CoverageFeed::restored(
                    head.coverage_sources,
                    head.coverage_position,
                ),
                blocks: super::day_records::read_all(lake, super::blocks::KEYED_BLOCKS)?,
                whales: super::day_records::read_all(lake, super::whales::KEYED_WHALES)?,
                rings: super::ring::read_all(lake)?,
                tags: super::tags::TagFeed::restored(
                    super::tags::read_all(lake)?,
                    head.tags_position,
                ),
                settled_through_day: head.settled_through_day,
                generations: head.generations,
                module_spans: head.module_spans,
                open_from_day: head.open_from_day,
            },
        ))
    }

    /// The tick the store last wrote: how many saves its head reflects.
    #[must_use]
    pub fn tick(&self) -> u64 {
        self.tick
    }

    /// The publication the last written tick's batch goes out under, `0` before the first.
    #[must_use]
    pub fn publication(&self) -> u64 {
        self.publication
    }

    /// Each `(instance, incarnation)`'s model, from the roster the store holds: what a drill's
    /// re-derivation reads a store's model dimension from, as the driver does.
    ///
    /// # Errors
    /// The working state failed, or a run does not decode.
    pub fn models(lake: &analyzer_lake::Lake) -> Result<super::Models, Error> {
        Ok(super::RosterFeed::restored(read_runs(lake)?, 0)
            .cells()
            .models())
    }

    /// The last day the store's head records settled, without restoring anything: through
    /// where every public module's output is stated.
    ///
    /// # Errors
    /// The working state failed, or the head does not decode.
    pub fn settled_through_day(lake: &analyzer_lake::Lake) -> Result<Option<i64>, Error> {
        Ok(read_head(lake)?.and_then(|h| h.settled_through_day))
    }

    /// Each enabled public module's first settled day, as the last save wrote them.
    ///
    /// # Errors
    /// The working state failed, or the head does not decode.
    pub fn module_spans(
        lake: &analyzer_lake::Lake,
    ) -> Result<std::collections::BTreeMap<String, i64>, Error> {
        Ok(read_head(lake)?.map(|h| h.module_spans).unwrap_or_default())
    }

    /// The last written tick's batch, as it was sent: what a start publishes again under
    /// [`Self::publication`] when the tick's intent was never applied.
    ///
    /// # Errors
    /// The working state failed, or a record does not decode.
    pub fn outbox(&self, lake: &analyzer_lake::Lake) -> Result<Batch, Error> {
        let mut batch = super::held::read_outbox(lake)?;
        batch.days = read_head(lake)?.map(|h| h.due).unwrap_or_default();
        Ok(batch)
    }

    /// Whether the working state still holds what this store last wrote: false after a
    /// rollback returned it to an older tick, when the operator in memory is ahead of it.
    ///
    /// # Errors
    /// The working state failed, or the head does not decode.
    pub fn current(&self, lake: &analyzer_lake::Lake) -> Result<bool, Error> {
        Ok(read_head(lake)?.map_or(0, |h| h.tick) == self.tick)
    }

    /// Write what the operators changed since the last write and what `save` names: the
    /// batch into the held cells and the outbox, the held days before its retirement dropped,
    /// the roster's changed runs, and the head naming the intake and the publication. A
    /// revision per relation, the head's last, so a head is never read over state it does not
    /// name. The caller holds the working state's unit of work around it.
    ///
    /// # Errors
    /// The working state failed.
    pub(crate) fn write(
        &mut self,
        lake: &analyzer_lake::Lake,
        (repeat, placement): (&mut RepeatCompute, &mut PlacementCompute<Seeds>),
        save: &Save<'_>,
    ) -> Result<(), Error> {
        let tick = self.tick + 1;
        #[allow(clippy::cast_precision_loss)] // ticks stay far below 2^52
        let version = tick as f64;
        let changes = repeat.take_changes();
        put(
            lake,
            KEYED_FRONTIER,
            changes.frontiers.iter().map(|(key, held)| Record {
                hash: crate::spine::identity_hash(&key.0),
                key: key_bytes(key),
                version,
                gone: held.is_none(),
                payload: held.as_deref().map(encode_frontiers).unwrap_or_default(),
            }),
        )?;
        put(
            lake,
            KEYED_SEEDS,
            placement
                .take_changes()
                .into_iter()
                .map(|(identity, held)| Record {
                    hash: crate::spine::identity_hash(&identity),
                    key: identity.into_bytes(),
                    version,
                    gone: held.is_none(),
                    payload: held.as_ref().map(encode_seeds).unwrap_or_default(),
                }),
        )?;
        let (runs, roster_position) = save.roster;
        put(
            lake,
            KEYED_ROSTER,
            runs.iter().map(|(incarnation, run)| Record {
                hash: crate::spine::identity_hash(incarnation),
                key: incarnation.as_bytes().to_vec(),
                version,
                gone: false,
                payload: super::roster::encode_run(run),
            }),
        )?;
        write_batch(lake, save, version)?;
        let head = Head {
            tick,
            publication: save.publication,
            semantics_version: save.meaning.semantics_version.to_string(),
            discriminator: save.meaning.discriminator.clone(),
            settled_through_day: save.settled_through_day,
            generations: save.generations.to_vec(),
            module_spans: save.module_spans.clone(),
            open_from_day: save.retire_before,
            due: save.batch.days.clone(),
            classified_to_ms: repeat.classified_to_ms(),
            placed_to_ms: placement.data_to_ms(),
            covered_to_ms: save.covered_to_ms,
            audit: save.audit,
            emitted_from_ms: placement.emitted_from_ms(),
            emitted_to_ms: placement.emitted_to_ms(),
            roster_position,
            tags_position: save.tags.1,
            coverage_position: save.coverage.position(),
            coverage_sources: save.coverage.sources().clone(),
            intake: save.intake.clone(),
        };
        put(
            lake,
            KEYED_HEAD,
            [Record {
                hash: 0,
                key: HEAD_KEY.to_vec(),
                version,
                gone: false,
                payload: serde_json::to_vec(&head)
                    .map_err(|e| Error::Row(format!("keyed head: {e}")))?,
            }],
        )?;
        self.tick = tick;
        self.publication = save.publication;
        Ok(())
    }
}

/// What a save writes for its batch: each family's held cells, the declared tags, the outbox,
/// the held days before the retirement dropped, the blocks' summaries and the rings.
fn write_batch(lake: &analyzer_lake::Lake, save: &Save<'_>, version: f64) -> Result<(), Error> {
    let batch = save.batch;
    super::held::write_cells::<analyzer_analysis::emitted::Cells>(lake, &batch.classes, version)?;
    super::held::write_cells::<analyzer_analysis::dup_cells::DupCells>(
        lake,
        &batch.placement,
        version,
    )?;
    super::held::write_cells::<analyzer_analysis::coverage_sums::CoverageSums>(
        lake,
        &batch.coverage,
        version,
    )?;
    super::held::write_cells::<analyzer_analysis::tags::TagCells>(lake, &batch.tags, version)?;
    super::held::write_cells::<analyzer_analysis::restated::Restated>(
        lake,
        &batch.restated,
        version,
    )?;
    super::held::write_cells::<analyzer_analysis::instance_spans::InstanceSpans>(
        lake,
        &batch.spans,
        version,
    )?;
    super::tags::write(lake, save.tags.0, version)?;
    super::held::write_outbox(lake, batch, version)?;
    super::spine::record_written(lake, save.spine, version)?;
    if let Some(day) = save.retire_before {
        super::held::drop_before(lake, day)?;
    }
    super::day_records::write(
        lake,
        super::blocks::KEYED_BLOCKS,
        save.blocks,
        save.retire_before,
        version,
    )?;
    super::day_records::write(
        lake,
        super::whales::KEYED_WHALES,
        save.whales,
        save.retire_before,
        version,
    )?;
    super::ring::write(lake, save.rings, version)?;
    Ok(())
}

const HEAD_KEY: &[u8] = b"head";
/// The head's key for the generations a reset carries into the next meaning's state.
const CARRIED_KEY: &[u8] = b"carried";

impl Store {
    /// Start a fresh state for another meaning: every relation of the keyed state dropped but
    /// the roster's runs and the tag declarations, which no classification parameter touches,
    /// and the log partitioned by block deleted, since the next meaning's re-derivation writes
    /// it again from the input log. The generations are carried, so the seams stay stated.
    ///
    /// # Errors
    /// The working state or the store failed.
    pub fn reset(lake: &analyzer_lake::Lake) -> Result<(), Error> {
        let mut carried = read_carried(lake)?;
        if let Some(head) = read_head(lake)? {
            carried = head.generations;
        }
        let working = lake.working()?;
        for name in [
            KEYED_FRONTIER,
            KEYED_SEEDS,
            KEYED_HEAD,
            super::held::KEYED_OUTBOX,
            super::blocks::KEYED_BLOCKS,
            super::whales::KEYED_WHALES,
            super::ring::KEYED_RING,
            super::spine::KEYED_SPINE,
        ] {
            working.drop_relation(&relation(name))?;
        }
        for day in working.days(super::held::KEYED_CELLS)? {
            working.drop_relation(&Relation {
                name: super::held::KEYED_CELLS,
                scope: Scope::Day(day),
            })?;
        }
        for object in lake.store().list(&format!("{}keyed-spine/", lake.root()))? {
            lake.store().delete(&object.key)?;
        }
        put(
            lake,
            KEYED_HEAD,
            [Record {
                hash: 0,
                key: CARRIED_KEY.to_vec(),
                version: 0.0,
                gone: false,
                payload: serde_json::to_vec(&carried)
                    .map_err(|e| Error::Row(format!("keyed carried generations: {e}")))?,
            }],
        )
    }
}

/// The roster's runs.
fn read_runs(
    lake: &analyzer_lake::Lake,
) -> Result<std::collections::BTreeMap<String, analyzer_analysis::roster::Run>, Error> {
    let mut runs = std::collections::BTreeMap::new();
    for stripe in 0..analyzer_lake::stripe::STRIPES {
        for record in crate::fold_state::read(lake, &relation(KEYED_ROSTER), stripe, None)? {
            let incarnation = std::str::from_utf8(&record.key)
                .map_err(|_| Error::Row("a keyed roster key does not decode".into()))?
                .to_string();
            runs.insert(incarnation, super::roster::decode_run(&record.payload)?);
        }
    }
    Ok(runs)
}

/// The generations a reset carried, none before the first reset.
fn read_carried(lake: &analyzer_lake::Lake) -> Result<Vec<Generation>, Error> {
    let records = crate::fold_state::read(lake, &relation(KEYED_HEAD), 0, Some(&[0]))?;
    records
        .iter()
        .find(|r| r.key == CARRIED_KEY)
        .map_or(Ok(Vec::new()), |r| {
            serde_json::from_slice(&r.payload)
                .map_err(|e| Error::Row(format!("keyed carried generations: {e}")))
        })
}

/// One revision of the lineage relation `name`: `records`, each in its hash's stripe.
fn put(
    lake: &analyzer_lake::Lake,
    name: &'static str,
    records: impl IntoIterator<Item = Record>,
) -> Result<(), Error> {
    let writer = KeyedWriter::open(lake, relation(name), false)?;
    writer.put(records.into_iter().map(placed).collect())?;
    writer.commit()
}

fn read_head(lake: &analyzer_lake::Lake) -> Result<Option<Head>, Error> {
    let records = crate::fold_state::read(lake, &relation(KEYED_HEAD), 0, Some(&[0]))?;
    records
        .iter()
        .find(|r| r.key == HEAD_KEY)
        .map(|r| {
            serde_json::from_slice(&r.payload).map_err(|e| Error::Row(format!("keyed head: {e}")))
        })
        .transpose()
}

/// A record with the stripe its hash falls in.
fn placed(record: Record) -> (usize, Record) {
    (analyzer_lake::stripe::of(record.hash), record)
}

/// A key's bytes: the block, a separator no block carries, and the epoch when there is one.
fn key_bytes((block, epoch): &IdentityKey) -> Vec<u8> {
    let mut key = Vec::with_capacity(block.len() + 5);
    key.extend_from_slice(block.as_bytes());
    key.push(0);
    if let Some(epoch) = epoch {
        key.extend_from_slice(&epoch.to_le_bytes());
    }
    key
}

fn identity_of(key: &[u8]) -> Result<IdentityKey, Error> {
    let bad = || Error::Row("a keyed frontier key does not decode".into());
    let at = key.iter().position(|b| *b == 0).ok_or_else(bad)?;
    let block = std::str::from_utf8(&key[..at])
        .map_err(|_| bad())?
        .to_string();
    let epoch = match &key[at + 1..] {
        [] => None,
        rest => Some(u32::from_le_bytes(rest.try_into().map_err(|_| bad())?)),
    };
    Ok((block, epoch))
}

/// A key's frontiers as bytes.
fn encode_frontiers(frontiers: &[Frontier]) -> Vec<u8> {
    let mut w = Writer::default();
    write_frontiers(&mut w, frontiers);
    w.bytes
}

/// A key's frontiers onto `w`: their count, then each row.
pub(super) fn write_frontiers(w: &mut Writer, frontiers: &[Frontier]) {
    w.varint(frontiers.len() as u64);
    for f in frontiers {
        w.string(&f.key.instance);
        w.u32(f.key.dp_rank);
        w.u32(f.key.group_idx);
        w.string(&f.key.identity);
        w.nullable(f.key.key_epoch, Writer::u32);
        w.nullable(f.key.tier.as_deref(), Writer::string);
        w.nullable(f.key.backend_id.as_deref(), Writer::string);
        w.nullable(f.last_close_ms, Writer::f64);
        w.nullable(f.last_close_reason.map(CloseReason::as_str), Writer::string);
        w.u8(u8::from(f.open));
        w.nullable(f.open_since_ms, Writer::f64);
        w.u64(f.open_readmits);
        w.nullable(f.open_workload_class.as_deref(), Writer::string);
        w.nullable(
            f.open_opener_class.as_ref().map(Class::as_str),
            Writer::string,
        );
    }
}

/// A key's frontiers from `r`, as [`write_frontiers`] wrote them.
pub(super) fn read_frontiers(r: &mut Reader<'_>) -> Result<Vec<Frontier>, Incomplete> {
    let owned = |r: &mut Reader<'_>| r.string().map(str::to_string);
    let n = r.varint()?;
    (0..n)
        .map(|_| {
            Ok(Frontier {
                key: IntervalKey {
                    instance: owned(r)?,
                    dp_rank: r.u32()?,
                    group_idx: r.u32()?,
                    identity: owned(r)?,
                    key_epoch: r.nullable(Reader::u32)?,
                    tier: r.nullable(owned)?,
                    backend_id: r.nullable(owned)?,
                },
                last_close_ms: r.nullable(Reader::f64)?,
                last_close_reason: r.nullable(owned)?.as_deref().and_then(CloseReason::parse),
                open: r.u8()? != 0,
                open_since_ms: r.nullable(Reader::f64)?,
                open_readmits: r.u64()?,
                open_workload_class: r.nullable(owned)?,
                open_opener_class: r.nullable(owned)?.as_deref().and_then(Class::parse),
            })
        })
        .collect()
}

fn decode_frontiers(bytes: &[u8]) -> Result<Vec<Frontier>, Error> {
    let mut r = Reader::new(bytes);
    let decoded = read_frontiers(&mut r)
        .map_err(|_| Error::Row("a keyed frontier record is truncated".into()))?;
    if !r.at_end() {
        return Err(Error::Row(
            "a keyed frontier record has trailing bytes".into(),
        ));
    }
    Ok(decoded)
}

/// An identity's placement seeds as bytes: its mass, then each interval key's seed.
fn encode_seeds(held: &IdentityState) -> Vec<u8> {
    let mut w = Writer::default();
    w.i64(held.n_tokens);
    w.varint(held.seeds.len() as u64);
    for (key, seed) in &held.seeds {
        w.string(&key.instance);
        w.u32(key.dp_rank);
        w.u32(key.group_idx);
        w.nullable(key.key_epoch, Writer::u32);
        w.nullable(key.tier.as_deref(), Writer::string);
        w.nullable(key.backend_id.as_deref(), Writer::string);
        w.nullable(seed.open.as_ref(), |w, open| {
            w.nullable(open.open_since_ms, Writer::f64);
            w.u64(open.readmits);
            w.nullable(open.workload_class.as_deref(), Writer::string);
        });
        w.nullable(seed.last_close, |w, (at, reason)| {
            w.f64(at);
            w.string(reason.as_str());
        });
    }
    w.nullable(held.whale.through_ms, Writer::f64);
    w.varint(held.whale.days.len() as u64);
    for (day, sum) in &held.whale.days {
        w.i64(*day);
        w.i64(sum.n_tokens);
        w.f64(sum.dup_token_ms);
        w.f64(sum.held_token_ms);
    }
    w.bytes
}

fn decode_seeds(identity: &str, bytes: &[u8]) -> Result<IdentityState, Error> {
    let mut r = Reader::new(bytes);
    let owned = |r: &mut Reader<'_>| r.string().map(str::to_string);
    let decoded = (|| -> Result<Option<IdentityState>, Incomplete> {
        let n_tokens = r.i64()?;
        let n = r.varint()?;
        let mut seeds = std::collections::BTreeMap::new();
        for _ in 0..n {
            let key = IntervalKey {
                instance: owned(&mut r)?,
                dp_rank: r.u32()?,
                group_idx: r.u32()?,
                identity: identity.to_string(),
                key_epoch: r.nullable(Reader::u32)?,
                tier: r.nullable(owned)?,
                backend_id: r.nullable(owned)?,
            };
            let open = r.nullable(|r| {
                Ok(FoldSeed {
                    open_since_ms: r.nullable(Reader::f64)?,
                    readmits: r.u64()?,
                    workload_class: r.nullable(owned)?,
                })
            })?;
            let last_close = r.nullable(|r| Ok((r.f64()?, owned(r)?)))?;
            let last_close = match last_close {
                Some((at, reason)) => match CloseReason::parse(&reason) {
                    Some(reason) => Some((at, reason)),
                    None => return Ok(None),
                },
                None => None,
            };
            seeds.insert(key, IntervalSeed { open, last_close });
        }
        let through_ms = r.nullable(Reader::f64)?;
        let mut days = std::collections::BTreeMap::new();
        for _ in 0..r.varint()? {
            let day = r.i64()?;
            days.insert(
                day,
                analyzer_analysis::dup_cells::WhaleSum {
                    n_tokens: r.i64()?,
                    dup_token_ms: r.f64()?,
                    held_token_ms: r.f64()?,
                },
            );
        }
        Ok(Some(IdentityState {
            seeds,
            n_tokens,
            whale: analyzer_analysis::dup_cells::WhaleAccrual { through_ms, days },
        }))
    })()
    .map_err(|_| Error::Row("a keyed seeds record is truncated".into()))?
    .ok_or_else(|| Error::Row("a keyed seeds record names an unknown close reason".into()))?;
    if !r.at_end() {
        return Err(Error::Row("a keyed seeds record has trailing bytes".into()));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{decode_frontiers, encode_frontiers, identity_of, key_bytes};
    use analyzer_analysis::classify::{Class, Frontier};
    use analyzer_analysis::events::IntervalKey;
    use analyzer_analysis::intervals::CloseReason;

    #[test]
    fn keys_and_frontiers_read_back_as_written() {
        for key in [("b".to_string(), None), ("block-7".to_string(), Some(3))] {
            assert_eq!(identity_of(&key_bytes(&key)).expect("decodes"), key);
        }
        let frontiers = vec![
            Frontier {
                key: IntervalKey {
                    instance: "i1".into(),
                    dp_rank: 1,
                    group_idx: 2,
                    identity: "b".into(),
                    key_epoch: Some(4),
                    tier: Some("GPU".into()),
                    backend_id: None,
                },
                last_close_ms: Some(12.5),
                last_close_reason: Some(CloseReason::Clear),
                open: false,
                open_since_ms: None,
                open_readmits: 0,
                open_workload_class: None,
                open_opener_class: None,
            },
            Frontier {
                key: IntervalKey {
                    instance: "i2".into(),
                    dp_rank: 0,
                    group_idx: 0,
                    identity: "b".into(),
                    key_epoch: Some(4),
                    tier: None,
                    backend_id: Some("x".into()),
                },
                last_close_ms: None,
                last_close_reason: None,
                open: true,
                open_since_ms: Some(3.0),
                open_readmits: 7,
                open_workload_class: Some("agentic".into()),
                open_opener_class: Some(Class::Cold),
            },
        ];
        assert_eq!(
            decode_frontiers(&encode_frontiers(&frontiers)).expect("decodes"),
            frontiers
        );
    }
}
