// SPDX-License-Identifier: AGPL-3.0-only

//! An input log for the keyed operators' engine-free tests: a local spine whose input log is
//! committed directly, one run's segments at a time, with a map of batch to records standing in
//! for the segments' objects.

#![allow(dead_code)] // each test binary uses its own part

use analyzer_analysis::events::{Event, Kind};
use analyzer_analysis::DAY_MS;
use analyzer_pipeline::keyed::{Delivery, SegmentReader};
use analyzer_pipeline::ledger::{BatchRecord, SegmentArrival, SHARD};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

/// A local spine in a scratch directory, removed on drop.
pub struct Log {
    pub spine: analyzer_pipeline::Spine,
    pub lake: analyzer_pipeline::Lake,
    dir: std::path::PathBuf,
}

impl Drop for Log {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

impl Log {
    #[must_use]
    pub fn new(name: &str) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "keyed-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let namespace =
            analyzer_pipeline::execution::Namespace::new("test", "f").expect("namespace");
        let spine = analyzer_pipeline::spine::local(&dir, &namespace).expect("a local spine");
        let lake = spine.lake();
        Self { spine, lake, dir }
    }

    /// Commit `events` as run r1's segment `seq`, and hold its records in `chunks`.
    pub fn commit(&self, chunks: &Chunks, seq: i64, events: Vec<Event>) {
        self.commit_run(chunks, ("r1", "i0", ""), seq, events);
    }

    /// Commit a declarer's segment `seq` of `run` carrying `tags` and no cache fact.
    pub fn commit_tags(
        &self,
        run: &str,
        seq: i64,
        tags: Vec<analyzer_pipeline::ledger::DeclaredTag>,
    ) {
        let batch = BatchRecord {
            batch_id: format!("{run}-{seq}"),
            carrier: "segment".into(),
            source: "fixture".into(),
            object_key: String::new(),
            etag: String::new(),
            input_position: 0,
            ingested_at_ms: 1,
            record_count: tags.len() as u64,
            unknown_kinds: 0,
            first_at_ms: None,
            last_at_ms: None,
            days: Vec::new(),
            segments: Vec::new(),
            drops: Vec::new(),
            lifecycle: Vec::new(),
            stored: Vec::new(),
            annotations: Vec::new(),
            tags,
        };
        let ledger = self.lake.ledger(SHARD).expect("input log");
        ledger
            .commit_cycle(ledger.next_position().expect("position"), vec![batch])
            .expect("commit");
    }

    /// Commit `events` as segment `seq` of `run` on `instance`, declaring `model`, and hold its
    /// records in `chunks`.
    pub fn commit_run(
        &self,
        chunks: &Chunks,
        (run, instance, model): (&str, &str, &str),
        seq: i64,
        events: Vec<Event>,
    ) {
        self.commit_dropping(chunks, (run, instance, model), seq, events, 0);
    }

    /// [`Self::commit_run`], the segment declaring `dropped` records lost at the producer.
    pub fn commit_dropping(
        &self,
        chunks: &Chunks,
        (run, instance, model): (&str, &str, &str),
        seq: i64,
        events: Vec<Event>,
        dropped: u64,
    ) {
        let first = events.first().map(|e| e.at_ms);
        let last = events.last().map(|e| e.at_ms);
        let id = format!("{run}-{seq}");
        let batch = BatchRecord {
            batch_id: id.clone(),
            carrier: "segment".into(),
            source: "fixture".into(),
            object_key: String::new(),
            etag: String::new(),
            input_position: 0,
            ingested_at_ms: 1,
            record_count: events.len() as u64,
            unknown_kinds: 0,
            first_at_ms: first,
            last_at_ms: last,
            days: Vec::new(),
            segments: vec![SegmentArrival {
                incarnation: run.into(),
                segment_seq: seq,
                instance: Some(instance.into()),
                first_record_at_ms: first.unwrap_or_default(),
                last_record_at_ms: last.unwrap_or_default(),
                heartbeat_secs: 60,
                max_segment_secs: 300,
                hb_count: 1,
                hb_first_at_ms: first,
                hb_last_at_ms: last,
                stop_at_ms: None,
                msgs_seen: 1,
                dropped,
                oversized: 0,
                dropped_at_hb_first: 0,
                oversized_at_hb_first: 0,
                publisher_restarts: 0,
                agent_version: Some("t".into()),
                topic_model: None,
                topic_pod: None,
                declared_model: (!model.is_empty()).then(|| model.to_string()),
                declared_pod: None,
                engine_version: None,
                attributed: true,
                canary: None,
            }],
            drops: Vec::new(),
            lifecycle: Vec::new(),
            stored: Vec::new(),
            annotations: Vec::new(),
            tags: Vec::new(),
        };
        chunks.0.lock().expect("chunks").insert(id, events);
        let ledger = self.lake.ledger(SHARD).expect("input log");
        ledger
            .commit_cycle(ledger.next_position().expect("position"), vec![batch])
            .expect("commit");
    }
}

/// The records by batch: what the segments' objects hold.
#[derive(Default)]
pub struct Chunks(pub Mutex<BTreeMap<String, Vec<Event>>>);

impl SegmentReader for Chunks {
    fn read(&self, delivery: &Delivery) -> Result<Vec<Event>, analyzer_pipeline::Error> {
        Ok(self
            .0
            .lock()
            .expect("chunks")
            .get(&delivery.batch_id)
            .cloned()
            .unwrap_or_default())
    }
}

/// `hours` of three instances storing and evicting `blocks` shared blocks.
#[must_use]
/// The operators every keyed suite drives: two lanes each, repeat compute watermarked at the
/// skew allowance and placement at half of it.
pub fn operators(
    params: &analyzer_analysis::Params,
) -> (
    analyzer_pipeline::keyed::RepeatCompute,
    analyzer_pipeline::keyed::PlacementCompute<analyzer_pipeline::keyed::Seeds>,
) {
    use analyzer_pipeline::keyed::{Emission, PlacementCompute, RamState, RepeatCompute};
    let skew = params.delta_skew_ms;
    (
        RepeatCompute::new(2, Emission::Watermarked { delay_ms: skew }),
        PlacementCompute::new(
            2,
            Emission::Watermarked {
                delay_ms: skew / 2.0,
            },
            RamState::default,
        ),
    )
}

/// A driver over `lake` on the suite's [`operators`], with its store.
///
/// # Errors
/// The working state or the store did not open.
pub fn open_driver(
    lake: &analyzer_pipeline::Lake,
    params: &analyzer_analysis::Params,
) -> Result<
    (
        analyzer_pipeline::keyed::Driver<analyzer_pipeline::keyed::Seeds>,
        analyzer_pipeline::keyed::Store,
    ),
    analyzer_pipeline::Error,
> {
    let (repeat, placement) = operators(params);
    analyzer_pipeline::keyed::Driver::open(lake, params, repeat, placement)
}

/// The suite's deterministic generator: xorshift64, seeded by the caller.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub fn stream(hours: f64, blocks: u64) -> Vec<Event> {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut events = Vec::new();
    let mut open: BTreeSet<(String, String)> = BTreeSet::new();
    let mut at = DAY_MS * 100.0;
    let end = at + hours * 3_600_000.0;
    while at < end {
        at += f64::from(u32::try_from(rng.below(300) + 1).expect("small")) * 100.0;
        let instance = format!("i{}", rng.below(3));
        let block = format!("b{}", rng.below(blocks));
        let key = (instance.clone(), block.clone());
        let kind = if open.remove(&key) {
            Kind::Evict
        } else {
            open.insert(key);
            Kind::Store
        };
        let mut e = Event::new(kind, &instance, at);
        e.dp_rank = Some(0);
        e.group_idx = Some(0);
        e.block_id = Some(block);
        e.n_tokens = 16;
        e.incarnation = "r1".into();
        e.workload_class = Some("agentic".into());
        events.push(e);
    }
    events
}

/// The stream cut into five-minute segments.
#[must_use]
pub fn segments(events: &[Event]) -> Vec<Vec<Event>> {
    let segment = 300_000.0;
    let mut out = Vec::new();
    let mut at = 0;
    let mut edge = (events[0].at_ms / segment).floor() * segment + segment;
    while at < events.len() {
        let upto = events[at..].partition_point(|e| e.at_ms < edge) + at;
        out.push(events[at..upto].to_vec());
        at = upto;
        edge += segment;
    }
    out
}
