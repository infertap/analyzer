// SPDX-License-Identifier: AGPL-3.0-only

//! Where a lineage derives: the object store, the namespace, the lineage, and the lineage's
//! lake (its journal, its input log, its published state and its working state), with the
//! identity every keyed relation stripes by and the row codec the log partitioned by block
//! (`crate::keyed::spine`) writes in.

use crate::Error;
use analyzer_analysis::events::{Event, Kind};
use analyzer_objectstore::ObjectStore;
use std::sync::Arc;

/// The stripe an identity falls in.
#[must_use]
pub fn stripe_of(identity: Option<&str>) -> usize {
    identity.map_or(0, |id| analyzer_lake::stripe::of(identity_hash(id)))
}

/// Where a lineage derives.
#[derive(Clone)]
pub struct Spine {
    store: Arc<dyn ObjectStore + Send + Sync>,
    /// Logical owner, validated before an executor reads or writes this spine.
    namespace: crate::execution::Namespace,
    /// The derivation this spine's journal records, and the writer instance.
    lineage: crate::lineage::Lineage,
    /// The lineage's lake, built once so its readers share one cache of the input log.
    lake: analyzer_lake::Lake,
}

impl std::fmt::Debug for Spine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spine")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl Spine {
    /// A whole-fleet spine with portable execution-namespace object keys. The store and its
    /// objects must survive worker replacement.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore + Send + Sync>,
        key_prefix: &str,
        namespace: &crate::execution::Namespace,
        lineage: crate::lineage::Lineage,
    ) -> Self {
        let lake = analyzer_lake::Lake::new(
            Arc::clone(&store),
            format!(
                "{key_prefix}{}{}{}",
                analyzer_lake::dated::ANALYZER_ROOT,
                namespace.object_prefix(),
                lineage.object_prefix()
            ),
            lineage.clone(),
        );
        Self {
            store,
            namespace: namespace.clone(),
            lineage,
            lake,
        }
    }

    /// The lineage's lake: its journal and input log live under one root,
    /// `<prefix>_analyzer/namespaces/<ns>/lineages/<id>/`.
    #[must_use]
    pub fn lake(&self) -> analyzer_lake::Lake {
        self.lake.clone()
    }

    /// The lineage whose execution journal this spine's writers commit.
    #[must_use]
    pub fn lineage(&self) -> &crate::lineage::Lineage {
        &self.lineage
    }

    /// The namespace this spine belongs to.
    #[must_use]
    pub fn namespace(&self) -> &crate::execution::Namespace {
        &self.namespace
    }

    pub(crate) fn require_namespace(
        &self,
        namespace: &crate::execution::Namespace,
    ) -> Result<(), Error> {
        if &self.namespace == namespace {
            Ok(())
        } else {
            Err(Error::IncompatibleState)
        }
    }

    /// This spine's lake reading its immutable objects through a local cache in `dir`, holding
    /// at most `bytes`: what a daemon on a bucket gives it, so a tick fetches only what
    /// changed.
    ///
    /// # Errors
    /// The directory cannot be made.
    pub fn with_object_cache(mut self, dir: &std::path::Path, bytes: u64) -> Result<Self, Error> {
        let cache = analyzer_lake::cache::ObjectCache::new(dir, bytes)?;
        self.lake = self.lake.with_cache(Arc::new(cache));
        Ok(self)
    }

    /// This spine's lake keeping the keyed operators' relations in its working state at `dir`
    /// (under a data directory, [`WORKING_STATE`]), rebuilt from the lineage's newest
    /// checkpoint (`docs/lake.md`). A spine that derives needs one; an
    /// ingest-only spine does not. `memory_bytes` bounds it whole: its cache, its index and
    /// filter blocks, and its memtables.
    ///
    /// # Errors
    /// The local disk, the store or the database failed.
    pub fn with_working_state(
        mut self,
        dir: &std::path::Path,
        memory_bytes: usize,
    ) -> Result<Self, Error> {
        let working = analyzer_lake::working::WorkingState::shared(
            dir,
            &self.lake,
            &crate::fold_state::DECLARED,
            memory_bytes,
        )?;
        self.lake = self.lake.with_working(working);
        Ok(self)
    }

    /// The store the spine writes to and reads from.
    #[must_use]
    pub fn store(&self) -> &dyn ObjectStore {
        self.store.as_ref()
    }
}

/// The working state's directory under a data directory: the daemon's, a local spine's.
pub const WORKING_STATE: &str = "working-state";

/// A spine in a local directory: the lineage's identity at `dir`, the journal and the input log
/// in `dir/store`, and the working state in [`WORKING_STATE`]. What a deployment without a
/// bucket runs on (the CLI's one-shot commands, the corpora); the daemon's spine is the bucket
/// through its port.
///
/// # Errors
/// The directory cannot be made, or the lineage's identity cannot be read or recorded.
pub fn local(
    dir: &std::path::Path,
    namespace: &crate::execution::Namespace,
) -> Result<Spine, Error> {
    std::fs::create_dir_all(dir)
        .map_err(|e| Error::Spine(format!("spine: {}: {e}", dir.display())))?;
    let spine = Spine::new(
        Arc::new(analyzer_objectstore::DirStore::new(dir.join("store"))),
        "",
        namespace,
        crate::lineage::Lineage::of_data_dir(dir)?,
    );
    spine.with_working_state(
        &dir.join(WORKING_STATE),
        analyzer_lake::working::DEFAULT_MEMORY_BYTES,
    )
}

/// The identity a row groups under: the content id, else the block.
#[must_use]
pub fn identity_of(ev: &Event) -> Option<&str> {
    ev.content_id.as_deref().or(ev.block_id.as_deref())
}

/// An identity's hash, the lake's key hash over its bytes: what a lane, a partition and a
/// stripe are chosen by.
#[must_use]
pub(crate) fn identity_hash(identity: &str) -> u64 {
    analyzer_lake::stripe::key_hash(identity.as_bytes())
}

/// One record in the log's wire order, [`decode_row`]'s columns.
pub(crate) fn encode_row(w: &mut crate::rowbinary::Writer, ev: &Event) {
    w.string(ev.kind.as_str());
    w.string(&ev.instance);
    w.nullable(ev.dp_rank, crate::rowbinary::Writer::u32);
    w.nullable(ev.group_idx, crate::rowbinary::Writer::u32);
    w.nullable(ev.block_id.as_deref(), crate::rowbinary::Writer::string);
    w.nullable(ev.content_id.as_deref(), crate::rowbinary::Writer::string);
    w.nullable(ev.key_epoch, crate::rowbinary::Writer::u32);
    w.nullable(ev.tier.as_deref(), crate::rowbinary::Writer::string);
    w.nullable(ev.backend_id.as_deref(), crate::rowbinary::Writer::string);
    w.f64(ev.at_ms);
    w.nullable(ev.locality.as_deref(), crate::rowbinary::Writer::string);
    w.i64(ev.n_tokens);
    w.string(&ev.incarnation);
    w.nullable(
        ev.workload_class.as_deref(),
        crate::rowbinary::Writer::string,
    );
    w.nullable(ev.parent_id.as_deref(), crate::rowbinary::Writer::string);
    w.u8(u8::from(ev.parent_unknown));
}

/// One record off the wire, [`encode_row`]'s columns. `Incomplete` is the section's end, never
/// a value; a kind the reader does not model is an error.
pub(crate) fn decode_row(
    r: &mut crate::rowbinary::Reader<'_>,
) -> Result<Result<Event, Error>, crate::rowbinary::Incomplete> {
    let kind = r.string()?;
    let instance = r.string()?;
    let dp_rank = r.nullable(crate::rowbinary::Reader::u32)?;
    let group_idx = r.nullable(crate::rowbinary::Reader::u32)?;
    let block_id = r.nullable(crate::rowbinary::Reader::string)?;
    let content_id = r.nullable(crate::rowbinary::Reader::string)?;
    let key_epoch = r.nullable(crate::rowbinary::Reader::u32)?;
    let tier = r.nullable(crate::rowbinary::Reader::string)?;
    let backend_id = r.nullable(crate::rowbinary::Reader::string)?;
    let at_ms = r.f64()?;
    let locality = r.nullable(crate::rowbinary::Reader::string)?;
    let n_tokens = r.i64()?;
    let incarnation = r.string()?;
    let workload_class = r.nullable(crate::rowbinary::Reader::string)?;
    let parent_id = r.nullable(crate::rowbinary::Reader::string)?;
    let parent_unknown = r.u8()?;
    let Some(kind) = Kind::parse(kind) else {
        return Ok(Err(Error::Spine(format!(
            "an unknown kind {kind:?} in a log row"
        ))));
    };
    let mut event = Event::new(kind, instance.to_string(), at_ms);
    event.dp_rank = dp_rank;
    event.group_idx = group_idx;
    event.block_id = block_id.map(str::to_string);
    event.content_id = content_id.map(str::to_string);
    event.key_epoch = key_epoch;
    event.tier = tier.map(str::to_string);
    event.backend_id = backend_id.map(str::to_string);
    event.locality = locality.map(str::to_string);
    event.parent_id = parent_id.map(str::to_string);
    event.parent_unknown = parent_unknown == 1;
    event.n_tokens = n_tokens;
    event.delivery_id = None;
    event.incarnation = incarnation.to_string();
    event.seq = None;
    event.reused = false;
    event.workload_class = workload_class.map(str::to_string);
    Ok(Ok(event))
}
