// SPDX-License-Identifier: AGPL-3.0-only

//! Fault injection for the execution journal: a store that refuses the head writes a test
//! names, and the spine, serving store and executor a suite drives it through.

pub use analyzer_objectstore::testing::FaultyStore;
use std::sync::Arc;

/// The serving store behind `engine`'s database, as the core's port.
pub fn serving(
    engine: &analyzer_serving_clickhouse::engine::Engine,
) -> std::sync::Arc<dyn analyzer_pipeline::publish::Serving> {
    std::sync::Arc::new(
        analyzer_serving_clickhouse::serving::ClickHouse::new(engine).expect("serving connection"),
    )
}

/// The namespace `engine`'s database is bound to, which must own `fleet`.
pub fn namespace(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> analyzer_pipeline::execution::Namespace {
    let namespace = serving(engine)
        .bound()
        .expect("read the binding")
        .expect("a namespace is bound");
    assert_eq!(
        namespace.fleet(),
        fleet,
        "the database is bound to another fleet"
    );
    namespace
}

/// Where a test database's spine lives: one directory per database on the shared server.
pub fn spine_dir(engine: &analyzer_serving_clickhouse::engine::Engine) -> std::path::PathBuf {
    let name: String = engine
        .identity()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    engine.data_dir().join("spine").join(name)
}

/// The spine of `engine`'s database for its bound namespace: the lake the harness ingests
/// into and every executor derives in.
pub fn spine(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> std::sync::Arc<analyzer_pipeline::Spine> {
    std::sync::Arc::new(
        analyzer_pipeline::spine::local(&spine_dir(engine), &namespace(engine, fleet))
            .expect("local spine"),
    )
}

/// The spine of `engine`'s database for `fleet`.
///
/// # Errors
/// The refusal an executor meets: no namespace bound, or one owning another fleet.
pub fn bound_spine(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> Result<std::sync::Arc<analyzer_pipeline::Spine>, analyzer_pipeline::Error> {
    let namespace = serving(engine).bound()?.ok_or_else(|| {
        analyzer_pipeline::Error::Derivation("no namespace is bound to the database".into())
    })?;
    if namespace.fleet() != fleet {
        return Err(analyzer_pipeline::Error::IncompatibleState);
    }
    Ok(std::sync::Arc::new(analyzer_pipeline::spine::local(
        &spine_dir(engine),
        &namespace,
    )?))
}

/// An executor over `engine`'s database: its spine, its serving store.
///
/// # Errors
/// The spine or the serving store did not open.
pub fn executor(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
    params: &analyzer_analysis::Params,
) -> Result<analyzer_pipeline::Executor<'static>, analyzer_pipeline::Error> {
    analyzer_pipeline::Executor::open(bound_spine(engine, fleet)?, params, serving(engine))
}

/// The durable consistency revision of `engine`'s database.
///
/// # Errors
/// The serving store did not answer.
pub fn revision(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> Result<Option<u64>, analyzer_pipeline::Error> {
    analyzer_pipeline::execution::revision(&*serving(engine), &*bound_spine(engine, fleet)?)
}

/// A faulty store over a directory, refusing nothing yet.
pub fn faulty_dir(root: impl Into<std::path::PathBuf>) -> FaultyStore {
    FaultyStore::over(Arc::new(analyzer_objectstore::DirStore::new(root)))
}

fn is_head(key: &str) -> bool {
    key.ends_with("journal/head")
}

/// Refuse journal writes whose body contains `marker`, or stop refusing (`None`).
pub fn fail_journal_writes_containing(store: &FaultyStore, marker: Option<&str>) {
    store.refuse(marker.map(|marker| {
        let marker = marker.to_string();
        let refusal: analyzer_objectstore::testing::Refusal = Arc::new(move |key, _, body| {
            (is_head(key) && String::from_utf8_lossy(body).contains(&marker))
                .then(|| marker.clone())
        });
        refusal
    }));
}

/// Refuse the journal write that marks an unapplied keyed tick applied: the tick's batch is
/// published and its hook has run, and the acknowledgment is lost. Every commit rewrites the
/// whole head, so only the write that flips the intent counts. The error names `name`.
pub fn lose_apply(store: &FaultyStore, name: &str) {
    let name = name.to_string();
    store.refuse(Some(Arc::new(move |key, current, body| {
        if !is_head(key) {
            return None;
        }
        let was: serde_json::Value = serde_json::from_slice(current?).ok()?;
        let now: serde_json::Value = serde_json::from_slice(body).ok()?;
        (was["keyed"]["applied"] == false && now["keyed"]["applied"] == true).then(|| name.clone())
    })));
}

/// Stop refusing.
pub fn heal(store: &FaultyStore) {
    store.refuse(None);
}

/// A spine over the same directory and layout as `engine`'s database spine, through a
/// faulty store: executors given it share that journal, and see its refusals.
pub fn faulty_default_spine(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> (FaultyStore, Arc<analyzer_pipeline::Spine>) {
    let default = spine(engine, fleet);
    let store = FaultyStore::over(Arc::from(default.store().sibling().expect("sibling")));
    let spine = analyzer_pipeline::Spine::new(
        Arc::new(store.clone()),
        "",
        &namespace(engine, fleet),
        default.lineage().clone(),
    )
    .with_working_state(
        &spine_dir(engine).join(analyzer_pipeline::spine::WORKING_STATE),
        analyzer_lake::working::DEFAULT_MEMORY_BYTES,
    )
    .expect("working state");
    (store, Arc::new(spine))
}

/// The journal head's key for `lineage` of `namespace` under a spine's `key_prefix`: the
/// layout `Journal::of` writes, restated so a test that edits the head also pins where it is.
pub fn head_key(
    key_prefix: &str,
    namespace: &analyzer_pipeline::execution::Namespace,
    lineage: &analyzer_pipeline::lineage::Lineage,
) -> String {
    format!(
        "{key_prefix}{}{}{}journal/head",
        analyzer_lake::dated::ANALYZER_ROOT,
        namespace.object_prefix(),
        lineage.object_prefix()
    )
}

/// Edit the journal head in place: a durable state no code path writes on its own (an
/// acknowledgment lost after its effect landed), set up the way an `INSERT` into the table
/// used to.
///
/// # Panics
/// The head is missing, does not decode, or changed under the edit.
pub fn edit_journal(
    store: &dyn analyzer_objectstore::ObjectStore,
    key_prefix: &str,
    namespace: &analyzer_pipeline::execution::Namespace,
    lineage: &analyzer_pipeline::lineage::Lineage,
    edit: impl FnOnce(&mut serde_json::Value),
) {
    let key = head_key(key_prefix, namespace, lineage);
    let (body, version) = store
        .get_versioned(&key)
        .expect("read the journal head")
        .expect("a journal head exists");
    let mut head: serde_json::Value = serde_json::from_slice(&body).expect("head decodes");
    edit(&mut head);
    let written = store
        .put_if(
            &key,
            &serde_json::to_vec(&head).expect("head encodes"),
            &analyzer_objectstore::Precondition::Matches(version),
        )
        .expect("write the journal head");
    assert!(
        matches!(written, analyzer_objectstore::Conditional::Written(_)),
        "the journal head changed under the edit"
    );
}

/// The journal head for `namespace` under `key_prefix` in `store`, or `Null` before any commit.
pub fn read_journal(
    store: &dyn analyzer_objectstore::ObjectStore,
    key_prefix: &str,
    namespace: &analyzer_pipeline::execution::Namespace,
    lineage: &analyzer_pipeline::lineage::Lineage,
) -> serde_json::Value {
    store
        .get_versioned(&head_key(key_prefix, namespace, lineage))
        .expect("read the journal head")
        .map_or(serde_json::Value::Null, |(body, _)| {
            serde_json::from_slice(&body).expect("head decodes")
        })
}

/// The default spine's journal head for `fleet` on `engine`, or `Null` before any commit.
pub fn default_journal(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
) -> serde_json::Value {
    let spine = spine(engine, fleet);
    read_journal(
        spine.store(),
        "",
        &namespace(engine, fleet),
        spine.lineage(),
    )
}

/// Edit the default spine's journal for `fleet` on `engine` (see [`edit_journal`]).
pub fn edit_default_journal(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    fleet: &str,
    edit: impl FnOnce(&mut serde_json::Value),
) {
    let spine = spine(engine, fleet);
    edit_journal(
        spine.store(),
        "",
        &namespace(engine, fleet),
        spine.lineage(),
        edit,
    );
}

/// The highest revision on any clock in a head, 0 before any: what `execution::revision`
/// reports when nothing is unfinished.
pub fn clock(head: &serde_json::Value) -> u64 {
    head["clocks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c[1].as_u64())
        .max()
        .unwrap_or(0)
}
