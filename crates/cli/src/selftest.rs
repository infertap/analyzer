// SPDX-License-Identifier: AGPL-3.0-only

//! `selftest`: the shipped conformance corpus through THIS install.
//!
//! The repository's conformance suite proves the code implements the contract; this proves
//! **the deployment** does — the installed binary, the installed `ClickHouse`, this host's
//! filesystem and clock — by running the same corpus through the same pipe and demanding
//! the same bytes. The corpus and its goldens are EMBEDDED at build time, so the binary
//! carries its own evidence and the check needs nothing but a scratch directory: it opens
//! its own engine session there and touches no production state, no matter what else runs
//! on the box.
//!
//! Each case is one tap emission fixture (the records the tap pins as its output) and the
//! byte-exact derived document a conforming reader must produce from it, the same document
//! `pipeline::derived::derive` produces for the repository's suites, so the bytes a
//! customer's host must reproduce and the bytes CI holds come from one definition. A
//! divergence names the case and writes the derived bytes beside the shipped ones for
//! support.

use crate::error::Error;
use crate::flags::Flags;
use analyzer_analysis::Params;
use analyzer_pipeline::facts;
use analyzer_pipeline::parquet_source::{FsStore, ParquetSource};
use analyzer_serving_clickhouse::engine::{migrate, server::Session, Engine, RetentionConfig};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// The `selftest` command: the corpus through this install, from a scratch directory it
/// makes and removes.
pub(crate) fn command(args: &[String]) -> Result<ExitCode, Error> {
    let flags = Flags::parse(args, &["--scratch", "--out", "--clickhouse"])?;
    let scratch = flags.value("--scratch").map_or_else(
        || std::env::temp_dir().join(format!("analyzer-selftest-{}", std::process::id())),
        PathBuf::from,
    );
    let out_dir = PathBuf::from(flags.value("--out").unwrap_or("."));
    std::fs::create_dir_all(&scratch)
        .and_then(|()| std::fs::create_dir_all(&out_dir))
        .map_err(|e| Error::io("selftest directories", e))?;
    let bin = crate::settings::resolve_bin(flags.value("--clickhouse"));
    let outcome = run(&bin, &scratch, &out_dir);
    let _ = std::fs::remove_dir_all(&scratch);
    let result = outcome.map_err(|e| {
        log::error!(
            "selftest could not run: {e} (this is an environment failure, not a contract \
             failure — nothing was proven either way)"
        );
        e
    })?;
    if result.failures.is_empty() {
        println!(
            "selftest: ok — {} conformance cases derived byte-identical snapshots on this \
             install",
            result.cases
        );
        return Ok(ExitCode::SUCCESS);
    }
    for f in &result.failures {
        eprintln!("selftest: {f}");
    }
    eprintln!(
        "selftest: FAILED — {}/{} cases diverged. This install does not implement the \
         contract it ships; do not trust its numbers until this is understood.",
        result.failures.len(),
        result.cases
    );
    Ok(ExitCode::FAILURE)
}

/// One embedded conformance case.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Case {
    /// The fixture's name, its file stem in both trees.
    pub(crate) name: &'static str,
    /// The tap emission fixture: the reader's input.
    fixture: &'static str,
    /// The bytes a conforming reader derives from it.
    golden: &'static str,
}

/// One embedded case: the fixture and its golden, `include_str!` at build time, so the
/// binary carries its own evidence.
macro_rules! case {
    ($name:literal) => {
        Case {
            name: $name,
            fixture: include_str!(concat!(
                "../../../contract/inference-events/conformance/telemetry/",
                $name,
                ".json"
            )),
            golden: include_str!(concat!(
                "../../../conformance/tap-corpus/",
                $name,
                ".derived.json"
            )),
        }
    };
}

/// Every telemetry fixture the contract pins, embedded with its golden. Completeness is
/// ratcheted by `embedded_corpus_matches_the_repository` below: a fixture added to the
/// contract without joining this list fails the build's tests, so the shipped selftest can
/// never quietly cover less than the repository's suite.
pub(crate) const CORPUS: &[Case] = &[
    case!("clear_is_scope_level"),
    case!("clear_scope_undeclared"),
    case!("clear_tier_undeclared"),
    case!("content_id_rides_beside_the_engine_id"),
    case!("evict_and_restore"),
    case!("holder_reset_closes_the_scope"),
    case!("lora_labelled"),
    case!("order_is_preserved"),
    case!("parent_unknown_is_not_a_root"),
    case!("prefix_chain"),
    case!("remote_locality_is_carried"),
    case!("reuse_labelled_rides_the_store_it_describes"),
    case!("salted_partition"),
    case!("same_content_distinct_engine_ids"),
    case!("same_hash_distinct_dp_ranks"),
    case!("same_hash_distinct_groups"),
    case!("same_hash_distinct_instances"),
    case!("seq_absent_where_the_transport_does_not_number"),
    case!("seq_rides_every_cache_record"),
    case!("sliding_window_is_carried_where_the_group_declares_one"),
    case!("tier_absent_stays_null"),
];

/// The fleet every case ingests and derives under; the goldens embed it.
const FLEET: &str = "conformance";

/// The result: which cases diverged, with everything else implied green.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Selftest {
    /// How many cases ran.
    pub(crate) cases: usize,
    /// One sentence per diverging case, naming the files written for support.
    pub(crate) failures: Vec<String>,
}

/// Run the embedded corpus against a scratch engine session in `scratch`.
///
/// One session and one migration; between cases the tables are truncated rather than the
/// database rebuilt, because the goldens embed the fleet id and per-case databases would
/// spend a server start per fixture to get isolation truncation already provides.
///
/// # Errors
/// Only environmental failure (the session, the migration, the filesystem). A case whose
/// bytes diverge is a finding, reported in [`Selftest::failures`]: "this box cannot run the
/// test" and "this box fails the contract" call for different phone calls.
pub(crate) fn run(bin: &Path, scratch: &Path, out_dir: &Path) -> Result<Selftest, Error> {
    let session = Session::open(bin, scratch.join("selftest-db"))?;
    migrate(session.engine(), &RetentionConfig::FIXTURE_WIDE)?;
    let mut failures = Vec::new();
    for case in CORPUS {
        let derived = derive_snapshot(session.engine(), scratch, case)?;
        if derived != case.golden.as_bytes() {
            // both sides on disk for support: the diff is the finding
            let derived_path = out_dir.join(format!("selftest-{}.derived.json", case.name));
            let shipped_path = out_dir.join(format!("selftest-{}.shipped.json", case.name));
            std::fs::write(&derived_path, &derived)
                .map_err(|e| Error::io(format!("write {}", derived_path.display()), e))?;
            std::fs::write(&shipped_path, case.golden)
                .map_err(|e| Error::io(format!("write {}", shipped_path.display()), e))?;
            failures.push(format!(
                "{}: this install derives DIFFERENT bytes than the contract pins — wrote {} \
                 and {} for support",
                case.name,
                derived_path.display(),
                shipped_path.display()
            ));
        }
    }
    session.close()?;
    Ok(Selftest {
        cases: CORPUS.len(),
        failures,
    })
}

/// The derivation's source over one case's bytes: the same JSONL the store ingested, written as
/// a sealed segment under a dated tree, read back through `ParquetSource` with the filesystem
/// filesystem fetch, the same reader, cache and discovery the deployment derives with,
/// minus only the S3 transport. The content hash is a function of the bytes alone, so the
/// delivery identity the two paths stamp is the same value.
fn segment_source(
    scratch: &Path,
    name: &str,
    jsonl: &str,
) -> Result<ParquetSource<FsStore>, Error> {
    let min_at = jsonl
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|r| r.get("at_ms").and_then(Value::as_f64))
        .fold(f64::INFINITY, f64::min);
    if !min_at.is_finite() {
        return Err(Error::usage(format!("{name}: no record carries an at_ms")));
    }
    let root = scratch.join(format!("segments-{name}"));
    let key = analyzer_lake::dated::segment_key("tap/", min_at, "n1", "records.jsonl")
        .ok_or_else(|| Error::usage(format!("{name}: at_ms {min_at} names no dated hour")))?;
    let path = root.join(&key);
    let dir = path
        .parent()
        .ok_or_else(|| Error::usage(format!("{name}: {key} names no directory")))?;
    std::fs::create_dir_all(dir).map_err(|e| Error::io(format!("{name}: segment tree"), e))?;
    std::fs::write(&path, jsonl.as_bytes())
        .map_err(|e| Error::io(format!("{name}: write segment"), e))?;
    // The corpus seam: the embedded conformance fixtures are the write-ahead JSONL form,
    // which production discovery does not list.
    Ok(ParquetSource::new(
        FsStore::new(root),
        scratch.join(format!("segment-cache-{name}")),
        "tap/",
        analyzer_pipeline::parquet_source::horizon_reaching(min_at),
    )
    .admitting_corpus_forms())
}

/// The same pipe the product runs, deterministically: ingest, then the derived document, read
/// from the segment bytes through `ParquetSource` exactly as the deployment derives.
fn derive_snapshot(engine: &Engine, scratch: &Path, case: &Case) -> Result<Vec<u8>, Error> {
    let name = case.name;
    analyzer_serving_clickhouse::engine::truncate_all_but_migrations(engine)?;
    // A corpus this deployment cannot pass is one it cannot serve, and finding that here beats
    // finding it live. Each case derives in a lake of its own, from nothing.
    let namespace = analyzer_pipeline::execution::Namespace::new("selftest-corpus", FLEET)?;
    let spine_dir = scratch.join(format!("spine-{name}"));
    if spine_dir.exists() {
        std::fs::remove_dir_all(&spine_dir)
            .map_err(|e| Error::io(format!("{name}: clear {}", spine_dir.display()), e))?;
    }
    let spine = std::sync::Arc::new(analyzer_pipeline::spine::local(&spine_dir, &namespace)?);
    let serving: std::sync::Arc<dyn analyzer_pipeline::publish::Serving> = std::sync::Arc::new(
        analyzer_serving_clickhouse::serving::ClickHouse::new(engine)?,
    );
    serving.bind(&namespace)?;
    let lake = spine.lake();
    let fx: Value = serde_json::from_str(case.fixture)
        .map_err(|e| Error::json(format!("{name}: fixture parse"), e))?;
    let mut jsonl = String::new();
    for r in fx["expect"]["records"]
        .as_array()
        .ok_or_else(|| Error::usage(format!("{name}: fixture has no expect.records")))?
    {
        jsonl.push_str(&r.to_string());
        jsonl.push('\n');
    }
    let ingest = analyzer_ingest::ingest_bytes(&lake, FLEET, "records.jsonl", jsonl.as_bytes())?;
    let source = segment_source(scratch, name, &jsonl)?;

    let params = Params::default();
    let window = facts::full_span(&lake)?.ok_or_else(|| {
        Error::usage(format!(
            "{name}: ingested {} records but no cache facts landed",
            ingest.records_inserted
        ))
    })?;
    analyzer_pipeline::Executor::open(spine, &params, serving)
        .and_then(|executor| executor.advance(&source))?;

    let doc = analyzer_pipeline::derived::derive(&source, FLEET, &window)?;
    // Trailing newline, matching how the repository writes its goldens: a file that differs
    // only in its last byte would report as a conformance failure at a customer.
    let text = serde_json::to_string_pretty(&doc)
        .map_err(|e| Error::json(format!("{name}: serialize"), e))?;
    Ok(format!("{text}\n").into_bytes())
}

#[cfg(test)]
mod tests {
    use super::CORPUS;
    use std::path::PathBuf;

    /// The embedded list against the repository, both directions: a contract fixture the
    /// binary does not carry cannot be selftested at a customer, and an embedded name with
    /// no fixture is a case that stopped meaning anything. `include_str!` already fails the
    /// build on a missing FILE; this fails it on a missing ENTRY.
    #[test]
    fn embedded_corpus_matches_the_repository() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../contract/inference-events/conformance/telemetry");
        let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
            .expect("telemetry corpus dir (is the submodule checked out?)")
            .filter_map(Result::ok)
            .filter_map(|e| {
                let n = e.file_name().to_string_lossy().to_string();
                n.strip_suffix(".json").map(str::to_string)
            })
            .filter(|n| n != "README")
            .collect();
        on_disk.sort();
        let mut embedded: Vec<String> = CORPUS.iter().map(|c| c.name.to_string()).collect();
        embedded.sort();
        assert_eq!(
            embedded, on_disk,
            "the embedded selftest corpus and the contract's telemetry fixtures have \
             drifted — update selftest::CORPUS in the same commit that moves the pin"
        );
    }
}
