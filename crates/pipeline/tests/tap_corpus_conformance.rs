// SPDX-License-Identifier: AGPL-3.0-only

//! **The tap's published telemetry corpora, derived and pinned byte-for-byte.**
//!
//! The inputs are the fixtures the tap ships as its own contract
//! (`contract/inference-events/conformance/telemetry`); the output is
//! [`harness::derived::derive`] — every figure the product prints for that window. A diff without
//! a deliberate re-bless is a semantics change, and this is where it surfaces.
//!
//! The goldens hold the DERIVED ROWS: a verdict is a sentence written about figures, so pinning
//! the figures is pinning the evidence itself.
//!
//! **What is deliberately unchanged:** the corpora are DISCOVERED rather than enumerated, and the
//! check is bidirectional. A fixture the tap adds that nothing here reads is silent and permanent;
//! a golden that outlives its fixture reports success forever. Both are the shape of every silent
//! failure in this system, so both are assertions.
//!
//! **Why this suite reads the same document as `derived_snapshot`.** Two definitions of "every
//! derived figure" would drift, and the drift would look like a semantics change in whichever
//! suite noticed second. One `derive`, two input sets: hand-authored cases that reach the seams a
//! captured corpus does not, and captured corpora that carry shapes nobody would think to author.
//!
//! Regenerate deliberately:
//! `BLESS=1 cargo test -p analyzer-pipeline --test tap_corpus_conformance -- --ignored`

use analyzer_analysis::Params;
use analyzer_pipeline::derived::derive;
use analyzer_pipeline::facts;
use serde_json::Value;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn corpus_dir() -> PathBuf {
    repo_root().join("contract/inference-events/conformance/telemetry")
}

/// Its own directory, NOT `conformance/derived`.
///
/// That one belongs to `derived_snapshot`'s hand-authored cases. Two suites writing one directory
/// makes each one's orphan check blind to the other's files — the check that exists to notice a
/// golden outliving its input, defeated by the layout.
fn golden_dir() -> PathBuf {
    repo_root().join("conformance/tap-corpus")
}

/// One engine for every corpus, with a retention that cannot age them out and cannot overflow
/// computing that.
///
/// **Both bounds are load-bearing and one of them already bit.** The corpora carry real
/// wall-clock timestamps, so the default 30-day horizon would delete them mid-run — ingest
/// reporting success against an empty table. It was 36,500 days for that reason, until
/// `toDateTime(at_ms / 1000) + INTERVAL 36500 DAY` on a 2026 timestamp exceeded `ClickHouse`
/// `DateTime`'s 2106 ceiling, the TTL expression overflowed, fired immediately, and deleted every
/// row anyway. Wide enough not to expire, narrow enough to stay representable.
///
/// **One database, not one per corpus.** This built a database and ran the migration for each of
/// the twenty fixtures — seventeen tables and a materialized view, twenty times, for a suite that
/// derives a few hundred rows. `selftest` runs the same twenty corpora against one session and
/// truncates between cases, and it is the shipped path, so the suite that pins the same goldens
/// has no reason to do it differently. Isolation is unchanged: truncation empties the same tables
/// a fresh database would not have had.
fn conformance_engine() -> (
    analyzer_serving_clickhouse::engine::Engine,
    analyzer_serving_clickhouse::engine::testing::TestDatabase,
) {
    let (engine, db) = analyzer_serving_clickhouse::engine::testing::database("tap-corpus");
    analyzer_serving_clickhouse::engine::migrate(
        &engine,
        &analyzer_serving_clickhouse::engine::RetentionConfig {
            ledger_retention_days: 3_650,
            rollup_retention_years: 100,
            instance_detail_retention_days: 3_650,
            minute_retention_days: 3_650,
            hour_retention_days: 3_650,
        },
    )
    .expect("migrate");
    (engine, db)
}

/// Every telemetry fixture the tap publishes, discovered rather than enumerated.
///
/// This was seven named test functions until the tap regenerated its corpus from captured traffic
/// and four of the seven names changed. That break was loud and cheap. The one it could not catch
/// is the opposite: a fixture ADDED upstream that no test here names, which is silent and
/// permanent — the analyzer would simply never be shown an input the tap considers part of its
/// contract.
fn telemetry_corpus_files() -> Vec<String> {
    let dir = corpus_dir();
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| {
            std::path::Path::new(n)
                .extension()
                .is_some_and(|e| e == "json")
        })
        .collect();
    assert!(
        !names.is_empty(),
        "no telemetry fixtures under {} — the contract pin is empty or wrong",
        dir.display()
    );
    names.sort();
    names
}

/// Derive one corpus, or `None` where it carries no cache facts to derive over.
///
/// **The lifecycle-only corpus is a real case, not a gap.** It has no stores, so there is no
/// window — and the honest answer is that there is nothing to pin rather than an empty document
/// that would look like a fleet with no waste.
fn derived_for_corpus(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    name: &str,
) -> Option<String> {
    let fixture: Value =
        serde_json::from_str(&std::fs::read_to_string(corpus_dir().join(name)).expect("corpus"))
            .expect("corpus parses");
    let mut jsonl = String::new();
    for r in fixture["expect"]["records"].as_array().expect("records") {
        jsonl.push_str(&r.to_string());
        jsonl.push('\n');
    }

    harness::reset(engine);
    // The SHIPPED reader, not a harness insert: same shaping, same probe, same commit. That is
    // what makes these goldens evidence about the product rather than about an equivalent path.
    let ingest = harness::ingest_bytes(engine, "conformance", "records.jsonl", jsonl.as_bytes())
        .expect("ingest");
    // The derivation reads the SAME bytes as sealed segments through `ParquetSource` with the
    // filesystem fetch — the shipped derivation path. The content hash is a function of the
    // bytes alone, so both seedings stamp one delivery identity.
    let source = harness::source_over_bytes(&format!("tap-corpus-{name}"), jsonl.as_bytes());

    let params = Params::default();
    let window = facts::full_span(&harness::lake(engine, "conformance")).expect("span")?;

    // The pass runs over the segment source, because a corpus that cannot be passed is a corpus
    // the product cannot serve, and finding that here beats finding it live.
    harness::executor(engine, "conformance", &params)
        .and_then(|executor| executor.advance(&source))
        .expect("tick");

    let doc = derive(&source, "conformance", &window).expect("derive");
    assert!(
        ingest.records_inserted > 0,
        "{name}: the reader accepted the corpus and inserted nothing"
    );
    Some(format!(
        "{}\n",
        serde_json::to_string_pretty(&doc).expect("render")
    ))
}

/// The corpora reproduce their goldens.
///
/// Failures are COLLECTED rather than short-circuited, so a corpus change reports all of its
/// consequences in one run instead of one per re-run.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn every_tap_telemetry_fixture_reproduces_its_derived_golden() {
    let blessing = std::env::var_os("BLESS").is_some();
    if blessing {
        std::fs::create_dir_all(golden_dir()).expect("golden dir");
    }
    let mut failures = Vec::new();
    let mut checked = 0usize;
    let (engine, _db) = conformance_engine();

    for corpus in telemetry_corpus_files() {
        let stem = corpus.trim_end_matches(".json");
        let path = golden_dir().join(format!("{stem}.derived.json"));
        let Some(rendered) = derived_for_corpus(&engine, &corpus) else {
            // No cache facts: nothing to pin, and a golden here would assert an empty fleet.
            if blessing {
                std::fs::remove_file(&path).ok();
            } else if path.exists() {
                failures.push(format!(
                    "{corpus}: carries no cache facts, but a golden exists at {} — it pins a \
                     window that cannot be derived",
                    path.display()
                ));
            }
            continue;
        };
        checked += 1;

        if blessing {
            std::fs::write(&path, rendered.as_bytes()).expect("bless");
            continue;
        }
        match std::fs::read_to_string(&path) {
            Err(_) => failures.push(format!(
                "{corpus}: no golden at {} — a fixture the tap added that nothing here has ever \
                 been shown. Run BLESS=1 DELIBERATELY after reading what it produces",
                path.display()
            )),
            Ok(expected) if expected != rendered => failures.push(format!(
                "{corpus}: derived output diverged from its committed golden — a semantics \
                 change; re-bless DELIBERATELY if intended"
            )),
            Ok(_) => {}
        }
    }

    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
    assert!(checked > 0, "no corpus produced a derivable window");
}

/// A golden with no fixture behind it is a test that cannot fail.
///
/// It survives the input it was blessed against and then reports success forever, which is the
/// shape of every silent failure in this system.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn no_derived_golden_outlives_the_fixture_it_was_blessed_against() {
    let expected: Vec<String> = telemetry_corpus_files()
        .iter()
        .map(|c| format!("{}.derived.json", c.trim_end_matches(".json")))
        .collect();
    let orphans: Vec<String> = std::fs::read_dir(golden_dir())
        .map(|rd| {
            rd.filter_map(std::result::Result::ok)
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| n.ends_with(".derived.json") && !expected.contains(n))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        orphans.is_empty(),
        "goldens with no corpus fixture behind them: {orphans:?}. Delete them — the fixture they \
         pinned is gone, so they assert nothing."
    );
}
