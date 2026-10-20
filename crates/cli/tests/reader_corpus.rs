// SPDX-License-Identifier: AGPL-3.0-only

//! This reader against the READER corpus: the contract's verdicts, reproduced.
//!
//! The tap's other corpora pin emission and cannot fail a reader; the reader corpus pairs
//! input streams with the verdict a conforming reader must reach, and this suite is where
//! this reader is held to them. Fixtures arrive as whole sealed segments and are consumed
//! through the real delivery path — written to a directory, ingested by the shipped
//! reader — because a shortcut into the arithmetic would pin the math while the defect
//! class lives in the plumbing.
//!
//! Discovery is by directory, like the goldens: a fixture the tap adds that this suite has
//! never seen fails loudly instead of being silently uncovered.

use analyzer_analysis::degradation;
use analyzer_analysis::staleness::RunStaleness;
use analyzer_analysis::{Params, Window};
use analyzer_pipeline::facts;
use analyzer_pipeline::parquet_source::SourceConfig;
use analyzer_serving_clickhouse::engine::testing::repo_root;
use analyzer_serving_clickhouse::engine::{migrate, Engine, RetentionConfig};
use serde_json::Value;
use std::sync::Arc;

fn fixtures() -> Vec<Value> {
    let dir = repo_root().join("contract/inference-events/conformance/reader");
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("reader corpus dir (submodule checked out?)") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|e| e == "json") {
            out.push(
                serde_json::from_str(&std::fs::read_to_string(&path).expect("read"))
                    .expect("fixture json"),
            );
        }
    }
    assert!(
        out.len() >= 7,
        "expected the reader corpus's opening families, found {} fixtures",
        out.len()
    );
    out
}

/// The corpus's namespace: one source, the reader fleet.
fn namespace() -> analyzer_pipeline::execution::Namespace {
    analyzer_pipeline::execution::Namespace::new("reader-corpus-source", "reader")
        .expect("namespace")
}

/// The serving store behind `engine`'s database.
fn serving(engine: &Engine) -> Arc<dyn analyzer_pipeline::publish::Serving> {
    Arc::new(analyzer_serving_clickhouse::serving::ClickHouse::new(engine).expect("serving"))
}

/// The corpus database's spine: a local lake per database on the shared server.
fn spine(engine: &Engine) -> Arc<analyzer_pipeline::Spine> {
    let name: String = engine
        .identity()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    Arc::new(
        analyzer_pipeline::spine::local(&engine.data_dir().join("spine").join(name), &namespace())
            .expect("spine"),
    )
}

/// Ingest a fixture's segments through the shipped delivery path into a fresh database, and
/// plant the same bytes under a dated tree the derivation source reads.
fn ingest_fixture(
    fx: &Value,
) -> (
    analyzer_serving_clickhouse::engine::Engine,
    SourceConfig,
    analyzer_serving_clickhouse::engine::testing::TestDatabase,
) {
    let (engine, db) = analyzer_serving_clickhouse::engine::testing::database("reader");
    // Wide enough that the corpus's fixed 2026 timestamps do not age out, narrow enough
    // that the TTL expression stays representable (the conformance suite's derivation).
    migrate(
        &engine,
        &RetentionConfig {
            ledger_retention_days: 3_650,
            instance_detail_retention_days: 3_650,
            minute_retention_days: 3_650,
            hour_retention_days: 3_650,
            rollup_retention_years: 100,
        },
    )
    .expect("migrate");
    // The namespace is bound before ingest, as a deployment binds it at start: the input log
    // lives under it.
    serving(&engine).bind(&namespace()).expect("bind");
    let lake = spine(&engine).lake();
    let dir = harness::fresh_dir("reader-corpus");
    let mut min_at = f64::INFINITY;
    for (i, seg) in fx["segments"]
        .as_array()
        .expect("segments")
        .iter()
        .enumerate()
    {
        let records = seg.as_array().expect("segment is a record array");
        let header = &records[0];
        // Dispatch on the first record, exactly as the delivery contract prescribes: a
        // sealed segment begins with segment_open. The marker-first arm is the pre-1.11
        // orphan form a reader may still accept (spec 4.1); no fixture carries it since
        // 1.11, when a headerless file became a discard declared inside a headered segment.
        let name = match header["kind"].as_str() {
            Some("segment_open") => format!(
                "seg-{}-{}.jsonl",
                header["incarnation"].as_str().expect("incarnation"),
                header["segment_seq"].as_u64().unwrap_or(i as u64)
            ),
            Some("segment_recovered") => format!("orphan-{i}.jsonl"),
            other => panic!("segment begins with {other:?}; the contract permits neither"),
        };
        let mut body = String::new();
        for r in records {
            body.push_str(&r.to_string());
            body.push('\n');
        }
        analyzer_ingest::ingest_bytes(&lake, "reader", &name, body.as_bytes()).expect("ingest");
        analyzer_pipeline::Executor::open(
            spine(&engine),
            &analyzer_analysis::Params::default(),
            serving(&engine),
        )
        .and_then(|executor| executor.publish_inputs())
        .expect("publish the input tables");
        let at = records
            .iter()
            .filter_map(|r| r["at_ms"].as_f64())
            .fold(f64::INFINITY, f64::min);
        assert!(at.is_finite(), "a segment carries at least one at_ms");
        min_at = min_at.min(at);
        harness::plant_segment(&dir.join("segments"), "tap/", at, &name, body.as_bytes());
    }
    let config =
        harness::directory_sources(dir.join("segments"), dir.join("segment-cache"), min_at);
    (engine, config, db)
}

/// The fixture's own span, as a fetch clip: staleness fixtures carry no cache facts,
/// so their window comes from the records themselves rather than from `full_span`.
fn fixture_span(fx: &Value) -> Window {
    let stamps: Vec<f64> = fx["segments"]
        .as_array()
        .expect("segments")
        .iter()
        .flat_map(|s| s.as_array().expect("records"))
        .filter_map(|r| r["at_ms"].as_f64())
        .collect();
    let lo = stamps.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = stamps.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    Window {
        start_ms: lo - 1.0,
        end_ms: hi + 1.0,
    }
}

/// Every family this suite grades. A fixture whose `expect` names anything else fails
/// loudly below — the mechanical form of "a fixture the tap adds that this suite has
/// never seen must not sit silently uncovered".
const FAMILIES: &[&str] = &[
    "audit",
    // spec 5.7: what a cross-instance zero means, as the replication read states it
    "residual_zero",
    "parent_conflicts",
    "staleness",
    "gap_degraded",
    "uncovered",
    "absence",
    "holder_register",
    "coverage",
    // spec 5.9: what a store record may be counted into, per the producer's declaration
    "insertions",
    "recompute",
    "first_insertion",
    // spec 5.4: the narrowing a seq permits
    "gap_span",
    // spec 2.2 / 2.7: the header's declarations, applied to every record behind it
    "attribution",
    // spec 2.8: stores attributed to declared tag intervals, the edges counted apart
    "tag_slices",
];

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine cli"]
#[allow(clippy::too_many_lines)] // one dispatch arm per corpus family; the flat roster is the point
fn every_reader_fixture_verdict_is_reproduced() {
    let mut failures = Vec::new();
    for fx in fixtures() {
        let name = fx["name"].as_str().expect("name");
        for key in fx["expect"].as_object().expect("expect").keys() {
            assert!(
                FAMILIES.contains(&key.as_str()),
                "{name}: expect names family {key:?}, which this suite does not grade — \
                 honor the new family before bumping the pin"
            );
        }
        let (engine, config, _db) = ingest_fixture(&fx);
        let source = config.source();
        // Cache-fact families window on the cache span; computed lazily because the
        // staleness fixtures are lifecycle-only streams with no cache facts at all.
        let window = facts::full_span(&spine(&engine).lake()).expect("span");
        let window = |family: &str| {
            window.unwrap_or_else(|| panic!("{name}: family {family} needs cache facts"))
        };

        if let Some(want) = fx["expect"].get("audit") {
            grade_audit(&source, name, &window("audit"), want, &mut failures);
        }

        if let Some(want) = fx["expect"].get("parent_conflicts") {
            let t =
                analyzer_pipeline::read::block_tree(&source, "reader", &window("parent_conflicts"))
                    .expect("tree");
            if t.parent_conflicts != want.as_u64().expect("count") {
                failures.push(format!(
                    "{name}: surfaced {} parent conflicts where the contract expects {want}",
                    t.parent_conflicts
                ));
            }
        }

        if let Some(want) = fx["expect"].get("coverage") {
            if let Some(failure) = grade_coverage(&source, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("holder_register") {
            if let Some(failure) = grade_holder_register(&source, &fx, name, want) {
                failures.push(failure);
            }
        }

        for (family, idx) in [
            ("insertions", 0usize),
            ("recompute", 1),
            ("first_insertion", 2),
        ] {
            if let Some(want) = fx["expect"].get(family) {
                if let Some(failure) = grade_figure(&source, &fx, name, family, idx, want) {
                    failures.push(failure);
                }
            }
        }

        if let Some(want) = fx["expect"].get("gap_span") {
            if let Some(failure) = grade_gap_span(&source, &fx, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("absence") {
            if let Some(failure) = grade_absence(&source, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("uncovered") {
            if let Some(failure) = grade_uncovered(&source, &fx, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("gap_degraded") {
            if let Some(failure) = grade_gap_degraded(&source, &fx, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("staleness") {
            if let Some(failure) = grade_staleness(&source, &fx, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("attribution") {
            if let Some(failure) = grade_attribution(&fx, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("residual_zero") {
            if let Some(failure) = grade_residual_zero(&engine, &config, &source, name, want) {
                failures.push(failure);
            }
        }

        if let Some(want) = fx["expect"].get("tag_slices") {
            if let Some(failure) = grade_tag_slices(&engine, &source, &fx, name, want) {
                failures.push(failure);
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}

/// Grade the tag family (spec 2.8): the declarations the ingest banked, taken as their latest
/// statements; the stores the source reads, attributed under the default skew allowance by
/// the production rule; per label the stores and tokens attributed, and the edge stores.
fn grade_tag_slices(
    engine: &analyzer_serving_clickhouse::engine::Engine,
    source: &dyn analyzer_analysis::events::EventSource,
    fx: &Value,
    name: &str,
    want: &Value,
) -> Option<String> {
    use analyzer_analysis::tags::{attribute, Attribution, TagIndex, TagStatement};
    let lake = spine(engine).lake();
    let mut index = TagIndex::default();
    for batch in lake
        .ledger(analyzer_pipeline::ledger::SHARD)
        .expect("input log")
        .committed_since(0)
        .expect("committed")
    {
        for t in &batch.tags {
            index.state(&TagStatement {
                declaration_id: t.declaration_id.clone(),
                identity: t.identity.clone(),
                tag: t.tag.clone(),
                from_ms: t.from_ms,
                until_ms: t.until_ms,
                at_ms: t.at_ms,
            });
        }
    }
    let skew = analyzer_analysis::Params::default().delta_skew_ms;
    let mut slices: std::collections::BTreeMap<String, (u64, i64)> =
        std::collections::BTreeMap::default();
    let mut ambiguous: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::default();
    let stores = source
        .cache_events("reader", &fixture_span(fx))
        .expect("cache events");
    for e in stores
        .iter()
        .filter(|e| e.kind == analyzer_analysis::events::Kind::Store)
    {
        let Some(identity) = e.content_id.as_deref() else {
            continue;
        };
        for i in index.of(identity) {
            match attribute(e.at_ms, i.from_ms, i.until_ms, skew) {
                Attribution::Inside => {
                    let slice = slices.entry(i.tag.clone()).or_default();
                    slice.0 += 1;
                    slice.1 += e.n_tokens;
                }
                Attribution::Ambiguous => *ambiguous.entry(i.tag.clone()).or_default() += 1,
                Attribution::Outside => {}
            }
        }
    }
    let mine = serde_json::json!({
        "slices": slices
            .iter()
            .map(|(t, (n, tokens))| (t.clone(), serde_json::json!({"stores": n, "tokens": tokens})))
            .collect::<serde_json::Map<_, _>>(),
        "ambiguous": ambiguous
            .iter()
            .map(|(t, n)| (t.clone(), serde_json::json!(n)))
            .collect::<serde_json::Map<_, _>>(),
    });
    (mine != *want).then(|| format!("{name}: tag_slices are {mine}, the corpus says {want}"))
}

/// Grade the zero-meaning family (spec 5.7). The basis is the served read's: the inputs the
/// corpus published, through the DAO the replication endpoint calls. The cross-instance mass is
/// the duplication module folded over the fixture's whole window — the fold the served cells
/// equal (`placement_corpus`), taken whole because a liveness-free fixture holds the tick's
/// edges at its first record.
fn grade_residual_zero(
    engine: &Engine,
    config: &SourceConfig,
    source: &dyn analyzer_analysis::events::EventSource,
    name: &str,
    want: &Value,
) -> Option<String> {
    use analyzer_analysis::dup_cells::{DupCtx, Duplication};
    use analyzer_analysis::events::ScopeClosers;
    use analyzer_analysis::module::{run_window, Persisted, Value as Cell};
    let params = Params::default();
    let span = facts::full_span(&spine(engine).lake())
        .expect("span")
        .expect("the family needs cache facts");
    // The fold's window reaches past the last record by the skew allowance, so a residency
    // open at the end is held through it rather than censored at the instant it was seen.
    let window = Window::new(span.start_ms, span.end_ms + params.delta_skew_ms).expect("window");
    let events = source.cache_events("reader", &window).expect("events");
    let closers = ScopeClosers::of(&events);
    let mut grouped = std::collections::BTreeMap::new();
    for e in events.iter() {
        if let Some(id) = e.identity() {
            grouped
                .entry(id.to_string())
                .or_insert_with(Vec::new)
                .push(e.clone());
        }
    }
    let ctx = DupCtx {
        closers: &closers,
        half_skew: params.delta_skew_ms / 2.0,
    };
    let cells = run_window(
        &Duplication,
        grouped,
        &std::collections::BTreeMap::new(),
        window,
        &ctx,
    )
    .cells;
    let cross: f64 = cells
        .days()
        .into_iter()
        .flat_map(|day| cells.render_day(day))
        .filter(|row| {
            row.table == "replication_daily"
                && row.key.first() == Some(&Cell::Str(String::new()))
                && row.key.get(2) == Some(&Cell::Str("cross".to_string()))
        })
        .map(|row| match row.measures.first() {
            Some(Cell::F64(token_ms)) => *token_ms,
            other => panic!("replication_daily token_ms: {other:?}"),
        })
        .sum();
    let dao = analyzer_serving_clickhouse::Dao::new(
        engine,
        config.clone(),
        &params,
        spine(engine),
        serving(engine),
    );
    let meaning = dao
        .zero_basis(&span)
        .expect("zero basis")
        .meaning(cross)
        .as_str();
    (want.as_str() != Some(meaning))
        .then(|| format!("{name}: the zero reads {meaning}, the corpus says {want}"))
}

/// Grade the audit family: five-class evict re-derivation counts per instance.
fn grade_audit(
    source: &dyn analyzer_analysis::events::EventSource,
    name: &str,
    w: &Window,
    want: &Value,
    failures: &mut Vec<String>,
) {
    let got = analyzer_pipeline::read::audit(source, "reader", w).expect("audit");
    for (instance, expect) in want.as_object().expect("audit map") {
        let g = got.instances.iter().find(|i| &i.instance == instance);
        let g = match g {
            Some(g) => g,
            None if expect["audited"] == 0 => continue,
            None => {
                failures.push(format!("{name}: no audit row for {instance}"));
                continue;
            }
        };
        let mine = serde_json::json!({
            "audited": g.audited, "agreed": g.agreed, "mismatched": g.mismatched,
            "underresolved": g.underresolved, "indeterminate": g.indeterminate,
            "unjoined": g.unjoined,
        });
        if &mine != expect {
            failures.push(format!(
                "{name}: {instance} graded {mine} where the contract expects {expect}"
            ));
        }
    }
}

/// Grade the coverage family: the section 5.7 floor's direction and endpoints — exactly 1.0
/// over a counted bracket, strictly below it otherwise. The corpus deliberately pins
/// no interior fraction, so the comparison is the two endpoints and nothing else.
fn grade_coverage(
    source: &dyn analyzer_analysis::events::EventSource,
    name: &str,
    want: &Value,
) -> Option<String> {
    let span = want["span_ms"].as_array().expect("span_ms");
    let w = Window {
        start_ms: span[0].as_f64().expect("span start"),
        end_ms: span[1].as_f64().expect("span end"),
    };
    let fraction =
        analyzer_pipeline::read::uncertainty_model(source, "reader", &w, &Params::default())
            .expect("uncertainty model")
            .coverage(None)
            .fraction;
    let mine = if (fraction - 1.0).abs() < 1e-9 {
        "complete"
    } else if fraction < 1.0 {
        "reduced"
    } else {
        return Some(format!("{name}: coverage {fraction} exceeds 1.0"));
    };
    (Some(mine) != want["verdict"].as_str()).then(|| {
        format!(
            "{name}: coverage over {:?} read {mine:?} ({fraction:.4}) where the contract expects {}",
            (w.start_ms, w.end_ms),
            want["verdict"]
        )
    })
}

/// Grade the holder-register family: each question names a holder and an engine-clock
/// span, and the answer is `Ledger::register_for` verbatim — `none` when the ledger
/// holds nothing against them, which the section 2.4 scoping questions pin as positively as
/// the degradation itself.
fn grade_holder_register(
    source: &dyn analyzer_analysis::events::EventSource,
    fx: &Value,
    name: &str,
    want: &Value,
) -> Option<String> {
    let span = fixture_span(fx);
    let model =
        analyzer_pipeline::read::uncertainty_model(source, "reader", &span, &Params::default())
            .expect("model");
    let ledger = analyzer_pipeline::degradation_io::ledger(
        source,
        "reader",
        &span,
        &model,
        &source
            .segment_opens_at_retention_horizon("reader")
            .expect("opens"),
    )
    .expect("ledger");
    for q in want.as_array().expect("questions") {
        let holder = analyzer_analysis::intervals::Holder {
            instance: q["instance"].as_str().expect("instance").to_string(),
            dp_rank: u32::try_from(q["dp_rank"].as_u64().expect("dp_rank")).unwrap(),
            group_idx: u32::try_from(q["group_idx"].as_u64().expect("group_idx")).unwrap(),
        };
        let span = q["span_ms"].as_array().expect("span_ms");
        let w = Window {
            start_ms: span[0].as_f64().expect("span start"),
            end_ms: span[1].as_f64().expect("span end"),
        };
        let got = ledger
            .register_for(&[holder], &w)
            .map_or("none", |r| r.as_str());
        if Some(got) != q["register"].as_str() {
            return Some(format!(
                "{name}: {} dp{} over {:?} read {got:?} where the contract expects {}",
                q["instance"], q["dp_rank"], w, q["register"]
            ));
        }
    }
    None
}

/// Grade the absence family: the fixture's question span, read at the register
/// `absence_register` reaches — the section 5.6 rule as one comparable word.
fn grade_absence(
    source: &dyn analyzer_analysis::events::EventSource,
    name: &str,
    want: &Value,
) -> Option<String> {
    let span = want["span_ms"].as_array().expect("span_ms");
    let span = (
        span[0].as_f64().expect("span start"),
        span[1].as_f64().expect("span end"),
    );
    let got = analyzer_pipeline::degradation_io::absence_register(
        source,
        "reader",
        span,
        &Params::default(),
    )
    .expect("absence register");
    (Some(got.as_str()) != want["register"].as_str()).then(|| {
        format!(
            "{name}: read absence over {span:?} as {:?} where the contract expects {}",
            got.as_str(),
            want["register"]
        )
    })
}

/// Grade the uncovered family: per instance, the exact section 5.3 brackets — the ledger's
/// incarnation-boundary entries, whose producer brackets are the verdict verbatim.
/// Instances absent from the expectation must carry no boundary at all.
fn grade_uncovered(
    source: &dyn analyzer_analysis::events::EventSource,
    fx: &Value,
    name: &str,
    want: &Value,
) -> Option<String> {
    let span = fixture_span(fx);
    let model =
        analyzer_pipeline::read::uncertainty_model(source, "reader", &span, &Params::default())
            .expect("model");
    let ledger = analyzer_pipeline::degradation_io::ledger(
        source,
        "reader",
        &span,
        &model,
        &source
            .segment_opens_at_retention_horizon("reader")
            .expect("opens"),
    )
    .expect("ledger");
    let mut mine: std::collections::BTreeMap<String, Vec<(f64, f64)>> =
        std::collections::BTreeMap::new();
    for d in ledger.in_window(&fixture_span(fx)) {
        if d.cause.class() != "incarnation_boundary" {
            continue;
        }
        let degradation::Scope::Instance(instance) = &d.scope else {
            return Some(format!(
                "{name}: a boundary entry with a non-instance scope"
            ));
        };
        let (Some(from), Some(to)) = (d.producer_bracket.start_ms, d.producer_bracket.end_ms)
        else {
            return Some(format!("{name}: a boundary entry with an open bracket"));
        };
        mine.entry(instance.clone()).or_default().push((from, to));
    }
    for v in mine.values_mut() {
        v.sort_by(|a, b| a.0.total_cmp(&b.0));
    }

    let want = want.as_object().expect("uncovered map");
    for (instance, intervals) in want {
        let expected: Vec<(f64, f64)> = intervals
            .as_array()
            .expect("interval list")
            .iter()
            .map(|p| {
                let p = p.as_array().expect("interval pair");
                (p[0].as_f64().expect("from"), p[1].as_f64().expect("to"))
            })
            .collect();
        let got = mine.remove(instance).unwrap_or_default();
        if got != expected {
            return Some(format!(
                "{name}: {instance} uncovered {got:?} where the contract expects {expected:?}"
            ));
        }
    }
    (!mine.is_empty())
        .then(|| format!("{name}: uncovered windows fabricated for undeclared instances {mine:?}"))
}

/// Grade the gap-degraded family: section 5.4's detected-gap classes present in the stream,
/// in the corpus's spec-worded vocabulary. One mapping, total over the ledger's
/// gap-class causes — other cause classes belong to other families and never enter.
fn grade_gap_degraded(
    source: &dyn analyzer_analysis::events::EventSource,
    fx: &Value,
    name: &str,
    want: &Value,
) -> Option<String> {
    let span = fixture_span(fx);
    let model =
        analyzer_pipeline::read::uncertainty_model(source, "reader", &span, &Params::default())
            .expect("model");
    let ledger = analyzer_pipeline::degradation_io::ledger(
        source,
        "reader",
        &span,
        &model,
        &source
            .segment_opens_at_retention_horizon("reader")
            .expect("opens"),
    )
    .expect("ledger");
    let mut mine: Vec<&str> = ledger
        .in_window(&fixture_span(fx))
        .iter()
        .filter_map(|d| match d.cause.class() {
            "sequence_gap" => Some("missing_segment_seq"),
            "drop_window" => Some("dropped_delta"),
            "declared_drop" => Some("segments_dropped"),
            _ => None,
        })
        .collect();
    mine.sort_unstable();
    mine.dedup();
    let want_list: Vec<&str> = want
        .as_array()
        .expect("gap class list")
        .iter()
        .map(|v| v.as_str().expect("class"))
        .collect();
    (mine != want_list).then(|| {
        format!("{name}: detected gap classes {mine:?} where the contract expects {want_list:?}")
    })
}

/// Grade the staleness family: every run's verdict, in the corpus's vocabulary.
fn grade_staleness(
    source: &dyn analyzer_analysis::events::EventSource,
    fx: &Value,
    name: &str,
    want: &Value,
) -> Option<String> {
    let got = analyzer_pipeline::read::run_verdicts(source, "reader", &fixture_span(fx))
        .expect("run verdicts");
    // the corpus names verdicts in its own vocabulary; one mapping, total, so a new
    // variant fails here rather than matching nothing silently
    let mine: std::collections::BTreeMap<String, &str> = got
        .into_iter()
        .map(|(run, v)| {
            let s = match v {
                RunStaleness::Live => "live",
                RunStaleness::Departed => "departed",
                RunStaleness::Unexplained => "unexplained",
                RunStaleness::NoBasis => "no_basis",
            };
            (run, s)
        })
        .collect();
    let want = want.as_object().expect("staleness map");
    let agree = mine.len() == want.len()
        && want
            .iter()
            .all(|(run, v)| mine.get(run.as_str()).copied() == v.as_str());
    (!agree).then(|| format!("{name}: graded runs {mine:?} where the contract expects {want:?}"))
}

/// Spec 5.9: the three figures, graded from the same computation so a reader cannot answer one
/// by a route that contradicts another.
fn grade_figure(
    source: &dyn analyzer_analysis::events::EventSource,
    fx: &Value,
    name: &str,
    family: &str,
    idx: usize,
    want: &Value,
) -> Option<String> {
    let figs = analyzer_pipeline::read::reuse_figures(source, "reader", &fixture_span(fx))
        .expect("reuse figures");
    let mine = [figs.0, figs.1, figs.2][idx];
    let want_countable = want["countable"].as_bool().expect("countable");
    let want_records = want["records"].as_u64();
    let want_tokens = want["tokens"].as_i64();
    if mine.countable == want_countable
        && mine.records == want_records
        && mine.tokens == want_tokens
    {
        return None;
    }
    Some(format!(
        "{name}: {family} is {{countable: {}, records: {:?}, tokens: {:?}}}, corpus says {{countable: {want_countable}, records: {want_records:?}, tokens: {want_tokens:?}}}",
        mine.countable, mine.records, mine.tokens
    ))
}

/// Spec 5.4: narrowing is the only weakening the section permits, so a reader that narrows on
/// an unsound bracket fails here rather than quietly exonerating a span nothing observed.
fn grade_gap_span(
    source: &dyn analyzer_analysis::events::EventSource,
    fx: &Value,
    name: &str,
    want: &Value,
) -> Option<String> {
    let mine =
        analyzer_pipeline::read::gap_span(source, "reader", &fixture_span(fx)).expect("gap span");
    let want_narrowed = want["narrowed"].as_bool().expect("narrowed");
    if !want_narrowed {
        return (mine.narrowed == want_narrowed).then_some(()).map_or(
            Some(format!("{name}: gap_span narrowed, corpus refuses it")),
            |()| None,
        );
    }
    // Reader fixtures are ingested with their own stamps, unrebased, so the corpus values
    // compare directly.
    let want_start = want["start_ms"].as_f64().expect("start_ms");
    let want_end = want["end_ms"].as_f64().expect("end_ms");
    let close = |a: Option<f64>, b: f64| a.is_some_and(|v| (v - b).abs() < 0.5);
    if mine.narrowed && close(mine.start_ms, want_start) && close(mine.end_ms, want_end) {
        return None;
    }
    Some(format!(
        "{name}: gap_span is {{narrowed: {}, start: {:?}, end: {:?}}}, corpus says {{narrowed: true, start: {want_start}, end: {want_end}}}",
        mine.narrowed, mine.start_ms, mine.end_ms
    ))
}

/// Grade the attribution family (spec 2.2, 2.7): everything a segment's header declares once —
/// its provenance keys and its workload class — applied to EVERY record behind it. Graded over
/// the shaped rows the shipped decoder emits, the same rows the real path stamps and every
/// consumer reads — so a reader that takes attribution per record instead of per header, or
/// bleeds one segment's declaration into its neighbour, fails here. A key the contract names
/// lands as its own typed field and an operator key lands in `extra` (the corpus's
/// `provenance` map spans both, its generator predating the bounds' promotion to named
/// fields); the lookup follows the reader's row either way, because the obligation is what a
/// reader APPLIES to each record, not which field it files the value under.
fn grade_attribution(fx: &Value, name: &str, want: &Value) -> Option<String> {
    for q in want.as_array().expect("attribution list") {
        let idx = usize::try_from(q["segment"].as_u64().expect("segment")).expect("index");
        let seg = fx["segments"][idx].as_array().expect("segment records");
        // The same bytes `ingest_fixture` delivered, through the same decoder.
        let mut body = String::new();
        for r in seg {
            body.push_str(&r.to_string());
            body.push('\n');
        }
        let decoded = analyzer_ingest::decode_segment("seg.jsonl", "reader", body.as_bytes())
            .expect("decode the segment");
        let rows: Vec<Value> = decoded
            .shaped
            .lines()
            .map(|l| serde_json::from_str(l).expect("shaped row parses"))
            .collect();
        if rows.len() != seg.len() {
            return Some(format!(
                "{name}: segment {idx} delivered {} records and the reader holds {} rows",
                seg.len(),
                rows.len()
            ));
        }
        // The class, then every declared key: the typed field where the contract names it,
        // the operator bag otherwise. Values compare as the wire's strings, matching how
        // the reader renders a non-string provenance value.
        let want_class = q["workload_class"].as_str().unwrap_or("");
        let render = |v: Option<&Value>| -> String {
            match v {
                Some(Value::String(s)) => s.clone(),
                Some(other) => other.to_string(),
                None => String::new(),
            }
        };
        for row in &rows {
            let got_class = row
                .get("workload_class")
                .and_then(Value::as_str)
                .unwrap_or("");
            if got_class != want_class {
                return Some(format!(
                    "{name}: segment {idx} carries a record with workload_class {got_class:?} \
                     where the contract expects every record to read {want_class:?}"
                ));
            }
            for (k, v) in q["provenance"].as_object().expect("provenance map") {
                let got = match k.as_str() {
                    "heartbeat_secs" | "max_segment_secs" | "key_epoch" => render(row.get(k)),
                    _ => render(row.get("extra").and_then(|e| e.get(k))),
                };
                let expect = render(Some(v));
                if got != expect {
                    return Some(format!(
                        "{name}: segment {idx} carries a record with {k} = {got:?} where the \
                         contract expects every record to read {expect:?}"
                    ));
                }
            }
        }
    }
    None
}
