// SPDX-License-Identifier: AGPL-3.0-only

//! The keyed driver against what the classification pass published (`docs/pipeline.md`):
//! over every gait fixture, wave by wave, the class and band cells the driver
//! published (each key's last whole value) are the cells the pass published, whether the log is
//! settled just past its last record or far past it. Settled just past it, each source's covered
//! time on each day is the pass's too, the denominator aside (see `agree`).
//!
//! What the pass published is evidence committed under `fixtures/keyed-parity/`, one file per
//! fixture, its instants as offsets from the fixture's base day. The pass is gone from this
//! tree; the README there names the commit that produced the files and how to reproduce them.

use analyzer_analysis::emitted::{Cells, COVERAGE_DAILY};
use analyzer_analysis::module::{Persisted, Row, Value as Measure};
use analyzer_analysis::Params;
use analyzer_pipeline::keyed::{
    Driver, Emission, FillReader, PlacementCompute, RamState, RepeatCompute, Seeds, SegmentSource,
    Watermark,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

fn driver(
    lake: &analyzer_pipeline::Lake,
    params: &Params,
) -> (Driver<Seeds>, analyzer_pipeline::keyed::Store) {
    let skew = params.delta_skew_ms;
    Driver::open(
        lake,
        params,
        RepeatCompute::new(2, Emission::Watermarked { delay_ms: skew }),
        PlacementCompute::new(
            2,
            Emission::Watermarked {
                delay_ms: skew / 2.0,
            },
            RamState::default,
        ),
    )
    .expect("open")
}

/// One keyed tick over everything `source` committed, settled at `settled_ms`, saved in its own
/// unit: the batch it published.
fn tick(
    (driver, store): (&mut Driver<Seeds>, &mut analyzer_pipeline::keyed::Store),
    lake: &analyzer_pipeline::Lake,
    source: &dyn analyzer_pipeline::source::FillSource,
    params: &Params,
    settled_ms: f64,
) -> analyzer_pipeline::keyed::Batch {
    let reader = FillReader::new(source, "f");
    let segments = SegmentSource::new(lake, &reader);
    let working = lake.working().expect("a working state");
    working.begin(lake).expect("begin");
    driver
        .tick(lake, &segments, params, Watermark::At(settled_ms))
        .expect("keyed tick");
    let next = store.tick() + 1;
    let batch = driver.save(lake, store, next).expect("save");
    lake.end_unit(working.position()).expect("end");
    batch
}

/// What one side published for a fixture: the class and band cells, and each source-day's
/// covered and stated milliseconds, instants as offsets from the fixture's base.
#[derive(Debug, Default, PartialEq)]
struct Published {
    cells: Vec<String>,
    coverage: BTreeMap<String, (f64, f64)>,
}

impl Published {
    /// From rows, as sent: the latest per key, tombstones left out.
    fn of(rows: &[Row], base_ms: f64) -> Self {
        let cell_tables: Vec<&str> = Cells::TABLES.iter().map(|t| t.name).collect();
        let mut cells: Vec<String> = rows
            .iter()
            .filter(|r| !r.gone && cell_tables.contains(&r.table))
            .map(|r| normal(r, base_ms))
            .collect();
        cells.sort();
        let f = |v: &Measure| match v {
            Measure::F64(x) => *x,
            other => panic!("not a float: {other:?}"),
        };
        let coverage = rows
            .iter()
            .filter(|r| !r.gone && r.table == COVERAGE_DAILY.name)
            .map(|r| (key(r, base_ms), (f(&r.measures[0]), f(&r.measures[1]))))
            .collect();
        Self { cells, coverage }
    }

    fn from_json(v: &Value) -> Self {
        Self {
            cells: v["cells"]
                .as_array()
                .expect("cells")
                .iter()
                .map(|c| c.as_str().expect("a cell").to_string())
                .collect(),
            coverage: v["coverage"]
                .as_object()
                .expect("coverage")
                .iter()
                .map(|(k, cw)| {
                    let n = |i: usize| cw[i].as_f64().expect("a figure");
                    (k.clone(), (n(0), n(1)))
                })
                .collect(),
        }
    }
}

/// A row's key as text, a date or a time as its offset from `base_ms`. A `month` column is left
/// out: it is a function of the bucket and does not move with the base.
fn key(row: &Row, base_ms: f64) -> String {
    let table = Cells::TABLES
        .iter()
        .chain([&COVERAGE_DAILY])
        .find(|t| t.name == row.table)
        .expect("a declared table");
    #[allow(clippy::cast_possible_truncation)] // the base is a whole day
    let (base_day, base_secs) = ((base_ms / 86_400_000.0) as i64, (base_ms / 1_000.0) as i64);
    let mut parts = vec![row.table.to_string()];
    for (column, value) in table.key.iter().zip(&row.key) {
        if column.name == "month" {
            continue;
        }
        parts.push(match value {
            Measure::Date(d) => format!("{}=day{:+}", column.name, d - base_day),
            Measure::DateTime(t) => format!("{}=t{:+}", column.name, t - base_secs),
            other => format!("{}={}", column.name, other.text()),
        });
    }
    parts.join("|")
}

/// A row as text: its key ([`key`]) and its measures.
fn normal(row: &Row, base_ms: f64) -> String {
    let measures: Vec<String> = row.measures.iter().map(Measure::text).collect();
    format!("{} -> {}", key(row, base_ms), measures.join("|"))
}

/// The committed record of what the pass published for fixture `name`.
fn golden_path(name: &str) -> std::path::PathBuf {
    harness::repo_root()
        .join("fixtures/keyed-parity")
        .join(format!("{name}.json"))
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine pipeline"]
fn the_keyed_driver_holds_the_cells_the_pass_published_over_every_gait_fixture() {
    let dir = harness::repo_root().join("fixtures/gait");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("fixtures/gait")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    let params = Params::default();
    // Settled just past the last record, and far past it: the second puts every later wave
    // behind the classified edge, through the correction path.
    for past in [1.0, params.examined_history_ms] {
        for path in &files {
            hold(path, past, &params);
        }
    }
}

/// One gait fixture, wave by wave through the pass and the driver, the driver settled `past`
/// its last record: what each published agrees.
fn hold(path: &std::path::Path, past: f64, params: &Params) {
    let case: Value = serde_json::from_str(&std::fs::read_to_string(path).expect("read"))
        .expect("fixture parses");
    let name = path
        .file_stem()
        .and_then(|s| s.to_str())
        .expect("stem")
        .to_string();
    let base = harness::rebase_offset();
    let root = harness::fresh_dir(&format!("{name}-keyed"));
    let source = harness::source_over_tree(&format!("{name}-keyed"), root.clone(), base);
    let (engine, _db) = harness::fresh_engine();
    let mut days = BTreeSet::new();
    let mut last_at = f64::NEG_INFINITY;
    let mut keyed: Option<Driver<Seeds>> = None;
    let mut sent: BTreeMap<String, Row> = BTreeMap::new();
    let mut store = None;
    for (i, wave) in case["waves"].as_array().expect("waves").iter().enumerate() {
        let records: Vec<Value> = wave["records"].as_array().expect("records").clone();
        for r in &records {
            if let Some(at) = r.get("at_ms").and_then(Value::as_f64) {
                days.insert(analyzer_analysis::emitted::day_index(at + base));
                last_at = last_at.max(at + base);
            }
        }
        let (tree, _) = harness::plant_tree(&records, base, &format!("{name}-w{i}"));
        let planted = harness::planted_files(&tree);
        for (file, bytes) in &harness::copy_planted(&planted, &tree, &root, &format!("w{i}")) {
            harness::ingest_bytes(&engine, "f", file, bytes).expect("ingest");
        }
        let lake = harness::lake(&engine, "f");
        if keyed.is_none() {
            let (d, s) = driver(&lake, params);
            keyed = Some(d);
            store = Some(s);
        }
        // The pass classified everything committed: the driver is told the log is settled
        // past its last record.
        let batch = tick(
            (
                keyed.as_mut().expect("driver"),
                store.as_mut().expect("store"),
            ),
            &lake,
            &source,
            params,
            last_at + past,
        );
        // What the driver published: each key's last whole value.
        for row in batch.classes.iter().chain(&batch.coverage) {
            sent.insert(format!("{} {:?}", row.table, row.key), row.clone());
        }
    }

    agree(
        &name,
        &Published::of(&sent.into_values().collect::<Vec<_>>(), base),
        past,
    );
    eprintln!("keyed equivalence: {name} holds, settled {past} ms past its last record");
}

/// What the driver published against the committed record of what the pass published.
fn agree(name: &str, keyed: &Published, past: f64) {
    let golden = Published::from_json(
        &serde_json::from_str(&std::fs::read_to_string(golden_path(name)).expect("the record"))
            .expect("the record parses"),
    );
    assert!(
        !keyed.cells.is_empty(),
        "{name}: the driver published no class cells"
    );
    assert_eq!(
        keyed.cells, golden.cells,
        "{name}: the class and band cells"
    );
    if past <= 1.0 {
        // Coverage: each source's covered time on each day is the pass's, the stretch before
        // its first heartbeat attested by an agent start included. The keyed statement ends
        // at the settled edge, where the pass states the unattested rest of the day
        // uncovered: its denominator is never the larger.
        assert_eq!(
            keyed.coverage.keys().collect::<Vec<_>>(),
            golden.coverage.keys().collect::<Vec<_>>(),
            "{name}: the source-days coverage states"
        );
        for (key, (covered, window)) in &keyed.coverage {
            let (pass_covered, pass_window) = golden.coverage[key];
            assert!(
                (covered - pass_covered).abs() < 1.0 && *window <= pass_window,
                "{name} {key}: keyed ({covered}, {window}), pass ({pass_covered}, {pass_window})"
            );
        }
    }
}
