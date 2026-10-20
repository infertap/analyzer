// SPDX-License-Identifier: AGPL-3.0-only

//! The duplication cells' round-trip pins.

use super::DupCells;
use crate::module::{round_trip_day, Persisted, Row, Value};

const DAY: i64 = 20_703;

fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}

fn row(table: &'static str, key: Vec<Value>, measures: Vec<Value>) -> Row {
    Row {
        table,
        key,
        measures,
        gone: false,
    }
}

/// One row of every table, in the spec's column order, as the fold renders them.
fn one_of_each() -> Vec<Row> {
    let band = || vec![Value::F64(2.5), Value::F64(4.0)];
    let ms = DAY * 86_400_000;
    let secs = DAY * 86_400;
    vec![
        row(
            "replication_minute",
            vec![s("agentic"), s("2"), s("cross"), Value::I64(ms + 60_000)],
            band(),
        ),
        row(
            "replication_hourly",
            vec![s("agentic"), s("2"), s("cross"), Value::I64(ms + 3_600_000)],
            band(),
        ),
        row(
            "replication_daily",
            vec![s("agentic"), s("2"), s("cross"), Value::I64(ms)],
            band(),
        ),
        row(
            "replication_instance_hourly",
            vec![s("i1"), s("2"), s("cross"), Value::DateTime(secs + 3_600)],
            band(),
        ),
        row(
            "replication_instance_minute",
            vec![s("i1"), s("2"), s("cross"), Value::DateTime(secs + 60)],
            band(),
        ),
        row(
            "replication_pairs_day",
            vec![
                s("agentic"),
                Value::I64(DAY),
                s("a"),
                Value::U32(0),
                Value::U32(0),
                s("b"),
                Value::U32(1),
                Value::U32(0),
            ],
            vec![Value::F64(3.5)],
        ),
    ]
}

fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by_key(|r| format!("{r:?}"));
    rows
}

#[test]
fn every_table_s_row_restores_from_its_text_and_renders_back_the_same() {
    let rows = one_of_each();
    let tables = <DupCells as Persisted>::TABLES;
    assert_eq!(rows.len(), tables.len(), "one row per table");
    let mut state = DupCells::default();
    for r in &rows {
        let table = tables
            .iter()
            .find(|t| t.name == r.table)
            .expect("a listed table");
        table.accepts(r).unwrap_or_else(|e| panic!("{e}"));
        let key: Vec<String> = r.key.iter().map(Value::text).collect();
        let measures: Vec<String> = r.measures.iter().map(Value::text).collect();
        let key: Vec<&str> = key.iter().map(String::as_str).collect();
        let measures: Vec<&str> = measures.iter().map(String::as_str).collect();
        Persisted::restore(&mut state, r.table, &key, &measures)
            .unwrap_or_else(|e| panic!("{}: {e}", r.table));
    }
    state.discard_changes();
    assert_eq!(
        sorted(state.render_day(DAY)),
        sorted(rows),
        "render_day is the inverse of restore over the read-back's text"
    );
    assert!(
        state.render_day(DAY + 1).is_empty(),
        "another day renders nothing"
    );
    let back = round_trip_day(&state, DAY).expect("the round trip");
    assert_eq!(back, state, "the state survives its own text");
}

#[test]
fn a_tombstone_renders_the_key_with_its_measures_at_rest() {
    let mut state = DupCells::default();
    state
        .place(
            "replication_minute",
            &["agentic", "2", "cross", "0"],
            &[2.0, 4.0],
        )
        .expect("placed");
    state.clear_day(0);
    let changes = state.drain_changes();
    let rows = DupCells::render(&changes);
    assert_eq!(rows.len(), 1);
    assert!(rows[0].gone);
    assert_eq!(rows[0].measures, vec![Value::F64(0.0), Value::F64(0.0)]);
    assert_eq!(rows[0].key[3], Value::I64(0));
}
