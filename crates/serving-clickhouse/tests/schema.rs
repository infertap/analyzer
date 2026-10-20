// SPDX-License-Identifier: AGPL-3.0-only

//! Storage integration tests against the pinned engine: the parity scan's assertions
//! promoted into the crate's own suite, running over the schema as MIGRATIONS apply it
//! (not as a test harness recreates it). Engine resolution mirrors the parity runner:
//! `CLICKHOUSE_BIN` or `.tools/clickhouse`, refused if it mismatches the pin.

use analyzer_serving_clickhouse::engine::testing;
use analyzer_serving_clickhouse::engine::{migrate, schema_dump, RetentionConfig};

/// Every test here runs against the SHIPPED backend — a database of its own on the
/// suite's shared server. `fresh` returns the guard rather than a bare engine, because the
/// guard is what keeps the server alive and takes the database with it when the test ends.
fn fresh() -> (
    analyzer_serving_clickhouse::engine::Engine,
    testing::TestDatabase,
) {
    testing::database("schema")
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_fresh_database_reaches_v1_by_migrations_only() {
    let (engine, _db) = fresh();
    let newly = migrate(&engine, &RetentionConfig::default()).expect("migrate");
    // Derived from MIGRATIONS, not a literal: a literal here says only "the set has N
    // members", which is a fact lib.rs already states, and it fails on every addition for a
    // reason that has nothing to do with a fresh database. What this asserts is the property
    // the test is named for -- a fresh database applies EVERY migration, in ascending order,
    // by this path alone.
    let all: Vec<u32> = analyzer_serving_clickhouse::engine::MIGRATIONS
        .iter()
        .map(|m| m.version)
        .collect();
    assert_eq!(newly, all);
    assert!(all.windows(2).all(|w| w[0] < w[1]), "out of order: {all:?}");
    let tables = engine
        .run("SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name")
        .expect("tables");
    // no class_rollup_mv, deliberately: the rollup is emitted by the fold
    for expected in [
        "input_batches",
        "class_rollup_daily",
        "class_rollup_hourly",
        // The thrash lens's band dimension,
        // daily-only — past the event horizon the band exists nowhere else
        "band_rollup_daily",
        // The shape-derived tables, one per panel shape that needs
        // one.
        "fleet_inventory",
        "coverage_daily",
        "markers",
        "facts_rollup_daily",
        // The derivation's standing facts as readers see them (emitted frontier, last pass,
        // audited through), stated by the core through the serving port.
        "derivation_status",
        // The pipeline creates no table, so a fresh database carries these before the first
        // tick.
        "schema_migrations",
    ] {
        assert!(
            tables.lines().any(|l| l == expected),
            "missing table {expected}; have:\n{tables}"
        );
    }
    assert!(
        !tables.lines().any(|l| l == "class_rollup_mv"),
        "class_rollup_mv must not exist: the rollup is emitted by the fold"
    );
    assert!(
        !tables.lines().any(|l| l == "records"),
        "the raw records table must not exist: the event tier is the bucket"
    );
    // idempotent: a second run applies nothing
    let again = migrate(&engine, &RetentionConfig::default()).expect("re-migrate");
    assert!(again.is_empty(), "migrations re-applied: {again:?}");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_retention_floor_refuses_with_its_derivation() {
    let (engine, _db) = fresh();
    let err = migrate(
        &engine,
        &RetentionConfig {
            ledger_retention_days: 1,
            rollup_retention_years: 5,
            instance_detail_retention_days: 14,
            minute_retention_days: 7,
            hour_retention_days: 365,
        },
    )
    .expect_err("below-floor must refuse");
    assert!(
        matches!(&err, analyzer_serving_clickhouse::engine::Error::Refused(m) if m.contains("below the floor") && m.contains("examined history")),
        "a below-floor knob is a refusal, and the derivation rides it: {err}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_exhibit_shows_substituted_retention_as_running_state() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    let exhibit = schema_dump(&engine).expect("dump");
    // SHOW CREATE renders the engine's CANONICAL forms (toIntervalDay, not "INTERVAL 30
    // DAY") — the exhibit shows true running state, so the assertions speak canonical too
    assert!(exhibit.contains("toIntervalYear(5)")); // rollup horizon, substituted

    // The per-table horizons are NOT asserted on the whole exhibit, where one table's horizon
    // could satisfy another's assertion: `every_horizon_reads_its_knob_and_the_literal_years_are_named`
    // asks each table for its own DDL.
    assert!(exhibit.contains("workload_class")); // the label dimension is running state
    assert!(exhibit.contains("derivation_status")); // the standing facts visible
    assert!(exhibit.contains(analyzer_serving_clickhouse::engine::MIGRATIONS[0].name));
    // the ledger rides the exhibit
}

/// **Day rows past one year stay day rows, and a mid-month window reads every one of them**,
/// with the DAO's own predicate shape: nothing ages a daily row into a coarser one, so a
/// window starting mid-month reads each day under its own label.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn day_rows_past_a_year_stay_day_rows_and_a_mid_month_window_reads_every_day() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    // Fifty-nine days across two months, all more than a year old and inside the five-year
    // delete horizon.
    for (table, extra_col, extra_val) in [
        ("class_rollup_daily", "", ""),
        ("band_rollup_daily", "band, ", "'<1m', "),
    ] {
        engine
            .run(&format!(
                "INSERT INTO {table} (fleet_id, discriminator, class, {extra_col}workload_class, \
                 semantics_version, instance_id, dp_rank, group_idx, month, bucket, \
                 events, tokens) \
                 SELECT 'f2', 's1000x3600000', 'ETR', {extra_val}'agentic', \
                        'js-1', 'i1', 0, 0, toStartOfMonth(d), d, 1, 10 \
                 FROM (SELECT toDate('2025-01-01') + number AS d FROM numbers(59))"
            ))
            .expect("insert");
        engine
            .run(&format!("OPTIMIZE TABLE {table} FINAL"))
            .expect("optimize");
        // The DAO's day predicate, verbatim in shape: inclusive on the last covered instant.
        let out = engine
            .run(&format!(
                "SELECT count(), sum(events), min(bucket), max(bucket) FROM {table} FINAL \
                 WHERE fleet_id = 'f2' AND bucket >= toDate('2025-01-15') \
                   AND bucket <= toDate('2025-02-28') GROUP BY fleet_id"
            ))
            .expect("select");
        assert_eq!(
            out.trim(),
            "45\t45\t2025-01-15\t2025-02-28",
            "{table}: a window from the 15th must read every day from the 15th; the retired \
             aging clause read one month row labelled the 1st and lost January entirely"
        );
    }
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn rollup_reinsert_replaces_and_never_doubles() {
    // Resolution 9's property through the applied schema: replacement is the only write
    // semantic, so a re-run replaces rather than doubles. Recent day via today() — the
    // TTL-race rule (expired data belongs only where expiry is the assertion).
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    // Named columns, not positional: a future dimension entering the keys must not silently
    // shift what this row means.
    let cols = "(fleet_id, discriminator, class, workload_class, semantics_version, \
                 instance_id, dp_rank, group_idx, month, bucket, events, tokens)";
    let row = "('f3','s1000x3600000','ETR','agentic','js-1','i1',0,0,\
               toStartOfMonth(today()),today(),10,40)";
    engine
        .run(&format!(
            "INSERT INTO class_rollup_daily {cols} VALUES {row}"
        ))
        .expect("insert");
    engine
        .run(&format!(
            "INSERT INTO class_rollup_daily {cols} VALUES {row}"
        ))
        .expect("re-insert");
    engine
        .run("OPTIMIZE TABLE class_rollup_daily FINAL")
        .expect("optimize");
    let out = engine
        .run("SELECT count(), sum(events) FROM class_rollup_daily WHERE fleet_id = 'f3'")
        .expect("select");
    assert_eq!(out.trim(), "1\t10", "re-insert must replace, never double");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn spill_settings_apply_on_every_invocation() {
    // The pin's defaults disable external sort/group-by spill
    // entirely (verified 0/0) — Engine::run must apply the labeled thresholds on every
    // invocation, so a window-scale sort degrades to disk instead of an OOM kill.
    let (engine, _db) = fresh();
    let out = engine
        .run(
            "SELECT name, value FROM system.settings \
             WHERE name IN ('max_bytes_before_external_sort', \
             'max_bytes_before_external_group_by') ORDER BY name",
        )
        .expect("settings");
    let expected = format!(
        "max_bytes_before_external_group_by\t{}\nmax_bytes_before_external_sort\t{}",
        analyzer_serving_clickhouse::engine::EXTERNAL_GROUP_BY_SPILL_BYTES,
        analyzer_serving_clickhouse::engine::EXTERNAL_SORT_SPILL_BYTES
    );
    assert_eq!(
        out.trim(),
        expected,
        "the spill thresholds ride every spawn"
    );
}

/// The crash window between the last applied statement and the ledger row.
///
/// The transport applies a migration statement by statement with the ledger row last, so
/// a kill in between leaves every object created and the migration unledgered. The next
/// start replays it — and before `IF NOT EXISTS`, that replay died on its own
/// half-applied work (`TABLE_ALREADY_EXISTS`), which under `Restart=always` turned the
/// first boot of a replacement VM into a permanent restart loop an operator had to break
/// by hand. Applied-but-unledgered is simulated exactly: migrate, erase the ledger row,
/// migrate again over the existing objects.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn an_applied_but_unledgered_migration_replays_to_completion() {
    let (engine, _db) = analyzer_serving_clickhouse::engine::testing::database("schema-resume");
    let first = migrate(&engine, &RetentionConfig::default()).expect("first application");
    assert!(!first.is_empty(), "a fresh database applies the migration");
    engine
        .run(
            "DELETE FROM schema_migrations WHERE version = 1 \
             SETTINGS lightweight_deletes_sync = 1, mutations_sync = 1",
        )
        .expect("erase the ledger row: the crash window's exact residue");
    let replay = migrate(&engine, &RetentionConfig::default())
        .expect("the replay must pass over its own half-applied work");
    assert_eq!(replay, vec![1], "the migration re-applies and re-ledgers");
    engine
        .run("SELECT count() FROM input_batches")
        .expect("the schema is whole after the replay");
}

/// **Every horizon reads its knob, and any literal year is named.**
///
/// Per table, from the engine's own `create_table_query`, never a substring of the whole
/// exhibit, where one table's horizon could satisfy another's assertion. The literal-year set is
/// asserted EXACTLY, and it is empty, so a new literal fails here by name.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn every_horizon_reads_its_knob_and_the_literal_years_are_named() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    let ddl = |table: &str| {
        engine
            .run(&format!(
                "SELECT create_table_query FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{table}'"
            ))
            .expect("ddl")
    };
    let expect = |table: &str, clause: &str| {
        let d = ddl(table);
        assert!(d.contains(clause), "{table}: wanted {clause:?} in {d}");
        assert!(
            !d.contains("GROUP BY"),
            "{table}: nothing ages rows any more: {d}"
        );
    };
    for t in ["recompute_minute", "band_minute"] {
        expect(t, "TTL bucket + toIntervalDay(7)");
    }
    for t in ["class_rollup_hourly", "band_rollup_hourly"] {
        expect(t, "TTL bucket + toIntervalDay(365)");
    }
    expect(
        "replication_instance_hourly",
        "TTL bucket + toIntervalDay(14)",
    );
    expect("class_rollup_daily", "TTL bucket + toIntervalYear(5)");
    expect("band_rollup_daily", "TTL bucket + toIntervalYear(5)");
    expect("replication_pairs_day", "toIntervalYear(5)");
    expect(
        "markers",
        "TTL toDate(toDateTime(at_ms / 1000)) + toIntervalYear(5)",
    );
    expect(
        "input_key_spaces",
        "TTL toDate(toDateTime(at_ms / 1000)) + toIntervalYear(5)",
    );
    expect("tag_minute", "TTL toDate(bucket) + toIntervalYear(5)");
    expect("restated_hourly", "TTL toDate(bucket) + toIntervalYear(5)");
    // The duplication tables: the minute cells on the minute knob, the hourly
    // rollup on the hour knob — the first producer table on it — the daily on the years.
    expect("replication_minute", "toIntervalDay(7)");
    expect("replication_hourly", "toIntervalDay(365)");
    expect("replication_daily", "toIntervalYear(5)");
    // The rank-grain pair: the minute knob, like every minute table.
    expect("recompute_rank_minute", "TTL bucket + toIntervalDay(7)");
    expect("band_rank_minute", "TTL bucket + toIntervalDay(7)");
    // The pair basis: the years, like the pairs matrix it serves.
    expect("replication_instance_spans_day", "toIntervalYear(5)");
    // The ledger knob: fourteen days, derived, on every relation it governs —
    // the served input facts, the roster and the whale register's two tables — asserted per table,
    // because the instance tier also reads fourteen and a whole-exhibit search would be
    // satisfied by it.
    for t in [
        "input_batches",
        "input_segments",
        "input_drops",
        "fleet_inventory",
        "replication_whale_day",
        "replication_whale_day_total",
    ] {
        expect(t, "toIntervalDay(14)");
    }
    let literal = engine
        .run(
            "SELECT name FROM system.tables WHERE database = currentDatabase() \
             AND create_table_query LIKE '%toIntervalYear(1)%' ORDER BY name",
        )
        .expect("the literal-year set");
    assert_eq!(literal.trim(), "", "no literal year is left in the schema");
}

/// **A horizon in years ages by date, so a long one keeps its rows.** `DateTime` ends in 2106;
/// a horizon computed in it past that overflows, and the TTL deletes every row at the next
/// merge. Migrated at the fixture-wide hundred years, a current row in every table on the years
/// knob must survive a forced merge.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_century_horizon_keeps_current_rows() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::FIXTURE_WIDE).expect("migrate");
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis();
    let seconds = now_ms / 1000;
    for (table, insert) in [
        (
            "markers",
            format!("INSERT INTO markers (fleet_id, at_ms, kind, label) VALUES ('f', {now_ms}, 'deploy', 'x')"),
        ),
        (
            "input_key_spaces",
            format!("INSERT INTO input_key_spaces (fleet_id, incarnation, canary, at_ms) VALUES ('f', 'r', 'c', {now_ms})"),
        ),
        (
            "tag_minute",
            format!("INSERT INTO tag_minute (fleet_id, bucket) VALUES ('f', toDateTime({seconds}))"),
        ),
        (
            "restated_hourly",
            format!("INSERT INTO restated_hourly (fleet_id, bucket) VALUES ('f', toDateTime({seconds}))"),
        ),
        (
            "replication_daily",
            format!("INSERT INTO replication_daily (fleet_id, bucket_ms) VALUES ('f', {now_ms})"),
        ),
        (
            "replication_pairs_day",
            format!("INSERT INTO replication_pairs_day (fleet_id, day) VALUES ('f', {})", seconds / 86_400),
        ),
        (
            "replication_instance_spans_day",
            format!("INSERT INTO replication_instance_spans_day (fleet_id, day) VALUES ('f', {})", seconds / 86_400),
        ),
    ] {
        engine.run(&insert).expect(table);
        engine
            .run(&format!("OPTIMIZE TABLE {table} FINAL"))
            .expect(table);
        assert_eq!(
            engine
                .run(&format!("SELECT count() FROM {table}"))
                .expect(table)
                .trim(),
            "1",
            "{table}: a hundred-year horizon deleted a current row"
        );
    }
}

/// **Every duplication table serves its latest version and hides a tombstone**: the seven
/// `replication_*` tables are keyed
/// replace written whole by the producer's keyed state, not summing tables fed deltas under
/// a token. Two versions of one cell serve the later alone; a tombstone leaves nothing for a
/// read that filters it. Stated once per table so a table that drifted back to summing
/// fails here.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
#[allow(clippy::too_many_lines)] // seven tables under one law: the inventory IS the test
fn every_duplication_table_serves_its_latest_version_and_hides_a_tombstone() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    // One instant for every statement: a minute, hour or day boundary crossed between two
    // inserts would put the versions of one cell on two keys.
    let pin = pinned_now(&engine);
    let meaning = "'f6', 'js-1', 's1000x3600000'";
    for (table, cols, vals, measures, first, later) in [
        (
            "replication_minute",
            "workload_class, copies_band, locality, minute_ms",
            "'', '2', 'cross', intDiv(toUnixTimestamp(now()), 60) * 60000",
            "token_ms, copies_token_ms",
            "10.0, 20.0",
            "7.0, 14.0",
        ),
        (
            "replication_hourly",
            "workload_class, copies_band, locality, bucket_ms",
            "'', '2', 'cross', intDiv(toUnixTimestamp(now()), 3600) * 3600000",
            "token_ms, copies_token_ms",
            "10.0, 20.0",
            "7.0, 14.0",
        ),
        (
            "replication_daily",
            "workload_class, copies_band, locality, bucket_ms",
            "'', '2', 'cross', intDiv(toUnixTimestamp(now()), 86400) * 86400000",
            "token_ms, copies_token_ms",
            "10.0, 20.0",
            "7.0, 14.0",
        ),
        (
            "replication_instance_hourly",
            "instance_id, copies_band, locality, bucket",
            "'i1', '2', 'within', toStartOfHour(now())",
            "token_ms, copies_token_ms",
            "10.0, 20.0",
            "7.0, 14.0",
        ),
        (
            "replication_instance_minute",
            "instance_id, copies_band, locality, bucket",
            "'i1', '2', 'within', toStartOfMinute(now())",
            "token_ms, copies_token_ms",
            "10.0, 20.0",
            "7.0, 14.0",
        ),
        (
            "replication_whale_day",
            "day, identity",
            "today(), 'blk'",
            "n_tokens, dup_token_ms, held_token_ms",
            "16, 10.0, 20.0",
            "16, 7.0, 14.0",
        ),
        (
            "replication_pairs_day",
            "workload_class, day, a_instance, a_dp_rank, a_group_idx, b_instance, b_dp_rank, \
             b_group_idx",
            "'', intDiv(toUnixTimestamp(now()), 86400), 'i0', 0, 0, 'i1', 0, 0",
            "token_ms",
            "10.0",
            "7.0",
        ),
    ] {
        let ddl = engine
            .run(&pin(format!(
                "SELECT create_table_query FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{table}'"
            )))
            .expect("ddl");
        assert!(
            ddl.contains("ReplacingMergeTree(version, gone)"),
            "{table} is not keyed replace by version with a tombstone: {ddl}"
        );
        assert!(
            !ddl.contains("SummingMergeTree"),
            "{table} drifted back to summing: {ddl}"
        );
        for (version, values) in [(2, first), (3, later)] {
            engine
                .run(&pin(format!(
                    "INSERT INTO {table} (fleet_id, semantics_version, discriminator, {cols}, \
                     {measures}, version, gone) \
                     VALUES ({meaning}, {vals}, {values}, {version}, 0)"
                )))
                .expect("a version of the cell");
        }
        let served = engine
            .run(&pin(format!(
                "SELECT {measures} FROM {table} FINAL WHERE fleet_id = 'f6' AND gone = 0"
            )))
            .expect("served");
        assert_eq!(
            served.trim(),
            later.replace(".0", "").replace(", ", "\t"),
            "{table}: the later version is the cell, never the sum of both"
        );
        engine
            .run(&pin(format!(
                "INSERT INTO {table} (fleet_id, semantics_version, discriminator, {cols}, \
                 version, gone) VALUES ({meaning}, {vals}, 4, 1)"
            )))
            .expect("the tombstone");
        let served = engine
            .run(&pin(format!(
                "SELECT count() FROM {table} FINAL WHERE fleet_id = 'f6' AND gone = 0"
            )))
            .expect("served");
        assert_eq!(served.trim(), "0", "{table}: a tombstone hides the key");
    }
}

/// **A cell is its latest version, and a tombstone hides it** (the fold owns the derivation,
/// pipeline 1): on every one of the eight cell tables, a second write of a key under a higher
/// version replaces the first under `FINAL`, and a `gone` row under a higher version still
/// leaves nothing for a read that filters it — the keyed-replace contract every cell read
/// assumes, stated once per table so a table that drifted back to summing fails here.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn every_cell_table_serves_its_latest_version_and_hides_a_tombstone() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    // One instant for every statement: a minute, hour or day boundary crossed between two
    // inserts would put the versions of one cell on two keys.
    let pin = pinned_now(&engine);
    for (table, cols, vals) in [
        (
            "class_rollup_daily",
            "fleet_id, discriminator, class, workload_class, instance_id, dp_rank, group_idx, \
          semantics_version, month, bucket",
            "'f8', 's1000x3600000', 'ETR', 'agentic', 'i1', 0, 0, 'js-1', \
          toStartOfMonth(today()), today()",
        ),
        (
            "class_rollup_hourly",
            "fleet_id, bucket, discriminator, class, workload_class, instance_id, dp_rank, \
          group_idx, semantics_version",
            "'f8', toStartOfHour(now()), 's1000x3600000', 'ETR', 'agentic', 'i1', 0, 0, 'js-1'",
        ),
        (
            "band_rollup_daily",
            "fleet_id, discriminator, class, band, workload_class, instance_id, dp_rank, \
          group_idx, semantics_version, month, bucket",
            "'f8', 's1000x3600000', 'ETR', '<10s', 'agentic', 'i1', 0, 0, 'js-1', \
          toStartOfMonth(today()), today()",
        ),
        (
            "band_rollup_hourly",
            "fleet_id, discriminator, class, band, workload_class, instance_id, dp_rank, \
          group_idx, semantics_version, bucket",
            "'f8', 's1000x3600000', 'ETR', '<10s', 'agentic', 'i1', 0, 0, 'js-1', \
          toStartOfHour(now())",
        ),
        (
            "recompute_minute",
            "fleet_id, semantics_version, discriminator, class, workload_class, bucket",
            "'f8', 'js-1', 's1000x3600000', 'ETR', 'agentic', toStartOfMinute(now())",
        ),
        (
            "band_minute",
            "fleet_id, semantics_version, discriminator, class, band, workload_class, bucket",
            "'f8', 'js-1', 's1000x3600000', 'ETR', '<10s', 'agentic', toStartOfMinute(now())",
        ),
        (
            "recompute_rank_minute",
            "fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, \
             workload_class",
            "'f8', 'js-1', 's1000x3600000', 'i1', 0, toStartOfMinute(now()), 'ETR', 'agentic'",
        ),
        (
            "band_rank_minute",
            "fleet_id, semantics_version, discriminator, instance_id, dp_rank, bucket, class, \
             band, workload_class",
            "'f8', 'js-1', 's1000x3600000', 'i1', 0, toStartOfMinute(now()), 'ETR', '<10s', \
             'agentic'",
        ),
    ] {
        let ddl = engine
            .run(&pin(format!(
                "SELECT create_table_query FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{table}'"
            )))
            .expect("ddl");
        assert!(
            ddl.contains("ReplacingMergeTree(version, gone)"),
            "{table} is not keyed replace by version with a tombstone: {ddl}"
        );
        for (events, tokens, version, gone) in [(3, 30, 1, 0), (5, 50, 2, 0)] {
            engine
                .run(&pin(format!(
                    "INSERT INTO {table} ({cols}, events, tokens, version, gone) \
                     VALUES ({vals}, {events}, {tokens}, {version}, {gone})"
                )))
                .expect("a version of the cell");
        }
        let served = engine
            .run(&pin(format!(
                "SELECT toInt64(events), toInt64(tokens) FROM {table} FINAL \
                 WHERE fleet_id = 'f8' AND gone = 0"
            )))
            .expect("served");
        assert_eq!(
            served.trim(),
            "5\t50",
            "{table}: the later version is the cell, never the sum of both"
        );
        engine
            .run(&pin(format!(
                "INSERT INTO {table} ({cols}, events, tokens, version, gone) \
                 VALUES ({vals}, 0, 0, 3, 1)"
            )))
            .expect("the tombstone");
        let served = engine
            .run(&pin(format!(
                "SELECT count() FROM {table} FINAL WHERE fleet_id = 'f8' AND gone = 0"
            )))
            .expect("served");
        assert_eq!(served.trim(), "0", "{table}: a tombstone hides the key");
    }
}

/// `now()` and `today()` in a test's statements, frozen at the instant this reads, so every
/// statement of the test names the same minute, hour and day.
fn pinned_now(engine: &analyzer_serving_clickhouse::engine::Engine) -> impl Fn(String) -> String {
    let at = engine
        .run("SELECT toUnixTimestamp(now())")
        .expect("the engine's now")
        .trim()
        .to_string();
    move |sql: String| {
        sql.replace("today()", &format!("toDate(toDateTime({at}))"))
            .replace("now()", &format!("toDateTime({at})"))
    }
}

/// **Two versions of one cell, and the later wins whatever the insert order** — the
/// single-writer law the fold's keyed replace rests on, where the summing tables held the
/// cross-writer law (two attempts, one total). A version that arrives second but is older
/// must not overwrite the newer cell.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_later_version_of_a_cell_wins_whatever_order_it_arrives_in() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    // One instant for every statement: a minute, hour or day boundary crossed between two
    // inserts would put the versions of one cell on two keys.
    let pin = pinned_now(&engine);
    for (events, tokens, version) in [(9, 90, 7), (3, 30, 2)] {
        engine
            .run(&pin(format!(
                "INSERT INTO class_rollup_daily (fleet_id, discriminator, class, \
                 workload_class, instance_id, dp_rank, group_idx, semantics_version, month, \
                 bucket, events, tokens, version, gone) \
                 VALUES ('f9', 's1000x3600000', 'ETR', 'agentic', 'i1', 0, 0, 'js-1', \
                         toStartOfMonth(today()), today(), {events}, {tokens}, {version}, 0)"
            )))
            .expect("one version");
    }
    let served = engine
        .run(
            "SELECT toInt64(events), toInt64(tokens), toInt64(version) FROM class_rollup_daily \
             FINAL WHERE fleet_id = 'f9' AND gone = 0",
        )
        .expect("served");
    assert_eq!(
        served.trim(),
        "9\t90\t7",
        "the higher version is the cell, though it was written first"
    );
}

/// **No table partitions by fleet**: in a cell of many fleets a
/// fleet-first partition key is thousands of partitions per table, and nothing on the
/// product path drops a fleet partition. Fleet stays first in every sort key instead.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn no_table_partitions_by_fleet() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    let by_fleet = engine
        .run(
            "SELECT name FROM system.tables WHERE database = currentDatabase() \
             AND partition_key LIKE '%fleet_id%' ORDER BY name",
        )
        .expect("partition keys");
    assert_eq!(by_fleet.trim(), "", "partitioned by fleet: {by_fleet}");
}

/// **Every knob moves its tables at apply, and no horizon escapes the knobs** (grain ladder
/// section 4.3.1). Non-default values on all five knobs, `apply_retention`, then every mapped table's
/// running DDL carries the new interval; and the set of tables with any TTL is exactly the map
/// plus the three named exceptions, so a table cannot grow a horizon nothing can move.
#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn every_knob_moves_its_tables_on_apply() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).expect("migrate");
    let cfg = RetentionConfig {
        ledger_retention_days: 10,
        rollup_retention_years: 2,
        instance_detail_retention_days: 16,
        minute_retention_days: 4,
        hour_retention_days: 20,
    };
    analyzer_serving_clickhouse::engine::apply_retention(&engine, &cfg).expect("apply");
    for (table, _, knob) in analyzer_serving_clickhouse::engine::RETENTION_TTLS {
        let ddl = engine
            .run(&format!(
                "SELECT create_table_query FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{table}'"
            ))
            .expect("ddl");
        let (n, unit) = knob.value(&cfg);
        let want = match unit {
            "YEAR" => format!("toIntervalYear({n})"),
            _ => format!("toIntervalDay({n})"),
        };
        assert!(
            ddl.contains(&want),
            "{table}: wanted {want} after apply, got {ddl}"
        );
    }
    let with_ttl = engine
        .run(
            "SELECT name FROM system.tables WHERE database = currentDatabase() \
             AND engine != 'MaterializedView' AND create_table_query LIKE '%TTL %' ORDER BY name",
        )
        .expect("tables with a TTL");
    let mut expected: Vec<&str> = analyzer_serving_clickhouse::engine::RETENTION_TTLS
        .iter()
        .map(|(t, _, _)| *t)
        .collect();
    expected.sort_unstable();
    let have: Vec<&str> = with_ttl.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        have, expected,
        "a horizon the knobs do not move, or a mapped table without one"
    );
    let below = RetentionConfig {
        minute_retention_days: 2,
        ..RetentionConfig::default()
    };
    let err = analyzer_serving_clickhouse::engine::apply_retention(&engine, &below)
        .expect_err("below the floor");
    assert!(
        matches!(&err, analyzer_serving_clickhouse::engine::Error::Refused(m) if m.contains("minute_retention_days = 2 is below the floor 3")),
        "{err}"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_superseded_schema_identity_is_rejected_without_rewriting_state() {
    let (engine, _db) = fresh();
    migrate(&engine, &RetentionConfig::default()).unwrap();
    engine.run("ALTER TABLE schema_migrations UPDATE name = '0001_schema' WHERE version = 1 SETTINGS mutations_sync = 1").unwrap();
    let before = engine
        .run("SELECT version, name FROM schema_migrations ORDER BY version")
        .unwrap();
    let error = migrate(&engine, &RetentionConfig::default()).unwrap_err();
    assert!(error.to_string().contains("unsupported schema identity"));
    assert_eq!(
        engine
            .run("SELECT version, name FROM schema_migrations ORDER BY version")
            .unwrap(),
        before
    );
}
