// SPDX-License-Identifier: AGPL-3.0-only

//! The transport's own properties, pinned directly against the shipped backend.
//!
//! **Pinned against the engine, not against a second implementation.** The server backend
//! contains two mini-parsers of our own making, and their properties are held here directly.
//! The statement splitter and the `?` escaping do not leave with the
//! oracle, so their pins do not either. They change shape instead: from "both backends
//! agree" to "the property holds," which is the stronger form anyway — a differential test
//! passes when both sides are wrong the same way.
//!
//! One case is deliberately NOT re-homed, and its absence is a decision rather than an
//! oversight: the float-formatting pin asserted that no SECOND renderer existed to
//! disagree with the engine's own. With one backend there is no second renderer to assert
//! about — the property is vacuous, not unproven — and formatting is carried from here by
//! the conformance goldens, which compare bytes against a fixture rather than against
//! another implementation.

use analyzer_serving_clickhouse::engine::testing;

/// A migrated-free scratch database: these tests own their own tables.
fn fresh() -> (
    analyzer_serving_clickhouse::engine::Engine,
    testing::TestDatabase,
) {
    testing::database("transport")
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_question_mark_in_customer_data_survives_the_client_untouched() {
    // The dangerous one. The client reads `?` in raw SQL as a bind placeholder EVEN
    // INSIDE A STRING LITERAL, so an unescaped question mark fails with "unbound query
    // argument". A fleet id or a workload pattern containing one is ordinary customer
    // data, and the failure would appear only on their data, never in our fixtures.
    let (engine, _db) = fresh();
    engine
        .run("CREATE TABLE q (fleet String, note String) ENGINE = MergeTree ORDER BY fleet")
        .expect("create");
    engine
        .run("INSERT INTO q VALUES ('prod?1', 'why??'), ('plain', 'no marks')")
        .expect("insert");

    assert_eq!(
        engine
            .run("SELECT note FROM q WHERE fleet = 'prod?1'")
            .expect("select")
            .trim(),
        "why??",
        "a literal question mark round-trips as data, in both the predicate and the value"
    );
    assert_eq!(
        engine
            .run("SELECT count() FROM q WHERE note LIKE '%?%'")
            .expect("select")
            .trim(),
        "1",
        "and it survives inside a LIKE pattern, where escaping is easiest to get wrong — \
         one row carries question marks, the other deliberately does not"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn semicolons_inside_literals_identifiers_and_comments_are_not_statement_boundaries() {
    // The splitter's reason to exist: the server refuses multi-statement bodies, so our
    // scripts are cut apart before they are sent. A cut in the wrong place turns one valid
    // statement into two invalid fragments, and reports it as bad customer data.
    let (engine, _db) = fresh();
    engine
        .run("CREATE TABLE s (`odd;name` String, note String) ENGINE = MergeTree ORDER BY `odd;name`")
        .expect("a semicolon inside a quoted identifier is not a boundary");

    engine
        .run(
            "-- a header; with a semicolon\n\
             INSERT INTO s VALUES ('a;b', 'semi;colon'); \
             /* a block; comment */ \
             INSERT INTO s VALUES ('it''s;quoted', 'apostrophe')",
        )
        .expect("script with semicolons in comments and literals");

    assert_eq!(
        engine.run("SELECT count() FROM s").expect("select").trim(),
        "2",
        "two statements ran, and neither was cut in half"
    );
    assert_eq!(
        engine
            .run("SELECT note FROM s WHERE `odd;name` = 'a;b'")
            .expect("select")
            .trim(),
        "semi;colon",
        "the semicolons that were content stayed content"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn an_empty_insert_is_a_no_op_rather_than_an_error() {
    // The contract the seam decides, because the engine's two modes disagreed about it:
    // `clickhouse local` refuses an insert with no rows, the server accepts it. A window
    // with nothing to write is ordinary — a quiet hour, a fleet with no stores that day —
    // so no caller should have to know which transport it holds to know that.
    let (engine, _db) = fresh();
    engine
        .run("CREATE TABLE e (x UInt8) ENGINE = MergeTree ORDER BY x")
        .expect("create");

    let written = engine
        .insert_stream("INSERT INTO e FORMAT JSONEachRow", &mut "".as_bytes())
        .expect("an empty insert succeeds");
    assert_eq!(written, 0);
    assert_eq!(
        engine.run("SELECT count() FROM e").expect("select").trim(),
        "0",
        "nothing was written, and nothing failed"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn the_carrier_lands_hostile_rows_intact() {
    // R1's carrier: rows travel as a request body, so the characters that break SQL text
    // are not escaped here — they are DATA. This pins that the boundary between "SQL we
    // build" and "rows we ship" stays where it belongs.
    let (engine, _db) = fresh();
    engine
        .run("CREATE TABLE c (fleet String, at_ms Float64, note String) ENGINE = MergeTree ORDER BY fleet")
        .expect("create");

    let rows = concat!(
        "{\"fleet\":\"prod?1\",\"at_ms\":1755123456789.25,\"note\":\"why?\"}\n",
        "{\"fleet\":\"a;b\",\"at_ms\":0.1,\"note\":\"semi;colon\"}\n",
        "{\"fleet\":\"quote's\",\"at_ms\":-0.0,\"note\":\"back\\\\slash\"}\n",
        "{\"fleet\":\"unicode\",\"at_ms\":1e308,\"note\":\"héllo — 世界\"}\n",
    );
    let written = engine
        .insert_stream("INSERT INTO c FORMAT JSONEachRow", &mut rows.as_bytes())
        .expect("carrier");
    assert_eq!(written, rows.len() as u64);

    assert_eq!(
        engine.run("SELECT count() FROM c").expect("select").trim(),
        "4"
    );
    // TabSeparated escapes backslashes on the way OUT, so the text a caller reads carries
    // `\\` for one stored backslash. That is the format's rule, not a transport artifact —
    // pinned in the form it actually arrives in, so nobody later "fixes" it into a bug.
    assert_eq!(
        engine
            .run("SELECT note FROM c WHERE fleet = 'quote''s'")
            .expect("select")
            .trim(),
        r"back\\slash",
        "a backslash in the DATA is data; the outbound escaping belongs to TabSeparated"
    );
    assert_eq!(
        engine
            .run("SELECT note FROM c WHERE fleet = 'unicode'")
            .expect("select")
            .trim(),
        "héllo — 世界"
    );
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn streaming_and_buffering_deliver_the_same_bytes() {
    // the API has to be a drop-in or it is not a fix: a caller switches on result size,
    // never on expected content.
    let (engine, _db) = fresh();
    engine
        .run(
            "CREATE TABLE b (id UInt32, at_ms Float64, blob String) ENGINE = MergeTree ORDER BY id",
        )
        .expect("create");
    engine
        .run("INSERT INTO b SELECT number, number * 1.5, repeat('x', 32) FROM numbers(50000)")
        .expect("fill");

    let sql = "SELECT id, at_ms, blob FROM b ORDER BY id";
    let buffered = engine.run(sql).expect("buffered");

    let mut streamed = Vec::new();
    let written = engine.run_to_writer(sql, &mut streamed).expect("streamed");
    assert_eq!(
        written,
        streamed.len() as u64,
        "the reported count is what reached the sink"
    );
    assert_eq!(
        String::from_utf8(streamed).expect("utf8"),
        buffered,
        "streaming and buffering must deliver identical bytes"
    );

    // and the streaming path carries the same splitting and escaping as the buffered one
    let mut sink = Vec::new();
    engine
        .run_to_writer("SELECT count() FROM b; SELECT 'q?mark'", &mut sink)
        .expect("multi-statement streaming");
    assert_eq!(String::from_utf8(sink).expect("utf8"), "50000\nq?mark\n");
}

#[test]
#[ignore = "integration: needs the pinned engine. Run: scripts/test.sh engine serving-clickhouse"]
fn a_construct_outside_the_splitters_grammar_is_refused_rather_than_miscut() {
    // The strict-subset rule: the splitter models a stated grammar, and dollar-quoted
    // strings are outside it. A `;` inside one would become a boundary, so their presence
    // is an error naming the construct rather than a silent miscut.
    let (engine, _db) = fresh();
    let error = engine
        .run("SELECT $$a;b$$")
        .expect_err("a dollar-quoted string is refused");
    assert!(
        matches!(&error, analyzer_serving_clickhouse::engine::Error::Refused(m) if m.contains("dollar-quoted")),
        "the refusal names the construct: {error}"
    );
}
