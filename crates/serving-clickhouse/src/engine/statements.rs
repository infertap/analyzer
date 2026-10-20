// SPDX-License-Identifier: AGPL-3.0-only

//! Preparing SQL for the server transport: splitting a script into statements, and
//! escaping the client's own templating syntax.
//!
//! **One entry point, [`prepare`], and every call to the client goes through it.** The
//! escaping is not a step a call site performs; it is a property of the only path that
//! reaches the client. This codebase never uses bind parameters — every value reaches SQL
//! through `quote_literal` — so there is no case where a `?` should survive as a
//! placeholder, and no reason for any call site to be able to decide otherwise. The
//! ratchet in `static_audit::the_server_backend_prepares_every_query` keeps it that way.
//!
//! **The grammar this splitter models, stated because it is a parser and parsers have
//! edges** (the strict-subset discipline the config grammar established):
//!
//! - single-quoted string literals, with `''` and `\'` escapes
//! - double-quoted and backtick-quoted identifiers, with doubling escapes
//! - `--` and `#` line comments, `/* */` block comments
//! - `;` as the only statement separator
//!
//! Anything outside that subset is REFUSED rather than mis-split, because a splitter that
//! guesses produces two invalid fragments from one valid statement and reports it as a
//! syntax error in the customer's data. Dollar-quoted strings (`$$…$$`, `$tag$…$tag$`)
//! are the named landmine: `ClickHouse` accepts them, our migrations do not use them, and a
//! `;` inside one would split wrongly — so their presence is an error with a pointer to
//! this module, not a silent miscut.
//!
//! Both steps exist because of properties of the transport, verified at the pin rather than
//! assumed:
//!
//! - **The HTTP interface refuses multi-statement bodies.** A whole migration file sent as
//!   one body answers `Syntax error (Multi-statements are not allowed)` and executes
//!   nothing: it fails at parse time, so there is no partial-application hazard to reason
//!   about. Statements are therefore sent one at a time, in order.
//! - **The client treats `?` in raw SQL as a bind placeholder**, and `??` is its escape.
//!   Unescaped, a customer's fleet id or workload pattern containing a question mark would
//!   fail with "unbound query argument", a divergence that only shows up on someone's
//!   data. This is the transport's version of the boundary law that already governs
//!   quoting: no SQL reaches the client unescaped.

use crate::engine::Error;

/// The one path from our SQL to the client: split into statements, escape each.
///
/// # Errors
/// If the script uses a construct this module's grammar does not model.
pub(crate) fn prepare(sql: &str) -> Result<Vec<String>, Error> {
    if let Some(construct) = unsupported_construct(sql) {
        return Err(Error::Refused(format!(
            "refusing to send SQL containing {construct}: the server backend splits \
             scripts into statements with a grammar that does not model it (see \
             crates/serving-clickhouse/src/engine/statements.rs), and a splitter that guessed here would \
             cut a valid statement in half. Rewrite the statement or extend the grammar \
             deliberately."
        )));
    }
    Ok(split_statements(sql)
        .into_iter()
        .map(escape_client_placeholders)
        .collect())
}

/// Escape the client's placeholder syntax so raw SQL survives it verbatim.
fn escape_client_placeholders(sql: &str) -> String {
    sql.replace('?', "??")
}

/// Constructs outside the stated grammar, refused rather than mis-split.
///
/// Dollar quoting is the one that exists today: `$$text$$` and `$tag$text$tag$` are valid
/// `ClickHouse` string literals in which a `;` is content, and this splitter would treat it
/// as a boundary. Detected conservatively — a bare `$` in an identifier or a number is
/// not enough, an opening delimiter is.
fn unsupported_construct(sql: &str) -> Option<&'static str> {
    let bytes = sql.as_bytes();
    for (i, byte) in bytes.iter().enumerate() {
        if *byte != b'$' {
            continue;
        }
        let mut j = i + 1;
        while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
            j += 1;
        }
        if bytes.get(j) == Some(&b'$') {
            return Some("a dollar-quoted string ($$…$$ or $tag$…$tag$)");
        }
    }
    None
}

/// Split a script into individual statements, respecting SQL's quoting and comments.
///
/// A naive split on `;` is wrong in a way that would corrupt data rather than fail
/// loudly: a semicolon inside a string literal is content, and one inside a comment is
/// nothing at all. Empty statements (a trailing `;`, a comment-only tail) are dropped —
/// the server refuses an empty body, and a script's trailing `;` is not a statement.
pub(crate) fn split_statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            // string literal: '' is an escaped quote inside one, and so is \'
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    match bytes[i] {
                        b'\\' => i += 2,
                        b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
                        b'\'' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
            }
            // quoted identifiers: backtick and double quote, both with doubling escapes
            q @ (b'`' | b'"') => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == q {
                        if bytes.get(i + 1) == Some(&q) {
                            i += 2;
                        } else {
                            i += 1;
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            // line comments: `--` and ClickHouse's `#`
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'#' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            b';' => {
                push_statement(&mut statements, &sql[start..i]);
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    push_statement(&mut statements, &sql[start..]);
    statements
}

fn push_statement<'a>(out: &mut Vec<&'a str>, candidate: &'a str) {
    let trimmed = candidate.trim();
    if !trimmed.is_empty() && !is_only_comments(trimmed) {
        out.push(trimmed);
    }
}

/// Whether a candidate statement carries no SQL at all — the tail after the last `;` in a
/// file that ends with a comment.
fn is_only_comments(candidate: &str) -> bool {
    let mut rest = candidate.trim();
    loop {
        if let Some(after) = rest.strip_prefix("--").or_else(|| rest.strip_prefix('#')) {
            rest = after.find('\n').map_or("", |n| &after[n + 1..]).trim();
        } else if let Some(after) = rest.strip_prefix("/*") {
            rest = after.find("*/").map_or("", |n| &after[n + 2..]).trim();
        } else {
            return rest.is_empty();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{escape_client_placeholders, prepare, split_statements};

    #[test]
    fn preparing_is_the_only_path_and_it_escapes_every_statement() {
        // The property the ratchet protects: escaping is not something a call site does,
        // it is what `prepare` IS. Two statements in, two escaped statements out.
        let prepared = prepare("SELECT 'a?b'; SELECT 'c?d'").expect("prepare");
        assert_eq!(prepared, vec!["SELECT 'a??b'", "SELECT 'c??d'"]);
    }

    #[test]
    fn a_construct_outside_the_grammar_is_refused_not_guessed_at() {
        // Dollar quoting is valid ClickHouse and invisible to this splitter: the `;`
        // inside would become a boundary and cut a valid statement in half. Refusing
        // names the construct and points at the grammar.
        let err = prepare("SELECT $$a;b$$").expect_err("must refuse");
        assert!(
            matches!(&err, crate::engine::Error::Refused(m) if m.contains("dollar-quoted")),
            "{err}"
        );
        let err = prepare("SELECT $tag$a;b$tag$").expect_err("must refuse");
        assert!(
            matches!(&err, crate::engine::Error::Refused(m) if m.contains("dollar-quoted")),
            "{err}"
        );
        // a lone `$` is not a delimiter and must not trip the guard
        assert!(prepare("SELECT 'costs $5' AS a").is_ok());
    }

    #[test]
    fn hash_comments_are_comments_too() {
        // ClickHouse accepts `#` line comments; unmodeled, a `;` inside one would split.
        assert_eq!(
            split_statements("# a; comment\nSELECT 1;\nSELECT 2"),
            vec!["# a; comment\nSELECT 1", "SELECT 2"]
        );
        assert!(split_statements("# nothing but a comment\n").is_empty());
    }

    #[test]
    fn a_semicolon_inside_a_literal_is_content_not_a_boundary() {
        // The case that would silently corrupt: splitting here sends two invalid
        // fragments instead of one valid statement, and the failure would look like bad
        // data rather than a bad splitter.
        let sql = "INSERT INTO t VALUES ('a;b'); SELECT 1";
        assert_eq!(
            split_statements(sql),
            vec!["INSERT INTO t VALUES ('a;b')", "SELECT 1"]
        );
        assert_eq!(
            split_statements("SELECT 'it''s; fine' AS a; SELECT 2"),
            vec!["SELECT 'it''s; fine' AS a", "SELECT 2"]
        );
        assert_eq!(
            split_statements(r"SELECT 'back\'; slash' AS a; SELECT 2"),
            vec![r"SELECT 'back\'; slash' AS a", "SELECT 2"]
        );
        assert_eq!(
            split_statements("SELECT 1 AS `odd;name`; SELECT 2"),
            vec!["SELECT 1 AS `odd;name`", "SELECT 2"]
        );
    }

    #[test]
    fn comments_are_not_statements_and_do_not_hide_boundaries() {
        let sql = "-- a header; with a semicolon\nCREATE TABLE a (x UInt8);\n\
                   /* block; comment */\nCREATE TABLE b (x UInt8);\n-- trailing\n";
        assert_eq!(
            split_statements(sql),
            vec![
                "-- a header; with a semicolon\nCREATE TABLE a (x UInt8)",
                "/* block; comment */\nCREATE TABLE b (x UInt8)",
            ],
            "comments ride with their statement; a comment-only tail is not sent"
        );
        assert!(split_statements("-- nothing but a comment\n").is_empty());
        assert!(split_statements("   \n\t ").is_empty());
        assert!(split_statements(";;;").is_empty());
    }

    #[test]
    fn a_single_statement_survives_unchanged() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
        assert_eq!(split_statements("SELECT 1;"), vec!["SELECT 1"]);
    }

    #[test]
    fn question_marks_are_escaped_for_the_client_never_dropped() {
        // A fleet id or workload pattern with a question mark is ordinary customer data.
        // Unescaped it becomes an "unbound query argument" on the server path only.
        assert_eq!(
            escape_client_placeholders("SELECT * FROM t WHERE fleet_id = 'prod?1'"),
            "SELECT * FROM t WHERE fleet_id = 'prod??1'"
        );
        assert_eq!(escape_client_placeholders("SELECT 1"), "SELECT 1");
        assert_eq!(escape_client_placeholders("a?b?c"), "a??b??c");
    }
}
