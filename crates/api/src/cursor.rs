// SPDX-License-Identifier: AGPL-3.0-only

//! Opaque, tick-bound cursors — `docs/read-api.md` section 8.
//!
//! What a cursor buys is what happens when **a tick lands mid-scroll**: without the binding,
//! page two is a claim about different cells than page one, presented under one heading, and
//! nothing detects it. That is the quiet-wrong class on a surface whose whole job is saying
//! which tick a figure belongs to.
//!
//! **The consistency contract.** A cursor is bound to five things and refused if any moved:
//!
//! - the tick stamp (`tick_at_ms`, the instant the last tick was applied, which every applied
//!   tick advances) and the classified day;
//! - the durable namespace revision (`analyzer_pipeline::execution::revision`): the boundary the
//!   tick stamp does not draw, advanced by every tick's intent and absent while one is
//!   unapplied, so a read that overlapped a tick's writes is told apart;
//! - the meaning generation (the discriminator);
//! - the query identity: the resolved window, the limit, every filter and the path subject,
//!   so page two cannot be asked under different parameters than page one;
//! - the position: where the previous page stopped, in the surface's own sort order.
//!
//! The binding is checked twice: when the token is parsed, before the read, and again after the
//! page is read (the state's `read_at`), so a publication during the read refuses the
//! page rather than presenting rows from two attempts under one stamp. A refusal is in-band
//! (`cursor_stale`, inside a `200`) with the current tick as its bound, and the consumer's one
//! action is to restart at the first page.
//!
//! **A token that is not a cursor is a different failure.** A cursor from an earlier token
//! format was minted by this API and is stale: restart at the first page. Anything else — the
//! wrong length, not hex, not text, too few fields — was never minted here, and is
//! `request_invalid`, a `400`: a client bug, never a page one.
//!
//! At the ≤500-GPU envelope one page holds the whole holders table, so the goldens page at
//! `limit = 1`: small enough to be multi-page at v1 scale, so both sides run the path where
//! getting it wrong is cheap.

use crate::envelope::Stamp;
use crate::error::{ApiError, Cause};

/// The token's format version. A format change bumps it, so a token of another format is
/// refused as stale — minted by this API, not honoured by this build — rather than misparsed.
const VERSION: &str = "c1.";

/// The longest token this parser reads. A cursor is a few hundred bytes; anything past this
/// is not one, and the bound keeps a hostile query string from costing a decode.
const MAX_TOKEN_BYTES: usize = 4096;

/// The field separator inside the payload: a control character no field carries, so the
/// payload splits without escaping. The position is the last field and may carry anything.
const SEP: char = '\u{1f}';

/// The absent-value spelling for an optional field.
const ABSENT: &str = "-";

/// A position in one surface's own sort order, bound to the tick, the meaning generation
/// and the query it was minted under.
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    /// The tick this page was cut after: the instant it was applied. `None` before the first
    /// tick.
    pub tick_at_ms: Option<f64>,
    /// The publication revision this page was cut between.
    pub revision: u64,
    /// The newest classified day when this page was cut.
    pub classified_day: Option<i64>,
    /// The meaning generation. A parameter change mid-scroll is as much a discontinuity as a
    /// tick, and a page across one would be two measurements under one heading.
    pub discriminator: String,
    /// The query identity this page answers: the resolved window, the limit, the filters and
    /// the subject, digested. Page two under other parameters is a different question.
    pub query: String,
    /// Where the previous page stopped, in whatever form the surface sorts by. Opaque to
    /// everything except the handler that minted it.
    pub position: String,
}

/// The identity of one paged question: everything about the request except the cursor
/// itself, canonically spelled and digested, so two requests that differ in any parameter
/// mint cursors that refuse each other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryIdentity(String);

impl QueryIdentity {
    /// Digest `parts`, each a `name=value` the handler resolved (the window as its instants,
    /// never the relative spelling, so a `14d` page two is bound to the day page one saw).
    #[must_use]
    pub fn of(parts: &[String]) -> Self {
        let canonical = parts.join("\n");
        let digest =
            analyzer_serving_clickhouse::engine::server::sha256_hex_upper(canonical.as_bytes());
        Self(digest[..16].to_ascii_lowercase())
    }

    /// The digest, as the token carries it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Cursor {
    /// Mint a cursor for `position` under `stamp`, for the question `query` identifies.
    #[must_use]
    pub fn mint(stamp: &Stamp, query: &QueryIdentity, position: &str) -> Self {
        Self {
            tick_at_ms: stamp.tick_at_ms,
            // A stamp read while a publication was in progress mints no page: `read_at`
            // refuses the read first. Zero here is the same "no revision", and a page under
            // any real revision refuses it.
            revision: stamp.revision.unwrap_or(0),
            classified_day: stamp.classified_day(),
            discriminator: stamp.discriminator.clone(),
            query: query.as_str().to_string(),
            position: position.to_string(),
        }
    }

    /// Render as an opaque token: the version prefix, then the payload as lowercase hex.
    ///
    /// Hex rather than base64 costs a factor of two on a string nobody reads and saves a
    /// dependency. It is opaque in the sense that matters: a consumer cannot construct one
    /// meaningfully, and the version prefix means a future format change is a refusal rather
    /// than a misparse.
    #[must_use]
    pub fn encode(&self) -> String {
        use std::fmt::Write as _;
        let tick = self
            .tick_at_ms
            .map_or_else(|| ABSENT.to_string(), |ms| format!("{:016x}", ms.to_bits()));
        let classified = self
            .classified_day
            .map_or_else(|| ABSENT.to_string(), |d| d.to_string());
        let payload = format!(
            "{tick}{SEP}{:x}{SEP}{classified}{SEP}{}{SEP}{}{SEP}{}",
            self.revision, self.discriminator, self.query, self.position
        );
        payload.bytes().fold(VERSION.to_string(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
    }

    /// Parse a token and **refuse it if anything it was bound to moved**.
    ///
    /// # Errors
    /// `cursor_stale` when the token was minted under a different tick, publication, meaning
    /// generation or question, or under an earlier token format — every case where the
    /// caller's one action is to restart at page one. `request_invalid` when the token is not
    /// a cursor at all.
    pub fn parse(token: &str, stamp: &Stamp, query: &QueryIdentity) -> Result<Self, ApiError> {
        Decoded::parse(token)?.bind(stamp, query)
    }
}

/// Format validation is independent of the database; revision binding happens later.
#[derive(Debug, Clone)]
pub(crate) enum Decoded {
    Current(Cursor),
    Older,
}
impl Decoded {
    pub fn parse(token: &str) -> Result<Self, ApiError> {
        let invalid = |why: &str| {
            ApiError::new(
                Cause::RequestInvalid,
                format!("the cursor is not one this API minted: {why}"),
            )
        };
        if token.len() > MAX_TOKEN_BYTES {
            return Err(invalid("it is longer than any cursor"));
        }
        let Some(hex) = token.strip_prefix(VERSION) else {
            return if is_versioned(token) {
                Ok(Self::Older)
            } else {
                Err(invalid("it carries no format version"))
            };
        };
        let bytes = decode_hex(hex).ok_or_else(|| invalid("it is not hex"))?;
        let payload = String::from_utf8(bytes).map_err(|_| invalid("it is not text"))?;

        let mut parts = payload.splitn(6, SEP);
        let mut field = |what: &str| parts.next().ok_or_else(|| invalid(what));
        let tick = field("it carries no tick")?;
        let revision = field("it carries no revision")?;
        let classified = field("it carries no classified day")?;
        let discriminator = field("it carries no meaning generation")?;
        let bound_query = field("it carries no question")?;
        let position = field("it carries no position")?;

        let tick_at_ms = match tick {
            ABSENT => None,
            v => Some(
                u64::from_str_radix(v, 16)
                    .map(f64::from_bits)
                    .map_err(|_| invalid("its tick is unreadable"))?,
            ),
        };
        let revision =
            u64::from_str_radix(revision, 16).map_err(|_| invalid("its revision is unreadable"))?;
        let classified_day = match classified {
            ABSENT => None,
            v => Some(
                v.parse()
                    .map_err(|_| invalid("its classified day is unreadable"))?,
            ),
        };

        Ok(Self::Current(Cursor {
            tick_at_ms,
            revision,
            classified_day,
            discriminator: discriminator.to_string(),
            query: bound_query.to_string(),
            position: position.to_string(),
        }))
    }
    pub fn bind(&self, stamp: &Stamp, query: &QueryIdentity) -> Result<Cursor, ApiError> {
        let stale = |why: &str| stale(stamp, why);
        let Self::Current(cursor) = self else {
            return Err(stale("it is from an earlier cursor format"));
        };
        // The binding, enforced. A page cut after a different tick is a claim about different
        // rows; presenting it as page two of this one is the failure the cursor exists to catch.
        if cursor.tick_at_ms.map(f64::to_bits) != stamp.tick_at_ms.map(f64::to_bits)
            || cursor.classified_day != stamp.classified_day()
        {
            return Err(stale("a tick has been applied since it was minted"));
        }
        if Some(cursor.revision) != stamp.revision {
            return Err(stale("a tick has published since it was minted"));
        }
        if cursor.discriminator != stamp.discriminator {
            return Err(stale(
                "the classification parameters changed since it was minted, so its rows are a \
                 different measurement",
            ));
        }
        if cursor.query != query.as_str() {
            return Err(stale(
                "it was minted for a different question: the window, the limit, a filter or \
                 the subject differs from this request's",
            ));
        }
        Ok(cursor.clone())
    }
}

/// The refusal every cursor failure renders, with the current tick as its bound so a console
/// shows the reader what moved.
pub(crate) fn stale(stamp: &Stamp, why: &str) -> ApiError {
    ApiError::new(
        Cause::CursorStale,
        format!("this cursor cannot be honoured: {why}. Restart at the first page."),
    )
    .with_bound(serde_json::json!({
        "current_tick_at_ms": stamp.tick_at_ms,
        "current_classified_day": stamp.classified_day(),
        "current_discriminator": stamp.discriminator,
    }))
}

/// Whether `token` carries a cursor format version at all — `c<digits>.` — which is how a
/// cursor from an earlier format is told from a string that was never a cursor.
fn is_versioned(token: &str) -> bool {
    token
        .strip_prefix('c')
        .and_then(|rest| rest.split_once('.'))
        .is_some_and(|(digits, _)| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

/// Lowercase-hex bytes to bytes, byte-wise: a pair that is not two ASCII hex digits is a
/// refusal, never a slice at a byte offset that might not be a character boundary.
fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    let digits = hex.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return None;
    }
    digits
        .chunks_exact(2)
        .map(|pair| {
            let hi = (pair[0] as char).to_digit(16)?;
            let lo = (pair[1] as char).to_digit(16)?;
            u8::try_from(hi * 16 + lo).ok()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Cursor, QueryIdentity};
    use crate::envelope::Stamp;

    #[allow(clippy::cast_precision_loss)] // day indexes are small integers
    fn stamp(tick_at_ms: Option<f64>, classified: Option<i64>, discriminator: &str) -> Stamp {
        Stamp {
            fleet_id: "f".into(),
            window: Some(analyzer_analysis::Window {
                start_ms: 0.0,
                end_ms: 1.0,
            }),
            revision: Some(40),
            tick_at_ms,
            classified_to_ms: classified.map(|d| d as f64 * analyzer_analysis::DAY_MS + 1.0),
            emitted_to_ms: None,
            covered_to_ms: None,
            audited_to_ms: None,
            settled_through_day: None,
            semantics_version: "js-7",
            discriminator: discriminator.to_string(),
            raw_from_ms: None,
            declared_raw_days: 14,
        }
    }

    fn question(parts: &[&str]) -> QueryIdentity {
        QueryIdentity::of(&parts.iter().map(|p| (*p).to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn a_cursor_round_trips_under_the_stamp_and_question_that_minted_it() {
        let s = stamp(Some(1.5), Some(20_668), "s1000x3600000");
        let q = question(&["window=0,1", "limit=1"]);
        let c = Cursor::mint(&s, &q, "i-001/0/0");
        let back = Cursor::parse(&c.encode(), &s, &q).expect("its own stamp honours it");
        assert_eq!(back.position, "i-001/0/0");
        assert_eq!(back.tick_at_ms, Some(1.5));
    }

    #[test]
    fn a_position_carrying_the_field_separator_survives_whole() {
        // The roster and evidence positions are two fields joined by the separator; a decoder
        // keeping only the first would resume a roster page two at nothing.
        let s = stamp(Some(1.0), Some(20_668), "d");
        let q = question(&["limit=100"]);
        let position = "instance\u{1f}incarnation";
        let back = Cursor::parse(&Cursor::mint(&s, &q, position).encode(), &s, &q)
            .expect("its own stamp honours it");
        assert_eq!(back.position, position);
    }

    #[test]
    fn a_tick_landing_mid_scroll_refuses_the_next_page() {
        // The failure the binding exists for: page two cut after a different tick is a claim
        // about different rows. A tick that moved the tick instant within the same day counts.
        let q = question(&["limit=1"]);
        let minted = stamp(Some(1.0), Some(20_668), "s1000x3600000");
        let token = Cursor::mint(&minted, &q, "i-001/0/0").encode();

        let ticked = stamp(Some(2.0), Some(20_668), "s1000x3600000");
        let err = Cursor::parse(&token, &ticked, &q).expect_err("a newer tick must refuse it");
        assert_eq!(err.cause.as_str(), "cursor_stale");
        assert!(
            err.bound.is_some(),
            "a refusal states its bound; a console shows the reader what moved"
        );
        let next_day = stamp(Some(1.0), Some(20_669), "s1000x3600000");
        assert!(Cursor::parse(&token, &next_day, &q).is_err());
    }

    #[test]
    fn a_parameter_change_mid_scroll_refuses_too() {
        // Same argument one axis over: rows under a different discriminator are a different
        // measurement, and never-sum applies across a page boundary exactly as it does within one.
        let q = question(&["limit=1"]);
        let minted = stamp(Some(1.0), Some(20_668), "s1000x3600000");
        let token = Cursor::mint(&minted, &q, "i-001/0/0").encode();

        let after = stamp(Some(1.0), Some(20_668), "s1000x172800000");
        let err = Cursor::parse(&token, &after, &q).expect_err("a new generation must refuse it");
        assert_eq!(err.cause.as_str(), "cursor_stale");
    }

    #[test]
    fn a_different_question_refuses_the_page() {
        // Page two under another window, limit or filter is a different question, and the
        // position it carries means nothing there.
        let s = stamp(Some(1.0), Some(20_668), "d");
        let token = Cursor::mint(&s, &question(&["window=0,1", "limit=2"]), "p").encode();
        let err = Cursor::parse(&token, &s, &question(&["window=0,2", "limit=2"]))
            .expect_err("another window is another question");
        assert_eq!(err.cause.as_str(), "cursor_stale");
        assert!(Cursor::parse(&token, &s, &question(&["window=0,1", "limit=3"])).is_err());
    }

    #[test]
    fn a_publication_mid_scroll_refuses_the_next_page_even_within_the_same_second() {
        // A publication whose tick instant equals the one page one saw moves the revision
        // alone.
        let q = question(&["limit=1"]);
        let minted = stamp(Some(1.0), Some(20_668), "d");
        let token = Cursor::mint(&minted, &q, "p").encode();
        let mut published = stamp(Some(1.0), Some(20_668), "d");
        published.revision = Some(42);
        let err = Cursor::parse(&token, &published, &q).expect_err("a publication refuses it");
        assert_eq!(err.cause.as_str(), "cursor_stale");
        // And while a publication is in progress no cursor is honoured.
        published.revision = None;
        assert!(Cursor::parse(&token, &published, &q).is_err());
    }

    #[test]
    fn a_token_this_api_did_not_mint_is_a_client_error_not_a_stale_page() {
        let s = stamp(Some(1.0), Some(1), "d");
        let q = question(&[]);
        for bad in [
            "",
            "nonsense",
            "c1.zz",
            "c1.abc",
            "c1.€a",
            "c1.6€",
            "c1.61ff",
            "c1.ff",
            &format!("c1.{}", "61".repeat(3000)),
        ] {
            let err = Cursor::parse(bad, &s, &q).expect_err("refused");
            assert_eq!(
                err.cause.as_str(),
                "request_invalid",
                "{bad:?} was never a cursor; a 400, not a restart"
            );
        }
    }

    #[test]
    fn a_cursor_of_another_format_is_stale_not_invalid() {
        // Minted by this API under another format: the reader's action is the same as for any
        // other stale page, restart, and a console must not surface it as a client bug. The
        // token carries a well-formed payload under another version, so only the version
        // refuses it.
        let s = stamp(Some(1.0), Some(1), "d");
        let q = question(&[]);
        let payload = format!(
            "{:016x}\u{1f}1\u{1f}d\u{1f}{}\u{1f}p",
            1.0f64.to_bits(),
            q.as_str()
        );
        let other = payload.bytes().fold("c2.".to_string(), |mut t, b| {
            use std::fmt::Write as _;
            let _ = write!(t, "{b:02x}");
            t
        });
        let err = Cursor::parse(&other, &s, &q).expect_err("refused");
        assert_eq!(err.cause.as_str(), "cursor_stale", "{other}");
        let err = Cursor::parse("c9.6162", &s, &q).expect_err("refused");
        assert_eq!(err.cause.as_str(), "cursor_stale");
        assert!(super::is_versioned("c1.6162"));
        assert!(!super::is_versioned("c.6162"));
        assert!(!super::is_versioned("cx.6162"));
        assert!(!super::is_versioned("x2.6162"));
    }
}
