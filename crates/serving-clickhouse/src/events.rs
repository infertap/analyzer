// SPDX-License-Identifier: AGPL-3.0-only

//! Shapes 4 and 5: the blocks by stored mass, a block's holders, and one holder's timeline.
//!
//! The blocks by stored mass are served cells: per UTC day, the heaviest blocks a fleet stored,
//! from a summary whose memory does not grow with the day (`analyzer_analysis::block_mass`), each
//! figure a bound with its error stated. A block's holders and a holder's timeline are event
//! grain, and no table holds an event: a block's holders are re-derived from the log
//! partitioned by block over its retention (`analyzer_pipeline::keyed::refold_block`), and a
//! holder's timeline is its ring, the newest stores repeat compute classified on it. Each page
//! says which, and how far back that source reaches: a drill that silently returns nothing
//! past its reach reads as "no problem here".

use crate::engine::{quote_literal, sql_i64};
use crate::row::Row;
use crate::Error;
use crate::{Dao, Holder};
use analyzer_analysis::emitted::ClassifiedFact;
use analyzer_analysis::Window;

/// Where a drill's rows came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFrom {
    /// Re-derived from the log partitioned by block, within its retention.
    Log,
    /// The holder's ring: the newest stores repeat compute classified on it.
    Ring,
}

impl ReadFrom {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Log => "log",
            Self::Ring => "ring",
        }
    }
}

/// One page of an event-grain drill, and where the next one starts.
///
/// **`next` is `Some` only when rows remain behind this page.** A cursor issued past the end is an
/// infinite scroll, which is the failure `dao::holders`' own paginator names.
#[derive(Debug, Clone, PartialEq)]
pub struct EventPage<T, C> {
    /// This page's events, in the drill's own order — at most `limit` of them.
    pub rows: Vec<T>,
    /// The page's last row as a cursor; `None` when no rows remain behind this page.
    pub next: Option<C>,
    /// Where the page's rows came from.
    pub read_from: ReadFrom,
}

/// A position in [`Dao::holders_of`]'s order — `(at_ms, instance, dp_rank, group_idx, key_epoch)`.
///
/// **The whole sort key, not a prefix.** One batch stamps ONE timestamp on every event it carries,
/// so `at_ms` alone is not total and a page boundary landing inside a batch would repeat or drop
/// rows. Holder and epoch complete the stored key within this fixed block.
#[derive(Debug, Clone, PartialEq)]
pub struct HolderEventCursor {
    /// The last row's store instant, engine-domain milliseconds — the sort's first key.
    pub at_ms: f64,
    /// The last row's holder triple, which makes the position total inside one batch's stamp.
    pub holder: Holder,
    /// Epoch distinguishes stores of the same block at the same instant; absent sorts first.
    pub key_epoch: Option<u32>,
}

/// A position in [`Dao::timeline`]'s order — `(at_ms, block_id, key_epoch)`, for one fixed holder.
#[derive(Debug, Clone, PartialEq)]
pub struct TimelineCursor {
    /// The last row's store instant, engine-domain milliseconds.
    pub at_ms: f64,
    /// The last row's engine-space block id — the tie-break inside one instant.
    pub block_id: String,
    /// Epoch distinguishes stores of the same block at the same instant; absent sorts first.
    pub key_epoch: Option<u32>,
}

/// One UTC day of the blocks by stored mass: the day's summary and its ranked blocks.
#[derive(Debug, Clone, PartialEq)]
pub struct BlockDay {
    /// The day, as a UTC date string.
    pub day: String,
    /// Tokens every store wrote on the day.
    pub tokens: i64,
    /// The blocks the summary monitors at once.
    pub capacity: u32,
    /// The blocks the day ranks; beyond them the ranking says nothing.
    pub cut: u32,
    /// What any block's figure may overstate by: the day's tokens over the capacity.
    pub error_bound: f64,
    /// The ranked blocks, heaviest upper bound first, at most the requested limit.
    pub blocks: Vec<RankedBlock>,
}

/// One block of a day's ranking. Stored mass is its own register: the tokens stores wrote,
/// never placement's held mass, and the two are never joined.
#[derive(Debug, Clone, PartialEq)]
pub struct RankedBlock {
    /// The engine's own id for the block.
    pub block_id: String,
    /// What the block's stored tokens are at least.
    pub tokens_at_least: i64,
    /// What they are at most.
    pub tokens_at_most: i64,
    /// Whether the block is certainly in the day's true top `cut`: its lower bound clears the
    /// upper bound of the first block below the cut.
    pub certain: bool,
    /// Holders seen storing it since it was monitored: a lower bound. The verb is the claim's
    /// limit: holders that stored it, never holders it was resident on at once.
    pub holders_at_least: u32,
}

/// One holder's store of one block.
#[derive(Debug, Clone, PartialEq)]
pub struct HolderEvent {
    /// The instance that stored the block — the operator's `instance_id` label for the slot.
    pub instance: String,
    /// The data-parallel rank within that instance; with `group_idx` it names the cache.
    pub dp_rank: u32,
    /// The KV cache group within that rank — the last part of the holder triple.
    pub group_idx: u32,
    /// When the store happened, engine-domain milliseconds.
    pub at_ms: f64,
    /// The store's classification bucket.
    pub class: String,
    /// Tokens the store carried.
    pub tokens: u64,
    /// The epoch scoping the block's identity. `None` where the producer declared none.
    pub key_epoch: Option<u32>,
}

/// One store on one holder's timeline.
#[derive(Debug, Clone, PartialEq)]
pub struct TimelineEvent {
    /// When the store happened, engine-domain milliseconds.
    pub at_ms: f64,
    /// The engine's own id for the block stored.
    pub block_id: String,
    /// The store's classification bucket — one of the `Class::as_str` spellings.
    pub class: String,
    /// Tokens the store carried — its `n_tokens`, the mass one store adds to its bucket.
    pub tokens: u64,
    /// Milliseconds since this identity's most recent observed prior close on this holder.
    pub gap_ms: Option<f64>,
    /// The epoch scoping the block's identity. `None` where the producer declared none.
    pub key_epoch: Option<u32>,
}

/// Cut `rows` to `limit`, the cursor at the page's last row when more remain.
///
/// **The `!rows.is_empty()` guard is not defensive padding — without it this PANICS at
/// `limit == 0`**, because `truncate(0)` empties the vector and `last()` is then `None`.
fn cut<T, C>(mut rows: Vec<T>, limit: u32, cursor: impl Fn(&T) -> C) -> (Vec<T>, Option<C>) {
    let limit = limit as usize;
    let more = rows.len() > limit;
    rows.truncate(limit);
    let next = (more && !rows.is_empty())
        .then(|| cursor(rows.last().expect("a non-empty page has a last row")));
    (rows, next)
}

impl Dao<'_> {
    /// The blocks by stored mass over `window`'s days, each day its own ranking with its
    /// summary, at most `limit` blocks a day, latest day first.
    ///
    /// # Errors
    /// Engine failure, or an unparseable row.
    pub fn block_days(&self, window: &Window, limit: u32) -> Result<Vec<BlockDay>, Error> {
        let fleet = quote_literal(&self.fleet_id);
        let meaning = self.meaning_pair();
        let (from, to) = (
            analyzer_analysis::emitted::day_index(window.start_ms),
            analyzer_analysis::emitted::day_index(window.end_ms - 1.0),
        );
        let days = self.engine.run(&format!(
            "SELECT toString(day), toInt64(day), tokens, capacity, cut FROM block_mass_day FINAL \
             WHERE fleet_id = {fleet} AND {meaning} AND gone = 0 \
               AND toInt64(day) >= {from} AND toInt64(day) <= {to} \
             ORDER BY day DESC",
            from = sql_i64(from),
            to = sql_i64(to),
        ))?;
        let mut out = Vec::new();
        for mut day in Row::each("block_mass_day", &days) {
            let date = day.text("day")?;
            let index: i64 = day.parse("day index")?;
            let tokens: i64 = day.parse("tokens")?;
            let capacity: u32 = day.parse("capacity")?;
            let cut = day.parse("cut")?;
            day.end()?;
            let ranked = self.engine.run(&format!(
                "SELECT block_id, tokens_upper, tokens_error, certain, holders_at_least \
                 FROM block_mass_daily FINAL \
                 WHERE fleet_id = {fleet} AND {meaning} AND gone = 0 AND toInt64(day) = {index} \
                 ORDER BY tokens_upper DESC, block_id LIMIT {limit}",
                index = sql_i64(index),
                limit = sql_i64(i64::from(limit)),
            ))?;
            let blocks = Row::each("block_mass_daily", &ranked)
                .map(|mut row| {
                    let block_id = row.text("block_id")?.to_string();
                    let upper: i64 = row.parse("tokens_upper")?;
                    let error: i64 = row.parse("tokens_error")?;
                    let certain = row.text("certain")? == "1";
                    let holders_at_least = row.parse("holders_at_least")?;
                    row.end()?;
                    Ok(RankedBlock {
                        block_id,
                        tokens_at_least: upper - error,
                        tokens_at_most: upper,
                        certain,
                        holders_at_least,
                    })
                })
                .collect::<Result<_, Error>>()?;
            #[allow(clippy::cast_precision_loss)] // token counts stay far below 2^52
            let error_bound = if capacity > 0 {
                tokens as f64 / f64::from(capacity)
            } else {
                0.0
            };
            out.push(BlockDay {
                day: date.to_string(),
                tokens,
                capacity,
                cut,
                error_bound,
                blocks,
            });
        }
        Ok(out)
    }

    /// Which holders stored `block`, and when, over `window`: the block re-derived from the log
    /// partitioned by block, within its retention and before the classified edge.
    ///
    /// # Errors
    /// The log or the working state could not be read.
    pub fn holders_of(
        &self,
        block: &str,
        window: &Window,
        limit: u32,
        after: Option<&HolderEventCursor>,
    ) -> Result<EventPage<HolderEvent, HolderEventCursor>, Error> {
        let facts = self.rederived(block, window)?;
        let order = |r: &HolderEvent| {
            (
                r.at_ms.to_bits(),
                r.instance.clone(),
                r.dp_rank,
                r.group_idx,
                r.key_epoch.map_or(-1, i64::from),
            )
        };
        let mut rows: Vec<HolderEvent> = facts
            .into_iter()
            .map(|f| HolderEvent {
                instance: f.holder.instance,
                dp_rank: f.holder.dp_rank,
                group_idx: f.holder.group_idx,
                at_ms: f.at_ms,
                class: f.class.as_str().to_string(),
                tokens: u64::try_from(f.n_tokens).unwrap_or(0),
                key_epoch: f.key_epoch,
            })
            .collect();
        rows.sort_by_key(order);
        if let Some(c) = after {
            let at = (
                c.at_ms.to_bits(),
                c.holder.instance.clone(),
                c.holder.dp_rank,
                c.holder.group_idx,
                c.key_epoch.map_or(-1, i64::from),
            );
            rows.retain(|r| order(r) > at);
        }
        let (rows, next) = cut(rows, limit, |r| HolderEventCursor {
            key_epoch: r.key_epoch,
            at_ms: r.at_ms,
            holder: Holder {
                instance: r.instance.clone(),
                dp_rank: r.dp_rank,
                group_idx: r.group_idx,
            },
        });
        Ok(EventPage {
            rows,
            next,
            read_from: ReadFrom::Log,
        })
    }

    /// One holder's store timeline over `window`: its ring, the newest stores repeat compute
    /// classified on it, so the page states how far back the ring reaches.
    ///
    /// # Errors
    /// The working state could not be read.
    pub fn timeline(
        &self,
        holder: &Holder,
        window: &Window,
        limit: u32,
        after: Option<&TimelineCursor>,
    ) -> Result<EventPage<TimelineEvent, TimelineCursor>, Error> {
        let ring = analyzer_pipeline::keyed::ring_of(&self.spine.lake(), holder)?;
        let order = |r: &TimelineEvent| {
            (
                r.at_ms.to_bits(),
                r.block_id.clone(),
                r.key_epoch.map_or(-1, i64::from),
            )
        };
        let mut rows: Vec<TimelineEvent> = ring
            .into_iter()
            .filter(|f| f.at_ms >= window.start_ms && f.at_ms < window.end_ms)
            .map(|f| TimelineEvent {
                at_ms: f.at_ms,
                block_id: f.block,
                class: f.class.as_str().to_string(),
                tokens: u64::try_from(f.n_tokens).unwrap_or(0),
                gap_ms: f.gap_ms,
                key_epoch: f.key_epoch,
            })
            .collect();
        rows.sort_by_key(order);
        if let Some(c) = after {
            let at = (
                c.at_ms.to_bits(),
                c.block_id.clone(),
                c.key_epoch.map_or(-1, i64::from),
            );
            rows.retain(|r| order(r) > at);
        }
        let (rows, next) = cut(rows, limit, |r| TimelineCursor {
            key_epoch: r.key_epoch,
            at_ms: r.at_ms,
            block_id: r.block_id.clone(),
        });
        Ok(EventPage {
            rows,
            next,
            read_from: ReadFrom::Ring,
        })
    }

    /// `block`'s stores over `window`, re-derived from the log: the window clipped to the log's
    /// oldest held hour and to the classified edge.
    pub(crate) fn rederived(
        &self,
        block: &str,
        window: &Window,
    ) -> Result<Vec<ClassifiedFact>, Error> {
        let lake = self.spine.lake();
        let reach_from_ms = self.log_reach_from_ms()?;
        let edge = self.status_fact("classified_to_ms")?;
        let (Some(from), Some(edge)) = (reach_from_ms, edge) else {
            return Ok(Vec::new());
        };
        let clipped = Window {
            start_ms: window.start_ms.max(from),
            end_ms: window.end_ms.min(edge),
        };
        if clipped.start_ms >= clipped.end_ms {
            return Ok(Vec::new());
        }
        let models = analyzer_pipeline::keyed::Store::models(&lake)?;
        let refold = analyzer_pipeline::keyed::refold_block(
            &lake,
            block,
            clipped,
            (&self.params, &models),
            &analyzer_pipeline::keyed::Written::default(),
        )?;
        Ok(refold.facts)
    }

    /// The log's oldest held hour, epoch milliseconds: how far back a re-derivation reaches.
    pub(crate) fn log_reach_from_ms(&self) -> Result<Option<f64>, Error> {
        #[allow(clippy::cast_precision_loss)] // hour indexes are small integers
        Ok(
            analyzer_pipeline::keyed::held_from_hour(&self.spine.lake())?
                .map(|hour| hour as f64 * 3_600_000.0),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::cut;

    #[test]
    fn a_page_of_nothing_has_no_cursor_and_a_full_page_resumes_at_its_last_row() {
        let (rows, next) = cut(vec![1, 2, 3], 0, |r| *r);
        assert!(rows.is_empty() && next.is_none(), "limit 0 never panics");
        let (rows, next) = cut(vec![1, 2, 3], 2, |r| *r);
        assert_eq!((rows, next), (vec![1, 2], Some(2)));
        let (rows, next) = cut(vec![1, 2], 2, |r| *r);
        assert_eq!((rows, next), (vec![1, 2], None), "nothing behind it");
    }
}
