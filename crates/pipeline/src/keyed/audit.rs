// SPDX-License-Identifier: AGPL-3.0-only

//! The sampled audit (`docs/pipeline.md`): each tick, a few of the blocks it classified are
//! re-derived from the log partitioned by block and compared with what the tick stated for them.
//!
//! Repeat compute and a block's re-fold run one classification over the same records: one on the
//! fleet's lanes as the log grows, the other on one lane over the block's history
//! (`keyed_refold` holds them equal over a whole run). The audit is that comparison taken by the
//! running deployment. A disagreement is a defect in the keyed path (state carried wrong across a
//! restore, a lane's order, a log object lost), never a difference to absorb. The blocks are
//! picked by a hash of the block and the tick's edge, so each tick samples others and a replay of
//! the tick samples the same.
//!
//! A block a correction path re-derived in the same tick is not sampled: the correction replaced
//! the tick's facts for it, and the correction is itself a re-fold.

use super::refold::{history_of, refold_block_under, Models};
use crate::Error;
use analyzer_analysis::emitted::ClassifiedFact;
use analyzer_analysis::{Params, Window};
use std::collections::{BTreeMap, BTreeSet};

/// How many blocks a tick re-derives.
pub const SAMPLES_PER_TICK: usize = 4;

/// One engine id in this many is in the evict audit's sample: the ids whose hash it divides.
pub const EVICT_SAMPLE_MODULUS: u64 = 1024;

/// One store the two derivations state differently.
#[derive(Debug, Clone, PartialEq)]
pub struct Disagreement {
    /// The block.
    pub block: String,
    /// The store's holder.
    pub holder: analyzer_analysis::intervals::Holder,
    /// The store's instant, epoch milliseconds.
    pub at_ms: f64,
    /// What the tick stated; `None` where it stated no fact for the store.
    pub stated: Option<Statement>,
    /// What the re-fold states.
    pub refolded: Option<Statement>,
}

/// What a derivation states about one store.
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    /// The class, as its spelling on the wire.
    pub class: String,
    /// The gap to the prior close, milliseconds, where there was one.
    pub gap_ms: Option<f64>,
    /// The store's tokens.
    pub n_tokens: i64,
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "block {} on {}/{}/{} at {:.0}: the tick stated {:?}, the re-fold {:?}",
            self.block,
            self.holder.instance,
            self.holder.dp_rank,
            self.holder.group_idx,
            self.at_ms,
            self.stated,
            self.refolded,
        )
    }
}

/// A tick's audit.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Audit {
    /// Blocks re-derived.
    pub blocks: u64,
    /// Their stores the tick classified.
    pub stores: u64,
    /// The stores the two derivations state differently.
    pub disagreements: Vec<Disagreement>,
}

/// Where the audit stands: the edge through which every sampled block agreed, and where the
/// first disagreement was found. A disagreement holds the edge there until the derived state is
/// derived afresh: what the keyed path stated after a defect is not audited by later agreement.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AuditEdge {
    /// Through where every sampled block agreed, epoch milliseconds.
    pub audited_to_ms: Option<f64>,
    /// The start of the window whose sample first disagreed, epoch milliseconds.
    pub disagreed_at_ms: Option<f64>,
}

impl AuditEdge {
    /// The edge after a tick that classified `window` and audited it as `audit`.
    #[must_use]
    pub fn after(self, window: Window, audit: &Audit) -> Self {
        match (self.disagreed_at_ms, audit.disagreements.is_empty()) {
            (Some(_), _) => self,
            (None, true) => Self {
                audited_to_ms: Some(window.end_ms),
                disagreed_at_ms: None,
            },
            (None, false) => Self {
                audited_to_ms: self.audited_to_ms,
                disagreed_at_ms: Some(window.start_ms),
            },
        }
    }
}

/// A fact's key as compared: its holder, instant and key epoch.
type FactKey = (analyzer_analysis::intervals::Holder, u64, Option<u32>);

fn by_key<'a>(facts: impl Iterator<Item = &'a ClassifiedFact>) -> BTreeMap<FactKey, Statement> {
    facts
        .map(|f| {
            (
                (f.holder.clone(), f.at_ms.to_bits(), f.key_epoch),
                Statement {
                    class: f.class.as_str().to_string(),
                    gap_ms: f.gap_ms,
                    n_tokens: f.n_tokens,
                },
            )
        })
        .collect()
}

/// The blocks a tick re-derives: of the blocks `facts` names that `corrected` does not, the
/// [`SAMPLES_PER_TICK`] with the lowest hash of the block and `edge`.
#[must_use]
pub fn sample(facts: &[ClassifiedFact], edge: f64, corrected: &BTreeSet<String>) -> Vec<String> {
    let blocks: BTreeSet<&str> = facts
        .iter()
        .map(|f| f.block.as_str())
        .filter(|b| !corrected.contains(*b))
        .collect();
    let mut ranked: Vec<(u64, &str)> = blocks
        .into_iter()
        .map(|b| {
            (
                crate::spine::identity_hash(&format!("{b}/{}", edge.to_bits())),
                b,
            )
        })
        .collect();
    ranked.sort_unstable();
    ranked
        .into_iter()
        .take(SAMPLES_PER_TICK)
        .map(|(_, b)| b.to_string())
        .collect()
}

/// Re-derive each of `blocks` over `window` and compare it with `facts`, what the tick stated
/// for the window.
///
/// # Errors
/// The log, the working state or the arrivals could not be read.
pub fn audit(
    lake: &analyzer_lake::Lake,
    (facts, window): (&[ClassifiedFact], Window),
    blocks: &[String],
    (params, models): (&Params, &Models),
    unsaved: &super::spine::Written,
) -> Result<Audit, Error> {
    let mut audit = Audit::default();
    if blocks.is_empty() {
        return Ok(audit);
    }
    // One completeness model for every sampled block: they share the window.
    let model = crate::arrivals::classifying_model_through(
        lake,
        &history_of(window, params),
        params,
        None,
    )?;
    for block in blocks {
        let stated = by_key(facts.iter().filter(|f| &f.block == block));
        let refold = refold_block_under(lake, block, window, (params, models), &model, unsaved)?;
        let refolded = by_key(refold.facts.iter());
        audit.blocks += 1;
        audit.stores += stated.len() as u64;
        let keys: BTreeSet<&FactKey> = stated.keys().chain(refolded.keys()).collect();
        for key in keys {
            let (s, r) = (stated.get(key), refolded.get(key));
            if s != r {
                audit.disagreements.push(Disagreement {
                    block: block.clone(),
                    holder: key.0.clone(),
                    at_ms: f64::from_bits(key.1),
                    stated: s.cloned(),
                    refolded: r.cloned(),
                });
            }
        }
    }
    Ok(audit)
}

#[cfg(test)]
mod tests {
    use super::{Audit, AuditEdge, Disagreement, Statement};
    use analyzer_analysis::Window;

    fn window(start_ms: f64, end_ms: f64) -> Window {
        Window { start_ms, end_ms }
    }

    fn disagreeing() -> Audit {
        Audit {
            blocks: 1,
            stores: 1,
            disagreements: vec![Disagreement {
                block: "b".into(),
                holder: analyzer_analysis::intervals::Holder {
                    instance: "i".into(),
                    dp_rank: 0,
                    group_idx: 0,
                },
                at_ms: 15.0,
                stated: None,
                refolded: Some(Statement {
                    class: "cold".into(),
                    gap_ms: None,
                    n_tokens: 16,
                }),
            }],
        }
    }

    #[test]
    fn the_edge_follows_agreement_and_holds_at_the_first_disagreement() {
        let agreed = AuditEdge::default().after(window(0.0, 10.0), &Audit::default());
        assert_eq!(agreed.audited_to_ms, Some(10.0));
        let held = agreed.after(window(10.0, 20.0), &disagreeing());
        assert_eq!(
            (held.audited_to_ms, held.disagreed_at_ms),
            (Some(10.0), Some(10.0))
        );
        // Later agreement does not move it: what followed a defect is not audited by it.
        let after = held.after(window(20.0, 30.0), &Audit::default());
        assert_eq!(after, held);
    }
}
