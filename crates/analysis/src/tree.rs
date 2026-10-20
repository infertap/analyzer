// SPDX-License-Identifier: AGPL-3.0-only

//! The block-tree substrate: store facts as nodes, `parent_id` as edges, per window.
//!
//! Rules this module owns: **dangling parents are retained as edges to
//! unknown ancestors, never invented and never dropped** — a late `store` observation
//! of the parent reconnects the chain (order-independence: construction is from the
//! windowed fact set, so arrival order cannot matter); a **root** is a node whose
//! `parent_id` is absent (R1: the empty ancestry claim is vacuously proven); a node
//! whose `parent_id` names an unobserved block has an **unprovable ancestry claim** —
//! the R1 distinction is the presence of that claim, not the block.
//!
//! In-memory and per-window only: the tree is rebuilt from the
//! windowed fetch each read, never carried across windows.

use std::collections::{BTreeMap, BTreeSet};

/// What a block claims about its ancestry. Three states, mirroring the tap's `Parent`.
///
/// The distinction that matters is `Root` vs `Unknown`. A root's ancestry claim is EMPTY and
/// therefore fully proven — R1 resolves the vacuous case in the honest direction and counts it
/// as contributing. "I cannot determine the parent" is not an empty claim, it is an unproven
/// one, and collapsing it into `Root` would manufacture contribution out of ignorance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ancestry {
    /// `parent_id` absent and no unknown flag: the block begins a prefix chain.
    Root,
    /// The store named a parent; whether that parent was observed decides dangling.
    Named(String),
    /// The tap could not determine it — a stored run with blocks skipped, where list order
    /// stops implying parenthood. Never a root, never an edge.
    Unknown,
}

/// One observed block in the window.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The block's token count: the max over every store of it in the window, remote included.
    pub n_tokens: i64,
    /// The block's ancestry claim, its named parent bridged into identity space where a store
    /// named that engine id; otherwise the raw engine id, retained as a dangling edge.
    pub ancestry: Ancestry,
    /// Every instance whose store observed the block — remote copies too, since the tree is
    /// structure rather than residency.
    pub instances: BTreeSet<String>,
}

impl Node {
    /// The ancestry claim as an edge, when it is one.
    #[must_use]
    pub fn parent(&self) -> Option<&String> {
        match &self.ancestry {
            Ancestry::Named(p) => Some(p),
            Ancestry::Root | Ancestry::Unknown => None,
        }
    }
}

/// The windowed tree: nodes by block id, with dangling-edge accounting.
#[derive(Debug, Default, PartialEq)]
pub struct BlockTree {
    /// Observed blocks by identity: `content_id` where the tap derived one, `block_id` otherwise.
    pub nodes: BTreeMap<String, Node>,
    /// Blocks observed with conflicting parent claims (content-derived identity makes
    /// this an anomaly, counted and reported, never silently resolved).
    pub parent_conflicts: u64,
}

/// The store facts a tree is built from: `kind = 'store'` with an engine id, in window.
///
/// **No own-copy filter, deliberately, and it is not an oversight.** `tree_facts_sql` has none
/// either: the tree is structure, not residency, and a remote block's ancestry claim is still a
/// claim about the prefix chain. The own-copy rule belongs to the residency substrate, where
/// admitting a shared copy would invent duplication — here it would only discard edges.
fn stores(ir: &[crate::events::Event]) -> impl Iterator<Item = &crate::events::Event> {
    ir.iter()
        .filter(|e| e.kind == crate::events::Kind::Store && e.block_id.is_some())
}

impl BlockTree {
    /// Build from the Ordered Event IR — the store-free path.
    ///
    /// **The cross-space resolution is the whole of this, and it is the part that is easy to get
    /// wrong.** Nodes are keyed on the block IDENTITY — `content_id` where the tap derived one,
    /// `block_id` otherwise — while `parent_id` is *always* an engine-space id. So an edge cannot
    /// be compared directly against a node key: where the child is content-keyed, its parent must
    /// be resolved through the store record that named it, and THAT record's identity is the edge.
    /// Comparing the raw `parent_id` against identities would splice the tree across two spaces —
    /// silently, since a content id and an engine id can never be equal and every edge would
    /// simply read as dangling.
    ///
    /// A parent with no store record in the window resolves to nothing and the edge stays
    /// dangling. The resolution decides
    /// which SPACE an edge is expressed in, never which edges exist.
    ///
    /// Order-independence is preserved: construction is from the windowed fact set, so a late
    /// observation of a parent reconnects the chain regardless of arrival order.
    #[must_use]
    pub fn from_events(ir: &[crate::events::Event]) -> Self {
        // The engine-space → identity bridge, built from the same store facts the nodes are.
        // `SELECT DISTINCT block_id, coalesce(content_id, block_id)` in the query it replaces.
        let mut bridge: BTreeMap<&str, &str> = BTreeMap::new();
        for ev in stores(ir) {
            if let (Some(engine_id), Some(identity)) = (ev.block_id.as_deref(), ev.identity()) {
                bridge.entry(engine_id).or_insert(identity);
            }
        }

        let mut tree = Self::default();
        for ev in stores(ir) {
            let Some(identity) = ev.identity() else {
                continue;
            };
            // Order matters: an explicit unknown outranks an absent parent, because a record
            // carries both and absence alone would read as a root.
            let ancestry = if ev.parent_unknown {
                Ancestry::Unknown
            } else {
                match ev.parent_id.as_deref() {
                    None => Ancestry::Root,
                    // **A named parent that does not resolve stays a NAMED edge, never a root.**
                    // Collapsing an unresolved parent to `Root` would credit it: a root's ancestry
                    // claim is vacuously proven and R1 counts it, so every dangling chain would
                    // begin contributing — duplication up, in the flattering direction, with
                    // nothing else failing. The edge is retained against the engine-space id it named,
                    // which is not a node key and therefore reads as dangling. "Never invented and
                    // never dropped" is this module's own law, and dropping the claim IS inventing
                    // one.
                    Some(p) => Ancestry::Named(
                        bridge
                            .get(p)
                            .map_or_else(|| p.to_string(), |r| (*r).to_string()),
                    ),
                }
            };
            match tree.nodes.get_mut(identity) {
                Some(node) => {
                    if node.ancestry != ancestry {
                        tree.parent_conflicts += 1;
                    }
                    node.n_tokens = node.n_tokens.max(ev.n_tokens);
                    node.instances.insert(ev.instance.clone());
                }
                None => {
                    tree.nodes.insert(
                        identity.to_string(),
                        Node {
                            n_tokens: ev.n_tokens,
                            ancestry,
                            instances: BTreeSet::from([ev.instance.clone()]),
                        },
                    );
                }
            }
        }
        tree
    }

    /// Root per R1: an EMPTY ancestry claim, which is why it is vacuously proven.
    ///
    /// `Unknown` is deliberately not a root. It is an unproven claim, not an absent one, and
    /// treating ignorance as proof is the one direction this metric must never fail in.
    #[must_use]
    pub fn is_root(&self, block: &str) -> bool {
        self.nodes
            .get(block)
            .is_some_and(|n| n.ancestry == Ancestry::Root)
    }

    /// The node's ancestry claim names a block with no observed store — the edge is
    /// retained (a late store reconnects it) but proves nothing.
    #[must_use]
    pub fn has_dangling_parent(&self, block: &str) -> bool {
        self.nodes
            .get(block)
            .is_some_and(|n| n.parent().is_some_and(|p| !self.nodes.contains_key(p)))
    }

    /// Count of retained dangling edges in the window.
    #[must_use]
    pub fn dangling_edges(&self) -> u64 {
        self.nodes
            .keys()
            .filter(|b| self.has_dangling_parent(b))
            .count() as u64
    }

    /// Proven ancestry depth: links walked upward through OBSERVED nodes only. A
    /// dangling parent caps the count (partial overlap at proven structure only — the
    /// design law); a root's claim is complete at its depth.
    #[must_use]
    pub fn proven_depth(&self, block: &str) -> u64 {
        let mut depth = 0;
        let mut cur = block;
        let mut seen = BTreeSet::new();
        while seen.insert(cur.to_string()) {
            let Some(node) = self.nodes.get(cur) else {
                break;
            };
            let Some(parent) = node.parent() else {
                break;
            };
            if !self.nodes.contains_key(parent) {
                break; // dangling: unproven ancestry contributes nothing
            }
            depth += 1;
            cur = parent;
        }
        depth
    }

    /// Observed child edges (both endpoints observed), child → parent.
    #[must_use]
    pub fn observed_edges(&self) -> Vec<(&str, &str)> {
        self.nodes
            .iter()
            .filter_map(|(block, node)| {
                let parent = node.parent().map(String::as_str)?;
                self.nodes
                    .contains_key(parent)
                    .then_some((block.as_str(), parent))
            })
            .collect()
    }
}
