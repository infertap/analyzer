// SPDX-License-Identifier: AGPL-3.0-only

//! Dropping a lineage's derived state while keeping its inputs: what `reset-derived` runs.
//!
//! Everything a lineage derives is a function of its input log, so deleting it and starting
//! again derives it afresh: a module enabled after days settled states those days, and a
//! deployment whose derived state is damaged recovers whole. It deletes:
//!
//! - in the lineage's lake, the journal's head, the published state (`state/`), the working
//!   state's checkpoints (`state-db/`), the day manifests (`fold/`) and the log partitioned by
//!   block (`keyed-spine/`);
//! - in the data directory, [`LOCAL_DERIVED`].
//!
//! It keeps the input log (`ledger/`), the journal's lease, the lineage's identity, the segment
//! cache and the serving store's input tables. Keeping the lease keeps the writer's epoch
//! rising, so no object a reset lineage writes again can take a key a deleted one held.
//!
//! The next tick finds no journal head and derives afresh from the log, which reaches back as
//! far as input retention: a day the log has retired is not derived again.

use crate::spine::Spine;
use crate::Error;
use std::path::{Path, PathBuf};

/// The directories under a data directory that hold derived state: the working state and the
/// lake's object cache.
pub const LOCAL_DERIVED: &[&str] = &[crate::spine::WORKING_STATE, "lake-cache"];

/// The lineage's derived prefixes, under its root.
const LINEAGE_DERIVED: &[&str] = &["state/", "state-db/", "fold/", "keyed-spine/"];

/// What a reset deleted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reset {
    /// Objects deleted from the store.
    pub objects: usize,
    /// Local directories removed.
    pub directories: Vec<PathBuf>,
}

/// Delete `spine`'s derived state from its store and `data_dir`'s derived directories, keeping
/// the input log, the lease and the lineage's identity. The caller holds the data directory, so
/// no daemon derives while this runs.
///
/// # Errors
/// A listing or a delete failed, or a directory could not be removed. A reset that fails part
/// way leaves less derived state, never less input, and may be run again.
pub fn reset_derived(spine: &Spine, data_dir: &Path) -> Result<Reset, Error> {
    let lake = spine.lake();
    let store = spine.store();
    let mut keys = vec![format!("{}head", lake.journal_dir())];
    for dir in LINEAGE_DERIVED {
        let prefix = format!("{}{dir}", lake.root());
        keys.extend(store.list(&prefix)?.into_iter().map(|object| object.key));
    }
    analyzer_lake::in_parallel(&keys, |key| store.delete(key))?;
    let mut directories = Vec::new();
    for name in LOCAL_DERIVED {
        let dir = data_dir.join(name);
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => directories.push(dir),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::Derivation(format!("reset: {}: {e}", dir.display()))),
        }
    }
    Ok(Reset {
        objects: keys.len(),
        directories,
    })
}

#[cfg(test)]
mod tests {
    use super::reset_derived;

    /// Every derived prefix goes, every input stays: the log, the lease, the identity, and a
    /// neighbouring namespace's objects.
    #[test]
    fn a_reset_deletes_the_derived_state_and_keeps_the_inputs() {
        let dir =
            std::env::temp_dir().join(format!("analyzer-reset-{}-{}", std::process::id(), line!()));
        let _ = std::fs::remove_dir_all(&dir);
        let namespace = crate::execution::Namespace::new("bucket", "fleet").expect("ns");
        let spine = crate::spine::local(&dir, &namespace).expect("spine");
        let root = spine.lake().root().to_string();
        let derived = [
            format!("{root}journal/head"),
            format!("{root}state/t/js-7/d/1/delta.bin"),
            format!("{root}state-db/00000000000000000001/CURRENT"),
            format!("{root}fold/days/1/head"),
            format!("{root}keyed-spine/42/chunk.bin"),
        ];
        let kept = [
            format!("{root}journal/lease"),
            format!("{root}ledger/shards/0/days/1/head"),
            "_analyzer/namespaces/other/lineages/l/state/t/js-7/d/1/delta.bin".to_string(),
        ];
        for key in derived.iter().chain(&kept) {
            spine.store().put(key, b"x").expect("put");
        }
        let reset = reset_derived(&spine, &dir).expect("reset");
        assert_eq!(reset.objects, derived.len());
        let left: Vec<String> = spine
            .store()
            .list("")
            .expect("list")
            .into_iter()
            .map(|o| o.key)
            .collect();
        for key in &derived {
            assert!(!left.contains(key), "{key} survived the reset");
        }
        for key in &kept {
            assert!(left.contains(key), "{key} was deleted");
        }
        assert_eq!(
            reset.directories,
            vec![dir.join(crate::spine::WORKING_STATE)]
        );
        assert!(
            crate::lineage::Lineage::read(&dir).expect("read").is_some(),
            "the lineage's identity is kept"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
