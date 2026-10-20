// SPDX-License-Identifier: AGPL-3.0-only

//! The fold's state store (`docs/pipeline.md`): what an operator keeps
//! per key between ticks. The per-event path reads and writes it in RAM; a store backed by
//! local disk implements the same trait, so an operator never names where its state lives.

use std::collections::HashMap;
use std::hash::Hash;

/// Keyed state an operator folds from and into.
pub trait KeyedState<K, V> {
    /// The state held for `key`.
    fn get(&self, key: &K) -> Option<&V>;
    /// Hold `value` for `key`, replacing what was held.
    fn put(&mut self, key: K, value: V);
    /// Forget `key`.
    fn remove(&mut self, key: &K);
    /// Every key and its state, in no promised order.
    fn scan(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_>;
    /// How many keys are held.
    fn len(&self) -> usize;
    /// Whether no key is held.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// [`KeyedState`] in RAM.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RamState<K: Eq + Hash, V> {
    map: HashMap<K, V>,
}

impl<K: Eq + Hash, V> Default for RamState<K, V> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

impl<K: Eq + Hash, V> KeyedState<K, V> for RamState<K, V> {
    fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    fn put(&mut self, key: K, value: V) {
        self.map.insert(key, value);
    }

    fn remove(&mut self, key: &K) {
        self.map.remove(key);
    }

    fn scan(&self) -> Box<dyn Iterator<Item = (&K, &V)> + '_> {
        Box::new(self.map.iter())
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}
