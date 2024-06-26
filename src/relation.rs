use std::collections::hash_map::Entry;

use crate::col::{map_new, HashMap, HashSet};

/// A data structure representing a relation K1 x K2. It allows for efficient removal of keys of either type.
pub struct Relation<K1, K2>
where
    K1: Eq + std::hash::Hash,
    K2: Eq + std::hash::Hash,
{
    // Assigns to each k1 the set of k2s such that (k1,k2) is in the relation.
    map_1: HashMap<K1, HashSet<K2>>,

    // Assigns to each k2 the set of k1s such that (k1,k2) is in the relation.
    map_2: HashMap<K2, HashSet<K1>>,
}

impl<K1, K2> Relation<K1, K2>
where
    K1: Eq + std::hash::Hash + Clone,
    K2: Eq + std::hash::Hash + Clone,
{
    /// For the given key k1, removes (k1,k2) for any k2 from the relation.
    /// Returns true if any (k1,k2) was removed.
    pub fn remove_k1(&mut self, k1: &K1) -> HashSet<K2> {
        remove_key(&mut self.map_1, &mut self.map_2, k1)
    }

    /// For the given key k2, removes (k1,k2) for any k1 from the relation.
    /// Returns true if any (k1,k2) was removed.
    pub fn remove_k2(&mut self, k2: &K2) -> HashSet<K1> {
        remove_key(&mut self.map_2, &mut self.map_1, k2)
    }

    /// Adds (k1, k2) to the relation.
    #[allow(dead_code)]
    pub fn add(&mut self, k1: K1, k2: K2) {
        self.map_1.entry(k1.clone()).or_default().insert(k2.clone());
        self.map_2.entry(k2).or_default().insert(k1);
    }

    /// Returns true if there exists a k2 such that (k1,k2) is in the relation.
    #[allow(dead_code)]
    pub fn contains_k1(&self, k1: &K1) -> bool {
        self.map_1.contains_key(k1)
    }

    /// Returns true if there exists a k1 such that (k1,k2) is in the relation.
    pub fn contains_k2(&self, k2: &K2) -> bool {
        self.map_2.contains_key(k2)
    }

    /// Sets k1 in relation to the given k2s (exclusively).
    /// Returns the set of k2s that were removed from the relation with k1.
    #[allow(dead_code)]
    pub fn set_k1_values(&mut self, k1: K1, k2s: HashSet<K2>) -> HashSet<K2> {
        set_values(k1, k2s, &mut self.map_1, &mut self.map_2)
    }

    /// Sets k2 in relation to the given k1s (exclusively).
    /// Returns the set of k1s that were removed from the relation with k2.
    pub fn set_k2_values(&mut self, k2: K2, k1s: HashSet<K1>) -> HashSet<K1> {
        set_values(k2, k1s, &mut self.map_2, &mut self.map_1)
    }

    pub fn new() -> Relation<K1, K2> {
        Self {
            map_1: map_new(),
            map_2: map_new(),
        }
    }
}

/// Returns the set of k2s that were removed from the relation with k1.
fn set_values<K1: Eq + std::hash::Hash + Clone, K2: Eq + std::hash::Hash + Clone>(
    k1: K1,
    k2s: HashSet<K2>,
    map_1: &mut HashMap<K1, HashSet<K2>>,
    map_2: &mut HashMap<K2, HashSet<K1>>,
) -> HashSet<K2> {
    match map_1.entry(k1.clone()) {
        Entry::Vacant(it) => {
            let k2s = it.insert(k2s);
            for k2 in k2s.iter() {
                map_2.entry(k2.clone()).or_default().insert(k1.clone());
            }
            HashSet::default()
        }
        Entry::Occupied(mut it) => {
            let mut old_k2s = it.insert(k2s);
            let new_k2s = it.get();
            for k2 in new_k2s {
                if !old_k2s.remove(k2) {
                    map_2.entry(k2.clone()).or_default().insert(k1.clone());
                }
            }
            // Now old_k2s only contains the k2s that are not in new_k2s.
            for k2 in &old_k2s {
                let mut entry = match map_2.entry(k2.clone()) {
                    Entry::Occupied(entry) => entry,
                    Entry::Vacant(_) => unreachable!("k2 not in map_2, but in map_1[k1]"),
                };
                let k1s = entry.get_mut();
                debug_assert!(k1s.remove(&k1));
                if k1s.is_empty() {
                    entry.remove_entry();
                }
            }
            old_k2s
        }
    }
}

fn remove_key<K1: Eq + std::hash::Hash + Clone, K2: Eq + std::hash::Hash + Clone>(
    map_1: &mut HashMap<K1, HashSet<K2>>,
    map_2: &mut HashMap<K2, HashSet<K1>>,
    k1: &K1,
) -> HashSet<K2> {
    if let Some(k2s) = map_1.remove(k1) {
        for k2 in k2s.iter() {
            let mut entry = match map_2.entry(k2.clone()) {
                Entry::Occupied(entry) => entry,
                Entry::Vacant(_) => unreachable!("k2 not in map_2, but in map_1[k1]"),
            };
            let k1s = entry.get_mut();
            debug_assert!(k1s.remove(k1));
            if k1s.is_empty() {
                entry.remove();
            }
        }
        return k2s;
    }
    HashSet::default()
}
