use std::collections::hash_map::Entry;
use std::fmt::Debug;

use crate::col::{HashMap, HashSet, map_new};

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

impl<K1, K2> Debug for Relation<K1, K2>
where
    K1: Eq + std::hash::Hash + Debug,
    K2: Eq + std::hash::Hash + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Relation")
            .field("map_1", &self.map_1)
            .field("map_2", &self.map_2)
            .finish()
    }
}

impl<K1, K2> Relation<K1, K2>
where
    K1: Eq + std::hash::Hash + Clone + Debug,
    K2: Eq + std::hash::Hash + Clone + Debug,
{
    pub fn by_k1(&self, k1: &K1) -> Option<&HashSet<K2>> {
        self.map_1.get(k1)
    }

    pub fn by_k2(&self, k2: &K2) -> Option<&HashSet<K1>> {
        self.map_2.get(k2)
    }

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
    pub fn contains_k1(&self, k1: &K1) -> bool {
        self.map_1.contains_key(k1)
    }

    /// Returns true if there exists a k1 such that (k1,k2) is in the relation.
    pub fn contains_k2(&self, k2: &K2) -> bool {
        self.map_2.contains_key(k2)
    }

    /// Sets k1 in relation to the given k2s (exclusively).
    /// Returns the set of k2s that were removed from the relation with k1.
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

    fn assert_invariants(&self) {
        assert_invariants(&self.map_1, &self.map_2);
        assert_invariants(&self.map_2, &self.map_1);
    }
}

fn assert_invariants<
    K1: Eq + std::hash::Hash + Clone + Debug,
    K2: Eq + std::hash::Hash + Clone + Debug,
>(
    map_1: &HashMap<K1, HashSet<K2>>,
    map_2: &HashMap<K2, HashSet<K1>>,
) {
    for (k1, k2s) in map_1.iter() {
        assert!(!k2s.is_empty());
        for k2 in k2s {
            let k1s = map_2.get(k2);
            assert!(
                k1s.is_some(),
                "m2[{:?}] is none, but {:?} is in m1[{:?}]",
                k2,
                k2,
                k1
            );
            assert!(k1s.unwrap().contains(k1));
        }
    }
}

/// Sets the image of the relation of a key.
///
/// Returns the set of k2s that were removed from the relation with k1.
fn set_values<K1: Eq + std::hash::Hash + Clone, K2: Eq + std::hash::Hash + Clone>(
    k1: K1,
    k2s: HashSet<K2>,
    map_1: &mut HashMap<K1, HashSet<K2>>,
    map_2: &mut HashMap<K2, HashSet<K1>>,
) -> HashSet<K2> {
    match map_1.entry(k1.clone()) {
        Entry::Vacant(it) => {
            if !k2s.is_empty() {
                let k2s = it.insert(k2s);
                for k2 in k2s.iter() {
                    let was_not_previously_set =
                        map_2.entry(k2.clone()).or_default().insert(k1.clone());
                    debug_assert!(was_not_previously_set);
                }
            }
            HashSet::default()
        }
        Entry::Occupied(mut it) => {
            let old_k2s_not_in_new = {
                if k2s.is_empty() {
                    it.remove()
                } else {
                    let mut old_k2s = it.insert(k2s);
                    let new_k2s = it.get();
                    for k2 in new_k2s {
                        if !old_k2s.remove(k2) {
                            // k2 is in new_k2s but was not in old_k2s.
                            // Thus, we have to add k1 to map_2[k2].
                            let was_not_previously_set =
                                map_2.entry(k2.clone()).or_default().insert(k1.clone());
                            debug_assert!(was_not_previously_set);
                        }
                    }
                    old_k2s
                }
            };
            // We have to remove k1 from map_2[k2] for each of these k2s in old_k2s_not_in_new.
            for k2 in &old_k2s_not_in_new {
                let mut entry = match map_2.entry(k2.clone()) {
                    Entry::Occupied(entry) => entry,
                    Entry::Vacant(_) => unreachable!("k2 not in map_2, but in map_1[k1]"),
                };
                let k1s = entry.get_mut();
                let was_removed = k1s.remove(&k1);
                debug_assert!(was_removed);
                if k1s.is_empty() {
                    entry.remove_entry();
                }
            }
            old_k2s_not_in_new
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
            let was_removed = k1s.remove(k1);
            debug_assert!(was_removed);
            if k1s.is_empty() {
                entry.remove();
            }
        }
        return k2s;
    }
    HashSet::default()
}
