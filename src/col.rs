pub type HashMap<K, V> = rustc_hash::FxHashMap<K, V>;
pub type HashSet<K> = rustc_hash::FxHashSet<K>;

pub fn map_new<K, V>() -> HashMap<K, V> {
    rustc_hash::FxHashMap::default()
}

pub fn map_with_capacity<K, V>(capacity: usize) -> HashMap<K, V> {
    rustc_hash::FxHashMap::with_capacity_and_hasher(capacity, Default::default())
}

pub fn set_new<K>() -> HashSet<K> {
    rustc_hash::FxHashSet::default()
}

pub fn set_with_capacity<K>(capacity: usize) -> HashSet<K> {
    rustc_hash::FxHashSet::with_capacity_and_hasher(capacity, Default::default())
}
