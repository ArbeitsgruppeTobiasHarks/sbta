use crate::col::{HashMap, map_new};
use std::hash::Hash;

pub struct Indexer<Id, Index, F>
where
    Id: Eq + Hash,
    Index: Eq + Copy,
    F: Fn(usize) -> Index,
{
    len: usize,
    index_by_id: HashMap<Id, Index>,
    to_index: F,
}

impl<OldK: Eq + Hash, Index: Eq + Copy, F: Fn(usize) -> Index> Indexer<OldK, Index, F> {
    pub fn new(to_index: F) -> Self {
        Self {
            len: 0,
            index_by_id: map_new(),
            to_index,
        }
    }

    pub fn index(&mut self, old_id: OldK) -> Index {
        *self.index_by_id.entry(old_id).or_insert_with(|| {
            let index = (self.to_index)(self.len);
            self.len += 1;
            index
        })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn drain(self) -> HashMap<OldK, Index> {
        self.index_by_id
    }
}
