use std::collections::VecDeque;

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    col::{set_new, HashSet},
    graph::EdgeIdx,
    paths_index::PathId,
    relation::Relation,
};

pub struct PathsQueue {
    // All (unique) path ids in the queue.
    queue_vec: VecDeque<PathId>,
    // A set of all paths in queue_vec.
    queue_set: HashSet<PathId>,

    // A relation R on ExP where (e, p) is in R iff e might unblock p.
    // No already queued path should be in this relation.
    path_unblocking_edge_relation: Relation<EdgeIdx, PathId>,
}

impl PathsQueue {
    pub fn len(&self) -> usize {
        self.queue_vec.len()
    }

    /// Enqueues a path when any of the edges is unblocked.
    /// All these edges must be blocked when calling this method.
    pub fn enqueue_on_unblock(&mut self, path_id: PathId, unblocking_edges: HashSet<EdgeIdx>) {
        self.path_unblocking_edge_relation
            .set_k2_values(path_id, unblocking_edges);
    }

    pub fn unblock(&mut self, edge_idx: EdgeIdx) {
        let paths = self.path_unblocking_edge_relation.remove_k1(&edge_idx);
        for path_id in paths {
            if self.queue_set.insert(path_id) {
                self.queue_vec.push_front(path_id);
            }
        }
    }

    pub fn enqueue_front(&mut self, path_id: PathId) {
        if self.queue_set.insert(path_id) {
            self.queue_vec.push_front(path_id);
            self.path_unblocking_edge_relation.remove_k2(&path_id);
        } else {
            debug_assert!(!self.path_unblocking_edge_relation.contains_k2(&path_id))
        }
    }

    pub fn enqueue_back(&mut self, path_id: PathId) {
        if self.queue_set.insert(path_id) {
            self.queue_vec.push_back(path_id);
            self.path_unblocking_edge_relation.remove_k2(&path_id);
        } else {
            debug_assert!(!self.path_unblocking_edge_relation.contains_k2(&path_id))
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &PathId> {
        self.queue_vec.iter()
    }

    pub fn par_iter(&self) -> impl ParallelIterator<Item = &PathId> {
        self.queue_vec.par_iter()
    }

    pub fn remove_first_n(&mut self, n: usize) {
        for i in self.queue_vec.drain(0..n) {
            self.queue_set.remove(&i);
        }
    }

    pub fn new() -> PathsQueue {
        Self {
            queue_vec: VecDeque::new(),
            queue_set: set_new(),
            path_unblocking_edge_relation: Relation::new(),
        }
    }
}
