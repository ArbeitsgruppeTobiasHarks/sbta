use itertools::Itertools;
use log::{debug, warn};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cmp::max;
use std::fmt::Write;
use std::{
    cmp::min,
    collections::{VecDeque, hash_map::Entry},
};

use crate::{
    col::{HashMap, HashSet, map_new, set_new},
    graph::{DescribePath, EdgeIdx, Graph},
    path_index::{PathId, PathsIndex},
    primitives::{EPS, FVal},
};

#[derive(Clone, Debug)]
pub struct Flow {
    path_flow: HashMap<PathId, FVal>,
    edge_flow: HashMap<EdgeIdx, FVal>,
    paths_by_edge: HashMap<EdgeIdx, HashSet<PathId>>,
}

impl Flow {
    pub fn new() -> Self {
        Self {
            path_flow: map_new(),
            edge_flow: map_new(),
            paths_by_edge: map_new(),
        }
    }

    pub fn describe(&self, graph: &Graph, paths: &PathsIndex) -> String {
        let mut out = String::new();
        let mut paths_vec = self.path_flow_map().iter().collect::<Vec<_>>();

        // Sort by commodity id, then by path id
        paths_vec.sort_unstable_by(|a, b| {
            let path_a = paths.path(*a.0);
            let commodity_a = path_a.commodity_idx();
            let path_b = paths.path(*b.0);
            let commodity_b = path_b.commodity_idx();
            commodity_a
                .0
                .cmp(&commodity_b.0)
                .then_with(|| a.0.0.cmp(&b.0.0))
        });

        for (&path, &flow_val) in paths_vec {
            if flow_val == 0.0 {
                continue;
            }
            writeln!(
                out,
                "{:} on {:?}: {:}",
                flow_val,
                path,
                paths.path(path).describe(graph),
            )
            .unwrap();
        }
        out
    }

    pub fn get_demand_outside(&self, paths: &PathsIndex) -> FVal {
        self.path_flow_map()
            .iter()
            .filter(|it| paths.path(*it.0).is_outside())
            .map(|it| it.1)
            .sum()
    }

    pub fn get_demand_inside(&self, paths: &PathsIndex) -> FVal {
        self.path_flow_map()
            .iter()
            .filter(|&(&path_id, _)| !paths.path(path_id).is_outside())
            .map(|(_, &flow_val)| flow_val)
            .sum()
    }

    pub fn add_flow_onto_path(
        &mut self,
        paths: &PathsIndex,
        path_id: PathId,
        value: FVal,
        ensure_nonnegative: bool,
        recompute_edge_flow: bool,
    ) {
        match self.path_flow.entry(path_id) {
            Entry::Occupied(mut entry) => {
                let val = entry.get_mut();
                *val += value;
                if ensure_nonnegative && *val < 0.0 {
                    assert!(
                        *val >= -EPS,
                        "Negative flow of value {:} on path {:?}",
                        *val,
                        path_id
                    );
                    *val = 0.0;
                }
                let abs = val.abs();
                if 0.0 < abs && abs < EPS {
                    warn!("Remove remaining {:} on path {:?}", *val, path_id);
                    *val = 0.0;
                }
                let remove_path = *val == 0.0;
                if remove_path {
                    entry.remove_entry();
                }
                for &edge_idx in paths.path(path_id).edges().iter() {
                    if remove_path {
                        let paths_on_edge = self.paths_by_edge.get_mut(&edge_idx).unwrap();
                        paths_on_edge.remove(&path_id);
                        if paths_on_edge.is_empty() {
                            self.paths_by_edge.remove(&edge_idx);
                            self.edge_flow.remove(&edge_idx);
                        } else if recompute_edge_flow {
                            *self.edge_flow.get_mut(&edge_idx).unwrap() =
                                sum_of_paths(&self.path_flow, paths_on_edge);
                        } else {
                            *self.edge_flow.get_mut(&edge_idx).unwrap() += value;
                        }
                    } else if recompute_edge_flow {
                        *self.edge_flow.get_mut(&edge_idx).unwrap() = sum_of_paths(
                            &self.path_flow,
                            self.paths_by_edge.get(&edge_idx).unwrap(),
                        );
                    } else {
                        *self.edge_flow.get_mut(&edge_idx).unwrap() += value;
                    }
                }
            }
            Entry::Vacant(entry) => {
                if value == 0.0 {
                    return;
                }
                if ensure_nonnegative && value < 0.0 {
                    assert!(
                        value >= -EPS,
                        "Negative flow of value {:} on path {:?}",
                        value,
                        path_id
                    );
                    return;
                }
                let abs = value.abs();
                if 0.0 < abs && abs < EPS {
                    warn!("Ignore addition of {:} on vacant path {:?}", value, path_id);
                    return;
                }
                entry.insert(value);
                for &edge_idx in paths.path(path_id).edges().iter() {
                    let paths_on_edge = self.paths_by_edge.entry(edge_idx).or_insert(set_new());
                    paths_on_edge.insert(path_id);
                    if recompute_edge_flow {
                        self.edge_flow
                            .insert(edge_idx, sum_of_paths(&self.path_flow, paths_on_edge));
                    } else {
                        *self.edge_flow.entry(edge_idx).or_insert(0.0) += value;
                    }
                }
            }
        }
    }

    pub fn add(
        &mut self,
        other: &Self,
        paths: &PathsIndex,
        ensure_nonnegative: bool,
        recompute_edge_flow: bool,
    ) {
        for (path_id, &flow_val) in other.path_flow.iter() {
            self.add_flow_onto_path(
                paths,
                *path_id,
                flow_val,
                ensure_nonnegative,
                recompute_edge_flow,
            );
        }
        // TODO: Add logic to remove paths with flow 0
        // TODO: Update paths_by_edge
        // TODO: Remove PathsIndex parameter
        /*
        for (path_id, &flow_val) in other.path_flow.iter() {
            match self.path_flow.entry(*path_id) {
                Entry::Vacant(entry) => {
                    entry.insert(flow_val);
                },
                Entry::Occupied(mut entry) => {
                    let val = entry.get_mut();
                    *val += flow_val;
                    if *val == 0.0 {
                        entry.remove_entry();
                        // Remove also from edges.
                        for &edge_id in paths.path(*path_id).iter() {
                            self.paths_by_edge
                                .get_mut(&edge_id)
                                .unwrap()
                                .remove(path_id);
                        }
                    }
                }
            }
        }

        for (path_id, &flow_val) in other.path_flow.iter() {
            self.path_flow
                .entry(*path_id)
                .and_modify(|it| *it += flow_val)
                .or_insert(flow_val);
        }
        for (edge_id, edge_val) in other.edge_flow.iter() {
            self.edge_flow
                .entry(*edge_id)
                .and_modify(|it| *it += edge_val)
                .or_insert(*edge_val);
        }
         */
    }

    pub fn on_path(&self, path_id: PathId) -> FVal {
        *self.path_flow.get(&path_id).unwrap_or(&0.0)
    }

    pub fn path_flow_map(&self) -> &HashMap<PathId, FVal> {
        &self.path_flow
    }

    pub fn edge_flow_map(&self) -> &HashMap<EdgeIdx, FVal> {
        &self.edge_flow
    }

    pub fn on_edge(&self, edge_idx: EdgeIdx) -> FVal {
        *self.edge_flow.get(&edge_idx).unwrap_or(&0.0)
    }

    pub fn paths_by_edge(&self) -> &HashMap<EdgeIdx, HashSet<PathId>> {
        &self.paths_by_edge
    }

    pub fn l1norm(&self) -> FVal {
        self.path_flow.values().map(|x| x.abs()).sum()
    }

    pub fn scale_by(&mut self, factor: FVal) {
        for val in self.path_flow.values_mut() {
            *val *= factor;
        }
        for val in self.edge_flow.values_mut() {
            *val *= factor;
        }
    }

    fn approx_equal(&self, other: &Flow, eps: f64) -> bool {
        if self.path_flow.len() != other.path_flow.len() {
            return false;
        }

        self.path_flow
            .iter()
            .all(|(&path_id, &flow_val)| (flow_val - other.on_path(path_id)).abs() <= eps)
    }

    pub fn recompute_edge_flow(&mut self) {
        for (&edge_idx, paths_in_edge) in self.paths_by_edge.iter() {
            self.edge_flow
                .insert(edge_idx, sum_of_paths(&self.path_flow, paths_in_edge));
        }
    }

    pub fn cost(&self, paths: &PathsIndex, graph: &Graph) -> f64 {
        self.path_flow
            .iter()
            .map(|(&path_id, &flow_val)| {
                let path = paths.path(path_id);
                let cost = path.cost(graph.commodity(path.commodity_idx()), graph);
                flow_val * (cost as f64)
            })
            .sum()
    }

    pub fn reset(&mut self) {
        self.edge_flow.clear();
        self.paths_by_edge.clear();
        self.path_flow.clear();
    }
}

fn sum_of_paths(path_flow: &HashMap<PathId, f64>, paths: &HashSet<PathId>) -> f64 {
    paths
        .iter()
        .map(|path_id| path_flow.get(path_id).unwrap())
        .sorted_by(|a, b| a.abs().partial_cmp(&b.abs()).unwrap())
        .sum()
}

const MAX_DIRECTIONS_CAPACITY: usize = 16384;

/// A flow that is aware of its own construction for the last N iterations.
/// Assume that in the last N iterations, the (directional) flows
/// d_1, d_2, ..., d_N were added to the flow (d_N being the most recent).
/// The construction ends on a cycle, if there exists some 0 < k < N/2 such that
/// d_{N-i}/||d_{N-i}|| = d_{N-i-k}/||d_{N-i-k}|| for all i = 0, ..., k-1.
/// For example, assume the directions are given by: 1 , 2 , 3 , 2 , 1 , 2 , 3 , 2
/// Then, the flow ends on a 4-cycle.
pub struct CycleAwareFlow {
    flow: Flow,
    // Contains the last N normalized directions and their L1 norms.
    directions: VecDeque<(Flow, f64)>,
}

impl CycleAwareFlow {
    pub fn new(initial_flow: Option<Flow>) -> Self {
        Self {
            flow: initial_flow.unwrap_or(Flow::new()),
            directions: VecDeque::with_capacity(MAX_DIRECTIONS_CAPACITY),
        }
    }

    pub fn flow(&self) -> &Flow {
        &self.flow
    }

    pub fn drain(self) -> Flow {
        self.flow
    }

    /// Adds the given non-zero direction to the flow, and keeps track of the last N directions.
    pub fn add_to_flow(
        &mut self,
        paths: &PathsIndex,
        mut direction: Flow,
        on_flow_changed: impl FnOnce(&Flow, &Flow),
    ) {
        self.flow.add(&direction, paths, true, false);
        on_flow_changed(&self.flow, &direction);
        let l1norm = direction.l1norm();
        assert!(l1norm > 0.0, "Direction has L1 norm 0");
        direction.scale_by(1.0 / l1norm);

        if self.directions.len() == self.directions.capacity() {
            self.directions.pop_front();
        }
        self.directions.push_back((direction, l1norm));
    }

    fn max_cycle_length(&self) -> usize {
        let rolling_avg: f64 = self.directions.iter().rev().take(3).map(|it| it.1).sum();
        let rolling_avg: f64 = rolling_avg / std::cmp::min(self.directions.len(), 3) as f64;

        // If the rolling average is smaller than the expected step size, we should afford to check longer cycles.
        // More specifically, we check cycles of length up to 32*2^(log_10(1/rolling_avg)).
        // E.g., for a rolling average of 0.001, we check cycles of length up to 256.
        //                             of 1.0  , we check cycles of length up to 32

        let max_cycle_length = (32.0 * 2.0_f64.powf((1.0 / rolling_avg).log10())) as usize;
        let max_cycle_length = max(max_cycle_length, 64);
        let max_cycle_length = min(max_cycle_length, self.directions.len() / 2);
        debug!(
            "Checking cycles of length up to {} (roll.avg. {:})",
            max_cycle_length, rolling_avg
        );
        max_cycle_length
    }

    /// Returns the length of the shortest possible cycle if one exists.
    pub fn has_cycle(&self, paths: &PathsIndex) -> Option<(usize, Flow)> {
        let cycle_len = (1..self.max_cycle_length())
            .into_par_iter()
            .find_first(|&k| {
                // Check if the last k directions are equal to the k directions before
                (0..k).all(|i| {
                    let flow_prev = &self.directions[self.directions.len() - 1 - i - k].0;
                    let flow_last = &self.directions[self.directions.len() - 1 - i].0;
                    flow_prev.approx_equal(flow_last, EPS)
                })
            });

        if let Some(k) = cycle_len {
            let mut cycle_direction = Flow::new();
            for i in 0..k {
                let (direction, l1norm) = &self.directions[self.directions.len() - 1 - i];
                let mut rescaled_direction = direction.clone();
                // Multiply norm by 64 to avoid numerical deletion.
                rescaled_direction.scale_by(*l1norm * 64.0);
                cycle_direction.add(&rescaled_direction, paths, false, false);
                // cycle_direction.scale_by(1.0 / cycle_direction.l1norm());
            }
            cycle_direction.recompute_edge_flow();
            return Some((k, cycle_direction));
        }
        None
    }

    pub fn recompute_edge_flow(&mut self) {
        self.flow.recompute_edge_flow();
    }
}
