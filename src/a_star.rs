use std::collections::BinaryHeap;

use rayon::{
    iter::{IndexedParallelIterator, ParallelIterator},
    slice::ParallelSliceMut,
};

use crate::{
    col::{set_new, set_with_capacity, HashSet},
    graph::{reachable_nodes, EdgeIdx, EdgePayload, Graph, NodeIdx, NodeType, StationIdx, Time},
};

pub struct AStarTable {
    // The distance table. Rows=stations, columns=nodes.
    distances_table: Box<[u32]>,
    num_nodes: usize,
}

impl AStarTable {
    pub fn new(num_stations: usize, num_nodes: usize) -> Self {
        let mut distances_table: Vec<u32> = Vec::new();
        let capacity = num_stations * num_nodes;
        distances_table.reserve_exact(capacity);
        distances_table.resize(capacity, 0);
        Self {
            distances_table: distances_table.into_boxed_slice(),
            num_nodes,
        }
    }

    #[allow(dead_code)]
    fn of_station_mut(&mut self, station: StationIdx) -> &mut [u32] {
        self.distances_table
            .chunks_mut(self.num_nodes)
            .nth(station.0 as usize)
            .unwrap()
    }

    fn of_station(&self, station: StationIdx) -> &[u32] {
        self.distances_table
            .chunks(self.num_nodes)
            .nth(station.0 as usize)
            .unwrap()
    }

    pub fn earliest_arrival(&self, from: NodeIdx, to: StationIdx) -> u32 {
        self.of_station(to)[from.0 as usize]
    }
}

pub fn compute_a_star_table(
    table: &mut AStarTable,
    graph: &Graph,
    edge_okay: impl Fn(EdgeIdx, &EdgePayload) -> bool + Sync,
) {
    table
        .distances_table
        .par_chunks_exact_mut(table.num_nodes)
        .enumerate()
        .for_each(|it| {
            let station = StationIdx(it.0 as u32);
            let chunk = it.1;

            // Use u32::MAX as earliest arrival for nodes not reaching the destination.
            for it in chunk.iter_mut() {
                *it = u32::MAX;
            }

            let mut wait_nodes: Vec<NodeIdx> = graph
                .nodes()
                .filter(|it| it.1.node_type == NodeType::Wait(station))
                .map(|it| it.0)
                .collect();
            wait_nodes.sort_by_key(|&it| graph.node(it).time);

            // Do a backwards search from the first station node, then continue with the second, etc.
            let mut touched: HashSet<NodeIdx> = set_new();
            let mut queue: Vec<NodeIdx> = vec![];

            for &destination_wait_node_id in &wait_nodes {
                let destination_wait_node = graph.node(destination_wait_node_id);
                queue.push(destination_wait_node_id);
                touched.insert(destination_wait_node_id);
                while let Some(node_id) = queue.pop() {
                    let node = graph.node(node_id);
                    chunk[node_id.0 as usize] = destination_wait_node.time;
                    for &edge_idx in node.incoming.iter() {
                        let edge = graph.edge(edge_idx);
                        if edge_okay(edge_idx, edge) && touched.insert(edge.from) {
                            queue.push(edge.from);
                        }
                    }
                }
            }
        });
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueItem {
    node_id: NodeIdx,
    arrival_at_station_lower_bound: Time,
}
impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .arrival_at_station_lower_bound
            .cmp(&self.arrival_at_station_lower_bound)
            .then_with(|| self.node_id.0.cmp(&other.node_id.0))
    }
}

/// Returns the set of nodes reachable from `source_id` that can reach
/// the destination within the specified time according to A* table.
/// As the A* table is a lower bound, the returned set is a superset
/// of the actual nodes that reach the destination within this time.
pub fn a_star_reachable_within(
    graph: &Graph,
    a_star_table: &AStarTable,
    source_id: NodeIdx,
    destination: StationIdx,
    latest_arrival_time: Time,
    mut edge_okay: impl FnMut(EdgeIdx, &EdgePayload) -> bool,
) -> HashSet<NodeIdx> {
    reachable_nodes(graph, source_id, |edge_idx, edge| {
        edge_okay(edge_idx, edge)
            && a_star_table.earliest_arrival(edge.to, destination) <= latest_arrival_time
    })
}

pub struct AStarResult {
    pub astar_reached: HashSet<NodeIdx>,
    pub destination: Option<NodeIdx>,
    pub unblocking_edges: HashSet<EdgeIdx>,
}
pub fn a_star(
    graph: &Graph,
    a_star_table: &AStarTable,
    source_id: NodeIdx,
    destination: StationIdx,
    mut edge_okay: impl FnMut(EdgeIdx, &EdgePayload) -> bool,
    collect_unblocking: bool,
    max_time: Time,
) -> AStarResult {
    debug_assert!(max_time < u32::MAX);

    let mut reachable: HashSet<NodeIdx> = set_with_capacity(a_star_table.num_nodes);
    reachable.insert(source_id);
    let mut maybe_unblocking_edges: Vec<(EdgeIdx, Time)> = Vec::new();
    let earliest_arrival = a_star_table.earliest_arrival(source_id, destination);

    let mut queue: BinaryHeap<QueueItem> = BinaryHeap::new();
    queue.push(QueueItem {
        node_id: source_id,
        arrival_at_station_lower_bound: earliest_arrival,
    });

    while let Some(QueueItem {
        node_id,
        arrival_at_station_lower_bound: _,
    }) = queue.pop()
    {
        let node = graph.node(node_id);
        if node.node_type == NodeType::Wait(destination) {
            // Add other nodes that can reach this destination in this time.
            // This is done, so that we can later select from the set of earliest arrival paths.
            while queue
                .peek()
                .is_some_and(|it| it.arrival_at_station_lower_bound == node.time)
            {
                let other_node_id = queue.pop().unwrap().node_id;
                for &edge_idx in graph.node(other_node_id).outgoing.iter() {
                    let edge = graph.edge(edge_idx);
                    if !edge_okay(edge_idx, edge) {
                        continue;
                    }
                    let to_node_id = edge.to;
                    if reachable.insert(to_node_id) {
                        let arrival_at_station_lower_bound =
                            a_star_table.earliest_arrival(to_node_id, destination);
                        if arrival_at_station_lower_bound <= node.time {
                            queue.push(QueueItem {
                                node_id: to_node_id,
                                arrival_at_station_lower_bound,
                            });
                        }
                    }
                }
            }
            let unblocking_edges: HashSet<EdgeIdx> = {
                if collect_unblocking {
                    let mut unblocking_edges = HashSet::default();
                    unblocking_edges.extend(maybe_unblocking_edges.iter().filter_map(|it| {
                        if it.1 < earliest_arrival {
                            Some(it.0)
                        } else {
                            None
                        }
                    }));
                    unblocking_edges
                } else {
                    HashSet::default()
                }
            };
            return AStarResult {
                astar_reached: reachable,
                destination: Some(node_id),
                unblocking_edges,
            };
        }

        for &edge_idx in node.outgoing.iter() {
            let edge = graph.edge(edge_idx);
            if !edge_okay(edge_idx, edge) {
                if collect_unblocking {
                    let maybe_earliest_arrival =
                        a_star_table.earliest_arrival(edge.to, destination);
                    maybe_unblocking_edges.push((edge_idx, maybe_earliest_arrival));
                }
                continue;
            }
            let to_node_id = edge.to;
            if reachable.insert(to_node_id) {
                let arrival_at_station_lower_bound =
                    a_star_table.earliest_arrival(to_node_id, destination);
                if arrival_at_station_lower_bound <= max_time {
                    queue.push(QueueItem {
                        node_id: to_node_id,
                        arrival_at_station_lower_bound,
                    });
                }
            }
        }
    }

    let unblocking_edges: HashSet<EdgeIdx> = {
        if collect_unblocking {
            let mut unblocking_edges = HashSet::default();
            unblocking_edges.extend(maybe_unblocking_edges.iter().filter_map(|it| {
                if it.1 < earliest_arrival {
                    Some(it.0)
                } else {
                    None
                }
            }));
            unblocking_edges
        } else {
            HashSet::default()
        }
    };
    AStarResult {
        astar_reached: reachable,
        destination: None,
        unblocking_edges,
    }
}
