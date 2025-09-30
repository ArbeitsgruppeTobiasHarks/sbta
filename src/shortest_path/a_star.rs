use std::{
    cmp::max,
    collections::{BinaryHeap, hash_map::Entry},
    fmt::Display,
    ops::Sub,
};

use num_traits::Zero;
use rayon::{
    iter::{IndexedParallelIterator, ParallelIterator},
    slice::ParallelSliceMut,
};

use crate::{
    col::{HashMap, HashSet, map_new, set_new, set_with_capacity},
    graph::{
        EdgeIdx, EdgePayload, Graph, NodeIdx, NodePayload, NodeType, StationIdx, reachable_nodes,
    },
    primitives::{EPS_L, Time},
    total_order::TotalOrder,
};

pub struct AStarTable {
    // The distance table. Rows=stations, columns=nodes.
    distances_table: Box<[Time]>,
    num_nodes: usize,
}

impl AStarTable {
    pub fn new(num_stations: usize, num_nodes: usize) -> Self {
        let mut distances_table: Vec<Time> = Vec::new();
        let capacity = num_stations * num_nodes;
        distances_table.reserve_exact(capacity);
        distances_table.resize(capacity, 0);
        Self {
            distances_table: distances_table.into_boxed_slice(),
            num_nodes,
        }
    }

    #[allow(dead_code)]
    fn of_station_mut(&mut self, station: StationIdx) -> &mut [Time] {
        self.distances_table
            .chunks_mut(self.num_nodes)
            .nth(station.0 as usize)
            .unwrap()
    }

    fn of_station(&self, station: StationIdx) -> &[Time] {
        self.distances_table
            .chunks(self.num_nodes)
            .nth(station.0 as usize)
            .unwrap()
    }

    pub fn earliest_arrival(&self, from: NodeIdx, to: StationIdx) -> Time {
        self.of_station(to)[from.0 as usize]
    }

    pub fn set_earliest_arrival(&mut self, from: NodeIdx, to: StationIdx, distance: Time) {
        self.of_station_mut(to)[from.0 as usize] = distance;
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
                *it = Time::MAX;
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
    pub reached: HashSet<NodeIdx>,
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
    debug_assert!(max_time < Time::MAX);

    let mut reachable: HashSet<NodeIdx> = set_with_capacity(a_star_table.num_nodes);
    reachable.insert(source_id);
    let mut maybe_unblocking_edges: Vec<(EdgeIdx, Time)> = Vec::new();

    let mut queue: BinaryHeap<QueueItem> = BinaryHeap::new();
    queue.push(QueueItem {
        node_id: source_id,
        arrival_at_station_lower_bound: a_star_table.earliest_arrival(source_id, destination),
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
            while let Some(other) = queue
                .pop()
                .filter(|it| it.arrival_at_station_lower_bound <= node.time)
            {
                debug_assert_eq!(other.arrival_at_station_lower_bound, node.time);
                for &edge_idx in graph.node(other.node_id).outgoing.iter() {
                    let edge = graph.edge(edge_idx);
                    if !edge_okay(edge_idx, edge) {
                        continue;
                    }
                    if reachable.insert(edge.to) {
                        let arrival_at_station_lower_bound =
                            a_star_table.earliest_arrival(edge.to, destination);
                        if arrival_at_station_lower_bound <= node.time {
                            queue.push(QueueItem {
                                node_id: edge.to,
                                arrival_at_station_lower_bound,
                            });
                        }
                    }
                }
            }
            let unblocking_edges: HashSet<EdgeIdx> = maybe_unblocking_edges
                .iter()
                .filter(|it| it.1 < node.time)
                .map(|it| it.0)
                .collect();
            return AStarResult {
                reached: reachable,
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
            if reachable.insert(edge.to) {
                let arrival_at_station_lower_bound =
                    a_star_table.earliest_arrival(edge.to, destination);
                if arrival_at_station_lower_bound <= max_time {
                    queue.push(QueueItem {
                        node_id: edge.to,
                        arrival_at_station_lower_bound,
                    });
                }
            }
        }
    }

    let unblocking_edges: HashSet<EdgeIdx> = maybe_unblocking_edges
        .iter()
        .filter(|it| it.1 <= max_time)
        .map(|it| it.0)
        .collect();
    AStarResult {
        reached: reachable,
        destination: None,
        unblocking_edges,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DTCQueueItemValue<T> {
    pub total_cost_lower_bound: T,
    pub cost_thus_far: T,
    pub lower_bound_cost_remaining_path: Time,
}
impl<T: PartialOrd> PartialOrd for DTCQueueItemValue<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.total_cost_lower_bound
            .partial_cmp(&other.total_cost_lower_bound)
    }
}

impl<T: TotalOrder + PartialEq> Eq for DTCQueueItemValue<T> {}
impl<T: TotalOrder + PartialOrd> Ord for DTCQueueItemValue<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.total_cost_lower_bound
            .total_cmp(&other.total_cost_lower_bound)
    }
}

fn compute_cost_lower_bound_remaining_path(
    from_idx: NodeIdx,
    from_node: &NodePayload,
    params: &AStarMultiSourceParams,
) -> Time {
    let earliest_arrival = params
        .a_star_table
        .earliest_arrival(from_idx, params.destination);
    if earliest_arrival == Time::MAX {
        return Time::MAX;
    }
    let earliest_arrival_with_waiting = max(earliest_arrival, params.target_arrival_time);
    let min_travel_time = earliest_arrival_with_waiting - from_node.time;
    let min_delay_cost =
        (earliest_arrival_with_waiting - params.target_arrival_time) * params.delay_penalty_factor;
    let min_cost = min_travel_time + min_delay_cost;
    min_cost
}

#[derive(Debug)]
pub struct AStarMultiSourceResult<T> {
    pub reached: HashMap<NodeIdx, DTCQueueItemValue<T>>,
    pub destinations: Vec<NodeIdx>,
    pub unblocking_edges: HashSet<EdgeIdx>,
}

pub struct AStarMultiSourceParams<'a> {
    pub graph: &'a Graph,
    pub a_star_table: &'a AStarTable,
    pub destination: StationIdx,
    pub target_arrival_time: Time,
    pub delay_penalty_factor: Time,
}

type IndexedMinHeap<K, V> = mut_binary_heap::BinaryHeap<K, V, mut_binary_heap::MinComparator>;

pub fn a_star_multi_source<
    I: Iterator<Item = (NodeIdx, T)>,
    T: num_traits::Bounded
        + TotalOrder
        + PartialOrd
        + Zero
        + MyEps
        + Sub<Output = T>
        + From<u32>
        + Copy
        + Display,
>(
    source_ids: impl Fn() -> I,
    collect_unblocking: bool,
    max_cost: T,
    params: &AStarMultiSourceParams,
    edge_okay: impl Fn(EdgeIdx, &EdgePayload) -> bool,
    get_edge_cost: impl Fn(EdgeIdx, &EdgePayload, &NodePayload, &NodePayload) -> T,
) -> AStarMultiSourceResult<T> {
    debug_assert!(max_cost < T::max_value());

    let mut queue: IndexedMinHeap<NodeIdx, DTCQueueItemValue<T>> = IndexedMinHeap::new();

    let mut reached: HashMap<NodeIdx, DTCQueueItemValue<T>> = map_new();

    source_ids().for_each(|(source_id, initial_cost)| {
        let lower_bound = compute_cost_lower_bound_remaining_path(
            source_id,
            params.graph.node(source_id),
            &params,
        );
        queue.push(
            source_id,
            DTCQueueItemValue {
                cost_thus_far: initial_cost,
                lower_bound_cost_remaining_path: lower_bound,
                total_cost_lower_bound: initial_cost + lower_bound.into(),
            },
        );
    });

    let mut max_cost = max_cost;
    let mut destinations: Vec<NodeIdx> = Vec::new();
    let mut maybe_unblocking_edges: Vec<(EdgeIdx, T)> = Vec::new();

    // Used only for debug assertions:
    let mut last_total_cost = T::min_value();

    while let Some((node_idx, node_value)) = queue
        .pop_with_key()
        .filter(|it| it.1.total_cost_lower_bound <= max_cost)
    {
        debug_assert!(
            last_total_cost <= node_value.total_cost_lower_bound,
            "last_total_cost: {}, node_value: {}",
            last_total_cost,
            node_value.total_cost_lower_bound
        );
        last_total_cost = node_value.total_cost_lower_bound;

        let node = params.graph.node(node_idx);
        match reached.entry(node_idx) {
            Entry::Vacant(entry) => entry.insert(node_value.clone()),
            Entry::Occupied(_) => {
                unreachable!("Node was already reached. Do we have negative costs?")
            }
        };

        for &edge_idx in node.outgoing.iter() {
            let edge = params.graph.edge(edge_idx);
            if reached.contains_key(&edge.to) {
                continue;
            }
            if !edge_okay(edge_idx, edge) {
                if collect_unblocking {
                    let node_to = params.graph.node(edge.to);
                    let lower_bound_cost_remaining_path = queue
                        .get(&edge.to)
                        .map(|it| it.lower_bound_cost_remaining_path)
                        .unwrap_or_else(|| {
                            compute_cost_lower_bound_remaining_path(edge.to, node_to, params)
                        });
                    if lower_bound_cost_remaining_path < Time::MAX {
                        let edge_travel_time = node_to.time - node.time;
                        let total_cost_lower_bound = node_value.cost_thus_far
                            + T::from(edge_travel_time + lower_bound_cost_remaining_path);
                        maybe_unblocking_edges.push((edge_idx, total_cost_lower_bound));
                    }
                }
                continue;
            }
            let node_to = params.graph.node(edge.to);
            let edge_cost = get_edge_cost(edge_idx, edge, node, node_to);
            relax_edge(
                &mut queue,
                &node_value,
                edge_cost,
                edge_idx,
                edge,
                node_to,
                params,
            );
        }

        if node.node_type == NodeType::Wait(params.destination) {
            // Add other nodes that can reach this destination in this time.
            // This is done, so that we can later select from the set of earliest arrival paths.
            max_cost = node_value.total_cost_lower_bound;
            destinations.push(node_idx);
        }
    }

    let found_destination = !destinations.is_empty();
    let unblocking_edges: HashSet<EdgeIdx> = maybe_unblocking_edges
        .iter()
        .filter(|(_, total_cost)| {
            if found_destination {
                *total_cost < max_cost
            } else {
                *total_cost <= max_cost
            }
        })
        .map(|(edge_idx, _)| *edge_idx)
        .collect();

    AStarMultiSourceResult {
        reached,
        destinations,
        unblocking_edges,
    }
}

pub trait MyEps {
    fn eps() -> Self;
}

impl MyEps for Time {
    fn eps() -> Self {
        0
    }
}

impl MyEps for f64 {
    fn eps() -> Self {
        EPS_L
    }
}

fn relax_edge<
    T: num_traits::Bounded
        + Sub<Output = T>
        + MyEps
        + TotalOrder
        + PartialOrd
        + Zero
        + From<u32>
        + Copy
        + Display,
>(
    queue: &mut IndexedMinHeap<NodeIdx, DTCQueueItemValue<T>>,
    node_from_value: &DTCQueueItemValue<T>,
    edge_cost: T,
    edge_idx: EdgeIdx,
    edge: &EdgePayload,
    node_to: &NodePayload,
    params: &AStarMultiSourceParams,
) {
    let cost_thus_far = node_from_value.cost_thus_far + edge_cost;
    match queue.get(&edge.to) {
        None => {
            let lower_bound_cost_remaining_path =
                compute_cost_lower_bound_remaining_path(edge.to, node_to, params);
            let node_to_total_cost_lower_bound = if lower_bound_cost_remaining_path == Time::MAX {
                T::max_value()
            } else {
                cost_thus_far + T::from(lower_bound_cost_remaining_path)
            };
            debug_assert!(
                node_to_total_cost_lower_bound >= node_from_value.total_cost_lower_bound - T::eps(),
                "total_cost_lower_bound: {}, node_value.total_cost_lower_bound: {},\nfrom_lower_bound: {}, to_lower_bound: {}\n from_cost_thus_far: {}, edge_cost: {}\nedge: {:?}, {:?}",
                node_to_total_cost_lower_bound,
                node_from_value.total_cost_lower_bound,
                node_from_value.lower_bound_cost_remaining_path,
                lower_bound_cost_remaining_path,
                node_from_value.cost_thus_far,
                edge_cost,
                edge_idx,
                edge,
            );
            let res = queue.push(
                edge.to,
                DTCQueueItemValue {
                    cost_thus_far,
                    lower_bound_cost_remaining_path,
                    total_cost_lower_bound: node_to_total_cost_lower_bound,
                },
            );
            assert!(res.is_none(), "queue.get(&edge.to) returned None earlier.");
        }
        Some(val) => {
            if cost_thus_far < val.cost_thus_far {
                let mut val_mut = queue.get_mut(&edge.to).expect("queue[edge.to] was Some");
                val_mut.cost_thus_far = cost_thus_far;
                if val_mut.lower_bound_cost_remaining_path < Time::MAX {
                    val_mut.total_cost_lower_bound =
                        cost_thus_far + T::from(val_mut.lower_bound_cost_remaining_path);
                }

                debug_assert!(
                    val_mut.total_cost_lower_bound >= node_from_value.total_cost_lower_bound,
                    "total_cost_lower_bound: {}, node_value.total_cost_lower_bound: {}",
                    val_mut.total_cost_lower_bound,
                    node_from_value.total_cost_lower_bound
                );
            } else {
                debug_assert!(
                    val.total_cost_lower_bound >= node_from_value.total_cost_lower_bound,
                    "total_cost_lower_bound: {}, node_value.total_cost_lower_bound: {}",
                    val.total_cost_lower_bound,
                    node_from_value.total_cost_lower_bound
                );
            }
        }
    }
}
