use std::iter::empty;

use itertools::Itertools;

use crate::{
    col::{HashSet, set_new},
    flow::Flow,
    graph::{
        CommodityIdx, CostCharacteristic, DepartureTimeChoice, EdgeIdx, EdgePayload, EdgeType,
        Graph, NodeIdx, NodePayload, NodeType, Path, PathBox, StationIdx,
    },
    primitives::{EPS, EPS_L, FVal, Time},
    shortest_path::a_star::{AStarMultiSourceParams, AStarTable, a_star},
};

use super::a_star::a_star_multi_source;

pub fn edge_is_not_a_boarding_edge_of_a_full_driving_edge(
    edge: &EdgePayload,
    flow: &Flow,
    graph: &Graph,
) -> bool {
    if !matches!(edge.edge_type, EdgeType::Board(_)) {
        return true;
    }
    let drive_edge = graph.node(edge.to).outgoing[0];
    let drive_edge_flow = flow.on_edge(drive_edge);

    let capacity = {
        let drive_edge = graph.edge(drive_edge);
        if let EdgeType::Drive(capacity) = drive_edge.edge_type {
            capacity
        } else {
            panic!("Drive edge is not a drive edge!");
        }
    };
    let slack = capacity - drive_edge_flow;
    slack > EPS_L
}

fn edge_is_not_a_boarding_edge_of_a_full_driving_edge_not_on_path(
    edge: &EdgePayload,
    path: &Path,
    flow: &Flow,
    graph: &Graph,
) -> bool {
    if !matches!(edge.edge_type, EdgeType::Board(_)) {
        return true;
    }
    let drive_edge = graph.node(edge.to).outgoing[0];
    let drive_edge_flow = flow.on_edge(drive_edge);

    let capacity = {
        let drive_edge = graph.edge(drive_edge);
        if let EdgeType::Drive(capacity) = drive_edge.edge_type {
            capacity
        } else {
            panic!("Drive edge is not a drive edge!");
        }
    };
    let slack = capacity - drive_edge_flow;
    slack > EPS_L || path.edges().contains(&drive_edge)
}

/// For a given path, tries to find a better and available path.
/// If no better path is found, returns a set of edges that block the better paths.
pub fn find_better_path_or_unblocking_edges(
    a_star_table: &AStarTable,
    graph: &Graph,
    flow: &Flow,
    path: &Path,
) -> Result<PathBox, HashSet<EdgeIdx>> {
    let commodity_idx = path.commodity_idx();
    let commodity = graph.commodity(commodity_idx);

    let destination = commodity.od_pair.destination;

    let edge_okay = |_: EdgeIdx, edge: &EdgePayload| -> bool {
        edge_is_not_a_boarding_edge_of_a_full_driving_edge_not_on_path(edge, path, flow, graph)
    };

    let (path, unblocking) = match &commodity.cost_characteristic {
        CostCharacteristic::FixedDeparture(fixed) => find_shortest_path_fixed(
            commodity_idx,
            destination,
            fixed.spawn_node,
            path.arrival_time(graph, fixed) - 1,
            edge_okay,
            graph,
            a_star_table,
            true,
        ),
        CostCharacteristic::DepartureTimeChoice(choice) => find_shortest_path_choice(
            commodity_idx,
            choice,
            commodity.od_pair.origin,
            destination,
            path.cost_choice(commodity, choice, graph) - 1,
            edge_okay,
            graph,
            a_star_table,
            true,
        ),
    };
    path.ok_or(unblocking)
}

pub fn find_better_path(
    a_star_table: &AStarTable,
    graph: &Graph,
    flow: &Flow,
    path: &Path,
) -> Option<PathBox> {
    let commodity_idx = path.commodity_idx();
    let commodity = graph.commodity(commodity_idx);

    let edge_okay = |_: EdgeIdx, edge: &EdgePayload| -> bool {
        edge_is_not_a_boarding_edge_of_a_full_driving_edge_not_on_path(edge, path, flow, graph)
    };

    match &commodity.cost_characteristic {
        CostCharacteristic::FixedDeparture(fixed) => find_shortest_path_fixed(
            commodity_idx,
            commodity.od_pair.destination,
            fixed.spawn_node,
            path.arrival_time(graph, fixed) - 1,
            edge_okay,
            graph,
            a_star_table,
            false,
        ),
        CostCharacteristic::DepartureTimeChoice(choice) => find_shortest_path_choice(
            commodity_idx,
            choice,
            commodity.od_pair.origin,
            commodity.od_pair.destination,
            path.cost_choice(commodity, choice, graph) - 1,
            edge_okay,
            graph,
            a_star_table,
            false,
        ),
    }
    .0
}

pub fn get_path_prefer_waiting_or_staying(
    graph: &Graph,
    destination: NodeIdx,
    is_source: impl Fn(NodeIdx, &NodePayload) -> bool,
    edge_okay: impl Fn(EdgeIdx, &EdgePayload) -> bool,
) -> Vec<EdgeIdx> {
    let mut edges = vec![];
    let mut cur_node = (destination, graph.node(destination));

    while !is_source(cur_node.0, cur_node.1) {
        let chosen_edge = {
            let mut chosen_edge = None;
            for &edge_idx in cur_node.1.incoming.iter() {
                let edge = graph.edge(edge_idx);
                if !edge_okay(edge_idx, edge) {
                    continue;
                }
                chosen_edge = Some((edge_idx, edge));
                if matches!(edge.edge_type, EdgeType::StayOn | EdgeType::Wait) {
                    break;
                }
            }
            chosen_edge.unwrap_or_else(|| {
                unreachable!(
                    "There should be a path to a source node, but {:?} is not a source.",
                    cur_node
                );
            })
        };
        edges.push(chosen_edge.0);

        cur_node = (chosen_edge.1.from, graph.node(chosen_edge.1.from));
    }
    edges.reverse();

    edges
}

fn edge_is_not_a_full_driving_edge(
    edge_idx: EdgeIdx,
    edge: &EdgePayload,
    flow: &Flow,
    direction: &Flow,
) -> bool {
    let capacity = match edge.edge_type {
        EdgeType::Drive(capacity) => capacity,
        _ => return true,
    };
    let slack = capacity - flow.on_edge(edge_idx);
    if slack > EPS_L {
        return true;
    }
    if direction.on_edge(edge_idx) < -EPS {
        return true;
    }
    false
}

pub fn find_shortest_path_fixed(
    commodity_idx: CommodityIdx,
    destination: StationIdx,
    spawn_node: Option<NodeIdx>,
    max_time: Time,
    edge_okay: impl Fn(EdgeIdx, &EdgePayload) -> bool,
    graph: &Graph,
    a_star_table: &AStarTable,
    collect_unblocking: bool,
) -> (Option<PathBox>, HashSet<EdgeIdx>) {
    let Some(spawn_node_id) = spawn_node else {
        return (None, set_new());
    };

    let result = a_star(
        graph,
        a_star_table,
        spawn_node_id,
        destination,
        &edge_okay,
        collect_unblocking,
        max_time,
    );

    let Some(destination_node_idx) = result.destination else {
        return (None, result.unblocking_edges);
    };

    let better_path = get_path_prefer_waiting_or_staying(
        graph,
        destination_node_idx,
        |node_idx, _| node_idx == spawn_node_id,
        |edge_idx, edge| result.reached.contains(&edge.from) && edge_okay(edge_idx, edge),
    );

    (
        Some(PathBox::new(commodity_idx, better_path.into_iter())),
        result.unblocking_edges,
    )
}

pub fn find_shortest_path_choice(
    commodity_idx: CommodityIdx,
    choice: &DepartureTimeChoice,
    origin: StationIdx,
    destination: StationIdx,
    max_cost: Time,
    edge_okay: impl Fn(EdgeIdx, &EdgePayload) -> bool,
    graph: &Graph,
    a_star_table: &AStarTable,
    collect_unblocking: bool,
) -> (Option<PathBox>, HashSet<EdgeIdx>) {
    let result = a_star_multi_source(
        || iter_source_nodes(graph, choice).map(|it| (it, 0)),
        collect_unblocking,
        max_cost,
        &AStarMultiSourceParams {
            graph,
            a_star_table,
            destination,
            target_arrival_time: choice.target_arrival_time,
            delay_penalty_factor: choice.delay_penalty_factor,
        },
        &edge_okay,
        |_, _, node_from, node_to| node_to.time - node_from.time,
    );

    let Some(destination_node_idx) = result.destinations.first() else {
        return (None, result.unblocking_edges);
    };

    let better_path = get_path_prefer_waiting_or_staying(
        graph,
        *destination_node_idx,
        |node_idx, node| {
            node.node_type == NodeType::Wait(origin)
                && result.reached.get(&node_idx).unwrap().cost_thus_far == 0
        },
        |edge_idx, edge| {
            let Some(from_value) = result.reached.get(&edge.from) else {
                return false;
            };
            if !edge_okay(edge_idx, edge) {
                return false;
            }
            let edge_travel_time = graph.node(edge.to).time - graph.node(edge.from).time;
            let to_value = result.reached.get(&edge.to).unwrap();
            debug_assert!(to_value.cost_thus_far <= from_value.cost_thus_far + edge_travel_time);
            return to_value.cost_thus_far >= from_value.cost_thus_far + edge_travel_time;
        },
    );

    (
        Some(PathBox::new(commodity_idx, better_path.into_iter())),
        result.unblocking_edges,
    )
}

/// Returns the best response path and a value d given a flow and a direction, such that
/// each edge on the path either has flow-slack or the direction is strictly negative and at most -d.
/// In other words, adding d*path to the direction will not lead to an infeasible flow due to the edges in the path.
///
/// The path is only used to determine the spawn node and the destination.
pub fn find_best_response_path_with_derivative_astar(
    graph: &Graph,
    a_star_table: &AStarTable,
    flow: &Flow,
    direction: &Flow,
    path: &Path,
) -> (PathBox, FVal) {
    let commodity_idx = path.commodity_idx();
    let commodity = graph.commodity(commodity_idx);

    let edge_okay = |edge_idx: EdgeIdx, edge: &EdgePayload| -> bool {
        edge_is_not_a_full_driving_edge(edge_idx, edge, flow, direction)
            && edge_is_not_a_boarding_edge_of_a_full_driving_edge(edge, flow, graph)
    };

    let destination = commodity.od_pair.destination;
    let best_response = match &commodity.cost_characteristic {
        CostCharacteristic::FixedDeparture(fixed) => find_shortest_path_fixed(
            commodity_idx,
            destination,
            fixed.spawn_node,
            fixed.outside_latest_arrival,
            &edge_okay,
            graph,
            a_star_table,
            false,
        ),
        CostCharacteristic::DepartureTimeChoice(choice) => find_shortest_path_choice(
            commodity_idx,
            choice,
            commodity.od_pair.origin,
            destination,
            commodity.outside_option,
            edge_okay,
            graph,
            a_star_table,
            false,
        ),
    }
    .0
    .unwrap_or_else(|| PathBox::outside(commodity_idx));

    let may_add: FVal = best_response
        .payload()
        .edges()
        .iter()
        .filter_map(|&edge_idx| match graph.edge(edge_idx).edge_type {
            EdgeType::Drive(capacity) => {
                let slack = capacity - flow.on_edge(edge_idx);
                if slack <= EPS_L {
                    let may_add = -direction.on_edge(edge_idx);
                    assert!(may_add > EPS);
                    Some(may_add)
                } else {
                    None
                }
            }
            _ => None,
        })
        .min_by(|a: &f64, b: &f64| a.total_cmp(b))
        .unwrap_or(FVal::INFINITY);

    (best_response, may_add)
}

pub fn iter_source_nodes<'a>(
    graph: &'a Graph,
    choice: &'a DepartureTimeChoice,
) -> impl Iterator<Item = NodeIdx> + use<'a> {
    match &choice.spawn_node_range {
        None => Box::<dyn Iterator<Item = NodeIdx>>::from(Box::new(empty())),
        Some((first_node, last_node)) => {
            struct NodeIterator<'a> {
                next_node: Option<NodeIdx>,
                graph: &'a Graph,
            }
            impl<'a> Iterator for NodeIterator<'a> {
                type Item = NodeIdx;

                fn next(&mut self) -> Option<Self::Item> {
                    let node = match self.next_node {
                        None => return None,
                        Some(it) => it,
                    };
                    let graph = self.graph;
                    self.next_node = graph.node(node).outgoing.iter().find_map(|e_idx| {
                        let e = graph.edge(*e_idx);
                        if e.edge_type == EdgeType::Wait {
                            Some(e.to)
                        } else {
                            None
                        }
                    });
                    return Some(node);
                }
            }

            Box::new(
                NodeIterator {
                    next_node: Some(*first_node),
                    graph,
                }
                .take_while_inclusive(|node_idx| *node_idx != *last_node),
            )
        }
    }
}
