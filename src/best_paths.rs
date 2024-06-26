use std::iter::empty;

use crate::{
    a_star::{a_star, AStarTable},
    col::{set_new, HashSet},
    flow::Flow,
    graph::{
        EdgeIdx, EdgePayload, EdgeType, FVal, Graph, NodeIdx, Path, PathBox, Time, EPS, EPS_L,
    },
    heuristic::arrival_of_path,
};

/// For a given path, tries to find a better and available path.
/// If no better path is found, returns a set of edges that block the better paths.
pub fn find_better_path(
    a_star_table: &AStarTable,
    graph: &Graph,
    flow: &Flow,
    path: &Path,
) -> Result<PathBox, HashSet<EdgeIdx>> {
    let commodity = graph.commodity(path.commodity_idx());
    let spawn_node_id = match commodity.spawn_node {
        None => {
            debug_assert!(path.is_outside());
            return Err(set_new());
        }
        Some(it) => it,
    };

    let destination = commodity.od_pair.destination;

    // Returns true, if the edge is blocked, ie., if it is a boarding edge of a full drive edge.
    let edge_okay = |_: EdgeIdx, edge: &EdgePayload| -> bool {
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
        if slack > EPS_L {
            return true;
        }
        if path.edges().contains(&drive_edge) {
            return true;
        }
        false
    };

    let result = a_star(
        graph,
        a_star_table,
        spawn_node_id,
        destination,
        edge_okay,
        true,
        arrival_of_path(path, graph, commodity) - 1,
    );
    let destination_node_idx = match result.destination {
        None => {
            // No faster path was found.
            return Err(result.unblocking_edges);
        }
        Some(it) => it,
    };

    let better_path = get_path_prefer_wait_or_dwell(
        graph,
        destination_node_idx,
        spawn_node_id,
        |edge_idx, edge| result.astar_reached.contains(&edge.from) && edge_okay(edge_idx, edge),
    );
    Ok(PathBox::new(path.commodity_idx(), better_path.into_iter()))
}

pub fn get_path_prefer_wait_or_dwell(
    graph: &Graph,
    destination: NodeIdx,
    source: NodeIdx,
    edge_okay: impl Fn(EdgeIdx, &EdgePayload) -> bool,
) -> Vec<EdgeIdx> {
    let mut edges = vec![];
    let mut cur_node = (destination, graph.node(destination));

    while cur_node.0 != source {
        let chosen_edge = {
            let mut chosen_edge = None;
            for &edge_idx in cur_node.1.incoming.iter() {
                let edge = graph.edge(edge_idx);
                if !edge_okay(edge_idx, edge) {
                    continue;
                }
                chosen_edge = Some((edge_idx, edge));
                if matches!(edge.edge_type, EdgeType::Dwell | EdgeType::Wait) {
                    break;
                }
            }
            chosen_edge.unwrap()
        };
        edges.push(chosen_edge.0);

        cur_node = (chosen_edge.1.from, graph.node(chosen_edge.1.from));
    }
    edges.reverse();

    edges
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
    let commodity = graph.commodity(path.commodity_idx());
    let spawn_node_id = match commodity.spawn_node {
        None => {
            debug_assert!(path.is_outside());
            return (PathBox::new(path.commodity_idx(), empty()), FVal::INFINITY);
        }
        Some(it) => it,
    };
    let destination = commodity.od_pair.destination;

    let edge_okay = |edge_idx: EdgeIdx, edge: &EdgePayload| -> bool {
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
    };

    let result = a_star(
        graph,
        a_star_table,
        spawn_node_id,
        destination,
        edge_okay,
        false,
        Time::MAX - 1,
    );

    let destination_node_idx = match result.destination {
        None => {
            // No available path was found.
            // Return outside path.
            return (PathBox::new(path.commodity_idx(), empty()), FVal::INFINITY);
        }
        Some(it) => it,
    };

    let better_path = get_path_prefer_wait_or_dwell(
        graph,
        destination_node_idx,
        spawn_node_id,
        |edge_idx, edge| result.astar_reached.contains(&edge.from) && edge_okay(edge_idx, edge),
    );
    let may_add: FVal = better_path
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
        .min_by(|a, b| a.total_cmp(b))
        .unwrap_or(FVal::INFINITY);

    (
        PathBox::new(path.commodity_idx(), better_path.into_iter()),
        may_add,
    )
}
