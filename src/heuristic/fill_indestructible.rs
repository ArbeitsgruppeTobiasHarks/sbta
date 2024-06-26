use log::{debug, info};

use crate::{
    a_star::{a_star, a_star_reachable_within, compute_a_star_table, AStarTable},
    col::{set_new, HashSet},
    flow::Flow,
    graph::{DescribePath, EdgeIdx, EdgePayload, EdgeType, FVal, Graph, NodeIdx, PathBox, EPS_L},
    heuristic::{arrival_of_path, get_longest_feasible_extension},
    iter::par_map_until::{ParMapResult, ParMapUntil},
    paths_index::{PathId, PathsIndex},
};

pub fn fill_indestructible_paths(
    graph: &Graph,
    paths: &mut PathsIndex,
    a_star_table: &mut AStarTable,
    flow: &mut Flow,
) {
    info!("Swapping indestructible paths first.");
    info!("Computing A*-Table...");
    compute_a_star_table(a_star_table, graph, |_, _| true);
    let mut outside_paths = paths
        .path_ids()
        .filter(|&path_id| flow.on_path(path_id) > EPS_L)
        .collect::<Vec<_>>();
    let mut start_with_path_idx = 0;
    let should_try_harder = false;
    loop {
        struct NoTryHard {}
        struct TryHard {}
        impl TryHarder for NoTryHard {
            const TRY_HARDER: bool = false;
        }
        impl TryHarder for TryHard {
            const TRY_HARDER: bool = true;
        }

        let next_idx = fill_indestructible_optimal_paths::<NoTryHard>(
            graph,
            flow,
            a_star_table,
            &mut outside_paths,
            paths,
            start_with_path_idx,
        );
        if let Some(idx) = next_idx {
            start_with_path_idx = idx;
        } else if should_try_harder {
            let next_idx = fill_indestructible_optimal_paths::<TryHard>(
                graph,
                flow,
                a_star_table,
                &mut outside_paths,
                paths,
                start_with_path_idx,
            );
            match next_idx {
                None => break,
                Some(idx) => start_with_path_idx = idx,
            }
        } else {
            break;
        }
    }
}

trait TryHarder {
    const TRY_HARDER: bool;
}

/// .
/// # Arguments
///
/// * `graph` - a reference to the graph
/// * `flow` - a mutable reference to the flow
/// * `outside_paths` - a mutable reference to all outside paths that have positive flow
/// * `paths_index` - a mutable reference to the paths index
/// * `start_with_path_idx` - the index of the path to start with (for round robin)
/// * `try_harder` - if true, we do a more advanced but more expensive check for indestructible paths
///
fn fill_indestructible_optimal_paths<B: TryHarder>(
    graph: &Graph,
    flow: &mut Flow,
    a_star_table: &mut AStarTable,
    outside_paths: &mut Vec<PathId>,
    paths_index: &mut PathsIndex,
    start_with_path_idx: usize,
) -> Option<usize> {
    // Fill only optimal paths, if they only use boarding edges
    // whose previous drive edge is already full or does not exist.
    // Move flow away from outside option only.

    // If try_harder is set, we do a more advanced but more expensive check:
    // Use boarding edges also, if the previous dwell edge is no longer
    // accessible by any commodity.
    // Here "accessible" means, it is not on any s-t-path of any commodity with positive flow on their outside option.

    let edge_okay_opt = |_: EdgeIdx, edge: &EdgePayload| -> bool {
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
    };

    let path_reachable: Option<HashSet<NodeIdx>> = if !B::TRY_HARDER {
        None
    } else {
        info!("TRYING HARDER...");
        info!("Rebuilding A*-Table...");
        compute_a_star_table(a_star_table, graph, edge_okay_opt);
        info!("Computing reachable nodes...");
        // The set of nodes that are reachable by some commodity
        let mut path_reach_set: HashSet<NodeIdx> = set_new();
        for &path_id in outside_paths.iter() {
            let path = paths_index.path(path_id);
            let commodity = graph.commodity(path.commodity_idx());
            let source_id = match commodity.spawn_node {
                None => continue,
                Some(it) => it,
            };
            let outside_time = commodity.outside_latest_arrival;
            let destination = commodity.od_pair.destination;
            let commodity_reachable = a_star_reachable_within(
                graph,
                a_star_table,
                source_id,
                destination,
                outside_time,
                edge_okay_opt,
            );

            path_reach_set.extend(commodity_reachable);
        }
        Some(path_reach_set)
    };

    let edge_okay_indestructible = |id: EdgeIdx, edge: &EdgePayload| -> bool {
        if !edge_okay_opt(id, edge) {
            return false;
        }
        if !matches!(edge.edge_type, EdgeType::Board(_)) {
            return true;
        }
        let previous_drive_edge = graph
            .node(edge.to)
            .incoming
            .iter()
            .find(|prev_edge_id| matches!(graph.edge(**prev_edge_id).edge_type, EdgeType::Dwell))
            .map(|dwell_edge| graph.node(graph.edge(*dwell_edge).from).incoming[0]);

        match previous_drive_edge {
            None => true,
            Some(previous_drive_edge) => {
                let flow = flow.on_edge(previous_drive_edge);
                let capacity = match graph.edge(previous_drive_edge).edge_type {
                    EdgeType::Drive(capacity) => capacity,
                    _ => panic!("Previous drive edge is not a drive edge!"),
                };
                let slack = capacity - flow;
                if slack <= EPS_L {
                    return true;
                }
                if !B::TRY_HARDER {
                    return false;
                }
                return !path_reachable.as_ref().unwrap().contains(&edge.to);
            }
        }
    };

    let indestructible_result = outside_paths
        .iter()
        .enumerate()
        .skip(start_with_path_idx)
        .chain(
            outside_paths
                .iter()
                .enumerate()
                .take(start_with_path_idx + 1),
        )
        .par_map_until(|(idx, &path_id)| {
            let path = paths_index.path(path_id);
            let commodity = graph.commodity(path.commodity_idx());
            let source_id = match commodity.spawn_node {
                None => return ParMapResult::NotFound(()),
                Some(it) => it,
            };
            let destination = commodity.od_pair.destination;
            let max_arrival_time = arrival_of_path(path, graph, commodity) - 1;
            let result_opt = a_star(
                graph,
                a_star_table,
                source_id,
                destination,
                edge_okay_opt,
                false,
                max_arrival_time,
            );
            let destination_node_idx = match result_opt.destination {
                None => {
                    // We can actually ignore any further demand of this commodity; but that's future work.
                    return ParMapResult::NotFound(());
                }
                Some(it) => it,
            };
            // Check if optimal time is reachable with said set of boarding edges.
            let arrival_opt = graph.node(destination_node_idx).time;
            let result_indestructible = a_star(
                graph,
                a_star_table,
                source_id,
                destination,
                edge_okay_indestructible,
                false,
                arrival_opt,
            );

            let destination_node_idx = match result_indestructible.destination {
                None => return ParMapResult::NotFound(()),
                Some(it) => it,
            };
            // There is an indestructible, optimal path!
            ParMapResult::Found(Box::new((
                idx,
                source_id,
                path_id,
                result_indestructible,
                destination_node_idx,
            )))
        });
    let (old_path_idx, source_id, old_path_id, result_indestructible, destination_node_idx) =
        match indestructible_result.last() {
            None => return None,
            Some(ParMapResult::NotFound(_)) => return None,
            Some(ParMapResult::Found(it)) => *it,
        };

    // There is a indestructible, optimal path!
    // Let's find it by backtracking.
    let mut path = vec![];
    let mut cur_node = (destination_node_idx, graph.node(destination_node_idx));

    while cur_node.0 != source_id {
        let chosen_edge = cur_node
            .1
            .incoming
            .iter()
            .find_map(|&edge_idx| {
                let edge = graph.edge(edge_idx);
                if !result_indestructible.astar_reached.contains(&edge.from) {
                    return None;
                }
                if !edge_okay_indestructible(edge_idx, edge) {
                    return None;
                }
                Some((edge_idx, edge))
            })
            .unwrap();
        path.push(chosen_edge.0);
        cur_node = (chosen_edge.1.from, graph.node(chosen_edge.1.from));
    }

    path.reverse();

    let old_path = paths_index.path(old_path_id);
    let path = PathBox::new(old_path.commodity_idx(), path.into_iter());

    let new_path_id = paths_index.transfer_path(path);

    let mut direction = Flow::new();
    direction.add_flow_onto_path(paths_index, old_path_id, -1.0, false, false);
    direction.add_flow_onto_path(paths_index, new_path_id, 1.0, false, false);

    if B::TRY_HARDER {
        print!("TRYHARD: ");
    }
    debug!(
        "Swap to indestructible: {:}",
        DescribePath::describe(paths_index.path(new_path_id), graph)
    );
    let swap_amount = get_longest_feasible_extension(graph, flow, &direction, true, true);
    assert!(swap_amount > 0.0);
    assert!(swap_amount < FVal::INFINITY);

    flow.add_flow_onto_path(paths_index, old_path_id, -swap_amount, true, true);
    flow.add_flow_onto_path(paths_index, new_path_id, swap_amount, true, true);

    if flow.on_path(old_path_id) <= EPS_L {
        outside_paths.swap_remove(old_path_idx);
    }
    Some(old_path_idx % outside_paths.len())
}
