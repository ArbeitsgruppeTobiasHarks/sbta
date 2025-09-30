use log::{debug, info};

use crate::{
    col::HashSet,
    flow::Flow,
    graph::{CostCharacteristic, DescribePath, EdgeIdx, EdgePayload, EdgeType, Graph, NodeIdx},
    heuristic::get_longest_feasible_extension,
    iter::par_map_until::{ParMapResult, ParMapUntil},
    path_index::{PathId, PathsIndex},
    primitives::{EPS_L, FVal},
    shortest_path::{
        a_star::{AStarTable, compute_a_star_table},
        best_paths::{
            edge_is_not_a_boarding_edge_of_a_full_driving_edge, find_shortest_path_choice,
            find_shortest_path_fixed,
        },
    },
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
    // Use boarding edges also, if the previous stay-on edge is no longer
    // accessible by any commodity.
    // Here "accessible" means, it is not on any s-t-path of any commodity with positive flow on their outside option.

    let edge_okay_opt = |_: EdgeIdx, edge: &EdgePayload| -> bool {
        edge_is_not_a_boarding_edge_of_a_full_driving_edge(edge, flow, graph)
    };

    let path_reachable: Option<HashSet<NodeIdx>> = None;
    /*
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
    };*/

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
            .find(|prev_edge_id| matches!(graph.edge(**prev_edge_id).edge_type, EdgeType::StayOn))
            .map(|stay_on_edge| graph.node(graph.edge(*stay_on_edge).from).incoming[0]);

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
            let commodity_idx = path.commodity_idx();
            let commodity = graph.commodity(commodity_idx);
            let destination = commodity.od_pair.destination;
            let indestructible_path_opt = match &commodity.cost_characteristic {
                CostCharacteristic::FixedDeparture(fixed) => {
                    let max_time = path.arrival_time(graph, fixed) - 1;
                    let Some(opt_path) = find_shortest_path_fixed(
                        commodity_idx,
                        destination,
                        fixed.spawn_node,
                        max_time,
                        edge_okay_opt,
                        graph,
                        a_star_table,
                        false,
                    )
                    .0
                    else {
                        return ParMapResult::NotFound(());
                    };
                    let opt_time = opt_path.payload().arrival_time(graph, fixed);
                    find_shortest_path_fixed(
                        commodity_idx,
                        destination,
                        fixed.spawn_node,
                        opt_time,
                        edge_okay_indestructible,
                        graph,
                        a_star_table,
                        false,
                    )
                    .0
                }
                CostCharacteristic::DepartureTimeChoice(choice) => {
                    let max_cost = path.cost_choice(commodity, choice, graph) - 1;
                    let origin = commodity.od_pair.origin;
                    let Some(opt_path) = find_shortest_path_choice(
                        commodity_idx,
                        choice,
                        origin,
                        destination,
                        max_cost,
                        edge_okay_opt,
                        graph,
                        a_star_table,
                        false,
                    )
                    .0
                    else {
                        return ParMapResult::NotFound(());
                    };
                    let opt_cost = opt_path.payload().cost_choice(commodity, choice, graph);
                    find_shortest_path_choice(
                        commodity_idx,
                        choice,
                        origin,
                        destination,
                        opt_cost,
                        edge_okay_indestructible,
                        graph,
                        a_star_table,
                        false,
                    )
                    .0
                }
            };
            let Some(indestructible_path) = indestructible_path_opt else {
                return ParMapResult::NotFound(());
            };
            ParMapResult::Found(Box::new((idx, path_id, indestructible_path)))
        });
    let (old_path_idx, old_path_id, indestructible_path) = match indestructible_result.last()? {
        ParMapResult::NotFound(_) => return None,
        ParMapResult::Found(it) => *it,
    };

    let new_path_id = paths_index.transfer_path(indestructible_path);

    let mut direction = Flow::new();
    direction.add_flow_onto_path(paths_index, old_path_id, -1.0, false, false);
    direction.add_flow_onto_path(paths_index, new_path_id, 1.0, false, false);

    debug!(
        "Swap to indestructible {:?}: {:}",
        new_path_id,
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
