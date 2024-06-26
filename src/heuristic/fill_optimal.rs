use std::cmp::min;

use log::info;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    a_star::{a_star, AStarTable},
    best_paths::get_path_prefer_wait_or_dwell,
    col::HashSet,
    flow::Flow,
    graph::{EdgeIdx, EdgeNavigate, Graph, PathBox, EPS, EPS_L},
    heuristic::{arrival_of_path, get_longest_feasible_extension},
    paths_index::{PathId, PathsIndex},
};

pub fn fill_all_optimal_paths(
    flow: &mut Flow,
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
) {
    let mut full_edges: HashSet<EdgeIdx> = flow
        .edge_flow_map()
        .par_iter()
        .filter(|&(edge_idx, value)| match graph.nav_edge(*edge_idx) {
            EdgeNavigate::Drive(edge) => edge.capacity() - value <= EPS_L,
            _ => false,
        })
        .map(|(&edge_idx, _)| edge_idx)
        .collect();

    let mut swappable_paths = flow.path_flow_map().keys().cloned().collect::<Vec<_>>();
    while !swappable_paths.is_empty() {
        swappable_paths = fill_optimal_single_round(
            graph,
            a_star_table,
            flow,
            paths,
            swappable_paths,
            &mut full_edges,
        );
    }
}

fn fill_optimal_single_round(
    graph: &Graph,
    a_star_table: &AStarTable,
    flow: &mut Flow,
    paths: &mut PathsIndex,
    check_paths: Vec<PathId>,
    full_edges: &mut HashSet<EdgeIdx>,
) -> Vec<PathId> {
    info!("Computing optimal paths given currently full edges.");
    let optimal_paths: Vec<_> = check_paths
        .par_iter()
        .filter_map(|old_path_id| {
            optimal_swap(*old_path_id, paths, graph, a_star_table, full_edges)
        })
        .collect();

    info!(
        "Found {:} optimal paths. Swapping where possible.",
        optimal_paths.len()
    );
    let mut direction = Flow::new();

    let mut check_paths_again = Vec::new();
    let num_optimal_paths = optimal_paths.len();
    for (old_path_id, optimal_path) in optimal_paths {
        direction.add_flow_onto_path(paths, old_path_id, -1.0, false, false);
        let new_path_id = paths.transfer_path(optimal_path);
        direction.add_flow_onto_path(paths, new_path_id, 1.0, false, false);
        let may_add = get_longest_feasible_extension(graph, flow, &direction, false, false);
        if may_add >= EPS {
            flow.add_flow_onto_path(paths, old_path_id, -may_add, true, false);
            flow.add_flow_onto_path(paths, new_path_id, may_add, true, false);

            for edge_idx in paths
                .path(new_path_id)
                .edges()
                .iter()
                .chain(paths.path(old_path_id).edges().iter())
            {
                if let EdgeNavigate::Drive(e) = graph.nav_edge(*edge_idx) {
                    let slack = e.capacity() - flow.on_edge(*edge_idx);
                    if slack <= EPS_L {
                        full_edges.insert(*edge_idx);
                    } else {
                        full_edges.remove(edge_idx);
                    }
                }
            }
        } else {
            check_paths_again.push(old_path_id);
        }
        direction.reset();
    }

    info!(
        "Performed {:} swaps to optimal paths.",
        num_optimal_paths - check_paths_again.len()
    );
    flow.recompute_edge_flow();
    info!("Done recomputing edge flow.");
    check_paths_again
}

fn optimal_swap(
    old_path_id: PathId,
    paths: &PathsIndex,
    graph: &Graph,
    a_star_table: &AStarTable,
    full_edges: &HashSet<EdgeIdx>,
) -> Option<(PathId, PathBox)> {
    let old_path = paths.path(old_path_id);
    let commodity = graph.commodity(old_path.commodity_idx());
    let spawn_idx = match commodity.spawn_node {
        None => return None,
        Some(it) => it,
    };
    let destination = commodity.od_pair.destination;
    let max_arrival_time = min(
        a_star_table.earliest_arrival(spawn_idx, destination),
        arrival_of_path(old_path, graph, commodity) - 1,
    );

    let a_star_res = a_star(
        graph,
        a_star_table,
        spawn_idx,
        destination,
        |it, _| !full_edges.contains(&it),
        false,
        max_arrival_time,
    );

    let destination_node_idx = match a_star_res.destination {
        None => return None,
        Some(it) => it,
    };

    let path =
        get_path_prefer_wait_or_dwell(graph, destination_node_idx, spawn_idx, |edge_idx, edge| {
            a_star_res.astar_reached.contains(&edge.from) && !full_edges.contains(&edge_idx)
        });

    let path = PathBox::new(old_path.commodity_idx(), path.into_iter());

    debug_assert!(
        arrival_of_path(path.payload(), graph, commodity)
            < arrival_of_path(old_path, graph, commodity)
    );

    Some((old_path_id, path))
}
