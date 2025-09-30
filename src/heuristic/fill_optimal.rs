use std::cmp::{max, min};

use log::info;
use mcra::MCRA;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    col::HashSet,
    flow::Flow,
    graph::{CostCharacteristic, EdgeIdx, EdgeNavigate, EdgeType, Graph, PathBox},
    heuristic::{FlowStats, get_longest_feasible_extension},
    opt::Router,
    path_index::{PathId, PathsIndex},
    primitives::{EPS, EPS_L, Time},
    shortest_path::{
        a_star::AStarTable,
        best_paths::{find_shortest_path_choice, find_shortest_path_fixed, iter_source_nodes},
    },
};

pub fn fill_using_system_optimum(
    flow: &mut Flow,
    graph: &Graph,
    a_star_table: &AStarTable,
    forbidden_edges: &HashSet<EdgeIdx>,
    paths: &mut PathsIndex,
) {
    let mut mcra: MCRA<gurobi::Model> = MCRA::new();
    for (edge_id, edge) in graph.edges() {
        if let EdgeType::Drive(capacity) = edge.edge_type {
            mcra.add_ressource(edge_id.0 as usize, capacity - flow.on_edge(edge_id));
        }
    }

    let mut outside_paths: Vec<PathId> = Vec::new();
    for (path_id, flow_val) in flow.path_flow_map() {
        let path = paths.path(*path_id);
        if !path.is_outside() {
            continue;
        }
        let commodity_idx = path.commodity_idx();
        let commodity = graph.commodity(commodity_idx);
        mcra.add_commodity(commodity_idx.0 as usize, *flow_val);
        mcra.add_bundle(
            path_id.0 as usize,
            commodity_idx.0 as usize,
            path.cost(commodity, graph) as f64,
            vec![],
        );
        outside_paths.push(*path_id);
    }
    for path_id in outside_paths {
        let flow_val = flow.on_path(path_id);
        flow.add_flow_onto_path(paths, path_id, -flow_val, true, false);
    }

    info!("Computing system optimum for outside paths.");

    let remaining_flow = mcra.solve(&mut Router {
        graph,
        paths,
        a_star_table,
        edge_okay: |edge_idx, _| !forbidden_edges.contains(&edge_idx),
    });
    for (path_id, flow_val) in remaining_flow {
        let path_id = PathId(path_id as u32);
        flow.add_flow_onto_path(paths, path_id, flow_val, true, false);
    }
    info!("Done.");
    info!(
        "Flow Stats: {:#?}",
        FlowStats::compute(graph, flow, a_star_table, paths)
    )
}

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
                .map(|it| (*old_path_id, it))
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
) -> Option<PathBox> {
    let old_path = paths.path(old_path_id);
    let commodity_idx = old_path.commodity_idx();
    let commodity = graph.commodity(commodity_idx);
    let destination = commodity.od_pair.destination;

    match &commodity.cost_characteristic {
        CostCharacteristic::FixedDeparture(fixed) => {
            let spawn_idx = fixed.spawn_node?;
            let max_arrival_time = min(
                a_star_table.earliest_arrival(spawn_idx, destination),
                old_path.arrival_time(graph, fixed) - 1,
            );
            find_shortest_path_fixed(
                commodity_idx,
                destination,
                fixed.spawn_node,
                max_arrival_time,
                |it, _| !full_edges.contains(&it),
                graph,
                a_star_table,
                false,
            )
        }
        CostCharacteristic::DepartureTimeChoice(choice) => {
            let opt_cost = iter_source_nodes(graph, choice)
                .map(|spawn_idx| {
                    let arrival = max(
                        a_star_table.earliest_arrival(spawn_idx, destination),
                        choice.target_arrival_time,
                    );
                    if arrival == Time::MAX {
                        return Time::MAX;
                    }
                    let delay = arrival - choice.target_arrival_time;
                    let travel_time = arrival - graph.node(spawn_idx).time;
                    travel_time + delay * choice.delay_penalty_factor
                })
                .max();
            let max_cost = min(
                old_path.cost_choice(commodity, choice, graph) - 1,
                opt_cost.unwrap_or(Time::MAX),
            );
            find_shortest_path_choice(
                commodity_idx,
                choice,
                commodity.od_pair.origin,
                destination,
                max_cost,
                |it, _| !full_edges.contains(&it),
                graph,
                a_star_table,
                false,
            )
        }
    }
    .0
}
