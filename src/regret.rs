use crate::{
    col::HashMap,
    flow::Flow,
    graph::Graph,
    path_index::{PathId, PathsIndex},
    primitives::{EPS_L, FVal, Time},
    shortest_path::{a_star::AStarTable, best_paths::find_better_path},
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

fn compute_regret_per_unit(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    path_id: PathId,
    paths: &PathsIndex,
) -> Time {
    let path = paths.path(path_id);
    match find_better_path(a_star_table, graph, flow, path) {
        None => 0,
        Some(best_response) => {
            let commodity = graph.commodity(path.commodity_idx());
            let arrival_best_response = best_response.payload().cost(commodity, graph);
            let arrival_current = path.cost(commodity, graph);
            arrival_current - arrival_best_response
        }
    }
}

pub struct PathRegret {
    pub regret: u32,
    pub relative_regret: f64,
}

pub fn get_regret_map(
    graph: &Graph,
    flow: &Flow,
    paths: &PathsIndex,
    a_star_table: &AStarTable,
) -> HashMap<PathId, PathRegret> {
    get_best_response_entries(graph, flow, a_star_table, paths)
        .map(|it| {
            (
                it.path_id,
                PathRegret {
                    regret: it.cost_current - it.cost_best_response,
                    relative_regret: (it.cost_current - it.cost_best_response) as f64
                        / it.cost_best_response as f64,
                },
            )
        })
        .collect()
}

pub struct BestResponseComparison {
    pub cost_current: Time,
    pub cost_best_response: Time,
}

fn compute_best_response_comparison(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    path_id: PathId,
    paths: &PathsIndex,
) -> BestResponseComparison {
    let path = paths.path(path_id);
    let cost_current = path.cost(graph.commodity(path.commodity_idx()), graph);
    match find_better_path(a_star_table, graph, flow, path) {
        None => BestResponseComparison {
            cost_current,
            cost_best_response: cost_current,
        },
        Some(best_response) => {
            let commodity = graph.commodity(path.commodity_idx());
            let cost_best_response = best_response.payload().cost(commodity, graph);
            BestResponseComparison {
                cost_current,
                cost_best_response,
            }
        }
    }
}

pub struct RegretFlowEntry {
    pub path_id: PathId,
    pub regret: Time,
    pub flow: FVal,
}

pub fn get_total_regret(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    paths: &PathsIndex,
) -> FVal {
    get_regret_flow_entries(graph, flow, &a_star_table, &paths)
        .map(|it| it.flow * it.regret as FVal)
        .sum()
}

pub fn get_total_relative_regret(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    paths: &PathsIndex,
) -> FVal {
    get_best_response_entries(graph, flow, a_star_table, paths)
        .map(|entry| {
            let relative_regret = (entry.cost_current - entry.cost_best_response) as FVal
                / entry.cost_best_response as FVal;
            entry.flow * relative_regret
        })
        .sum()
}

pub fn get_mean_relative_regret(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    paths: &PathsIndex,
) -> FVal {
    get_total_relative_regret(graph, flow, a_star_table, paths) / graph.total_demand()
}

pub fn get_max_relative_regret(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    paths: &PathsIndex,
) -> FVal {
    get_best_response_entries(graph, flow, a_star_table, paths)
        .filter(|entry| entry.flow >= EPS_L)
        .map(|entry| {
            let relative_regret = (entry.cost_current - entry.cost_best_response) as FVal
                / entry.cost_best_response as FVal;
            relative_regret
        })
        .max_by(|a, b| a.total_cmp(b))
        .unwrap_or(0.0)
}

pub struct BestResponseEntry {
    pub path_id: PathId,
    pub flow: FVal,
    pub cost_current: Time,
    pub cost_best_response: Time,
}

pub fn get_best_response_entries<'a>(
    graph: &'a Graph,
    flow: &'a Flow,
    a_star_table: &'a AStarTable,
    paths: &'a PathsIndex,
) -> impl ParallelIterator<Item = BestResponseEntry> + 'a {
    flow.path_flow_map()
        .par_iter()
        .map(|(&path_id, &flow_val)| {
            let comparison =
                compute_best_response_comparison(graph, flow, a_star_table, path_id, paths);
            BestResponseEntry {
                path_id,
                flow: flow_val,
                cost_current: comparison.cost_current,
                cost_best_response: comparison.cost_best_response,
            }
        })
}

pub fn get_regret_flow_entries<'a>(
    graph: &'a Graph,
    flow: &'a Flow,
    a_star_table: &'a AStarTable,
    paths: &'a PathsIndex,
) -> impl ParallelIterator<Item = RegretFlowEntry> + 'a {
    flow.path_flow_map()
        .par_iter()
        .map(|(&path_id, &flow_val)| {
            let regret = compute_regret_per_unit(graph, flow, a_star_table, path_id, paths);
            RegretFlowEntry {
                path_id,
                regret,
                flow: flow_val,
            }
        })
}

pub fn get_num_regretting_paths(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    paths: &PathsIndex,
) -> usize {
    flow.path_flow_map()
        .par_iter()
        .filter(|(path_id, _)| {
            compute_regret_per_unit(graph, flow, a_star_table, **path_id, paths) > 0
        })
        .count()
}

pub fn get_regretting_demand(
    graph: &Graph,
    flow: &Flow,
    a_star_table: &AStarTable,
    paths: &PathsIndex,
) -> FVal {
    flow.path_flow_map()
        .par_iter()
        .filter(|(path_id, _)| {
            compute_regret_per_unit(graph, flow, a_star_table, **path_id, paths) > 0
        })
        .map(|(_, flow_val)| flow_val)
        .sum()
}
