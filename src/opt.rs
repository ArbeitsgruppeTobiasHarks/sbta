use std::collections::HashMap;

use log::info;
use mcra::{
    Bundle, BundleRessourceUsage, MCRA, MinCostBundleSolver, MinimalCostBundleJob,
    lp::LinearProgram,
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    flow::Flow,
    graph::{
        CommodityIdx, CostCharacteristic, EdgeIdx, EdgePayload, EdgeType, Graph, NodePayload,
        NodeType, PathBox,
    },
    heuristic::{FlowStats, Stats},
    path_index::{PathId, PathsIndex},
    primitives::EPS_L,
    shortest_path::{
        a_star::{AStarMultiSourceParams, AStarTable, a_star_multi_source, compute_a_star_table},
        best_paths::{get_path_prefer_waiting_or_staying, iter_source_nodes},
    },
};

pub fn init_mcra<LP: LinearProgram>(graph: &Graph, paths: &mut PathsIndex) -> MCRA<LP> {
    let mut mcra = mcra::MCRA::<LP>::new();
    graph.edges().for_each(|(edge_idx, edge)| {
        let EdgeType::Drive(capacity) = edge.edge_type else {
            return;
        };
        mcra.add_ressource(edge_idx.0 as usize, capacity);
    });
    graph.commodities().for_each(|(idx, commodity)| {
        mcra.add_commodity(idx.0 as usize, commodity.demand);
    });
    graph.commodities().for_each(|(idx, commodity)| {
        let path_id = paths.transfer_path(PathBox::outside(idx));
        let original_cost = paths.path(path_id).cost(commodity, graph) as f64;
        mcra.add_bundle(path_id.0 as usize, idx.0 as usize, original_cost, vec![]);
    });
    mcra
}

fn assert_le_by_eps(a: f64, b: f64) {
    assert!(
        a <= b + EPS_L,
        "assert_le_by_eps failed: {} > {} + {} = {}",
        a,
        b,
        EPS_L,
        b + EPS_L
    );
}

fn debug_assert_le_by_eps(a: f64, b: f64) {
    debug_assert!(
        a <= b + EPS_L,
        "assert_le_by_eps failed: {} > {} + {} = {}",
        a,
        b,
        EPS_L,
        b + EPS_L
    );
}

pub struct Router<'a, 'b, F: Fn(EdgeIdx, &EdgePayload) -> bool + Sync> {
    pub graph: &'a Graph,
    pub paths: &'a mut PathsIndex<'b>,
    pub a_star_table: &'a AStarTable,
    pub edge_okay: F,
}

impl<'a, 'b, F: Fn(EdgeIdx, &EdgePayload) -> bool + Sync> Router<'a, 'b, F> {
    fn process_job(
        &self,
        job: &MinimalCostBundleJob,
        shadow_prices: &HashMap<usize, f64>,
    ) -> Option<(PathBox, Bundle)> {
        let get_edge_cost = |edge_idx: EdgeIdx,
                             edge: &EdgePayload,
                             node_from: &NodePayload,
                             node_to: &NodePayload| {
            let travel_time = node_to.time - node_from.time;
            if !matches!(edge.edge_type, EdgeType::Drive(_)) {
                return travel_time as f64;
            }
            shadow_prices.get(&(edge_idx.0 as usize)).unwrap() + travel_time as f64
        };

        let commodity_idx = CommodityIdx(job.commodity_id as u32);
        let commodity = self.graph.commodity(commodity_idx);

        let result = match &commodity.cost_characteristic {
            CostCharacteristic::FixedDeparture(fixed) => {
                let params = AStarMultiSourceParams {
                    graph: self.graph,
                    a_star_table: self.a_star_table,
                    destination: commodity.od_pair.destination,
                    target_arrival_time: fixed.departure,
                    delay_penalty_factor: 0,
                };

                let source_nodes: Option<(crate::graph::NodeIdx, f64)> = fixed
                    .spawn_node
                    .map(|it| (it, (self.graph.node(it).time - fixed.departure) as f64));
                a_star_multi_source(
                    || source_nodes.iter().cloned(),
                    false,
                    job.cost_upper_bound,
                    &params,
                    &self.edge_okay,
                    get_edge_cost,
                )
            }
            CostCharacteristic::DepartureTimeChoice(choice) => {
                let source_nodes = || iter_source_nodes(self.graph, choice).map(|it| (it, 0.0));

                let params = AStarMultiSourceParams {
                    graph: self.graph,
                    a_star_table: self.a_star_table,
                    destination: commodity.od_pair.destination,
                    target_arrival_time: choice.target_arrival_time,
                    delay_penalty_factor: choice.delay_penalty_factor,
                };

                a_star_multi_source(
                    source_nodes,
                    false,
                    job.cost_upper_bound,
                    &params,
                    &self.edge_okay,
                    get_edge_cost,
                )
            }
        };

        let Some(destination_node_idx) = result.destinations.first() else {
            return None;
        };

        let edges = get_path_prefer_waiting_or_staying(
            self.graph,
            *destination_node_idx,
            |node_idx, node| match &commodity.cost_characteristic {
                CostCharacteristic::FixedDeparture(fixed) => fixed.spawn_node == Some(node_idx),
                CostCharacteristic::DepartureTimeChoice(_) => {
                    node.node_type == NodeType::Wait(commodity.od_pair.origin)
                        && result.reached.get(&node_idx).unwrap().cost_thus_far == 0.0
                }
            },
            |edge_idx, edge| {
                let Some(from_value) = result.reached.get(&edge.from) else {
                    return false;
                };
                let node_from = self.graph.node(edge.from);
                let node_to = self.graph.node(edge.to);
                let edge_cost = get_edge_cost(edge_idx, edge, node_from, node_to);
                let to_value = result.reached.get(&edge.to).unwrap();
                debug_assert_le_by_eps(
                    to_value.cost_thus_far,
                    from_value.cost_thus_far + edge_cost,
                );
                return to_value.cost_thus_far >= from_value.cost_thus_far + edge_cost - EPS_L;
            },
        );

        let path = PathBox::new(commodity_idx, edges.into_iter());
        let path_priced_cost = result
            .reached
            .get(destination_node_idx)
            .unwrap()
            .total_cost_lower_bound;
        if path_priced_cost > job.cost_upper_bound - EPS_L {
            return None;
        }

        let original_cost = path.payload().cost(commodity, self.graph) as f64;
        let resource_usages = path
            .payload()
            .edges()
            .iter()
            .map(|edge_idx| (edge_idx, self.graph.edge(*edge_idx)))
            .filter(|(_, edge)| matches!(edge.edge_type, EdgeType::Drive(_)))
            .map(|(idx, _)| BundleRessourceUsage {
                ressource_id: idx.0 as usize,
                usage_per_unit: 1.0,
            })
            .collect::<Vec<_>>();

        let bundle = mcra::Bundle {
            ressource_usages: resource_usages,
            original_cost,
        };
        Some((path, bundle))
    }
}

impl<'a, 'c, F: Fn(EdgeIdx, &EdgePayload) -> bool + Sync> MinCostBundleSolver
    for Router<'a, 'c, F>
{
    fn compute_batch<'b>(
        &mut self,
        shadow_prices: &HashMap<usize, f64>,
        jobs: &'b [mcra::MinimalCostBundleJob],
    ) -> Vec<(&'b mcra::MinimalCostBundleJob, usize, mcra::Bundle)> {
        let responses = jobs
            .par_iter()
            .filter_map(|job| {
                self.process_job(job, &shadow_prices)
                    .map(|it| (job, it.0, it.1))
            })
            .collect::<Vec<_>>();

        responses
            .into_iter()
            .map(|(job, path, bundle)| {
                let path_id = self.paths.transfer_path(path);
                (job, path_id.0 as usize, bundle)
            })
            .collect()
    }
}

pub fn compute_opt_with_stats(graph: &Graph, paths: &mut PathsIndex) -> (Flow, AStarTable, Stats) {
    let start_time = std::time::Instant::now();
    let (flow, a_star_table) = compute_opt(&graph, paths);
    let computation_time = start_time.elapsed();
    let stats = Stats {
        computation_time,
        flow: FlowStats::compute(graph, &flow, &a_star_table, paths),
        heuristic: None,
    };
    (flow, a_star_table, stats)
}

pub fn compute_opt<'a>(graph: &'a Graph, paths: &'a mut PathsIndex) -> (Flow, AStarTable) {
    info!("Initializing MCRA...");
    let mut mcra = init_mcra::<gurobi::Model>(graph, paths);

    paths.path(PathId(0));
    info!("Computing A* table...");
    let mut a_star_table = AStarTable::new(graph.num_stations(), graph.num_nodes());
    compute_a_star_table(&mut a_star_table, graph, |_, _| true);

    let mut router = Router {
        graph,
        paths: paths,
        a_star_table: &a_star_table,
        edge_okay: |_, _| true,
    };
    let path_flow = mcra.solve(&mut router);
    drop(router);

    let mut flow = Flow::new();
    for (path_id, value) in path_flow.iter() {
        let path_id = PathId(*path_id as u32);
        flow.add_flow_onto_path(&paths, path_id, *value, true, false);
    }
    (flow, a_star_table)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_compute_opt() {
        let graph = crate::test::sample::create_sample().0;
        let mut paths = PathsIndex::new();
        let (flow, _) = compute_opt(&graph, &mut paths);
        println!("Flow:\n{}", flow.describe(&graph, &paths));
    }
}
