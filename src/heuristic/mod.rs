mod fill_indestructible;
mod fill_optimal;
pub mod multi_phase_strategy;
pub mod paths_queue;
pub mod paths_queue_strategy;
pub mod term_cond;

use std::path::Path;

use crate::{
    graph::CommodityIdx,
    heuristic::fill_indestructible::fill_indestructible_paths,
    primitives::{EPS, EPS_L, FVal, Time},
    regret::{self, get_max_relative_regret, get_num_regretting_paths, get_regretting_demand},
    serialization::flow::{export_flow, import_flow},
    shortest_path::best_paths::edge_is_not_a_boarding_edge_of_a_full_driving_edge,
    timer::Timer,
};
use fill_optimal::fill_using_system_optimum;
use log::{debug, error, info, trace, warn};
use rayon::{iter::IndexedParallelIterator, prelude::*};
use sqlite::{OpenFlags, State};
use term_cond::TerminationCondition;

use crate::{
    col::{HashSet, set_new},
    flow::{CycleAwareFlow, Flow},
    graph::{DescribePath, DriveNavigate, EdgeIdx, EdgeType, Graph, PathBox},
    path_index::{PathId, PathsIndex},
    shortest_path::a_star::{AStarTable, compute_a_star_table},
    shortest_path::best_paths::{find_best_response_path_with_derivative_astar, find_better_path},
};

#[derive(Debug)]
pub struct HeuristicStats {
    pub demand_prerouted: FVal,
    pub num_boarding_edges_blocked_by_prerouting: usize,
    pub num_commodities_fully_prerouted: usize,
    pub num_iterations: usize,
    pub num_iterations_postprocessing: usize,
    pub initial_len_paths_queue: usize,
}

#[derive(Debug)]
pub struct FlowStats {
    pub total_regret: f64,
    pub mean_relative_regret: f64,
    pub max_relative_regret: f64,
    pub num_regretting_paths: usize,
    pub regretting_demand: f64,
    /// The social cost of the computed flow: $ \sum_p f_p \cdot c_p $
    pub cost: f64,
    /// The total demand routed on their outside option.
    pub demand_outside: f64,
}

impl Default for FlowStats {
    fn default() -> Self {
        FlowStats {
            total_regret: 0.0,
            mean_relative_regret: 0.0,
            max_relative_regret: 0.0,
            num_regretting_paths: 0,
            regretting_demand: 0.0,
            cost: 0.0,
            demand_outside: 0.0,
        }
    }
}

impl FlowStats {
    pub fn compute(
        graph: &Graph,
        flow: &Flow,
        a_star_table: &AStarTable,
        paths: &PathsIndex,
    ) -> FlowStats {
        FlowStats {
            total_regret: regret::get_total_regret(graph, flow, a_star_table, paths),
            mean_relative_regret: regret::get_mean_relative_regret(
                graph,
                flow,
                a_star_table,
                paths,
            ),
            max_relative_regret: get_max_relative_regret(graph, flow, a_star_table, paths),
            num_regretting_paths: get_num_regretting_paths(graph, flow, a_star_table, paths),
            regretting_demand: get_regretting_demand(graph, flow, a_star_table, paths),
            cost: flow.cost(paths, graph),
            demand_outside: flow.get_demand_outside(paths),
        }
    }
}

#[derive(Debug)]
pub struct Stats {
    /// The computation time for the heuristic
    pub computation_time: std::time::Duration,

    pub heuristic: Option<HeuristicStats>,

    pub flow: FlowStats,
}

fn get_num_blocked_boarding_edges(graph: &Graph, flow: &Flow) -> usize {
    graph
        .edges()
        .filter(|&(_, edge)| matches!(edge.edge_type, EdgeType::Board(_)))
        .filter(|&(_, edge)| {
            let drive_edge = graph.node(edge.to).outgoing[0];
            let drive_edge_flow = flow.on_edge(drive_edge);

            let capacity = {
                let drive_edge = graph.edge(drive_edge);
                match drive_edge.edge_type {
                    EdgeType::Drive(capacity) => capacity,
                    _ => panic!("Drive edge is not a drive edge!"),
                }
            };
            let slack = capacity - drive_edge_flow;
            slack <= EPS_L
        })
        .count()
}

fn get_num_commodities_not_using_outside_option(
    graph: &Graph,
    flow: &Flow,
    paths: &mut PathsIndex,
) -> usize {
    graph
        .commodities()
        .filter(|&(commodity_id, _)| {
            let outside_path = graph.outside_path(commodity_id);
            let outside_path_flow = flow.on_path(paths.transfer_path(outside_path));
            outside_path_flow <= EPS_L
        })
        .count()
}

pub trait StrategyBuilder {
    type S: Strategy + TerminationCondition;

    fn create(
        self: Self,
        graph: &Graph,
        flow: &Flow,
        paths: &mut PathsIndex,
        a_star_table: &AStarTable,
        h_stats: &mut HeuristicStats,
        timer: &mut Timer,
    ) -> Self::S;
}

pub trait Strategy {
    fn info(&self) -> String;

    fn on_iterate(
        &mut self,
        graph: &Graph,
        flow: &Flow,
        a_star_table: &AStarTable,
        paths: &mut PathsIndex,
        h_stats: &HeuristicStats,
        timer: &mut Timer,
    );

    fn get_non_eq_witness(
        &mut self,
        graph: &Graph,
        flow: &Flow,
        a_star_table: &AStarTable,
        paths: &PathsIndex,
    ) -> Option<(PathId, PathBox)>;

    fn on_add_to_flow(&mut self, graph: &Graph, flow: &Flow, direction: &Flow, paths: &PathsIndex);

    fn on_flow_added(&mut self, graph: &Graph, updated_flow: &Flow, direction: &Flow);

    fn found_infinite_cycle(&mut self, non_eq_path: PathId);

    fn into_flow(
        self,
        flow: Flow,
        graph: &Graph,
        a_star_table: &AStarTable,
        paths: &PathsIndex,
    ) -> Flow;
}

pub fn initialize<'a>(graph: &Graph) -> (Flow, PathsIndex<'a>, AStarTable, Stats, Vec<EdgeIdx>) {
    let time_started = std::time::Instant::now();
    let mut paths = PathsIndex::new();
    let mut flow = Flow::new();

    let mut a_star_table: AStarTable = AStarTable::new(graph.num_stations(), graph.num_nodes());

    // We assume that each commodity already has its own SPAWN node and outside option edge.
    info!("Filling initial solution with outside options...");
    for (commodity_id, commodity) in graph.commodities() {
        let path_id = paths.transfer_path(graph.outside_path(commodity_id));
        flow.add_flow_onto_path(&paths, path_id, commodity.demand, true, true);
    }

    fill_indestructible_paths(graph, &mut paths, &mut a_star_table, &mut flow);

    // Collect stats:
    // (1) How much demand could be routed on indestructible paths?
    // (2) How many boarding edges are blocked now?
    // (3) How many commodities were completely prerouted?

    let mut h_stats = HeuristicStats {
        demand_prerouted: 0.0,
        num_boarding_edges_blocked_by_prerouting: 0,
        num_commodities_fully_prerouted: 0,
        num_iterations: 0,
        num_iterations_postprocessing: 0,
        initial_len_paths_queue: 0,
    };

    h_stats.demand_prerouted = flow.get_demand_inside(&paths);
    h_stats.num_boarding_edges_blocked_by_prerouting = get_num_blocked_boarding_edges(graph, &flow);
    h_stats.num_commodities_fully_prerouted =
        get_num_commodities_not_using_outside_option(graph, &flow, &mut paths);

    let forbidden_boarding_edges = graph
        .edges()
        .filter(|(_, edge)| !edge_is_not_a_boarding_edge_of_a_full_driving_edge(edge, &flow, graph))
        .map(|(edge_idx, _)| edge_idx)
        .collect::<Vec<_>>();

    info!("Computing A*-Table...");
    let forbidden_edges = forbidden_boarding_edges
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    compute_a_star_table(&mut a_star_table, graph, |edge_idx, _| {
        !forbidden_edges.contains(&edge_idx)
    });
    fill_using_system_optimum(
        &mut flow,
        graph,
        &a_star_table,
        &forbidden_edges,
        &mut paths,
    );

    trace!("Computing stats...");
    let stats = Stats {
        computation_time: time_started.elapsed(),
        heuristic: Some(h_stats),
        flow: FlowStats::compute(graph, &flow, &a_star_table, &paths),
    };
    info!("{:#?}", stats);

    (flow, paths, a_star_table, stats, forbidden_boarding_edges)
}

pub fn serialize_initial(
    graph: &Graph,
    (flow, paths, a_star_table, stats, forbidden_boarding_edges): (
        &Flow,
        &PathsIndex<'_>,
        &AStarTable,
        &Stats,
        &Vec<EdgeIdx>,
    ),
    path: &str,
) {
    export_flow(
        graph,
        Some(stats),
        flow,
        paths,
        &regret::get_regret_map(graph, flow, paths, a_star_table),
        path,
    );
    let connection = sqlite::Connection::open_with_flags(
        path,
        OpenFlags::default()
            .with_create()
            .with_no_mutex()
            .with_read_write(),
    )
    .unwrap();
    connection.execute("BEGIN TRANSACTION;").unwrap();

    connection
        .execute(
            "CREATE TABLE forbidden_edges (
            edge_id INTEGER NOT NULL
        )",
        )
        .unwrap();

    let mut stmt = connection
        .prepare("INSERT INTO forbidden_edges (edge_id) VALUES (?)")
        .unwrap();
    for id in forbidden_boarding_edges {
        stmt.bind((1, id.0 as i64)).unwrap();
        stmt.next().unwrap();
        stmt.reset().unwrap();
    }

    connection.execute("END TRANSACTION;").unwrap();
}

fn deserialize_initial<'a>(graph: &Graph, path: &str) -> (Flow, PathsIndex<'a>, AStarTable, Stats) {
    let (flow, paths) = import_flow(graph, None, path).unwrap();

    let connection =
        sqlite::Connection::open_with_flags(path, OpenFlags::default().with_read_only()).unwrap();

    let mut stmt = connection
        .prepare("SELECT edge_id FROM forbidden_edges")
        .unwrap();
    let mut forbidden_boarding_edges = set_new();
    while let sqlite::State::Row = stmt.next().unwrap() {
        let edge_idx: i64 = stmt.read(0).unwrap();
        forbidden_boarding_edges.insert(EdgeIdx(edge_idx as u32));
    }

    info!("Computing A*-Table...");
    let mut a_star_table: AStarTable = AStarTable::new(graph.num_stations(), graph.num_nodes());
    compute_a_star_table(&mut a_star_table, graph, |edge_idx, _| {
        !forbidden_boarding_edges.contains(&edge_idx)
    });

    let mut h_stats = HeuristicStats {
        demand_prerouted: 0.0,
        num_boarding_edges_blocked_by_prerouting: 0,
        num_commodities_fully_prerouted: 0,
        num_iterations: 0,
        num_iterations_postprocessing: 0,
        initial_len_paths_queue: 0,
    };

    let mut stmt = connection
        .prepare("SELECT COMPUTATION_TIME_MS, DEMAND_PREROUTED, NUM_BOARDING_EDGES_BLOCKED_BY_PREROUTING, NUM_COMMODITIES_FULLY_PREROUTED FROM flow_stats")
        .unwrap();
    assert_eq!(stmt.next().unwrap(), State::Row);
    let computation_time = std::time::Duration::from_millis(stmt.read::<i64, _>(0).unwrap() as u64);
    h_stats.demand_prerouted = stmt.read(1).unwrap();
    h_stats.num_boarding_edges_blocked_by_prerouting = stmt.read::<i64, _>(2).unwrap() as usize;
    h_stats.num_commodities_fully_prerouted = stmt.read::<i64, _>(3).unwrap() as usize;

    (
        flow,
        paths,
        a_star_table,
        Stats {
            computation_time,
            heuristic: Some(h_stats),
            flow: FlowStats::default(),
        },
    )
}

pub fn eq_heuristic_main_loop<'a>(
    graph: &Graph,
    (flow, mut paths, a_star_table, stats): (Flow, PathsIndex<'a>, AStarTable, Stats),
    log_iteration_count: usize,
    strategy_builder: impl StrategyBuilder,
) -> (Flow, PathsIndex<'a>, Stats, AStarTable) {
    let mut timer = Timer::new();
    timer.add(stats.computation_time);
    timer.start();
    let mut h_stats = stats.heuristic.unwrap();

    let mut strategy = strategy_builder.create(
        graph,
        &flow,
        &mut paths,
        &a_star_table,
        &mut h_stats,
        &mut timer,
    );

    let mut ca_flow = CycleAwareFlow::new(Some(flow));

    while strategy.advance(h_stats.num_iterations, &mut timer) {
        strategy.on_iterate(
            graph,
            ca_flow.flow(),
            &a_star_table,
            &mut paths,
            &h_stats,
            &mut timer,
        );

        let should_log = h_stats.num_iterations % log_iteration_count == 0;
        if should_log {
            info!(
                "Iteration {:}, {:}",
                h_stats.num_iterations,
                strategy.info()
            );
        }

        let reached_eq = eq_heuristic_step(
            graph,
            &a_star_table,
            &mut paths,
            &mut ca_flow,
            &mut strategy,
            should_log,
        );

        h_stats.num_iterations += 1;
        if reached_eq {
            info!("REACHED EQUILIBRIUM");
            break;
        }
    }
    info!(
        "Main loop terminated after {:} iterations",
        h_stats.num_iterations
    );

    let flow = strategy.into_flow(ca_flow.drain(), graph, &a_star_table, &paths);

    info!("Computing stats...");
    let stats = Stats {
        computation_time: timer.elapsed(),
        heuristic: Some(h_stats),
        flow: FlowStats::compute(graph, &flow, &a_star_table, &paths),
    };
    info!("{:#?}", stats);

    (flow, paths, stats, a_star_table)
}

pub fn eq_heuristic_with_buffer_initial<'a>(
    graph: &Graph,
    log_iteration_count: usize,
    strategy_builder: impl StrategyBuilder,
    initial_solution_path: &str,
) -> (Flow, PathsIndex<'a>, Stats, AStarTable) {
    if Path::exists(Path::new(initial_solution_path)) {
        info!("Loading initial solution from file...");
        let (flow, paths, a_star_table, stats) = deserialize_initial(graph, initial_solution_path);
        info!("Initial solution loaded.");
        return eq_heuristic_main_loop(
            graph,
            (flow, paths, a_star_table, stats),
            log_iteration_count,
            strategy_builder,
        );
    }
    info!("No initial solution found. Starting from scratch...");
    let (flow, paths, a_star_table, stats, forbidden_boarding_edges) = initialize(graph);
    info!("Serializing initial solution...");
    serialize_initial(
        graph,
        (
            &flow,
            &paths,
            &a_star_table,
            &stats,
            &forbidden_boarding_edges,
        ),
        initial_solution_path,
    );
    info!("Initial solution serialized. Starting heuristic...");
    eq_heuristic_main_loop(
        graph,
        (flow, paths, a_star_table, stats),
        log_iteration_count,
        strategy_builder,
    )
}

pub fn eq_heuristic<'a>(
    graph: &Graph,
    log_iteration_count: usize,
    strategy_builder: impl StrategyBuilder,
) -> (Flow, PathsIndex<'a>, Stats, AStarTable) {
    let (flow, paths, a_star_table, stats, _) = initialize(graph);
    eq_heuristic_main_loop(
        graph,
        (flow, paths, a_star_table, stats),
        log_iteration_count,
        strategy_builder,
    )
}

fn get_non_eq_witness_round_robin(
    round_robin_idx: &mut usize,
    flow: &Flow,
    a_star_table: &AStarTable,
    graph: &Graph,
    paths: &PathsIndex,
) -> Option<(PathId, PathBox)> {
    let paths_to_check: Vec<PathId> = flow
        .path_flow_map()
        .iter()
        .filter_map(|it| if *it.1 > EPS_L { Some(*it.0) } else { None })
        .collect();
    let start_idx = *round_robin_idx % paths_to_check.len();
    paths_to_check
        .par_iter()
        .enumerate()
        .skip(start_idx)
        .chain(paths_to_check.par_iter().enumerate().take(start_idx))
        .find_map_first(|(idx, &path_id)| {
            find_better_path(a_star_table, graph, flow, paths.path(path_id))
                .map(|best_response| (idx, path_id, best_response))
        })
        .map(|(idx, path, best_response)| {
            *round_robin_idx = idx % paths_to_check.len();
            (path, best_response)
        })
}

fn get_non_eq_withness_from_beginning(
    flow: &Flow,
    graph: &Graph,
    paths: &PathsIndex,
    a_star_table: &AStarTable,
) -> Option<(PathId, PathBox)> {
    flow.path_flow_map()
        .par_iter()
        .find_map_first(|(&path_id, &flow_val)| {
            if flow_val <= EPS_L {
                return None;
            }
            find_better_path(a_star_table, graph, flow, paths.path(path_id))
                .map(|best_response| (path_id, best_response))
        })
}

/// Returns a feasible direction, if the flow is not at equilibrium, otherwise None.
fn get_feasible_direction(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    strategy: &mut impl Strategy,
) -> Option<(Flow, PathId)> {
    let (non_eq_path, best_response) =
        strategy.get_non_eq_witness(graph, flow, a_star_table, paths)?;

    let mut direction = Flow::new();
    let best_response = paths.transfer_path(best_response);

    direction.add_flow_onto_path(paths, non_eq_path, -1.0, false, false);
    direction.add_flow_onto_path(paths, best_response, 1.0, false, false);

    debug!("Originally: 1 {:?} -> {:?}", non_eq_path, best_response);

    assert!(direction.path_flow_map().iter().any(|it| *it.1 > 0.0));
    let did_reroute: bool = reroute_excessors(graph, a_star_table, paths, flow, &mut direction);

    assert!(direction.path_flow_map().iter().any(|it| *it.1 > 0.0));
    if did_reroute {
        move_to_better_paths(graph, a_star_table, paths, flow, &mut direction);
    }

    debug!("Final Direction:\n{}", direction.describe(graph, paths));

    assert!(direction.path_flow_map().iter().any(|it| *it.1 > 0.0));

    Some((direction, non_eq_path))
}

fn add_direction_to_flow(
    ca_flow: &mut CycleAwareFlow,
    direction: Flow,
    strategy: &mut impl Strategy,
    graph: &Graph,
    paths: &PathsIndex,
) {
    strategy.on_add_to_flow(graph, ca_flow.flow(), &direction, paths);
    ca_flow.add_to_flow(paths, direction, |updated_flow, direction| {
        strategy.on_flow_added(graph, updated_flow, direction);
    });
}

/// Returns true, if the flow is at equilibrium.
fn eq_heuristic_step(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    ca_flow: &mut CycleAwareFlow,
    strategy: &mut impl Strategy,
    should_log: bool,
) -> bool {
    let Some((mut direction, mut non_eq_path)) =
        get_feasible_direction(graph, a_star_table, paths, ca_flow.flow(), strategy)
    else {
        return true;
    };
    let mut may_increase =
        get_longest_feasible_extension(graph, ca_flow.flow(), &direction, false, true);

    if may_increase <= 0.0 {
        warn!("INFEASIBLE DIRECTION. Recomputing edge flow and try again...");
        ca_flow.recompute_edge_flow();
        (direction, non_eq_path) =
            match get_feasible_direction(graph, a_star_table, paths, ca_flow.flow(), strategy) {
                None => return true,
                Some(it) => it,
            };
        may_increase =
            get_longest_feasible_extension(graph, ca_flow.flow(), &direction, true, true);
    }

    assert!(may_increase > 0.0);
    assert!(may_increase < FVal::INFINITY);

    if should_log {
        info!("Extending flow by {:}", may_increase);
    }

    direction.scale_by(may_increase);

    add_direction_to_flow(ca_flow, direction, strategy, graph, paths);

    if let Some((cycle_len, cycle_direction)) = ca_flow.has_cycle(paths) {
        // Check if the direction on the cycle is feasible.
        let may_extend =
            get_longest_feasible_extension(graph, ca_flow.flow(), &cycle_direction, false, true);
        if may_extend <= 0.0 {
            debug!(
                "IGNORING INFEASIBLE CYCLE OF LENGTH {:}:\n{}",
                cycle_len,
                cycle_direction.describe(graph, paths)
            );
        } else if may_extend >= f64::INFINITY {
            warn!(
                "DETECTED INFINITELY FEASIBLE CYCLE OF LENGTH {:}! Changing path selection strategy...",
                cycle_len
            );

            strategy.found_infinite_cycle(non_eq_path);
        } else {
            debug!(
                "EXTENDING ALONG CYCLE OF LENGTH {:} WITH EXTENSIBILITY {:}:\n{}",
                cycle_len,
                may_extend,
                cycle_direction.describe(graph, paths)
            );
            let mut cycle_direction = cycle_direction;
            cycle_direction.scale_by(may_extend);

            add_direction_to_flow(ca_flow, cycle_direction, strategy, graph, paths);
        }
    }

    false
}

fn move_to_better_paths(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex<'_>,
    flow: &Flow,
    direction: &mut Flow,
) {
    let paths_to_check = direction
        .path_flow_map()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for path_id in paths_to_check {
        if direction.on_path(path_id) <= 0.0 {
            continue;
        }
        let path = paths.path(path_id);
        let commodity = graph.commodity(path.commodity_idx());
        let (best_response, mut may_add) = find_best_response_path_with_derivative_astar(
            graph,
            a_star_table,
            flow,
            direction,
            path,
        );
        let cost_best_response = best_response.payload().cost(commodity, graph);
        if cost_best_response >= path.cost(commodity, graph) {
            continue;
        }

        let best_response_id = paths.transfer_path(best_response);
        let path = paths.path(path_id);

        if may_add == FVal::INFINITY {
            // As direction on path_id is positive, we should not be able to move INF to a better path.
            error!(
                "Could move INF to better path {:?} -> {:?}. (dir={:}). Ignoring.",
                path_id,
                best_response_id,
                direction.on_path(path_id)
            );
            continue;
        }
        assert!(may_add < FVal::INFINITY);

        // We remove the flow from the worst used path of the commodity.
        while may_add > 0.0 {
            let worst_used_path =
                get_worst_used_path(graph, paths, flow, direction, path.commodity_idx());
            assert!(worst_used_path.cost >= cost_best_response);
            // TODO: Check if we should not assert that worst used path is greater than best response.
            if worst_used_path.cost == cost_best_response {
                break;
            }
            let may_swap = may_add.min(worst_used_path.may_remove);
            direction.add_flow_onto_path(paths, worst_used_path.path_id, -may_swap, false, false);
            direction.add_flow_onto_path(paths, best_response_id, may_swap, false, false);

            debug!(
                "Fill better: {:} {:?} -> {:?}",
                may_swap, worst_used_path.path_id, best_response_id
            );
            may_add -= may_swap;
        }
    }
}

struct WorstUsedPath {
    path_id: PathId,
    cost: Time,
    may_remove: FVal,
}
fn get_worst_used_path(
    graph: &Graph,
    paths: &PathsIndex,
    flow: &Flow,
    direction: &Flow,
    commodity_idx: CommodityIdx,
) -> WorstUsedPath {
    let mut worst_path = None;

    let mut check_path = |new_path_id: PathId, may_remove: FVal| {
        let new_path = paths.path(new_path_id);
        if new_path.commodity_idx() != commodity_idx {
            return;
        }
        let new_cost = new_path.cost(graph.commodity(commodity_idx), graph);
        match worst_path {
            None => {
                worst_path = Some(WorstUsedPath {
                    path_id: new_path_id,
                    cost: new_cost,
                    may_remove,
                })
            }
            Some(WorstUsedPath {
                path_id: _,
                cost: arrival,
                may_remove: _,
            }) if new_cost > arrival => {
                worst_path = Some(WorstUsedPath {
                    path_id: new_path_id,
                    cost: new_cost,
                    may_remove,
                })
            }
            _ => {}
        };
    };

    for (&path, &value) in flow.path_flow_map().iter() {
        if value <= EPS_L {
            continue;
        }
        check_path(path, FVal::INFINITY);
    }

    for (&path, &value) in direction.path_flow_map().iter() {
        if value <= EPS_L {
            continue;
        }
        check_path(path, value);
    }
    worst_path.unwrap()
}

fn describe_drive_edge(graph: &Graph, edge_idx: EdgeIdx) -> String {
    let boarding_edge = graph.boarding_edge(edge_idx).unwrap();
    let offboarding_edge = graph.offboarding_edge(edge_idx).unwrap();
    PathBox::new(
        CommodityIdx(0),
        [boarding_edge.0, offboarding_edge.0].into_iter(),
    )
    .payload()
    .describe(graph)
}

fn get_longest_feasible_extension(
    graph: &Graph,
    flow: &Flow,
    direction: &Flow,
    assert_feasible: bool,
    warn_on_infeasible: bool,
) -> FVal {
    // Let's find out how long we can increase the flow in the direction.
    let mut may_increase = FVal::INFINITY;
    // First check, that no path becomes negative.
    for (&path, &dir_val) in direction.path_flow_map().iter() {
        if dir_val >= 0.0 {
            continue;
        }
        let flow_val = flow.on_path(path);
        may_increase = may_increase.min(-flow_val / dir_val);
        if -flow_val / dir_val < 0.0001 && warn_on_infeasible {
            warn!(
                "Path {:?} has flow {:} and direction {:}",
                path, flow_val, dir_val
            );
        }
        if assert_feasible {
            assert!(
                may_increase > 0.0,
                "may_increase={}, path={:?}, flow_val={}, dir_val={}",
                may_increase,
                path,
                flow_val,
                dir_val
            );
        }
    }
    // Now check, that no edge exceeds a capacity.
    for (&edge_idx, &dir_val) in direction.edge_flow_map().iter() {
        if dir_val <= EPS / 100.0 {
            continue;
        }
        let edge = graph.edge(edge_idx);
        let capacity = match edge.edge_type {
            EdgeType::Drive(capacity) => capacity,
            _ => continue,
        };
        let slack = capacity - flow.on_edge(edge_idx);

        if slack / dir_val < 0.0001 && warn_on_infeasible {
            warn!(
                "Edge {:} has slack {:} and direction {:}",
                describe_drive_edge(graph, edge_idx),
                slack,
                dir_val
            );
        }
        may_increase = may_increase.min(slack / dir_val);
        if assert_feasible {
            assert!(
                may_increase > 0.0,
                "Edge {:} has slack {:} and direction {:}",
                describe_drive_edge(graph, edge_idx),
                slack,
                dir_val
            );
        }
    }
    if assert_feasible {
        assert!(may_increase < FVal::INFINITY);
    }
    may_increase
}

fn reroute_exceeded_drive_edge(
    drive_edge: DriveNavigate,
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    direction: &mut Flow,
    swap_list: &mut Vec<(PathId, PathId, FVal)>,
) {
    let slack = drive_edge.capacity() - flow.on_edge(drive_edge.id());
    let mut need_remove = direction.on_edge(drive_edge.id());
    assert!(slack <= EPS_L && need_remove > 0.0);

    // Edge is being exceeded.
    // There is at least one path used (meaning flow or direction is positive)
    // that boards an already full edge of the same line and which also uses this edge.
    let original_edge_id = drive_edge.id();
    let mut check_drive_edge = drive_edge;
    loop {
        let slack = check_drive_edge.capacity() - flow.on_edge(check_drive_edge.id());
        if slack > EPS_L {
            warn!(
                "Edge {:} is not full, slack {:}!",
                describe_drive_edge(graph, check_drive_edge.id()),
                slack
            );
        }
        if direction.on_edge(check_drive_edge.id()) <= -EPS {
            // This assertion fails, if ...
            // The next drive edge is full, f_e = v, and the direction is positive, d_e > 0.
            // The boarding edge of the next drive edge has no flow and the direction is not positive (=> so it's zero).
            // => The stay-on edge must also be full, and its direction must be positive.
            // => The previous drive edge must be full, and d_off + d_stay = d_prevdrive <= 0.
            // => d_off <= -d_stay < 0.
            //... ????

            error!("Assertion failed...");
            error!("Current Direction:\n{}", direction.describe(graph, paths));
            error!("Flow on relevant edges:");

            error!(
                " B {:}. f={:}, d={:}",
                describe_drive_edge(graph, check_drive_edge.id()),
                flow.on_edge(check_drive_edge.pre_boarding().id()),
                direction.on_edge(check_drive_edge.pre_boarding().id())
            );
            error!(
                " D {:}. f={:}, d={:}",
                describe_drive_edge(graph, check_drive_edge.id()),
                flow.on_edge(check_drive_edge.id()),
                direction.on_edge(check_drive_edge.id())
            );
            error!(
                " O {:}. f={:}, d={:}",
                describe_drive_edge(graph, check_drive_edge.id()),
                flow.on_edge(check_drive_edge.post_get_off().id()),
                direction.on_edge(check_drive_edge.post_get_off().id())
            );
            let stay_on = check_drive_edge.post_stay_on().unwrap();
            error!(
                " S {:}. f={:}, d={:}",
                describe_drive_edge(graph, check_drive_edge.id()),
                flow.on_edge(stay_on.id()),
                direction.on_edge(stay_on.id())
            );
            let next_drive = stay_on.post_drive();
            error!(
                " B {:}. f={:}, d={:}",
                describe_drive_edge(graph, next_drive.id()),
                flow.on_edge(next_drive.pre_boarding().id()),
                direction.on_edge(next_drive.pre_boarding().id())
            );
            error!(
                " D {:}. f={:}, d={:}",
                describe_drive_edge(graph, next_drive.id()),
                flow.on_edge(next_drive.id()),
                direction.on_edge(next_drive.id())
            );

            panic!(
                "Direction on drive edge {:} is not positive!",
                describe_drive_edge(graph, check_drive_edge.id())
            );
        }
        let boarding_edge = check_drive_edge.pre_boarding();
        if direction.on_edge(boarding_edge.id()) > 0.0 {
            let check_paths_direction = direction
                .paths_by_edge()
                .get(&boarding_edge.id())
                .unwrap()
                .clone();
            for &path_id in check_paths_direction.iter() {
                let may_remove = {
                    if flow.on_path(path_id) > 0.0 {
                        FVal::INFINITY
                    } else {
                        direction.on_path(path_id)
                    }
                };
                if may_remove <= 0.0 {
                    continue;
                }
                let path = paths.path(path_id);
                if !path.edges().contains(&original_edge_id) {
                    continue;
                }
                reroute_excessors_remove_path(
                    graph,
                    a_star_table,
                    paths,
                    flow,
                    direction,
                    path_id,
                    may_remove,
                    &mut need_remove,
                    original_edge_id,
                    swap_list,
                );
                if need_remove <= 0.0 {
                    return;
                }
            }
        }

        // Check if there is a path with positive flow and if so, take the one with maximum flow.
        let path_id = flow
            .paths_by_edge()
            .get(&boarding_edge.id())
            .and_then(|paths_set| {
                paths_set
                    .iter()
                    .filter(|&&path_id| paths.path(path_id).edges().contains(&original_edge_id))
                    .max_by(|&&a, &&b| flow.on_path(a).total_cmp(&flow.on_path(b)))
            });
        if let Some(&path_id) = path_id {
            let may_remove = FVal::INFINITY;
            reroute_excessors_remove_path(
                graph,
                a_star_table,
                paths,
                flow,
                direction,
                path_id,
                may_remove,
                &mut need_remove,
                original_edge_id,
                swap_list,
            );
            assert!(need_remove <= 0.0);
            return;
        }
        debug!(
            "Checking a previous boarding edge due to {:}...",
            describe_drive_edge(graph, original_edge_id)
        );
        let stay_on = check_drive_edge.pre_stay_on().unwrap();
        assert!(direction.on_edge(stay_on.id()) > 0.0);
        assert!(
            check_drive_edge.capacity() - flow.on_edge(stay_on.id()) <= EPS_L,
            "Flow on stay-on edge is {:}, but capacity is {:}. Flow on drive edge is {:} and flow on boarding edge is {:}.",
            flow.on_edge(stay_on.id()),
            check_drive_edge.capacity(),
            flow.on_edge(check_drive_edge.id()),
            flow.on_edge(check_drive_edge.pre_boarding().id()),
        );
        check_drive_edge = stay_on.pre_drive();
    }
}

fn reroute_excessors_remove_path(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    direction: &mut Flow,
    path_id: PathId,
    mut may_remove: FVal,
    need_remove: &mut FVal,
    exceeded_edge: EdgeIdx,
    swap_list: &mut Vec<(PathId, PathId, FVal)>,
) {
    may_remove = may_remove.min(*need_remove);
    direction.add_flow_onto_path(paths, path_id, -may_remove, false, false);
    *need_remove -= may_remove;
    while may_remove > 0.0 {
        let (best_response, may_add) = find_best_response_path_with_derivative_astar(
            graph,
            a_star_table,
            flow,
            direction,
            paths.path(path_id),
        );

        let may_swap = may_remove.min(may_add);
        let best_response = paths.transfer_path(best_response);
        direction.add_flow_onto_path(paths, best_response, may_swap, false, false);
        swap_list.push((path_id, best_response, may_swap));
        debug!(
            "Excessor:   {:} {:?} -> {:?}  (due to edge {:})",
            may_swap,
            path_id,
            best_response,
            describe_drive_edge(graph, exceeded_edge)
        );
        may_remove -= may_swap;
    }
}

fn reroute_excessors(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    direction: &mut Flow,
) -> bool {
    let mut edges_to_check: HashSet<EdgeIdx> = set_new();

    for (&edge_idx, &value) in direction.edge_flow_map() {
        if value <= 0.0 {
            continue;
        }
        edges_to_check.insert(edge_idx);
    }

    let mut swap_list: Vec<(PathId, PathId, FVal)> = Vec::new();

    let mut found_exceeded_capacity = false;

    let swap_exceeded_edges =
        |mut edges_to_check: HashSet<EdgeIdx>,
         direction: &mut Flow,
         found_exceeded_capacity: &mut bool,
         paths: &mut PathsIndex,
         swap_list: &mut Vec<(PathId, PathId, f64)>| {
            for edge_idx in edges_to_check.drain() {
                let edge = match graph.nav_edge(edge_idx) {
                    crate::graph::EdgeNavigate::Drive(edge) => edge,
                    _ => continue,
                };

                let slack = edge.capacity() - flow.on_edge(edge_idx);
                assert!(!slack.is_nan());
                if slack > EPS_L || direction.on_edge(edge_idx) <= 0.0 {
                    continue;
                }
                // Edge is being exceeded.
                *found_exceeded_capacity = true;
                // There is at least one path used (meaning flow or direction is positive)
                // that boards an already full edge of the same line and which also uses this edge.
                reroute_exceeded_drive_edge(
                    edge,
                    graph,
                    a_star_table,
                    paths,
                    flow,
                    direction,
                    swap_list,
                );
            }
        };

    // Returns a new set of edges to check.
    let revert_swaps = |swap_list: &mut Vec<(PathId, PathId, FVal)>,
                        direction: &mut Flow,
                        paths: &mut PathsIndex|
     -> Option<HashSet<EdgeIdx>> {
        for (path_id, best_response_id, swapped) in swap_list.iter_mut() {
            if *swapped <= 0.0 {
                continue;
            }
            let may_readd = {
                let path = paths.path(*path_id);
                let best_response = paths.path(*best_response_id);
                let mut may_readd = FVal::INFINITY;
                for &edge_idx in path.edges() {
                    let edge = graph.edge(edge_idx);
                    let drive_edge_id = match edge.edge_type {
                        EdgeType::Board(_) => graph.node(edge.to).outgoing[0],
                        _ => continue,
                    };
                    let drive_edge = graph.edge(drive_edge_id);
                    let capacity = match drive_edge.edge_type {
                        EdgeType::Drive(capacity) => capacity,
                        _ => panic!("Drive edge is not a drive edge!"),
                    };
                    let slack = capacity - flow.on_edge(drive_edge_id);
                    if slack > EPS_L {
                        continue;
                    }
                    if best_response.edges().contains(&drive_edge_id) {
                        continue;
                    }
                    may_readd = may_readd.min(-direction.on_edge(drive_edge_id));
                    if may_readd <= 0.0 {
                        debug!(
                            "No Revert:    {:?} -> {:?} due to {:}",
                            path_id,
                            best_response_id,
                            describe_drive_edge(graph, drive_edge_id)
                        );
                        break;
                    }
                }
                may_readd
            };
            let may_remove = {
                if flow.on_path(*best_response_id) > 0.0 {
                    FVal::INFINITY
                } else if direction.on_path(*best_response_id) > 0.0 {
                    direction.on_path(*best_response_id)
                } else {
                    0.0
                }
            };
            let may_revert = swapped.min(may_readd).min(may_remove);
            if may_revert <= 0.0 {
                continue;
            }
            debug!(
                "Reverting:  {:} {:?} -> {:?} (may_revert={:})",
                swapped, path_id, best_response_id, may_revert
            );
            direction.add_flow_onto_path(paths, *path_id, may_revert, false, false);
            direction.add_flow_onto_path(paths, *best_response_id, -may_revert, false, false);
            *swapped -= may_revert;
            // In an attempt to prevent a loop: First handle possibly new excessed edges.
            return Some(paths.path(*path_id).edges().iter().cloned().collect());
        }
        return None;
    };

    const MAX_ITERATIONS: usize = 1000;
    for i in 0..MAX_ITERATIONS {
        swap_exceeded_edges(
            edges_to_check,
            direction,
            &mut found_exceeded_capacity,
            paths,
            &mut swap_list,
        );

        if i < MAX_ITERATIONS - 1 {
            match revert_swaps(&mut swap_list, direction, paths) {
                Some(edges) => edges_to_check = edges,
                None => break,
            }
        } else {
            warn!("MAX_ITERATIONS reached. Stopping reverting swaps.");
            break;
        }
    }

    found_exceeded_capacity
}
