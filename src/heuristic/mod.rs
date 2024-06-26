mod fill_indestructible;
mod fill_optimal;
mod paths_queue;

use std::mem::swap;

use crate::{
    graph::{CommodityIdx, CommodityPayload, Path, EPS_L},
    heuristic::{
        fill_indestructible::fill_indestructible_paths, fill_optimal::fill_all_optimal_paths,
        paths_queue::PathsQueue,
    },
    iter::par_map_until::{ParMapResult, ParMapUntil},
};
use log::{debug, error, info, warn};
use rayon::prelude::*;

use crate::{
    a_star::{compute_a_star_table, AStarTable},
    best_paths::{find_best_response_path_with_derivative_astar, find_better_path},
    col::{set_new, HashSet},
    flow::{CycleAwareFlow, Flow},
    graph::{
        DescribePath, DriveNavigate, EdgeIdx, EdgePayload, EdgeType, FVal, Graph, PathBox, Time,
        EPS,
    },
    paths_index::{PathId, PathsIndex},
};

#[derive(Debug)]
pub struct Stats {
    pub demand_prerouted: FVal,
    pub num_boarding_edges_blocked_by_prerouting: usize,
    pub num_commodities_fully_prerouted: usize,
    pub num_iterations: usize,
    pub initial_len_paths_queue: usize,
    /// The social cost of the computed flow: $ \sum_p f_p \cdot c_p $
    pub cost: f64,
    /// The total demand routed on their outside option.
    pub demand_outside: f64,
    /// The computation time for the heuristic
    pub computation_time: std::time::Duration,
}

pub fn eq_heuristic<'a>(
    graph: &Graph,
    initial_solution: Option<(Flow, PathsIndex<'a>)>,
    log_iteration_count: usize,
) -> (Flow, PathsIndex<'a>, Stats) {
    let time_started = std::time::Instant::now();

    let mut a_star_table: AStarTable = AStarTable::new(graph.num_stations(), graph.num_nodes());

    let (mut flow, mut paths) = initial_solution.unwrap_or_else(|| {
        let mut paths = PathsIndex::new();
        let mut flow = Flow::new();

        // We assume that each commodity already has its own SPAWN node and outside option edge.
        info!("Filling initial solution with outside options...");
        for (commodity_id, commodity) in graph.commodities() {
            let path_id = paths.transfer_path(graph.outside_path(commodity_id));
            flow.add_flow_onto_path(&paths, path_id, commodity.demand, true, true);
        }

        fill_indestructible_paths(graph, &mut paths, &mut a_star_table, &mut flow);

        (flow, paths)
    });

    // Collect stats:
    // (1) How much demand could be routed on indestructible paths?
    // (2) How many boarding edges are blocked now?
    // (3) How many commodities were completely prerouted?
    let mut stats = Stats {
        demand_prerouted: 0.0,
        num_boarding_edges_blocked_by_prerouting: 0,
        num_commodities_fully_prerouted: 0,
        num_iterations: 0,
        computation_time: std::time::Duration::new(0, 0),
        cost: 0.0,
        demand_outside: 0.0,
        initial_len_paths_queue: 0,
    };

    stats.demand_prerouted = flow
        .path_flow_map()
        .iter()
        .filter(|&(&path_id, _)| !paths.path(path_id).is_outside())
        .map(|(_, &flow_val)| flow_val)
        .sum();

    stats.num_boarding_edges_blocked_by_prerouting = graph
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
        .count();

    stats.num_commodities_fully_prerouted = graph
        .commodities()
        .filter(|&(commodity_id, _)| {
            let outside_path = graph.outside_path(commodity_id);
            let outside_path_flow = flow.on_path(paths.transfer_path(outside_path));
            outside_path_flow <= EPS_L
        })
        .count();

    info!("Computing A*-Table...");
    compute_a_star_table(&mut a_star_table, graph, |_, edge| {
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
    });

    fill_all_optimal_paths(&mut flow, graph, &a_star_table, &mut paths);

    let mut paths_queue = {
        let mut q = PathsQueue::new();
        for &path_id in flow.path_flow_map().keys() {
            q.enqueue_front(path_id);
        }
        purge_paths_queue(graph, &a_star_table, &mut paths, &flow, &mut q);
        stats.initial_len_paths_queue = q.len();
        q
    };

    let mut ca_flow = CycleAwareFlow::new(Some(flow));

    let mut iteration = 0;
    loop {
        let should_log = iteration % log_iteration_count == 0;
        if should_log {
            info!("Iteration {:}, |q|={:}", iteration, paths_queue.len());
        }
        let result = eq_heuristic_step(
            graph,
            &a_star_table,
            &mut paths,
            &mut ca_flow,
            &mut paths_queue,
            should_log,
        );

        if result {
            break;
        }
        iteration += 1;
    }

    info!("REACHED EQUILIBRIUM");

    let time_finished = std::time::Instant::now();

    debug!("Computing stats...");
    stats.num_iterations = iteration;
    stats.cost = ca_flow.flow().cost(&paths, graph);
    stats.computation_time = time_finished - time_started;
    stats.demand_outside = ca_flow
        .flow()
        .path_flow_map()
        .iter()
        .filter(|it| paths.path(*it.0).is_outside())
        .map(|it| it.1)
        .sum();

    info!("{:#?}", stats);

    (ca_flow.drain(), paths, stats)
}

pub fn arrival_of_path(path: &Path, graph: &Graph, commodity: &CommodityPayload) -> Time {
    match path.edges().last() {
        None => commodity.outside_latest_arrival,
        Some(&edge_idx) => {
            let edge: &EdgePayload = graph.edge(edge_idx);
            let to_node = graph.node(edge.to);
            to_node.time
        }
    }
}

fn get_non_eq_witness_with_paths_queue(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    paths_queue: &mut PathsQueue,
) -> Option<(PathId, PathBox)> {
    struct BetterResponse(PathId, PathBox);
    enum NotFound {
        NoFlowOnPath,
        NoBetterResponse(PathId, HashSet<EdgeIdx>),
    }

    let mut better_response: Option<BetterResponse> = None;

    let count = paths_queue
        .iter()
        .par_map_until(|&path_id| {
            if flow.on_path(path_id) <= EPS_L {
                return ParMapResult::NotFound(NotFound::NoFlowOnPath);
            }
            let path = paths.path(path_id);
            match find_better_path(a_star_table, graph, flow, path) {
                Err(unblocking_edges) => {
                    ParMapResult::NotFound(NotFound::NoBetterResponse(path_id, unblocking_edges))
                }
                Ok(better_path) => ParMapResult::Found(BetterResponse(path_id, better_path)),
            }
        })
        .for_each_count(|it| match it {
            ParMapResult::Found(it) => better_response = Some(it),
            ParMapResult::NotFound(NotFound::NoFlowOnPath) => {}
            ParMapResult::NotFound(NotFound::NoBetterResponse(path_id, unblocking_edges)) => {
                paths_queue.enqueue_on_unblock(path_id, unblocking_edges);
            }
        });
    paths_queue.remove_first_n(if better_response.is_some() {
        count - 1
    } else {
        count
    });

    better_response.map(|it| (it.0, it.1))
}

fn purge_paths_queue(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    paths_queue: &mut PathsQueue,
) {
    info!("Purgin paths queue...");
    let reenqueue = paths_queue
        .par_iter()
        .filter_map(|&path_id| {
            if flow.on_path(path_id) <= EPS_L {
                return None;
            }
            let path = paths.path(path_id);
            Some(match find_better_path(a_star_table, graph, flow, path) {
                Ok(_) => (path_id, None),
                Err(unblocking_edges) => (path_id, Some(unblocking_edges)),
            })
        })
        .collect::<Vec<_>>();

    paths_queue.remove_first_n(paths_queue.len());

    for result in reenqueue {
        match result {
            (path_id, None) => paths_queue.enqueue_back(path_id),
            (path_id, Some(unblocking_edges)) => {
                paths_queue.enqueue_on_unblock(path_id, unblocking_edges)
            }
        }
    }
}

/// Returns a feasible direction, if the flow is not at equilibrium, otherwise None.
fn get_feasible_direction(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    paths_queue: &mut PathsQueue,
) -> Option<Flow> {
    let non_eq_witness: Option<(PathId, PathBox)> =
        get_non_eq_witness_with_paths_queue(graph, a_star_table, paths, flow, paths_queue);

    let (non_eq_path, best_response) = match non_eq_witness {
        Some(paths) => paths,
        None => return None,
    };

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

    Some(direction)
}

fn add_direction_to_flow(
    ca_flow: &mut CycleAwareFlow,
    direction: Flow,
    paths_queue: &mut PathsQueue,
    graph: &Graph,
    paths: &PathsIndex,
) {
    // Enqueue all paths that were zero before and are positive afterwards.
    for (&path_id, &path_dir) in direction.path_flow_map() {
        if path_dir > 0.0 && ca_flow.flow().on_path(path_id) <= EPS_L {
            paths_queue.enqueue_front(path_id);
        }
    }

    ca_flow.add_to_flow(paths, direction, |flow, direction| {
        // Unblock all edges that are now unblocked.
        for (&edge_idx, &edge_dir) in direction.edge_flow_map() {
            if edge_dir < 0.0 {
                let flow = flow.on_edge(edge_idx);
                let capacity = {
                    let edge = graph.edge(edge_idx);
                    match edge.edge_type {
                        EdgeType::Drive(capacity) => capacity,
                        _ => continue,
                    }
                };
                let slack = capacity - flow;
                if slack > EPS_L {
                    continue;
                }
                paths_queue.unblock(edge_idx);
            }
        }
    });
}

/// Returns true, if the flow is at equilibrium.
fn eq_heuristic_step(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    ca_flow: &mut CycleAwareFlow,
    paths_queue: &mut PathsQueue,
    should_log: bool,
) -> bool {
    let mut direction =
        match get_feasible_direction(graph, a_star_table, paths, ca_flow.flow(), paths_queue) {
            None => return true,
            Some(direction) => direction,
        };
    let mut may_increase =
        get_longest_feasible_extension(graph, ca_flow.flow(), &direction, false, true);

    if may_increase <= 0.0 {
        warn!("INFEASIBLE DIRECTION. Recomputing edge flow and try again...");
        ca_flow.recompute_edge_flow();
        direction =
            match get_feasible_direction(graph, a_star_table, paths, ca_flow.flow(), paths_queue) {
                None => return true,
                Some(direction) => direction,
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

    add_direction_to_flow(ca_flow, direction, paths_queue, graph, paths);

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
                "IGNORING INFINITELY FEASIBLE CYCLE OF LENGTH {:}!",
                cycle_len
            );
        } else {
            debug!(
                "EXTENDING ALONG CYCLE OF LENGTH {:} WITH EXTENSIBILITY {:}:\n{}",
                cycle_len,
                may_extend,
                cycle_direction.describe(graph, paths)
            );
            let mut cycle_direction = cycle_direction;
            cycle_direction.scale_by(may_extend);

            add_direction_to_flow(ca_flow, cycle_direction, paths_queue, graph, paths);
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
        let arrival_best_response = arrival_of_path(best_response.payload(), graph, commodity);
        if arrival_best_response >= arrival_of_path(path, graph, commodity) {
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
            assert!(worst_used_path.arrival >= arrival_best_response);
            // TODO: Check if we should not assert that worst used path is greater than best response.
            if worst_used_path.arrival == arrival_best_response {
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
    arrival: Time,
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
        let new_arrival = arrival_of_path(new_path, graph, graph.commodity(commodity_idx));
        match worst_path {
            None => {
                worst_path = Some(WorstUsedPath {
                    path_id: new_path_id,
                    arrival: new_arrival,
                    may_remove,
                })
            }
            Some(WorstUsedPath {
                path_id: _,
                arrival,
                may_remove: _,
            }) if new_arrival > arrival => {
                worst_path = Some(WorstUsedPath {
                    path_id: new_path_id,
                    arrival: new_arrival,
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
            // => The dwell edge must also be full, and its direction must be positive.
            // => The previous drive edge must be full, and d_alight + d_dwell = d_prevdrive <= 0.
            // => d_alight <= -d_dwell < 0.
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
                flow.on_edge(check_drive_edge.post_alight().id()),
                direction.on_edge(check_drive_edge.post_alight().id())
            );
            let dwell = check_drive_edge.post_dwell().unwrap();
            error!(
                " S {:}. f={:}, d={:}",
                describe_drive_edge(graph, check_drive_edge.id()),
                flow.on_edge(dwell.id()),
                direction.on_edge(dwell.id())
            );
            let next_drive = dwell.post_drive();
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
        let dwell = check_drive_edge.pre_dwell().unwrap();
        assert!(direction.on_edge(dwell.id()) > 0.0);
        assert!(
            check_drive_edge.capacity() - flow.on_edge(dwell.id()) <= EPS_L,
            "Flow on dwell edge is {:}, but capacity is {:}. Flow on drive edge is {:} and flow on board edge is {:}.",
            flow.on_edge(dwell.id()),
            check_drive_edge.capacity(),
            flow.on_edge(check_drive_edge.id()),
            flow.on_edge(check_drive_edge.pre_boarding().id()),
        );
        check_drive_edge = dwell.pre_drive();
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
    let mut next_edges_to_check: HashSet<EdgeIdx> = set_new();

    for (&edge_idx, &value) in direction.edge_flow_map() {
        if value <= 0.0 {
            continue;
        }
        next_edges_to_check.insert(edge_idx);
    }

    let mut swap_list: Vec<(PathId, PathId, FVal)> = Vec::new();

    let mut found_exceeded_capacity = false;

    while !next_edges_to_check.is_empty() {
        swap(&mut edges_to_check, &mut next_edges_to_check);

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
            found_exceeded_capacity = true;
            // There is at least one path used (meaning flow or direction is positive)
            // that boards an already full edge of the same line and which also uses this edge.
            reroute_exceeded_drive_edge(
                edge,
                graph,
                a_star_table,
                paths,
                flow,
                direction,
                &mut swap_list,
            );
        }
        if next_edges_to_check.is_empty() {
            // Now, we revert unnecessary swaps.
            // TODO: Check if we can somehow avoid the looping here.
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
                for &edge_idx in paths.path(*path_id).edges() {
                    // TOOD: Check if we can avoid readding edges here (or at least only readd actually relevant edges).
                    next_edges_to_check.insert(edge_idx);
                }

                // In an attempt to prevent a loop: First handle possibly new excessed edges.
                break;
            }
        }
    }

    found_exceeded_capacity
}
