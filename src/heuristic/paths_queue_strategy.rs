use crate::{
    col::HashSet,
    flow::Flow,
    graph::{EdgeIdx, EdgeType, Graph, PathBox},
    heuristic::{describe_drive_edge, paths_queue::PathsQueue},
    iter::par_map_until::{ParMapResult, ParMapUntil},
    path_index::{PathId, PathsIndex},
    primitives::{EPS_L, FVal},
    regret,
    shortest_path::{a_star::AStarTable, best_paths::find_better_path_or_unblocking_edges},
    timer::Timer,
};
use itertools::{Either, Itertools};
use log::{info, trace, warn};
use rayon::{
    iter::{IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};

use super::{HeuristicStats, Strategy};

pub struct PathsQueueStrategy {
    pub queue: PathsQueue,
    pub dir: PathsQueueDir,
    pub sort_by: Option<SortBy>,
    pub purge_interval: usize,
    pub instrument_file: Option<Box<dyn std::io::Write>>,

    pub best_flow: (Flow, FVal),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SortBy {
    TotalRegret,
    TotalRelRegret,
    MaxRelRegret,
}

#[derive(Clone, Copy)]
pub enum PathsQueueDir {
    Front,
    Back,
}
impl PathsQueueDir {
    fn opposite(&self) -> PathsQueueDir {
        match self {
            PathsQueueDir::Front => PathsQueueDir::Back,
            PathsQueueDir::Back => PathsQueueDir::Front,
        }
    }
}

impl Strategy for PathsQueueStrategy {
    fn info(&self) -> String {
        format!("|q|={}", self.queue.len())
    }

    fn on_iterate(
        &mut self,
        graph: &Graph,
        flow: &Flow,
        a_star_table: &AStarTable,
        paths: &mut PathsIndex,
        h_stats: &HeuristicStats,
        timer: &mut Timer,
    ) {
        if self.purge_interval > 0 && h_stats.num_iterations % self.purge_interval == 0 {
            purge_paths_queue(
                graph,
                &a_star_table,
                paths,
                flow,
                &mut self.queue,
                self.sort_by,
            );
        }
        if let Some(file) = &mut self.instrument_file {
            timer.pause();
            let (max_rel_regret, total_rel_regret, total_regret) = self
                .queue
                .par_iter()
                .map(|&path_id| (path_id, flow.on_path(path_id)))
                .filter(|(path_id, flow_on_path)| {
                    let has_flow = *flow_on_path > EPS_L;
                    if !has_flow {
                        trace!(
                            "Dropping path {:?} from queue as it has (almost) no flow {:}.",
                            path_id, *flow_on_path
                        );
                    }
                    has_flow
                })
                .filter_map(|(path_id, flow_on_path)| {
                    let path = paths.path(path_id);
                    find_better_path_or_unblocking_edges(a_star_table, graph, flow, path)
                        .ok()
                        .map(|better_path| (path_id, flow_on_path, better_path))
                })
                .map(|(path_id, flow_on_path, better_path)| {
                    let path = paths.path(path_id);
                    let commodity = graph.commodity(path.commodity_idx());
                    let best_alternative = better_path.payload().cost(commodity, graph);
                    let regret = FVal::from(path.cost(commodity, graph) - best_alternative);
                    let rel_regret = regret / FVal::from(best_alternative);
                    let total_regret = flow_on_path * FVal::from(regret);
                    (rel_regret, flow_on_path * rel_regret, total_regret)
                })
                .map(|it| (Some(it.0), it.1, it.2))
                .reduce(
                    || (None, 0.0, 0.0),
                    |a, b| {
                        let (a0, a1, a2) = a;
                        let (b0, b1, b2) = b;
                        (
                            a0.iter()
                                .chain(b0.iter())
                                .copied()
                                .max_by(|a, b| a.total_cmp(b)),
                            a1 + b1,
                            a2 + b2,
                        )
                    },
                );
            file.write(
                format!(
                    "{},{},{},{},{},{}\n",
                    timer.elapsed().as_millis(),
                    h_stats.num_iterations,
                    self.queue.len(),
                    max_rel_regret.unwrap_or(0.0),
                    total_rel_regret,
                    total_regret,
                )
                .as_bytes(),
            )
            .unwrap();
            file.flush().unwrap();
            timer.start();
            if total_rel_regret < self.best_flow.1 {
                self.best_flow = (flow.clone(), total_rel_regret);
                info!(
                    "New best flow found with total relative regret: {}",
                    total_rel_regret
                );
            }
        }
    }

    fn get_non_eq_witness(
        &mut self,
        graph: &Graph,
        flow: &Flow,
        a_star_table: &AStarTable,
        paths: &PathsIndex,
    ) -> Option<(PathId, PathBox)> {
        let res = get_non_eq_witness_with_paths_queue_inner(
            graph,
            a_star_table,
            paths,
            flow,
            &mut self.queue,
        );
        if res.is_none() && self.queue.len() > 0 {
            warn!(
                "Releasing locks on the remaining {:} paths.",
                self.queue.len()
            );
            self.queue.release_locks();
            get_non_eq_witness_with_paths_queue_inner(
                graph,
                a_star_table,
                paths,
                flow,
                &mut self.queue,
            )
        } else {
            res
        }
    }

    fn on_add_to_flow(
        &mut self,
        _graph: &Graph,
        flow: &Flow,
        direction: &Flow,
        _paths: &PathsIndex,
    ) {
        // Enqueue all paths that were zero before and are positive afterwards.
        for (&path_id, &path_dir) in direction.path_flow_map() {
            if path_dir > 0.0 && flow.on_path(path_id) <= EPS_L {
                match self.dir {
                    PathsQueueDir::Front => self.queue.enqueue_front(path_id),
                    PathsQueueDir::Back => self.queue.enqueue_back(path_id),
                }
            }
        }
    }

    fn on_flow_added(&mut self, graph: &Graph, updated_flow: &Flow, direction: &Flow) {
        // Unblock edges
        for (&edge_idx, &edge_dir) in direction.edge_flow_map() {
            if edge_dir < 0.0 {
                let flow = updated_flow.on_edge(edge_idx);
                let EdgeType::Drive(capacity) = graph.edge(edge_idx).edge_type else {
                    continue;
                };
                let slack = capacity - flow;
                if slack <= EPS_L {
                    continue;
                }

                trace!(
                    "Unblocking driving edge {}",
                    describe_drive_edge(graph, edge_idx)
                );
                self.queue.unblock(graph.boarding_edge(edge_idx).unwrap().0);
            }
        }
    }

    fn found_infinite_cycle(&mut self, non_eq_path: PathId) {
        self.queue.lock_path(non_eq_path);
    }

    fn into_flow(
        self,
        flow: Flow,
        graph: &Graph,
        a_star_table: &AStarTable,
        paths: &PathsIndex,
    ) -> Flow {
        if regret::get_total_relative_regret(graph, &flow, a_star_table, paths) < self.best_flow.1 {
            return flow;
        }
        info!(
            "Using a better flow with total relative regret: {}",
            self.best_flow.1
        );
        return self.best_flow.0;
    }
}

fn get_non_eq_witness_with_paths_queue_inner(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &PathsIndex,
    flow: &Flow,
    paths_queue: &mut PathsQueue,
) -> Option<(PathId, PathBox)> {
    struct BetterResponse(PathId, PathBox);
    enum NotFound {
        NoFlowOnPath(PathId),
        PathLocked(PathId),
        NoBetterResponse(PathId, HashSet<EdgeIdx>),
    }

    let mut better_response: Option<BetterResponse> = None;

    let results = paths_queue
        .iter()
        .par_map_until(|&path_id| {
            if flow.on_path(path_id) <= EPS_L {
                return ParMapResult::NotFound(NotFound::NoFlowOnPath(path_id));
            }
            if paths_queue.is_locked(path_id) {
                return ParMapResult::NotFound(NotFound::PathLocked(path_id));
            }
            let path = paths.path(path_id);
            match find_better_path_or_unblocking_edges(a_star_table, graph, flow, path) {
                Err(unblocking_edges) => {
                    ParMapResult::NotFound(NotFound::NoBetterResponse(path_id, unblocking_edges))
                }
                Ok(better_path) => ParMapResult::Found(BetterResponse(path_id, better_path)),
            }
        })
        .collect_vec();

    let count_no_better_path = if results
        .last()
        .is_some_and(|it| matches!(it, ParMapResult::Found(_)))
    {
        results.len() - 1
    } else {
        results.len()
    };

    paths_queue.remove_first_n(count_no_better_path);
    for it in results {
        match it {
            ParMapResult::Found(it) => better_response = Some(it),
            ParMapResult::NotFound(NotFound::NoFlowOnPath(_)) => {}
            ParMapResult::NotFound(NotFound::PathLocked(path_id)) => {
                paths_queue.enqueue_front(path_id);
            }
            ParMapResult::NotFound(NotFound::NoBetterResponse(path_id, unblocking_edges)) => {
                paths_queue.enqueue_on_unblock(path_id, unblocking_edges);
            }
        }
    }

    better_response.map(|it| (it.0, it.1))
}

pub fn purge_paths_queue(
    graph: &Graph,
    a_star_table: &AStarTable,
    paths: &mut PathsIndex,
    flow: &Flow,
    paths_queue: &mut PathsQueue,
    sort_by: Option<SortBy>,
) -> FVal {
    info!("Purging paths queue from |q|={}...", paths_queue.len());

    struct Reenqueue {
        pub path_id: PathId,
        pub rel_regret: FVal,
        pub total_regret: FVal,
        pub total_rel_regret: FVal,
    }

    let (mut reenqueue, purged): (Vec<_>, Vec<_>) = paths_queue
        .par_iter()
        .map(|&path_id| (path_id, flow.on_path(path_id)))
        .filter(|(path_id, flow_on_path)| {
            let has_flow = *flow_on_path > EPS_L;
            if !has_flow {
                trace!(
                    "Dropping path {:?} from queue as it has (almost) no flow {:}.",
                    path_id, *flow_on_path
                );
            }
            has_flow
        })
        .partition_map(|(path_id, flow_on_path)| {
            let path = paths.path(path_id);
            match find_better_path_or_unblocking_edges(a_star_table, graph, flow, path) {
                Ok(better_path) => {
                    let commodity = graph.commodity(path.commodity_idx());
                    let best_alternative = better_path.payload().cost(commodity, graph);
                    let regret = FVal::from(path.cost(commodity, graph) - best_alternative);
                    let rel_regret = if sort_by == Some(SortBy::MaxRelRegret)
                        || sort_by == Some(SortBy::TotalRelRegret)
                    {
                        regret / FVal::from(best_alternative)
                    } else {
                        0.0
                    };
                    let total_rel_regret = flow_on_path * rel_regret;
                    let total_regret = flow_on_path * FVal::from(regret);
                    Either::Left(Reenqueue {
                        path_id,
                        rel_regret,
                        total_rel_regret,
                        total_regret,
                    })
                }
                Err(unblocking_edges) => Either::Right((path_id, unblocking_edges)),
            }
        });

    if let Some(sort_by) = sort_by {
        match sort_by {
            SortBy::TotalRegret => {
                reenqueue.par_sort_by(|a, b| (b.total_regret).total_cmp(&a.total_regret))
            }
            SortBy::TotalRelRegret => {
                reenqueue.par_sort_by(|a, b| (b.total_rel_regret).total_cmp(&a.total_rel_regret));
                if let Some(first) = reenqueue.first() {
                    info!(
                        "Max total relative regret: {} (first: {:?})",
                        first.total_rel_regret, first.path_id
                    );
                }
            }
            SortBy::MaxRelRegret => {
                reenqueue.par_sort_by(|a, b| (b.rel_regret).total_cmp(&a.rel_regret));
                if let Some(first) = reenqueue.first() {
                    let num_paths = reenqueue
                        .iter()
                        .enumerate()
                        .find(|(_, it)| it.rel_regret >= first.rel_regret - EPS_L)
                        .unwrap()
                        .0
                        + 1;
                    info!(
                        "Max relative regret: {} (#paths with this value: {}, first: {})",
                        first.rel_regret, num_paths, first.path_id.0
                    );
                }
            }
        }
    }

    let total_regret: FVal = reenqueue.par_iter().map(|it| it.total_regret).sum();
    info!("Total regret: {}", total_regret);

    paths_queue.clear_queue();
    for item in reenqueue {
        paths_queue.enqueue_back(item.path_id)
    }
    for (path_id, unblocking_edges) in purged {
        trace!("Path {:?} has no better response.", path_id);
        paths_queue.enqueue_on_unblock(path_id, unblocking_edges);
    }

    total_regret
}
