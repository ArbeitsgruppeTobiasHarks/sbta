use std::{fs::File, io::Write, time::Duration};

use log::info;

use crate::{
    flow::Flow,
    graph::{Graph, PathBox},
    path_index::{PathId, PathsIndex},
    regret,
    shortest_path::a_star::AStarTable,
    timer::Timer,
};

use super::{
    HeuristicStats, Strategy, StrategyBuilder,
    paths_queue::PathsQueue,
    paths_queue_strategy::{PathsQueueDir, PathsQueueStrategy, SortBy, purge_paths_queue},
    term_cond::TerminationCondition,
};

pub struct MultiPhaseStrategyPlan {
    pub iterations_simple: usize,
    pub duration_simple: Duration,
    pub iterations_total_regret: usize,
    pub duration_total_regret: Duration,
    pub iterations_total_rel_regret: usize,
    pub duration_total_rel_regret: Duration,
    pub iterations_max_rel_regret: usize,
    pub duration_max_rel_regret: Duration,
}

impl StrategyBuilder for MultiPhaseStrategyPlan {
    type S = MultiPhaseStrategy;

    fn create(
        self: Self,
        graph: &Graph,
        flow: &Flow,
        paths: &mut PathsIndex,
        a_star_table: &AStarTable,
        h_stats: &mut HeuristicStats,
        timer: &mut Timer,
    ) -> Self::S {
        let mut queue = PathsQueue::new();
        for &path_id in flow.path_flow_map().keys() {
            queue.enqueue_front(path_id);
        }
        purge_paths_queue(
            graph,
            a_star_table,
            paths,
            flow,
            &mut queue,
            Some(SortBy::TotalRegret),
        );
        h_stats.initial_len_paths_queue = queue.len();
        let mut instrumentation_file = File::create("instrumentation.csv").unwrap();
        instrumentation_file
            .write(
                "elapsed,iteration,queue,max_rel_regret,total_rel_regret,total_regret\n".as_bytes(),
            )
            .unwrap();
        let inner = PathsQueueStrategy {
            queue,
            sort_by: None,
            dir: PathsQueueDir::Back,
            purge_interval: 0,
            instrument_file: Some(Box::new(instrumentation_file)),
            best_flow: (
                flow.clone(),
                regret::get_total_relative_regret(graph, flow, a_star_table, paths),
            ),
        };
        MultiPhaseStrategy {
            phase: Phase::Simple,
            phase_started_at: timer.elapsed(),
            inner,
            plan: self,
            should_instrument: true,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Simple,
    TotalRegret,
    TotalRelRegret,
    MaxRelRegret,
}

pub struct MultiPhaseStrategy {
    phase: Phase,
    phase_started_at: Duration,
    inner: PathsQueueStrategy,
    plan: MultiPhaseStrategyPlan,
    should_instrument: bool,
}

impl MultiPhaseStrategy {
    fn update_inner_strategy(&mut self, num_iterations: usize, timer: &mut Timer) {
        let elapsed = timer.elapsed();
        let elapsed_phase = elapsed - self.phase_started_at;
        match self.phase {
            Phase::Simple => {
                if num_iterations >= self.plan.iterations_simple
                    || elapsed_phase >= self.plan.duration_simple
                {
                    info!("Starting Phase TotalRegret.");
                    self.phase = Phase::TotalRegret;
                    self.inner.sort_by = Some(SortBy::TotalRegret);
                    self.inner.dir = PathsQueueDir::Back;
                    self.inner.purge_interval = 1;
                    self.phase_started_at = elapsed;
                }
            }
            Phase::TotalRegret => {
                if num_iterations >= self.plan.iterations_total_regret
                    || elapsed_phase >= self.plan.duration_total_regret
                {
                    info!("Starting Phase TotalRelRegret.");
                    self.phase = Phase::TotalRelRegret;
                    self.inner.sort_by = Some(SortBy::TotalRelRegret);
                    self.inner.dir = PathsQueueDir::Back;
                    self.inner.purge_interval = 1;
                    self.phase_started_at = elapsed;
                }
            }
            Phase::TotalRelRegret => {
                if num_iterations >= self.plan.iterations_total_rel_regret
                    || elapsed_phase >= self.plan.duration_total_rel_regret
                {
                    info!("Starting Phase MaxRelRegret.");
                    self.phase = Phase::MaxRelRegret;
                    self.inner.sort_by = Some(SortBy::MaxRelRegret);
                    self.inner.dir = PathsQueueDir::Back;
                    self.inner.purge_interval = 1;
                    self.phase_started_at = elapsed;
                }
            }
            Phase::MaxRelRegret => {}
        }
    }
}

impl TerminationCondition for MultiPhaseStrategy {
    fn advance(&mut self, num_iterations: usize, timer: &mut Timer) -> bool {
        return self.phase != Phase::MaxRelRegret
            || !(timer.elapsed() - self.phase_started_at >= self.plan.duration_max_rel_regret
                || num_iterations >= self.plan.iterations_max_rel_regret);
    }
}

impl Strategy for MultiPhaseStrategy {
    fn info(&self) -> String {
        self.inner.info()
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
        self.update_inner_strategy(h_stats.num_iterations, timer);
        self.inner
            .on_iterate(graph, flow, a_star_table, paths, h_stats, timer);
    }

    fn get_non_eq_witness(
        &mut self,
        graph: &Graph,
        flow: &Flow,
        a_star_table: &AStarTable,
        paths: &PathsIndex,
    ) -> Option<(PathId, PathBox)> {
        self.inner
            .get_non_eq_witness(graph, flow, a_star_table, paths)
    }

    fn on_add_to_flow(&mut self, graph: &Graph, flow: &Flow, direction: &Flow, paths: &PathsIndex) {
        self.inner.on_add_to_flow(graph, flow, direction, paths);
    }

    fn on_flow_added(&mut self, graph: &Graph, updated_flow: &Flow, direction: &Flow) {
        self.inner.on_flow_added(graph, updated_flow, direction);
    }

    fn found_infinite_cycle(&mut self, non_eq_path: PathId) {
        self.inner.found_infinite_cycle(non_eq_path);
    }

    fn into_flow(
        self,
        flow: Flow,
        graph: &Graph,
        a_star_table: &AStarTable,
        paths: &PathsIndex,
    ) -> Flow {
        self.inner.into_flow(flow, graph, a_star_table, paths)
    }
}
