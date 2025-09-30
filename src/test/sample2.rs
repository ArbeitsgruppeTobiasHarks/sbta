use std::{time::Duration, usize};

use log::info;

use crate::{
    col::HashMap,
    flow::Flow,
    graph::{
        CommodityIdx, ExtCommodity, ExtCostCharacteristic, ExtDepartureTimeChoice,
        ExtFixedDeparture, ExtODPair, FirstStop, Graph, LastStop, PathDescription, Ride,
        StationIdx,
    },
    heuristic::{
        FlowStats, HeuristicStats, Stats, eq_heuristic_main_loop,
        multi_phase_strategy::MultiPhaseStrategyPlan,
    },
    path_index::PathsIndex,
    shortest_path::a_star::{AStarTable, compute_a_star_table},
    vehicle::{ExtFirstStop, ExtLastStop, ExtMiddleStop, ExtStationId, ExtVehicle, VehicleId},
};

pub fn create_graph() -> (Graph, Vec<ExtStationId>, HashMap<ExtStationId, StationIdx>) {
    let stations = (0..8).map(ExtStationId).collect::<Vec<_>>();
    let (graph, station_idx) = Graph::create(
        vec![
            // Green vehicle
            ExtVehicle {
                id: VehicleId(0),
                capacity: 1.0,
                first_stop: ExtFirstStop {
                    departure: 1,
                    station: stations[0],
                },
                middle_stops: vec![
                    ExtMiddleStop {
                        arrival: 2,
                        departure: 3,
                        station: stations[1],
                    },
                    ExtMiddleStop {
                        arrival: 5,
                        departure: 6,
                        station: stations[3],
                    },
                ],
                last_stop: ExtLastStop {
                    arrival: 7,
                    station: stations[4],
                },
            },
            // Orange vehicle
            ExtVehicle {
                id: VehicleId(1),
                capacity: 1.0,
                first_stop: ExtFirstStop {
                    departure: 6,
                    station: stations[2],
                },
                middle_stops: vec![
                    ExtMiddleStop {
                        arrival: 8,
                        departure: 9,
                        station: stations[4],
                    },
                    ExtMiddleStop {
                        arrival: 10,
                        departure: 11,
                        station: stations[5],
                    },
                ],
                last_stop: ExtLastStop {
                    station: stations[6],
                    arrival: 12,
                },
            },
            // Red vehicle
            ExtVehicle {
                id: VehicleId(2),
                capacity: 1.0,
                first_stop: ExtFirstStop {
                    departure: 10,
                    station: stations[3],
                },
                middle_stops: vec![ExtMiddleStop {
                    arrival: 13,
                    departure: 14,
                    station: stations[6],
                }],
                last_stop: ExtLastStop {
                    station: stations[7],
                    arrival: 15,
                },
            },
        ],
        &[
            ExtCommodity {
                od_pair: ExtODPair {
                    origin: ExtStationId(0),
                    destination: stations[5],
                },
                demand: 2.0,
                outside_option: 20,
                cost: ExtCostCharacteristic::FixedDeparture(ExtFixedDeparture {
                    departure_time: 0,
                }),
            },
            ExtCommodity {
                od_pair: ExtODPair {
                    origin: stations[1],
                    destination: stations[7],
                },
                demand: 2.0,
                outside_option: 20,
                cost: ExtCostCharacteristic::DepartureTimeChoice(ExtDepartureTimeChoice {
                    target_arrival_time: 0,
                    delay_penalty_factor: 1,
                }),
            },
            ExtCommodity {
                od_pair: ExtODPair {
                    origin: stations[2],
                    destination: stations[7],
                },
                demand: 2.0,
                outside_option: 20,
                cost: ExtCostCharacteristic::FixedDeparture(ExtFixedDeparture {
                    departure_time: 0,
                }),
            },
        ],
    );
    (graph, stations, station_idx)
}

pub fn create_sample_without_eq() -> (Graph, Vec<ExtStationId>, HashMap<ExtStationId, StationIdx>) {
    let stations = (0..3).map(ExtStationId).collect::<Vec<_>>();
    let (graph, station_idx) = Graph::create(
        vec![
            // Green vehicle
            ExtVehicle {
                id: VehicleId(0),
                capacity: 1.0,
                first_stop: ExtFirstStop {
                    departure: 1,
                    station: stations[0],
                },
                middle_stops: vec![
                    ExtMiddleStop {
                        arrival: 2,
                        departure: 2,
                        station: stations[1],
                    },
                    ExtMiddleStop {
                        arrival: 3,
                        departure: 3,
                        station: stations[0],
                    },
                ],
                last_stop: ExtLastStop {
                    arrival: 4,
                    station: stations[2],
                },
            },
        ],
        &[ExtCommodity {
            od_pair: ExtODPair {
                origin: stations[0],
                destination: stations[2],
            },
            demand: 2.0,
            outside_option: 20,
            cost: ExtCostCharacteristic::DepartureTimeChoice(ExtDepartureTimeChoice {
                target_arrival_time: 4,
                delay_penalty_factor: 0,
            }),
        }],
    );
    (graph, stations, station_idx)
}

pub fn run_sample() {
    let (graph, stations, station_idx) = create_graph();
    let initial_solution = {
        let mut paths = PathsIndex::new();
        let mut flow = Flow::new();

        // We assume that each commodity already has its own SPAWN node and outside option edge.
        for (commodity_id, commodity) in graph.commodities() {
            let outside_path_id = paths.transfer_path(graph.outside_path(commodity_id));
            if commodity_id != CommodityIdx(1) {
                flow.add_flow_onto_path(&paths, outside_path_id, commodity.demand, true, true);
            } else {
                flow.add_flow_onto_path(&paths, outside_path_id, 1.0, true, true);
                let boxed_path = graph
                    .path_from_description(&PathDescription {
                        commodity_idx: commodity_id,
                        departure_time: 3,
                        rides: vec![
                            Ride {
                                vehicle: VehicleId(0),
                                first_stop: FirstStop {
                                    station: station_idx[&stations[1]],
                                    departure: 3,
                                },
                                last_stop: LastStop {
                                    station: station_idx[&stations[4]],
                                    arrival: 7,
                                },
                            },
                            Ride {
                                vehicle: VehicleId(1),
                                first_stop: FirstStop {
                                    station: station_idx[&stations[4]],
                                    departure: 9,
                                },
                                last_stop: LastStop {
                                    station: station_idx[&stations[6]],
                                    arrival: 12,
                                },
                            },
                            Ride {
                                vehicle: VehicleId(2),
                                first_stop: FirstStop {
                                    station: station_idx[&stations[6]],
                                    departure: 14,
                                },
                                last_stop: LastStop {
                                    station: station_idx[&stations[7]],
                                    arrival: 15,
                                },
                            },
                        ],
                    })
                    .unwrap();
                let path_id = paths.transfer_path(boxed_path);
                flow.add_flow_onto_path(&paths, path_id, 1.0, true, true);
            }
        }
        (flow, paths)
    };

    let mut a_star_table = AStarTable::new(graph.num_stations(), graph.num_nodes());
    compute_a_star_table(&mut a_star_table, &graph, |_, _| true);
    let stats = Stats {
        computation_time: Duration::ZERO,
        heuristic: Some(HeuristicStats {
            demand_prerouted: 0.0,
            num_boarding_edges_blocked_by_prerouting: 0,
            num_commodities_fully_prerouted: 0,
            num_iterations: 0,
            num_iterations_postprocessing: 0,
            initial_len_paths_queue: 0,
        }),
        flow: FlowStats::default(),
    };

    let (flow, paths, _stats, _) = eq_heuristic_main_loop(
        &graph,
        (initial_solution.0, initial_solution.1, a_star_table, stats),
        1,
        MultiPhaseStrategyPlan {
            iterations_simple: 0,
            duration_simple: Duration::MAX,
            iterations_total_regret: usize::MAX,
            duration_total_regret: Duration::MAX,
            iterations_total_rel_regret: usize::MAX,
            duration_total_rel_regret: Duration::MAX,
            iterations_max_rel_regret: usize::MAX,
            duration_max_rel_regret: Duration::MAX,
        },
    );

    info!("Equilibrium flow:\n{}", flow.describe(&graph, &paths));
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use crate::{
        heuristic::{eq_heuristic, multi_phase_strategy::MultiPhaseStrategyPlan},
        primitives::EPS_L,
        regret::{self, get_total_regret},
    };

    #[test]
    pub fn test_heuristic() {
        let (graph, _, _) = super::create_graph();
        let (flow, paths, _, _) = eq_heuristic(
            &graph,
            1,
            MultiPhaseStrategyPlan {
                iterations_simple: 0,
                duration_simple: Duration::MAX,
                iterations_total_regret: usize::MAX,
                duration_total_regret: Duration::MAX,
                iterations_total_rel_regret: usize::MAX,
                duration_total_rel_regret: Duration::MAX,
                iterations_max_rel_regret: usize::MAX,
                duration_max_rel_regret: Duration::MAX,
            },
        );
        println!("Flow: {}", flow.describe(&graph, &paths));
    }

    #[test]
    pub fn test_heuristic_does_not_terminate_if_no_eq_exists() {
        let (graph, _, _) = super::create_sample_without_eq();
        let (flow, paths, _, a_star_table) = eq_heuristic(
            &graph,
            1,
            MultiPhaseStrategyPlan {
                iterations_simple: 0,
                duration_simple: Duration::MAX,
                iterations_total_regret: 100,
                duration_total_regret: Duration::MAX,
                iterations_total_rel_regret: 0,
                duration_total_rel_regret: Duration::MAX,
                iterations_max_rel_regret: 0,
                duration_max_rel_regret: Duration::MAX,
            },
        );
        println!("Flow: {}", flow.describe(&graph, &paths));
        println!(
            "Regret: {}",
            get_total_regret(&graph, &flow, &a_star_table, &paths)
        );
        assert!(regret::get_total_regret(&graph, &flow, &a_star_table, &paths) > EPS_L);
    }
}
