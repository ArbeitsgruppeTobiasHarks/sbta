use crate::{
    col::HashMap,
    graph::{ExtCommodity, ExtCostCharacteristic, ExtFixedDeparture, ExtODPair, Graph, StationIdx},
    vehicle::{ExtFirstStop, ExtLastStop, ExtMiddleStop, ExtStationId, ExtVehicle, VehicleId},
};

pub fn create_sample() -> (Graph, HashMap<ExtStationId, StationIdx>) {
    Graph::create(
        vec![
            ExtVehicle {
                id: VehicleId(0),
                capacity: 1.0,
                first_stop: ExtFirstStop {
                    departure: 1,
                    station: ExtStationId(0),
                },
                middle_stops: vec![
                    ExtMiddleStop {
                        arrival: 2,
                        departure: 2,
                        station: ExtStationId(2),
                    },
                    ExtMiddleStop {
                        arrival: 3,
                        departure: 3,
                        station: ExtStationId(3),
                    },
                ],
                last_stop: ExtLastStop {
                    arrival: 4,
                    station: ExtStationId(5),
                },
            },
            ExtVehicle {
                id: VehicleId(1),
                capacity: 1.0,
                first_stop: ExtFirstStop {
                    departure: 0,
                    station: ExtStationId(1),
                },
                middle_stops: vec![],
                last_stop: ExtLastStop {
                    station: ExtStationId(2),
                    arrival: 1,
                },
            },
            ExtVehicle {
                id: VehicleId(2),
                capacity: 1.0,
                first_stop: ExtFirstStop {
                    departure: 6,
                    station: ExtStationId(1),
                },
                middle_stops: vec![
                    ExtMiddleStop {
                        station: ExtStationId(3),
                        arrival: 7,
                        departure: 7,
                    },
                    ExtMiddleStop {
                        station: ExtStationId(4),
                        arrival: 8,
                        departure: 8,
                    },
                ],
                last_stop: ExtLastStop {
                    station: ExtStationId(5),
                    arrival: 9,
                },
            },
        ],
        &[
            ExtCommodity {
                od_pair: ExtODPair {
                    origin: ExtStationId(0),
                    destination: ExtStationId(4),
                },
                demand: 1.0,
                outside_option: 10,
                cost: ExtCostCharacteristic::FixedDeparture(ExtFixedDeparture {
                    departure_time: 1,
                }),
            },
            ExtCommodity {
                od_pair: ExtODPair {
                    origin: ExtStationId(1),
                    destination: ExtStationId(5),
                },
                cost: ExtCostCharacteristic::FixedDeparture(ExtFixedDeparture {
                    departure_time: 0,
                }),
                demand: 1.0,
                outside_option: 10,
            },
        ],
    )
}

#[cfg(test)]
mod tests {
    use std::{path::Path, time::Duration, usize};

    use itertools::Itertools;

    use crate::{
        heuristic::{eq_heuristic, multi_phase_strategy::MultiPhaseStrategyPlan},
        serialization::graph::{export_graph, import_graph},
        timpass::{LineDirection, LineInfo, LineInfoMap},
    };

    #[test]
    pub fn test_heuristic() {
        let (graph, _idx) = super::create_sample();
        let (flow, paths, stats, a_star_table) = eq_heuristic(
            &graph,
            1,
            MultiPhaseStrategyPlan {
                iterations_simple: usize::MAX,
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
    pub fn test_export_import() {
        let filename = "./data/sample-sbta-graph.sqlite3";
        if Path::new(filename).exists() {
            std::fs::remove_file(filename).unwrap();
        }

        let (graph, _idx) = super::create_sample();
        export_graph(
            &graph,
            &LineInfoMap(
                graph
                    .vehicles()
                    .map(|_| LineInfo {
                        line_direction: LineDirection::Forward,
                        line_freq_repetition: 0,
                        line_id: 0,
                        period_repetition: 0,
                    })
                    .collect_vec(),
            ),
            filename,
        );

        let imported = import_graph(filename).unwrap();

        let (_flow, _paths, stats, _) = eq_heuristic(
            &imported,
            1,
            MultiPhaseStrategyPlan {
                iterations_simple: usize::MAX,
                duration_simple: Duration::MAX,
                iterations_total_regret: usize::MAX,
                duration_total_regret: Duration::MAX,
                iterations_total_rel_regret: usize::MAX,
                duration_total_rel_regret: Duration::MAX,
                iterations_max_rel_regret: usize::MAX,
                duration_max_rel_regret: Duration::MAX,
            },
        );

        assert_eq!(
            stats.heuristic.unwrap().num_iterations,
            eq_heuristic(
                &graph,
                1,
                MultiPhaseStrategyPlan {
                    iterations_simple: usize::MAX,
                    duration_simple: Duration::MAX,
                    iterations_total_regret: usize::MAX,
                    duration_total_regret: Duration::MAX,
                    iterations_total_rel_regret: usize::MAX,
                    duration_total_rel_regret: Duration::MAX,
                    iterations_max_rel_regret: usize::MAX,
                    duration_max_rel_regret: Duration::MAX,
                },
            )
            .2
            .heuristic
            .unwrap()
            .num_iterations
        )
    }
}
