use std::time::Duration;

use itertools::Itertools;
use log::info;
use rand::{self, Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::col::HashMap;
use crate::graph::{
    CommodityIdx, ExtCommodity, ExtCostCharacteristic, ExtFixedDeparture, ExtODPair, Graph,
};
use crate::heuristic::eq_heuristic;
use crate::heuristic::multi_phase_strategy::MultiPhaseStrategyPlan;
use crate::regret;
use crate::vehicle::{
    ExtFirstStop, ExtLastStop, ExtMiddleStop, ExtStationId, ExtVehicle, VehicleId,
};

pub fn run(seed: u64) {
    let num_stations = 100;
    let num_vehicles = 100;
    let num_commodities = 10000;
    let num_stations_per_vehicle = 15;
    let demand_range = 10.0..12.0;
    let capacity_range = 800.0..1200.0;
    let time_step_range = 0..100;

    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let gen_vehicle = |id| {
        let capacity: f64 = rng.random_range(capacity_range.clone());
        let first_stop = ExtFirstStop {
            station: ExtStationId(rng.random_range(0..num_stations)),
            departure: rng.random_range(time_step_range.clone()),
        };
        let num_middle_stops = rng.random_range(0..num_stations_per_vehicle - 2);
        let mut last_time = first_stop.departure;
        let middle_stops = (0..num_middle_stops)
            .map(|_| {
                let station = ExtStationId(rng.random_range(0..num_stations));
                let arrival = last_time + rng.random_range(time_step_range.clone());
                let departure = arrival + rng.random_range(time_step_range.clone());
                last_time = departure;
                ExtMiddleStop {
                    station,
                    arrival,
                    departure,
                }
            })
            .collect();
        let last_stop = ExtLastStop {
            station: ExtStationId(rng.random_range(0..num_stations)),
            arrival: last_time + rng.random_range(time_step_range.clone()),
        };
        ExtVehicle {
            id: VehicleId(id),
            capacity,
            first_stop,
            middle_stops,
            last_stop,
        }
    };

    let vehicles = (0..num_vehicles).map(gen_vehicle).collect();

    let commodities = (0..num_commodities)
        .map(|_| {
            let mut origin = ExtStationId(rng.random_range(0..num_stations));
            // let departure = rng.random_range(0..100);
            let mut destination = ExtStationId(rng.random_range(0..num_stations));
            while origin == destination {
                origin = ExtStationId(rng.random_range(0..num_stations));
                destination = ExtStationId(rng.random_range(0..num_stations));
            }

            let demand = rng.random_range(demand_range.clone());
            let outside_option = 50000;
            ExtCommodity {
                od_pair: ExtODPair {
                    origin,
                    destination,
                },
                demand,
                outside_option,
                cost: ExtCostCharacteristic::FixedDeparture(ExtFixedDeparture {
                    departure_time: 0,
                }),
            }
        })
        .collect::<Vec<_>>();

    let (graph, _station_idx) = Graph::create(vehicles, &commodities);
    let (flow, paths, _stats, a_star_table) = eq_heuristic(
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

    let flow_by_comm = flow
        .path_flow_map()
        .iter()
        .map(|(path_id, f_val)| (path_id, f_val, paths.path(*path_id).commodity_idx()))
        .sorted_by_key(|it| it.2.0)
        .chunk_by(|it| it.2)
        .into_iter()
        .map(|(commodity_id, group)| (commodity_id, group.map(|(_, f_val, _)| f_val).sum::<f64>()))
        .collect::<HashMap<CommodityIdx, f64>>();

    for (commodity_id, commodity) in graph.commodities() {
        assert!(
            (flow_by_comm[&commodity_id] - commodity.demand).abs() < 1e-8,
            "Commodity {:?} has flow {:?} but demand {:?}",
            commodity_id,
            flow_by_comm[&commodity_id],
            commodity.demand
        );
    }
    let used_paths = flow
        .path_flow_map()
        .iter()
        .filter(|it| *it.1 != 0.0)
        .count();
    info!("#UsedPaths: {}", used_paths);
    info!(
        "Final Total regret: {}",
        regret::get_total_regret(&graph, &flow, &a_star_table, &paths)
    );
}

pub fn run_samples() {
    for seed in 0..1 {
        info!("\nSeed: {:}\n", seed);
        run(seed);
    }
}

#[cfg(test)]
mod tests {
    use log::LevelFilter;

    use super::*;

    #[test]
    fn test_random_samples() {
        env_logger::builder().filter_level(LevelFilter::Info).init();
        run_samples();
    }
}
