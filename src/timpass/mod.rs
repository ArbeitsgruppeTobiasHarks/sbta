use std::fs::File;

use serde::Deserialize;

use itertools::Itertools;

use crate::{
    DynamicProfileArgs,
    graph::{
        ExtCommodity, ExtCostCharacteristic, ExtDepartureTimeChoice, ExtFixedDeparture, ExtODPair,
    },
    primitives::{FVal, Time},
    vehicle::{ExtFirstStop, ExtLastStop, ExtMiddleStop, ExtStationId, ExtVehicle, VehicleId},
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub enum EventType {
    #[serde(rename = "\"departure\"")]
    Departure,

    #[serde(rename = "\"arrival\"")]
    Arrival,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Hash)]
pub enum LineDirection {
    #[serde(rename = ">")]
    Forward,
    #[serde(rename = "<")]
    Backward,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Event {
    #[serde(rename = "# event_id")]
    event_id: usize,

    #[serde(rename = "type")]
    event_type: EventType,
    stop_id: usize,
    line_id: usize,
    line_direction: LineDirection,
    line_freq_repetition: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub enum ActivityType {
    #[serde(rename = "\"drive\"")]
    Drive,
    #[serde(rename = "\"wait\"")]
    Wait,
    #[serde(rename = "\"change\"")]
    Change,
    #[serde(rename = "\"headway\"")]
    Headway,
    #[serde(rename = "\"sync\"")]
    Sync,
    #[serde(rename = "\"turnaround\"")]
    Turnaround,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Activity {
    #[serde(rename = "# activity_index")]
    activity_index: usize,
    #[serde(rename = "type")]
    activity_type: ActivityType,
    from_event: usize,
    to_event: usize,
    lower_bound: usize,
    upper_bound: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Demand {
    #[serde(rename = "# origin")]
    pub origin: usize,
    pub destination: usize,
    pub customers: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    ptn_name: String,
    period_length: usize,
    ean_change_penalty: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TimetableEntry {
    #[serde(rename = "# event_id")]
    event_id: usize,
    time: usize,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct LineCapacity {
    #[serde(rename = "# line_id")]
    line_id: usize,
    capacity: f64,
}

fn reader() -> csv::ReaderBuilder {
    let mut builder = csv::ReaderBuilder::new();
    builder.trim(csv::Trim::All).delimiter(b';');

    builder
}

pub fn parse_events(stream: impl std::io::Read) -> Result<Box<[Event]>, csv::Error> {
    reader().from_reader(stream).deserialize().collect()
}
pub fn parse_activities(stream: impl std::io::Read) -> Result<Box<[Activity]>, csv::Error> {
    reader().from_reader(stream).deserialize().collect()
}
pub fn parse_demands(stream: impl std::io::Read) -> Result<Box<[Demand]>, csv::Error> {
    reader().from_reader(stream).deserialize().collect()
}
pub fn parse_timetable(stream: impl std::io::Read) -> Result<Box<[TimetableEntry]>, csv::Error> {
    reader().from_reader(stream).deserialize().collect()
}
pub fn parse_lines(stream: impl std::io::Read) -> Result<Box<[LineCapacity]>, csv::Error> {
    reader().from_reader(stream).deserialize().collect()
}

#[derive(Debug)]
pub enum ParseConfigError {
    CsvError(csv::Error),
    MissingKey(Box<str>),
    InvalidValue(Box<str>),
}
pub fn parse_config(stream: impl std::io::Read) -> Result<Box<Config>, ParseConfigError> {
    let key_value_pairs = reader()
        .from_reader(stream)
        .deserialize()
        .collect::<Result<Box<[(String, String)]>, csv::Error>>()
        .map_err(ParseConfigError::CsvError)?;
    let mut key_value_map = key_value_pairs
        .iter()
        .cloned()
        .collect::<std::collections::HashMap<_, _>>();
    Ok(Box::new(Config {
        ptn_name: key_value_map
            .remove("ptn_name")
            .ok_or(ParseConfigError::MissingKey("ptn_name".into()))?,
        period_length: key_value_map
            .remove("period_length")
            .ok_or(ParseConfigError::MissingKey("period_length".into()))?
            .parse::<usize>()
            .map_err(|_e| {
                ParseConfigError::InvalidValue("Could not parse value of period_length!".into())
            })?,
        ean_change_penalty: key_value_map
            .remove("ean_change_penalty")
            .ok_or(ParseConfigError::MissingKey("ean_change_penalty".into()))?
            .parse::<usize>()
            .map_err(|_e| {
                ParseConfigError::InvalidValue(
                    "Could not parse value of ean_change_penalty!".into(),
                )
            })?,
    }))
}

pub fn find_first_departures(events: &[Event], activities: &[Activity]) -> Vec<usize> {
    let mut first_departures = Vec::new();
    for event in events {
        if event.event_type != EventType::Departure {
            continue;
        }
        let mut incoming_activities = activities
            .iter()
            .filter(|activity| activity.to_event == event.event_id);
        if incoming_activities.any(|activity| activity.activity_type == ActivityType::Wait) {
            continue;
        }
        first_departures.push(event.event_id);
    }
    first_departures
}

fn floor_to_multiple(value: u32, multiple: u32) -> u32 {
    (value / multiple) * multiple
}

pub fn extract_vehicle(
    events: &[Event],
    activities: &[Activity],
    timetable: &[TimetableEntry],
    line_capacities: &[LineCapacity],
    period_length: Time,
    first_departure_event_id: usize,
    default_capacity: FVal,
) -> ExtVehicle {
    let event_by_id = |event_id| {
        events
            .iter()
            .find(|event| event.event_id == event_id)
            .unwrap()
    };
    let time_by_event_id = |event_id| {
        timetable
            .iter()
            .find(|entry| entry.event_id == event_id)
            .unwrap()
            .time as Time
    };

    let first_departure_event = event_by_id(first_departure_event_id);
    let first_departure_time = time_by_event_id(first_departure_event.event_id);
    let first_stop = ExtFirstStop {
        station: ExtStationId(first_departure_event.stop_id as u32),
        departure: first_departure_time,
    };
    let mut last_departure_time = first_departure_time;
    let mut last_departure_event = first_departure_event;
    let mut middle_stops = Vec::new();
    loop {
        let drive_activity = activities
            .iter()
            .find(|activity| {
                activity.from_event == last_departure_event.event_id
                    && activity.activity_type == ActivityType::Drive
            })
            .unwrap();
        let arrival_event = event_by_id(drive_activity.to_event);
        assert!(arrival_event.event_type == EventType::Arrival);

        let earliest_arrival_time = last_departure_time + drive_activity.lower_bound as u32;
        let earliest_arrival_mod_period = earliest_arrival_time % period_length;
        let arrival_time_mod_period = time_by_event_id(arrival_event.event_id);
        let arrival_time = if arrival_time_mod_period < earliest_arrival_mod_period {
            earliest_arrival_time.next_multiple_of(period_length) + arrival_time_mod_period
        } else {
            floor_to_multiple(earliest_arrival_time, period_length) + arrival_time_mod_period
        };
        assert!(arrival_time <= last_departure_time + drive_activity.upper_bound as u32);
        assert!(arrival_time >= last_departure_time);

        let station = ExtStationId(arrival_event.stop_id as u32);

        let wait_activity = activities.iter().find(|activity| {
            activity.from_event == arrival_event.event_id
                && activity.activity_type == ActivityType::Wait
        });
        if let Some(wait_activity) = wait_activity {
            let departure_event = event_by_id(wait_activity.to_event);
            assert!(departure_event.event_type == EventType::Departure);
            let departure_time_mod_period = time_by_event_id(departure_event.event_id);
            let mut departure_time =
                floor_to_multiple(arrival_time, period_length) + departure_time_mod_period;
            if departure_time < arrival_time {
                departure_time += period_length;
            }
            assert!(departure_time >= arrival_time);

            middle_stops.push(ExtMiddleStop {
                station,
                arrival: arrival_time,
                departure: departure_time,
            });
            last_departure_time = departure_time;
            last_departure_event = departure_event;
        } else {
            let last_stop = ExtLastStop {
                station,
                arrival: arrival_time,
            };
            let capacity = line_capacities
                .iter()
                .find(|it| it.line_id == first_departure_event.line_id)
                .map(|it| it.capacity)
                .unwrap_or(default_capacity);
            return ExtVehicle {
                id: VehicleId(0),
                capacity,
                first_stop,
                middle_stops,
                last_stop,
            };
        }
    }
}

pub fn offset_vehicle_times(vehicle: &mut ExtVehicle, offset: Time) {
    vehicle.first_stop.departure += offset;
    for middle_stop in &mut vehicle.middle_stops {
        middle_stop.arrival += offset;
        middle_stop.departure += offset;
    }
    vehicle.last_stop.arrival += offset;
}

pub struct LineInfo {
    pub line_id: usize,
    pub line_direction: LineDirection,
    pub line_freq_repetition: usize,
    pub period_repetition: usize,
}

pub struct LineInfoMap(pub Vec<LineInfo>);
impl LineInfoMap {
    pub fn get(&self, vehicle_id: VehicleId) -> &LineInfo {
        &self.0[vehicle_id.0 as usize]
    }
}

#[derive(Deserialize)]
pub struct DynamicProfileEntry {
    pub time: u64,
    pub demand_share: f64,
}

pub fn parse_dynamic_profile(
    stream: impl std::io::Read,
) -> Result<Box<[DynamicProfileEntry]>, csv::Error> {
    csv::ReaderBuilder::default()
        .from_reader(stream)
        .deserialize()
        .collect()
}

pub fn unroll_timetable(
    events: &[Event],
    activities: &[Activity],
    demands: &[Demand],
    timetable: &[TimetableEntry],
    line_capacities: &[LineCapacity],
    default_vehicle_capacity: FVal,
    config: &Config,
    rolls: usize,
    rolls_without_demand: usize,
    commodity_generation_interval: Time,
    outside_option: Time,
    departure_time_choice: bool,
    dynamic_profile_args: Option<&DynamicProfileArgs>,
) -> (Vec<ExtVehicle>, LineInfoMap, Vec<ExtCommodity>) {
    assert!(2 * rolls_without_demand < rolls);
    let mut vehicles = Vec::new();
    let mut line_infos: Vec<LineInfo> = Vec::new();

    let first_departures = find_first_departures(events, activities);

    let mut id = 0;
    for roll in 0..rolls {
        for dep_event_id in &first_departures {
            let mut vehicle = extract_vehicle(
                events,
                activities,
                timetable,
                line_capacities,
                config.period_length as Time,
                *dep_event_id,
                default_vehicle_capacity,
            );
            let dep_event = events
                .iter()
                .find(|event| event.event_id == *dep_event_id)
                .unwrap();
            vehicle.id = VehicleId(id);
            id += 1;
            offset_vehicle_times(&mut vehicle, (roll * config.period_length) as Time);

            let line_info = LineInfo {
                line_id: dep_event.line_id,
                line_direction: dep_event.line_direction,
                line_freq_repetition: dep_event.line_freq_repetition,
                period_repetition: roll,
            };
            vehicles.push(vehicle);
            line_infos.push(line_info);
        }
    }
    let dynamic_profile = dynamic_profile_args
        .as_ref()
        .map(|it| parse_dynamic_profile(File::open(it.dp_path.clone()).unwrap()).unwrap());

    let get_customer_share = |time: Time| {
        if dynamic_profile.is_none() {
            return 1.0;
        }
        let args = dynamic_profile_args.as_ref().unwrap();
        let dynamic_profile = dynamic_profile.as_ref().unwrap();
        let time_in_h = time as f64 / dynamic_profile_args.as_ref().unwrap().time_units_per_hour
            + args.dp_shift;
        dynamic_profile
            .iter()
            .find(|entry| (entry.time + 1) as f64 > time_in_h)
            .map(|entry| entry.demand_share)
            .unwrap_or(0.0)
    };

    let day_duration = ((rolls - 2 * rolls_without_demand) * config.period_length) as Time;
    let commodity_rolls = day_duration / commodity_generation_interval;
    let time_offset = (rolls_without_demand * config.period_length) as Time;
    let mut commodities = demands
        .iter()
        .cartesian_product(0..commodity_rolls)
        .map(|(demand, roll_index)| {
            let origin = ExtStationId(demand.origin as u32);
            let destination = ExtStationId(demand.destination as u32);
            let time = time_offset + roll_index * commodity_generation_interval;
            let customers = demand.customers * get_customer_share(time);
            ExtCommodity {
                od_pair: ExtODPair {
                    origin,
                    destination,
                },
                demand: customers,
                outside_option,
                cost: if departure_time_choice {
                    ExtCostCharacteristic::DepartureTimeChoice(ExtDepartureTimeChoice {
                        target_arrival_time: time,
                        delay_penalty_factor: 3,
                    })
                } else {
                    ExtCostCharacteristic::FixedDeparture(ExtFixedDeparture {
                        departure_time: time,
                    })
                },
            }
        })
        .collect_vec();

    let new_demand = commodities.iter().map(|it| it.demand).sum::<FVal>();
    let original_demand = demands.iter().map(|it| it.customers).sum::<FVal>();
    let scale_factor = original_demand / new_demand;
    for commodity in &mut commodities {
        commodity.demand *= scale_factor;
    }

    (vehicles, LineInfoMap(line_infos), commodities)
}

#[cfg(test)]
mod tests {
    use crate::timpass::{
        Activity, ActivityType, Config, Demand, Event, EventType, LineDirection, TimetableEntry,
        parse_demands, parse_timetable,
    };

    use super::{parse_activities, parse_config, parse_events};

    #[test]
    fn test_parse_events() {
        let content = r#"# event_id; type; stop_id; line_id; line_direction; line_freq_repetition
1; "departure"; 67; 1; >; 1
2; "arrival"; 53; 1; >; 1"#;

        let events = parse_events(content.as_bytes()).unwrap();
        assert_eq!(
            *events,
            [
                Event {
                    event_id: 1,
                    event_type: EventType::Departure,
                    stop_id: 67,
                    line_id: 1,
                    line_direction: LineDirection::Forward,
                    line_freq_repetition: 1
                },
                Event {
                    event_id: 2,
                    event_type: EventType::Arrival,
                    stop_id: 53,
                    line_id: 1,
                    line_direction: LineDirection::Forward,
                    line_freq_repetition: 1
                }
            ]
        )
    }

    #[test]
    fn test_parse_activities() {
        let content = r#"# activity_index; type; from_event; to_event; lower_bound; upper_bound
        1; "drive"; 1; 2; 4; 4
        2; "wait"; 2; 3; 0; 0"#;
        let activities = parse_activities(content.as_bytes()).unwrap();
        assert_eq!(
            *activities,
            [
                Activity {
                    activity_index: 1,
                    activity_type: ActivityType::Drive,
                    from_event: 1,
                    to_event: 2,
                    lower_bound: 4,
                    upper_bound: 4
                },
                Activity {
                    activity_index: 2,
                    activity_type: ActivityType::Wait,
                    from_event: 2,
                    to_event: 3,
                    lower_bound: 0,
                    upper_bound: 0
                }
            ]
        );
    }

    #[test]
    fn test_parse_demands() {
        let content = r#"# origin; destination; customers
1; 14; 730
1; 30; 733"#;
        let demands = parse_demands(content.as_bytes()).unwrap();
        assert_eq!(
            *demands,
            [
                Demand {
                    origin: 1,
                    destination: 14,
                    customers: 730.0
                },
                Demand {
                    origin: 1,
                    destination: 30,
                    customers: 733.0
                }
            ]
        );
    }

    #[test]
    fn test_parse_timetable() {
        let content = r#"# event_id; time
1; 0
2; 4"#;
        let timetable = parse_timetable(content.as_bytes()).unwrap();
        assert_eq!(
            *timetable,
            [
                TimetableEntry {
                    event_id: 1,
                    time: 0
                },
                TimetableEntry {
                    event_id: 2,
                    time: 4
                }
            ]
        );
    }

    #[test]
    fn test_parse_config() {
        let content = r#"# config_key; value
ptn_name; "S-Bahn Hamburg"
period_length; 10
ean_change_penalty; 5
"#;
        let config = parse_config(content.as_bytes());
        assert_eq!(
            *config.unwrap(),
            Config {
                ptn_name: "\"S-Bahn Hamburg\"".into(),
                period_length: 10,
                ean_change_penalty: 5
            }
        );
    }
}
