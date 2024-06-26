use serde::Deserialize;

use itertools::Itertools;

use crate::graph::{
    ExtCommodity, ExtFirstStop, ExtLastStop, ExtMiddleStop, ExtStationId, ExtVehicle, FVal,
    VehicleId,
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

pub fn extract_vehicle(
    events: &[Event],
    activities: &[Activity],
    timetable: &[TimetableEntry],
    period_length: u32,
    first_departure_event_id: usize,
    capacity: FVal,
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
            .time as u32
    };

    let first_departure_event = event_by_id(first_departure_event_id);
    let first_departure_time = time_by_event_id(first_departure_event.event_id);
    let first_stop = ExtFirstStop {
        station: ExtStationId(first_departure_event.stop_id as u32),
        departure: first_departure_time,
    };
    let mut last_departure_time = first_departure_time;
    let mut last_departure_event = first_departure_event;
    let mut time_offset: u32 = 0;
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
        let mut arrival_time = time_by_event_id(arrival_event.event_id) + time_offset;
        if arrival_time < last_departure_time {
            arrival_time += period_length;
            time_offset += period_length;
        }
        assert!(arrival_time >= last_departure_time);

        let station = ExtStationId(arrival_event.stop_id as u32);

        let wait_activity = activities.iter().find(|activity| {
            activity.from_event == arrival_event.event_id
                && activity.activity_type == ActivityType::Wait
        });
        if let Some(wait_activity) = wait_activity {
            let departure_event = event_by_id(wait_activity.to_event);
            assert!(departure_event.event_type == EventType::Departure);
            let mut departure_time = time_by_event_id(departure_event.event_id) + time_offset;
            if departure_time < arrival_time {
                departure_time += period_length;
                time_offset += period_length;
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

pub fn offset_vehicle_times(vehicle: &mut ExtVehicle, offset: u32) {
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

pub fn unroll_timetable(
    events: &[Event],
    activities: &[Activity],
    demands: &[Demand],
    timetable: &[TimetableEntry],
    vehicle_capacity: FVal,
    config: &Config,
    rolls: usize,
    commodity_generation_interval: u32,
    outside_option: u32,
) -> (Vec<ExtVehicle>, LineInfoMap, Vec<ExtCommodity>) {
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
                config.period_length as u32,
                *dep_event_id,
                vehicle_capacity,
            );
            let dep_event = events
                .iter()
                .find(|event| event.event_id == *dep_event_id)
                .unwrap();
            vehicle.id = VehicleId(id);
            id += 1;
            offset_vehicle_times(&mut vehicle, (roll * config.period_length) as u32);

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

    let day_duration = (rolls * config.period_length) as u32;
    let commodity_rolls = day_duration / commodity_generation_interval;
    let commodities = demands
        .iter()
        .cartesian_product(0..commodity_rolls)
        .map(|(demand, roll_index)| {
            let origin = ExtStationId(demand.origin as u32);
            let destination = ExtStationId(demand.destination as u32);
            let customers = demand.customers / (commodity_rolls as f64);
            let departure = roll_index * commodity_generation_interval;
            ExtCommodity {
                od_pair: crate::graph::ExtODPair {
                    origin,
                    departure,
                    destination,
                },
                demand: customers,
                outside_option,
            }
        })
        .collect();

    (vehicles, LineInfoMap(line_infos), commodities)
}
