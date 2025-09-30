use std::alloc::Layout;
use std::cmp::max;
use std::fmt::{Debug, Write};
use std::hash::Hash;
use std::iter::{empty, once};
use std::mem::MaybeUninit;

use itertools::Itertools;
use log::warn;
use rand::Rng;
use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;

use crate::col::{HashMap, HashSet, map_new, set_new};
use crate::primitives::{FVal, Time};
use crate::vehicle::{ExtStationId, ExtVehicle, VehicleId};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct StationIdx(pub u32);
impl Debug for StationIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("_s#{}", self.0))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeIdx(pub u32);
impl Debug for EdgeIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("e#{}", self.0))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeIdx(pub u32);

impl Debug for NodeIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("n#{}", self.0))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommodityIdx(pub u32);
impl Debug for CommodityIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("c#{}", self.0))
    }
}

#[derive(Debug, PartialEq)]
pub enum EdgeType {
    Drive(f64),
    /// Waiting at a node. Also from spawn to first node.
    Wait,
    StayOn,
    Board(VehicleId),
    GetOff,
}

#[derive(Debug)]
pub struct EdgePayload {
    pub from: NodeIdx,
    pub to: NodeIdx,
    pub edge_type: EdgeType,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NodeType {
    Wait(StationIdx),
    Depart(),
    Arrive(),
}

#[derive(Debug)]
pub struct NodePayload {
    pub incoming: Vec<EdgeIdx>,
    pub outgoing: Vec<EdgeIdx>,
    pub time: Time,
    pub node_type: NodeType,
}

#[derive(Debug)]
pub struct NodeRawPayload {
    pub time: Time,
    pub node_type: NodeType,
}

#[derive(PartialEq, Eq, Hash)]
pub struct Path {
    commodity_idx: CommodityIdx,
    edges: [EdgeIdx],
}

impl Path {
    pub fn commodity_idx(&self) -> CommodityIdx {
        self.commodity_idx
    }

    pub fn edges(&self) -> &[EdgeIdx] {
        &self.edges
    }

    pub fn is_outside(&self) -> bool {
        self.edges.is_empty()
    }

    pub fn arrival_time(&self, graph: &Graph, fixed: &FixedDeparture) -> Time {
        match self.edges().last() {
            None => fixed.outside_latest_arrival,
            Some(&edge_idx) => {
                let edge: &EdgePayload = graph.edge(edge_idx);
                let to_node = graph.node(edge.to);
                to_node.time
            }
        }
    }

    pub fn travel_time(
        &self,
        graph: &Graph,
        commodity: &CommodityPayload,
        fixed: &FixedDeparture,
    ) -> Time {
        match self.edges().last() {
            None => commodity.outside_option,
            Some(&edge_idx) => {
                let edge: &EdgePayload = graph.edge(edge_idx);
                let to_node = graph.node(edge.to);
                to_node.time - fixed.departure
            }
        }
    }

    pub fn cost(&self, commodity: &CommodityPayload, graph: &Graph) -> Time {
        match &commodity.cost_characteristic {
            CostCharacteristic::FixedDeparture(fixed) => self.travel_time(graph, commodity, fixed),
            CostCharacteristic::DepartureTimeChoice(choice) => {
                self.cost_choice(commodity, choice, graph)
            }
        }
    }

    pub fn cost_choice(
        &self,
        commodity: &CommodityPayload,
        choice: &DepartureTimeChoice,
        graph: &Graph,
    ) -> Time {
        match self.edges().last() {
            None => commodity.outside_option,
            Some(&last_edge_idx) => {
                let last_edge = graph.edge(last_edge_idx);
                let last_node = graph.node(last_edge.to);
                let arrival = max(choice.target_arrival_time, last_node.time);

                let first_edge_idx = self.edges().first().unwrap();
                let first_node_idx = graph.edge(*first_edge_idx).from;
                let first_node = graph.node(first_node_idx);
                let departure = first_node.time;

                let travel_time = arrival - departure;
                let delay = arrival - choice.target_arrival_time;
                travel_time + choice.delay_penalty_factor * delay
            }
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct PathBox {
    payload: Box<Path>,
}

impl Clone for PathBox {
    fn clone(&self) -> Self {
        Self::new(
            self.payload.commodity_idx,
            self.payload.edges.iter().copied(),
        )
    }
}

impl PathBox {
    pub fn payload(&self) -> &Path {
        &self.payload
    }

    pub fn outside(commodity_idx: CommodityIdx) -> Self {
        PathBox::new(commodity_idx, empty())
    }

    pub fn new(commodity_idx: CommodityIdx, edges: impl ExactSizeIterator<Item = EdgeIdx>) -> Self {
        let layout = Layout::new::<CommodityIdx>()
            .extend(Layout::new::<EdgeIdx>().repeat(edges.len()).unwrap().0)
            .unwrap()
            .0
            .pad_to_align();

        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout)
        } else {
            let ptr = std::ptr::from_raw_parts_mut::<Path>(ptr as *mut (), edges.len());
            // safety: ptr points to allocated (but not initialized) Dst
            unsafe {
                let commodity_idx_ptr = std::ptr::addr_of_mut!((*ptr).commodity_idx);
                commodity_idx_ptr.write(commodity_idx);

                let edges_ptr: *mut [EdgeIdx] = std::ptr::addr_of_mut!((*ptr).edges);
                // cast tail to MaybeUninit slice for convenience
                let tail_uninit = &mut *(edges_ptr as *mut [MaybeUninit<EdgeIdx>]);
                for (edge_in_box, edge) in tail_uninit.iter_mut().zip(edges) {
                    edge_in_box.write(edge);
                }
            }
            // safety: ptr points to an allocated Dst, and all fields have been initialized
            let payload = unsafe { Box::from_raw(ptr) };
            Self { payload }
        }
    }
}

#[derive(Debug)]
pub struct FirstStop {
    pub station: StationIdx,
    pub departure: Time,
}

#[derive(Debug)]
pub struct LastStop {
    pub station: StationIdx,
    pub arrival: Time,
}

#[derive(Debug)]
pub struct Graph {
    edges: Vec<EdgePayload>,
    nodes: Vec<NodePayload>,
    vehicles: Vec<ExtVehicle>,
    commodities: Vec<CommodityPayload>,
    station_by_node: Vec<StationIdx>,
    num_stations: usize,
}

#[derive(Debug)]
pub struct ExtODPair {
    pub origin: ExtStationId,
    pub destination: ExtStationId,
}

#[derive(Debug, Clone)]
pub struct ODPair {
    pub origin: StationIdx,
    pub destination: StationIdx,
}

#[derive(Debug)]
pub struct ExtCommodity {
    pub od_pair: ExtODPair,
    pub demand: FVal,
    pub cost: ExtCostCharacteristic,
    pub outside_option: Time,
}

#[derive(Debug)]
pub struct ExtDepartureTimeChoice {
    pub target_arrival_time: Time,
    pub delay_penalty_factor: Time,
}

#[derive(Debug)]
pub struct ExtFixedDeparture {
    pub departure_time: Time,
}

#[derive(Debug)]
pub enum ExtCostCharacteristic {
    FixedDeparture(ExtFixedDeparture),
    DepartureTimeChoice(ExtDepartureTimeChoice),
}

#[derive(Debug, Clone)]
pub struct CommodityPayload {
    pub od_pair: ODPair,
    pub demand: FVal,
    pub outside_option: Time,
    pub cost_characteristic: CostCharacteristic,
}

#[derive(Debug, Clone)]
pub struct FixedDeparture {
    pub departure: Time,
    pub outside_latest_arrival: Time,
    pub spawn_node: Option<NodeIdx>,
}

#[derive(Debug, Clone)]
pub struct DepartureTimeChoice {
    pub target_arrival_time: Time,
    pub delay_penalty_factor: Time,
    pub spawn_node_range: Option<(NodeIdx, NodeIdx)>,
}

#[derive(Debug, Clone)]
pub enum CostCharacteristic {
    FixedDeparture(FixedDeparture),
    DepartureTimeChoice(DepartureTimeChoice),
}

#[derive(Clone)]
pub struct DriveNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}
pub struct BoardNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}
pub struct StayOnNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}
pub struct GetOffNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}
pub struct WaitNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}

impl<'a> DriveNavigate<'a> {
    pub fn pre_boarding(&self) -> BoardNavigate<'a> {
        self.graph
            .node(self.edge.from)
            .incoming
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::Board(_)) {
                    Some(BoardNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
            .unwrap()
    }

    pub fn pre_stay_on(&self) -> Option<StayOnNavigate<'a>> {
        self.graph
            .node(self.edge.from)
            .incoming
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::StayOn) {
                    Some(StayOnNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
    }

    pub fn post_stay_on(&self) -> Option<StayOnNavigate<'a>> {
        self.graph
            .node(self.edge.to)
            .outgoing
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::StayOn) {
                    Some(StayOnNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
    }

    pub fn post_get_off(&self) -> GetOffNavigate<'a> {
        self.graph
            .node(self.edge.to)
            .outgoing
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::GetOff) {
                    Some(GetOffNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
            .unwrap()
    }

    pub fn capacity(&self) -> f64 {
        match self.edge.edge_type {
            EdgeType::Drive(capacity) => capacity,
            _ => panic!("Not a drive edge"),
        }
    }

    pub fn id(&self) -> EdgeIdx {
        self.edge_idx
    }

    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> StayOnNavigate<'a> {
    pub fn pre_drive(&self) -> DriveNavigate<'a> {
        self.graph
            .node(self.edge.from)
            .incoming
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::Drive(_)) {
                    Some(DriveNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
            .unwrap()
    }

    pub fn post_drive(&self) -> DriveNavigate<'a> {
        self.graph
            .node(self.edge.to)
            .outgoing
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::Drive(_)) {
                    Some(DriveNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
            .unwrap()
    }

    pub fn id(&self) -> EdgeIdx {
        self.edge_idx
    }

    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> GetOffNavigate<'a> {
    pub fn pre_drive(&self) -> DriveNavigate<'a> {
        let drive_edge_id = self.graph.node(self.edge.from).incoming[0];

        DriveNavigate {
            edge_idx: drive_edge_id,
            edge: self.graph.edge(drive_edge_id),
            graph: self.graph,
        }
    }

    pub fn id(&self) -> EdgeIdx {
        self.edge_idx
    }

    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> BoardNavigate<'a> {
    pub fn post_drive(&self) -> DriveNavigate<'a> {
        let drive_edge_id = self.graph.node(self.edge.to).outgoing[0];
        DriveNavigate {
            graph: self.graph,
            edge_idx: drive_edge_id,
            edge: self.graph.edge(drive_edge_id),
        }
    }

    pub fn id(&self) -> EdgeIdx {
        self.edge_idx
    }

    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> WaitNavigate<'a> {
    pub fn id(&self) -> EdgeIdx {
        self.edge_idx
    }

    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

pub enum EdgeNavigate<'a> {
    Drive(DriveNavigate<'a>),
    StayOn(StayOnNavigate<'a>),
    Board(BoardNavigate<'a>),
    GetOff(GetOffNavigate<'a>),
    Wait(WaitNavigate<'a>),
}

#[derive(Debug)]
pub enum FromRawError {
    NodeFromOutOfRange {
        edge_idx: u32,
        node_from: u32,
    },
    NodeToOutOfRange {
        edge_idx: u32,
        node_to: u32,
    },
    MissingStationIdx {
        idx: u32,
    },
    SpawnNodeOutOfBounds {
        commodity_idx: CommodityIdx,
        node_idx: NodeIdx,
    },
    SpawnNodeIsNotAWaitNode {
        commodity_idx: CommodityIdx,
        node_idx: NodeIdx,
    },
    SpawnNodeIsAtWrongStation,
    TimeChoiceRangeIsInvalid,
}

fn get_wait_nodes_by_station(nodes: &Vec<NodePayload>, num_stations: usize) -> Vec<Vec<NodeIdx>> {
    let mut wait_nodes_by_station: Vec<Vec<NodeIdx>> = vec![Vec::new(); num_stations];
    for (node_idx, node) in nodes.iter().enumerate() {
        if let NodeType::Wait(station_id) = node.node_type {
            wait_nodes_by_station[station_id.0 as usize].push(NodeIdx(node_idx as u32));
        }
    }
    for wait_nodes in wait_nodes_by_station.iter_mut() {
        wait_nodes.sort_by_key(|id| nodes[id.0 as usize].time);
    }
    wait_nodes_by_station
}

impl Graph {
    pub fn create(
        vehicles: Vec<ExtVehicle>,
        ext_commodities: &[ExtCommodity],
    ) -> (Self, HashMap<ExtStationId, StationIdx>) {
        let mut station_ids: HashSet<ExtStationId> = vehicles
            .iter()
            .flat_map(|it| {
                once(it.first_stop.station)
                    .chain(it.middle_stops.iter().map(|it| it.station))
                    .chain(once(it.last_stop.station))
            })
            .chain(
                ext_commodities
                    .iter()
                    .flat_map(|it| once(it.od_pair.origin).chain(once(it.od_pair.destination))),
            )
            .collect();
        let mut station_ids = station_ids.drain().collect::<Vec<_>>();
        station_ids.par_sort_by_key(|it| it.0);

        let station_idx = station_ids
            .iter()
            .enumerate()
            .map(|(a, &b)| (b, StationIdx(a as u32)))
            .collect::<HashMap<ExtStationId, StationIdx>>();

        let mut wait_node_by_station_time: HashMap<(StationIdx, Time), NodeIdx> = map_new();
        let mut graph = Graph {
            edges: vec![],
            nodes: vec![],
            vehicles: vec![],
            commodities: vec![],
            station_by_node: vec![],
            num_stations: station_idx.len(),
        };

        for vehicle in vehicles.iter() {
            let first_wait_node = graph.add_node(
                &mut wait_node_by_station_time,
                vehicle.first_stop.departure,
                NodeType::Wait(station_idx[&vehicle.first_stop.station]),
                station_idx[&vehicle.first_stop.station],
            );
            let first_departure_node = graph.add_node(
                &mut wait_node_by_station_time,
                vehicle.first_stop.departure,
                NodeType::Depart(),
                station_idx[&vehicle.first_stop.station],
            );
            graph.add_edge(
                first_wait_node,
                first_departure_node,
                EdgeType::Board(vehicle.id),
            );

            let mut cur_departure_node = first_departure_node;
            for middle_stop in &vehicle.middle_stops {
                let arrival_node = graph.add_node(
                    &mut wait_node_by_station_time,
                    middle_stop.arrival,
                    NodeType::Arrive(),
                    station_idx[&middle_stop.station],
                );
                graph.add_edge(
                    cur_departure_node,
                    arrival_node,
                    EdgeType::Drive(vehicle.capacity),
                );
                let arrival_wait_node = graph.add_node(
                    &mut wait_node_by_station_time,
                    middle_stop.arrival,
                    NodeType::Wait(station_idx[&middle_stop.station]),
                    station_idx[&middle_stop.station],
                );
                graph.add_edge(arrival_node, arrival_wait_node, EdgeType::GetOff);

                let departure_wait_node = graph.add_node(
                    &mut wait_node_by_station_time,
                    middle_stop.departure,
                    NodeType::Wait(station_idx[&middle_stop.station]),
                    station_idx[&middle_stop.station],
                );

                cur_departure_node = graph.add_node(
                    &mut wait_node_by_station_time,
                    middle_stop.departure,
                    NodeType::Depart(),
                    station_idx[&middle_stop.station],
                );

                graph.add_edge(arrival_node, cur_departure_node, EdgeType::StayOn);
                graph.add_edge(
                    departure_wait_node,
                    cur_departure_node,
                    EdgeType::Board(vehicle.id),
                );
            }
            let last_arrival_node = graph.add_node(
                &mut wait_node_by_station_time,
                vehicle.last_stop.arrival,
                NodeType::Arrive(),
                station_idx[&vehicle.last_stop.station],
            );
            graph.add_edge(
                cur_departure_node,
                last_arrival_node,
                EdgeType::Drive(vehicle.capacity),
            );
            let wait_node = graph.add_node(
                &mut wait_node_by_station_time,
                vehicle.last_stop.arrival,
                NodeType::Wait(station_idx[&vehicle.last_stop.station]),
                station_idx[&vehicle.last_stop.station],
            );
            graph.add_edge(last_arrival_node, wait_node, EdgeType::GetOff);
        }

        // Add missing wait edges
        let wait_nodes_by_station: Vec<Vec<NodeIdx>> =
            get_wait_nodes_by_station(&graph.nodes, graph.num_stations);
        for wait_nodes in wait_nodes_by_station {
            wait_nodes.windows(2).for_each(|window| {
                graph.add_edge(window[0], window[1], EdgeType::Wait);
            });
        }

        graph.vehicles = vehicles;

        graph.set_commodities(ext_commodities, &station_idx);
        (graph, station_idx)
    }

    pub fn set_commodities(
        &mut self,
        ext_commodities: &[ExtCommodity],
        station_idx: &HashMap<ExtStationId, StationIdx>,
    ) {
        let wait_nodes_by_station: Vec<Vec<NodeIdx>> =
            get_wait_nodes_by_station(&self.nodes, self.num_stations);

        let first_wait_node_not_earlier_than =
            |station: StationIdx, min_time: Time| -> Option<NodeIdx> {
                match wait_nodes_by_station[station.0 as usize]
                    .binary_search_by_key(&min_time, |node_idx| self.node(*node_idx).time)
                {
                    Ok(idx) => Some(wait_nodes_by_station[station.0 as usize][idx]),
                    Err(idx) => wait_nodes_by_station[station.0 as usize].get(idx).copied(),
                }
            };

        let last_wait_node_not_later_than =
            |station: StationIdx, max_time: Time| -> Option<NodeIdx> {
                match wait_nodes_by_station[station.0 as usize]
                    .binary_search_by_key(&max_time, |node_idx| self.node(*node_idx).time)
                {
                    Ok(idx) => Some(wait_nodes_by_station[station.0 as usize][idx]),
                    Err(0) => None,
                    Err(idx) => wait_nodes_by_station[station.0 as usize]
                        .get(idx - 1)
                        .copied(),
                }
            };

        self.commodities = ext_commodities
            .iter()
            .map(
                |ExtCommodity {
                     od_pair,
                     demand,
                     outside_option,
                     cost,
                 }| {
                    let origin_idx = station_idx[&od_pair.origin];
                    let cost = match cost {
                        ExtCostCharacteristic::FixedDeparture(fixed) => {
                            // The spawn node is the first wait node of the station with time >= spawn time.
                            let spawn_node =
                                first_wait_node_not_earlier_than(origin_idx, fixed.departure_time);
                            CostCharacteristic::FixedDeparture(FixedDeparture {
                                outside_latest_arrival: fixed.departure_time + outside_option,
                                departure: fixed.departure_time,
                                spawn_node: spawn_node,
                            })
                        }
                        ExtCostCharacteristic::DepartureTimeChoice(choice) => {
                            let first_spawn_node_min_time =
                                choice.target_arrival_time.saturating_sub(*outside_option);
                            let last_spawn_node_max_time = choice.target_arrival_time
                                + outside_option.div_ceil(1 + choice.delay_penalty_factor);
                            debug_assert!(first_spawn_node_min_time <= last_spawn_node_max_time);

                            let spawn_node_range = {
                                let first_node = first_wait_node_not_earlier_than(
                                    origin_idx,
                                    first_spawn_node_min_time,
                                );
                                let last_node = last_wait_node_not_later_than(
                                    origin_idx,
                                    last_spawn_node_max_time,
                                );
                                match (first_node, last_node) {
                                    (Some(first_node), Some(last_node)) => Some((first_node, last_node)),
                                    _ => {
                                        warn!("No valid spawn node range for commodity {:?} with time range {:}-{:} ({:?}, {:?})", od_pair, first_spawn_node_min_time, last_spawn_node_max_time, first_node, last_node);
                                        None
                                    },
                                }
                            };

                            CostCharacteristic::DepartureTimeChoice(DepartureTimeChoice {
                                spawn_node_range,
                                target_arrival_time: choice.target_arrival_time,
                                delay_penalty_factor: choice.delay_penalty_factor,
                            })
                        }
                    };
                    CommodityPayload {
                        od_pair: ODPair {
                            origin: station_idx[&od_pair.origin],
                            destination: station_idx[&od_pair.destination],
                        },
                        demand: *demand,
                        outside_option: *outside_option,
                        cost_characteristic: cost,
                    }
                },
            )
            .collect();
    }

    pub fn try_from_raw(
        nodes: Vec<NodeRawPayload>,
        edges: Vec<EdgePayload>,
        commodities: Vec<CommodityPayload>,
        station_by_node: Vec<StationIdx>,
    ) -> Result<Self, FromRawError> {
        let mut nodes = nodes
            .into_iter()
            .map(|it| NodePayload {
                incoming: Vec::new(),
                outgoing: Vec::new(),
                time: it.time,
                node_type: it.node_type,
            })
            .collect_vec();
        for (edge_idx, edge) in edges.iter().enumerate() {
            nodes
                .get_mut(edge.from.0 as usize)
                .ok_or(FromRawError::NodeFromOutOfRange {
                    edge_idx: edge_idx as u32,
                    node_from: edge.from.0,
                })?
                .outgoing
                .push(EdgeIdx(edge_idx as u32));
            nodes
                .get_mut(edge.to.0 as usize)
                .ok_or(FromRawError::NodeToOutOfRange {
                    edge_idx: edge_idx as u32,
                    node_to: edge.to.0,
                })?
                .incoming
                .push(EdgeIdx(edge_idx as u32));
        }
        let nodes = nodes;

        // Check if stations are indexed.
        let stations = nodes
            .iter()
            .filter_map(|it| match it.node_type {
                NodeType::Wait(station_idx) => Some(station_idx),
                _ => None,
            })
            .collect::<HashSet<StationIdx>>();
        let stations: Vec<_> = stations.into_iter().sorted_by_key(|it| it.0).collect();
        let num_stations = stations.len();
        for (idx, station_idx) in stations.iter().enumerate() {
            if idx as u32 != station_idx.0 {
                return Err(FromRawError::MissingStationIdx { idx: idx as u32 });
            }
        }
        drop(stations);

        // Check if spawn nodes of commodities are valid wait nodes.
        for (commodity_idx, commodity) in commodities.iter().enumerate() {
            match &commodity.cost_characteristic {
                CostCharacteristic::FixedDeparture(fixed) => {
                    if let Some(spawn_node) = fixed.spawn_node {
                        if spawn_node.0 >= nodes.len() as u32 {
                            return Err(FromRawError::SpawnNodeOutOfBounds {
                                commodity_idx: CommodityIdx(commodity_idx as u32),
                                node_idx: spawn_node,
                            });
                        }
                        let node = &nodes[spawn_node.0 as usize];
                        let NodeType::Wait(station) = node.node_type else {
                            return Err(FromRawError::SpawnNodeIsNotAWaitNode {
                                commodity_idx: CommodityIdx(commodity_idx as u32),
                                node_idx: spawn_node,
                            });
                        };
                        if station != commodity.od_pair.origin {
                            return Err(FromRawError::SpawnNodeIsAtWrongStation);
                        }
                    }
                }
                CostCharacteristic::DepartureTimeChoice(choice) => {
                    if let Some(range) = &choice.spawn_node_range {
                        if range.0.0 as usize >= nodes.len() {
                            return Err(FromRawError::SpawnNodeOutOfBounds {
                                commodity_idx: CommodityIdx(commodity_idx as u32),
                                node_idx: range.0,
                            });
                        }
                        if range.1.0 as usize >= nodes.len() {
                            return Err(FromRawError::SpawnNodeOutOfBounds {
                                commodity_idx: CommodityIdx(commodity_idx as u32),
                                node_idx: range.1,
                            });
                        }
                        let node_from = &nodes[range.0.0 as usize];
                        let node_to = &nodes[range.1.0 as usize];
                        let NodeType::Wait(station_from) = node_from.node_type else {
                            return Err(FromRawError::SpawnNodeIsNotAWaitNode {
                                commodity_idx: CommodityIdx(commodity_idx as u32),
                                node_idx: range.0,
                            });
                        };
                        if station_from != commodity.od_pair.origin {
                            return Err(FromRawError::SpawnNodeIsAtWrongStation);
                        }
                        let NodeType::Wait(station_to) = node_to.node_type else {
                            return Err(FromRawError::SpawnNodeIsNotAWaitNode {
                                commodity_idx: CommodityIdx(commodity_idx as u32),
                                node_idx: range.1,
                            });
                        };
                        if station_to != commodity.od_pair.origin {
                            return Err(FromRawError::SpawnNodeIsAtWrongStation);
                        }
                        if node_from.time > node_from.time {
                            return Err(FromRawError::TimeChoiceRangeIsInvalid);
                        }
                    }
                }
            }
        }

        assert_eq!(station_by_node.len(), nodes.len());

        // TODO: Verify, if the graph is actually valid

        // TODO: Regenerate? the vehicles

        Ok(Self {
            edges,
            nodes,
            vehicles: Vec::new(),
            commodities,
            num_stations,
            station_by_node,
        })
    }

    pub fn nodes(&self) -> impl Iterator<Item = (NodeIdx, &NodePayload)> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(i, n)| (NodeIdx(i as u32), n))
    }

    pub fn edges(&self) -> impl Iterator<Item = (EdgeIdx, &EdgePayload)> {
        self.edges
            .iter()
            .enumerate()
            .map(|(i, e)| (EdgeIdx(i as u32), e))
    }

    pub fn vehicles(&self) -> impl Iterator<Item = &ExtVehicle> {
        self.vehicles.iter()
    }

    pub fn commodity(&self, commodity_idx: CommodityIdx) -> &CommodityPayload {
        &self.commodities[commodity_idx.0 as usize]
    }

    pub fn commodities(&self) -> impl Iterator<Item = (CommodityIdx, &CommodityPayload)> {
        self.commodities
            .iter()
            .enumerate()
            .map(|(i, c)| (CommodityIdx(i as u32), c))
    }

    pub fn num_commodities(&self) -> usize {
        self.commodities.len()
    }

    pub fn outgoing_with(
        &self,
        node_id: NodeIdx,
        predicate: impl Fn(EdgeIdx, &EdgePayload) -> bool,
    ) -> Option<(EdgeIdx, &EdgePayload)> {
        self.node(node_id).outgoing.iter().find_map(|&edge_idx| {
            let edge = self.edge(edge_idx);
            match predicate(edge_idx, edge) {
                true => Some((edge_idx, edge)),
                false => None,
            }
        })
    }

    /// Adds a node to the graph unless it is a wait node which already exists for the specified station and time.
    fn add_node(
        &mut self,
        wait_node_by_station_time: &mut HashMap<(StationIdx, Time), NodeIdx>,
        time: Time,
        node_type: NodeType,
        station: StationIdx,
    ) -> NodeIdx {
        let mut new_node = || {
            let id = NodeIdx(self.nodes.len().try_into().unwrap());
            self.nodes.push(NodePayload {
                incoming: vec![],
                outgoing: vec![],
                time,
                node_type,
            });
            self.station_by_node.push(station);
            id
        };

        let node_id = match node_type {
            NodeType::Wait(station_id) => *wait_node_by_station_time
                .entry((station_id, time))
                .or_insert_with(new_node),
            _ => new_node(),
        };
        node_id
    }

    pub fn node(&self, node_id: NodeIdx) -> &NodePayload {
        &self.nodes[node_id.0 as usize]
    }

    pub fn edge(&self, edge_idx: EdgeIdx) -> &EdgePayload {
        &self.edges[edge_idx.0 as usize]
    }

    /// Returns the boarding edge, if edge is a drive edge, otherwise None.
    pub fn boarding_edge(&self, edge_idx: EdgeIdx) -> Option<(EdgeIdx, &EdgePayload)> {
        self.node(self.edge(edge_idx).from)
            .incoming
            .iter()
            .map(|&edge_idx| (edge_idx, self.edge(edge_idx)))
            .find(|(_, edge)| matches!(edge.edge_type, EdgeType::Board(_)))
    }

    /// Returns the offboarding edge, if edge is a drive edge, otherwise None.
    pub fn offboarding_edge(&self, edge_idx: EdgeIdx) -> Option<(EdgeIdx, &EdgePayload)> {
        self.node(self.edge(edge_idx).to)
            .outgoing
            .iter()
            .map(|&edge_idx| (edge_idx, self.edge(edge_idx)))
            .find(|(_, edge)| matches!(edge.edge_type, EdgeType::GetOff))
    }

    fn node_mut(&mut self, node_id: NodeIdx) -> &mut NodePayload {
        &mut self.nodes[node_id.0 as usize]
    }

    pub fn nav_edge(&self, edge_idx: EdgeIdx) -> EdgeNavigate {
        let edge = self.edge(edge_idx);
        match edge.edge_type {
            EdgeType::Board(_) => EdgeNavigate::Board(BoardNavigate {
                edge_idx,
                edge,
                graph: self,
            }),
            EdgeType::Drive(_) => EdgeNavigate::Drive(DriveNavigate {
                edge_idx,
                edge,
                graph: self,
            }),
            EdgeType::StayOn => EdgeNavigate::StayOn(StayOnNavigate {
                edge_idx,
                edge,
                graph: self,
            }),
            EdgeType::GetOff => EdgeNavigate::GetOff(GetOffNavigate {
                edge_idx,
                edge,
                graph: self,
            }),
            EdgeType::Wait => EdgeNavigate::Wait(WaitNavigate {
                edge_idx,
                edge,
                graph: self,
            }),
        }
    }

    fn add_edge(&mut self, from: NodeIdx, to: NodeIdx, edge_type: EdgeType) -> EdgeIdx {
        let edge_idx = EdgeIdx(self.edges.len().try_into().unwrap());
        let payload = EdgePayload {
            from,
            to,
            edge_type,
        };
        self.edges.push(payload);
        self.node_mut(from).outgoing.push(edge_idx);
        self.node_mut(to).incoming.push(edge_idx);
        edge_idx
    }

    pub fn outside_path(&self, commodity_idx: CommodityIdx) -> PathBox {
        PathBox::new(commodity_idx, empty())
    }

    pub fn station(&self, node_idx: NodeIdx) -> StationIdx {
        self.station_by_node[node_idx.0 as usize]
    }

    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    pub fn num_edges(&self) -> usize {
        self.edges.len()
    }

    pub fn num_stations(&self) -> usize {
        self.num_stations
    }

    pub fn stations(&self) -> impl Iterator<Item = StationIdx> + use<> {
        (0_u32..(self.num_stations as u32)).map(StationIdx)
    }

    pub fn path_from_description(&self, path_description: &PathDescription) -> Option<PathBox> {
        let mut edges: Vec<EdgeIdx> = Vec::new();
        let commodity = self.commodity(path_description.commodity_idx);

        let mut cur_node = match &commodity.cost_characteristic {
            CostCharacteristic::FixedDeparture(fixed) => fixed.spawn_node?,
            CostCharacteristic::DepartureTimeChoice(_) => todo!(),
        };

        for ride in path_description.rides.iter() {
            if self.station(cur_node) != ride.first_stop.station {
                return None;
            }
            // Wait until boarding.
            while self.node(cur_node).time < ride.first_stop.departure {
                let cur_edge =
                    self.outgoing_with(cur_node, |_, e| e.edge_type == EdgeType::Wait)?;
                cur_node = cur_edge.1.to;
                edges.push(cur_edge.0);
            }

            // Board the ride
            let cur_edge = self.outgoing_with(cur_node, |_, e| {
                e.edge_type == EdgeType::Board(ride.vehicle)
            })?;
            cur_node = cur_edge.1.to;
            edges.push(cur_edge.0);

            // Drive first edge
            let cur_edge =
                self.outgoing_with(cur_node, |_, e| matches!(e.edge_type, EdgeType::Drive(_)))?;
            cur_node = cur_edge.1.to;
            edges.push(cur_edge.0);

            // Drive until arrival
            while self.node(cur_node).time < ride.last_stop.arrival {
                // Stay on ride
                let cur_edge =
                    self.outgoing_with(cur_node, |_, e| matches!(e.edge_type, EdgeType::StayOn))?;
                cur_node = cur_edge.1.to;
                edges.push(cur_edge.0);

                // Drive
                let cur_edge =
                    self.outgoing_with(cur_node, |_, e| matches!(e.edge_type, EdgeType::Drive(_)))?;
                cur_node = cur_edge.1.to;
                edges.push(cur_edge.0);
            }

            // Get off
            if self.station(cur_node) != ride.last_stop.station {
                return None;
            }
            let cur_edge =
                self.outgoing_with(cur_node, |_, e| matches!(e.edge_type, EdgeType::GetOff))?;
            cur_node = cur_edge.1.to;
            edges.push(cur_edge.0);
        }

        Some(PathBox::new(
            path_description.commodity_idx,
            edges.into_iter(),
        ))
    }

    pub fn shuffle_commodities(&mut self, rng: &mut impl Rng) {
        let mut old_by_new: Vec<u32> = (0..self.num_commodities() as u32).collect();
        old_by_new.shuffle(rng);

        self.commodities = old_by_new
            .iter()
            .map(|&old| self.commodities[old as usize].clone())
            .collect();
    }

    pub fn total_demand(&self) -> FVal {
        self.commodities.par_iter().map(|it| it.demand).sum()
    }
}

pub trait DescribePath {
    fn describe(&self, graph: &Graph) -> String;
}

impl DescribePath for Path {
    fn describe(&self, graph: &Graph) -> String {
        let mut s = String::new();
        let commodity_idx = self.commodity_idx();
        let commodity = graph.commodity(commodity_idx);
        write!(s, "{:?}", commodity_idx).unwrap();
        if self.edges().is_empty() {
            write!(
                s,
                " OUT:[ {:} {:} ]",
                commodity.od_pair.origin.0, commodity.od_pair.destination.0,
            )
            .unwrap();
        }
        for &edge_idx in self.edges() {
            let edge = graph.edge(edge_idx);
            match edge.edge_type {
                EdgeType::Board(vehicle_id) => {
                    write!(
                        s,
                        " {}:[ {:}:{:}",
                        vehicle_id.0,
                        graph.station(edge.from).0,
                        graph.node(edge.to).time
                    )
                    .unwrap();
                }
                EdgeType::Drive(_) => {}
                EdgeType::StayOn => {
                    write!(
                        s,
                        " {:}:{:}-{:}",
                        graph.station(edge.to).0,
                        graph.node(edge.from).time,
                        graph.node(edge.to).time
                    )
                    .unwrap();
                }
                EdgeType::GetOff => {
                    write!(
                        s,
                        " {:}:{:} ]",
                        graph.station(edge.to).0,
                        graph.node(edge.to).time
                    )
                    .unwrap();
                }
                EdgeType::Wait => {}
            }
        }

        s
    }
}

pub struct Ride {
    pub vehicle: VehicleId,
    pub first_stop: FirstStop,
    pub last_stop: LastStop,
}
pub struct PathDescription {
    pub commodity_idx: CommodityIdx,
    pub departure_time: Time,
    pub rides: Vec<Ride>,
}

pub fn reachable_nodes(
    graph: &Graph,
    source: NodeIdx,
    mut edge_okay: impl FnMut(EdgeIdx, &EdgePayload) -> bool,
) -> HashSet<NodeIdx> {
    let mut reachable: HashSet<NodeIdx> = set_new();
    let mut next = vec![source];
    while let Some(node_id) = next.pop() {
        if reachable.insert(node_id) {
            for &edge_idx in graph.node(node_id).outgoing.iter() {
                let edge = graph.edge(edge_idx);
                if edge_okay(edge_idx, edge) {
                    next.push(edge.to);
                }
            }
        }
    }
    reachable
}
