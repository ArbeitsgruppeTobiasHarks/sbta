use std::alloc::Layout;
use std::fmt::{Debug, Write};
use std::hash::Hash;
use std::iter::{empty, once};
use std::mem::MaybeUninit;

use itertools::Itertools;
use rand::seq::SliceRandom;
use rand::Rng;
use rayon::slice::ParallelSliceMut;

use crate::col::{map_new, set_new, HashMap, HashSet};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExtStationId(pub u32);
impl Debug for ExtStationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("s#{}", self.0))
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VehicleId(pub u32);
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommodityIdx(pub u32);
impl Debug for CommodityIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("c#{}", self.0))
    }
}

pub type Time = u32;

#[derive(Debug, PartialEq)]
pub enum EdgeType {
    Drive(f64),
    Wait,
    Dwell,
    Board(VehicleId),
    Alight,
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
pub struct ExtFirstStop {
    pub station: ExtStationId,
    pub departure: Time,
}

#[derive(Debug)]
pub struct ExtMiddleStop {
    pub station: ExtStationId,
    pub arrival: Time,
    pub departure: Time,
}

#[derive(Debug)]
pub struct ExtLastStop {
    pub station: ExtStationId,
    pub arrival: Time,
}

#[derive(Debug)]
pub struct ExtVehicle {
    pub id: VehicleId,
    pub capacity: f64,
    pub first_stop: ExtFirstStop,
    pub middle_stops: Vec<ExtMiddleStop>,
    pub last_stop: ExtLastStop,
}

#[derive(Debug)]
pub struct ExtODPair {
    pub origin: ExtStationId,
    pub departure: Time,
    pub destination: ExtStationId,
}

#[derive(Debug, Clone)]
pub struct ODPair {
    pub origin: StationIdx,
    pub departure: Time,
    pub destination: StationIdx,
}

pub type FVal = f64;

pub const EPS: FVal = 1e-9;
pub const EPS_L: FVal = 1e-6;

#[derive(Debug)]
pub struct ExtCommodity {
    pub od_pair: ExtODPair,
    pub demand: FVal,
    pub outside_option: Time,
}

#[derive(Debug, Clone)]
pub struct CommodityPayload {
    pub od_pair: ODPair,
    pub demand: FVal,
    pub outside_latest_arrival: Time,
    pub spawn_node: Option<NodeIdx>,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct DriveNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}

#[allow(dead_code)]
pub struct BoardNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}
#[allow(dead_code)]
pub struct DwellNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}
pub struct AlightNavigate<'a> {
    graph: &'a Graph,
    edge_idx: EdgeIdx,
    edge: &'a EdgePayload,
}
#[allow(dead_code)]
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

    pub fn pre_dwell(&self) -> Option<DwellNavigate<'a>> {
        self.graph
            .node(self.edge.from)
            .incoming
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::Dwell) {
                    Some(DwellNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
    }

    pub fn post_dwell(&self) -> Option<DwellNavigate<'a>> {
        self.graph
            .node(self.edge.to)
            .outgoing
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::Dwell) {
                    Some(DwellNavigate {
                        edge_idx,
                        edge,
                        graph: self.graph,
                    })
                } else {
                    None
                }
            })
    }

    pub fn post_alight(&self) -> AlightNavigate<'a> {
        self.graph
            .node(self.edge.to)
            .outgoing
            .iter()
            .find_map(|&edge_idx| {
                let edge = self.graph.edge(edge_idx);
                if matches!(edge.edge_type, EdgeType::Alight) {
                    Some(AlightNavigate {
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

    #[allow(dead_code)]
    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> DwellNavigate<'a> {
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

    #[allow(dead_code)]
    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> AlightNavigate<'a> {
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> BoardNavigate<'a> {
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

impl<'a> WaitNavigate<'a> {
    #[allow(dead_code)]
    pub fn id(&self) -> EdgeIdx {
        self.edge_idx
    }

    #[allow(dead_code)]
    pub fn edge(&self) -> &'a EdgePayload {
        self.edge
    }
}

pub enum EdgeNavigate<'a> {
    Drive(DriveNavigate<'a>),
    Dwell(DwellNavigate<'a>),
    Board(BoardNavigate<'a>),
    Alight(AlightNavigate<'a>),
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
                graph.add_edge(arrival_node, arrival_wait_node, EdgeType::Alight);

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

                graph.add_edge(arrival_node, cur_departure_node, EdgeType::Dwell);
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
            graph.add_edge(last_arrival_node, wait_node, EdgeType::Alight);
        }

        // Add missing wait edges
        let mut wait_nodes_by_station: Vec<Vec<NodeIdx>> = vec![Vec::new(); station_ids.len()];
        for (node_idx, node) in graph.nodes.iter().enumerate() {
            if let NodeType::Wait(station_id) = node.node_type {
                wait_nodes_by_station[station_id.0 as usize].push(NodeIdx(node_idx as u32));
            }
        }
        for wait_nodes in wait_nodes_by_station.iter_mut() {
            wait_nodes.sort_by_key(|id| graph.node(*id).time);

            wait_nodes.windows(2).for_each(|window| {
                graph.add_edge(window[0], window[1], EdgeType::Wait);
            });
        }

        graph.commodities = ext_commodities
            .iter()
            .map(
                |ExtCommodity {
                     od_pair,
                     demand,
                     outside_option,
                 }| {
                    let origin_idx = station_idx[&od_pair.origin];
                    // The spawn node is the first wait node of the station with time >= spawn time.
                    let spawn_node = match wait_nodes_by_station[origin_idx.0 as usize]
                        .binary_search_by_key(&od_pair.departure, |node_idx| {
                            graph.node(*node_idx).time
                        }) {
                        Ok(idx) => Some(wait_nodes_by_station[origin_idx.0 as usize][idx]),
                        Err(idx) => wait_nodes_by_station[origin_idx.0 as usize]
                            .get(idx)
                            .copied(),
                    };
                    CommodityPayload {
                        demand: *demand,
                        outside_latest_arrival: od_pair.departure + outside_option,
                        spawn_node,
                        od_pair: ODPair {
                            origin: station_idx[&od_pair.origin],
                            departure: od_pair.departure,
                            destination: station_idx[&od_pair.destination],
                        },
                    }
                },
            )
            .collect();

        graph.vehicles = vehicles;
        (graph, station_idx)
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
            if let Some(spawn_node) = commodity.spawn_node {
                if spawn_node.0 >= nodes.len() as u32 {
                    return Err(FromRawError::SpawnNodeOutOfBounds {
                        commodity_idx: CommodityIdx(commodity_idx as u32),
                        node_idx: spawn_node,
                    });
                }
                if !matches!(nodes[spawn_node.0 as usize].node_type, NodeType::Wait(_)) {
                    return Err(FromRawError::SpawnNodeIsNotAWaitNode {
                        commodity_idx: CommodityIdx(commodity_idx as u32),
                        node_idx: spawn_node,
                    });
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
            .find(|(_, edge)| matches!(edge.edge_type, EdgeType::Alight))
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
            EdgeType::Dwell => EdgeNavigate::Dwell(DwellNavigate {
                edge_idx,
                edge,
                graph: self,
            }),
            EdgeType::Alight => EdgeNavigate::Alight(AlightNavigate {
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

    #[allow(dead_code)]
    pub fn num_edges(&self) -> usize {
        self.edges.len()
    }

    pub fn num_stations(&self) -> usize {
        self.num_stations
    }

    #[allow(dead_code)]
    pub fn stations(&self) -> impl Iterator<Item = StationIdx> {
        (0_u32..(self.num_stations as u32)).map(StationIdx)
    }

    pub fn shuffle_commodities(&mut self, rng: &mut impl Rng) {
        let mut old_by_new: Vec<u32> = (0..self.num_commodities() as u32).collect();
        old_by_new.shuffle(rng);

        self.commodities = old_by_new
            .iter()
            .map(|&old| self.commodities[old as usize].clone())
            .collect();
    }
}

pub trait DescribePath {
    fn describe(&self, graph: &Graph) -> String;
    fn cost(&self, graph: &Graph, commodity: &CommodityPayload) -> Time;
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
                " OUT:[ {:}:{:} {:}:{:} ]",
                commodity.od_pair.origin.0,
                commodity.od_pair.departure,
                commodity.od_pair.destination.0,
                commodity.outside_latest_arrival
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
                EdgeType::Dwell => {
                    write!(
                        s,
                        " {:}:{:}-{:}",
                        graph.station(edge.to).0,
                        graph.node(edge.from).time,
                        graph.node(edge.to).time
                    )
                    .unwrap();
                }
                EdgeType::Alight => {
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

    fn cost(&self, graph: &Graph, commodity: &CommodityPayload) -> Time {
        let arrival = match self.edges().last() {
            None => commodity.outside_latest_arrival,
            Some(&it) => graph.node(graph.edge(it).to).time,
        };
        arrival - commodity.od_pair.departure
    }
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
