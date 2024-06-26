use itertools::Itertools;
use log::info;
use num_derive::FromPrimitive;
use sqlite::OpenFlags;

use num_traits::FromPrimitive;

use crate::{
    graph::{
        CommodityPayload, EdgePayload, EdgeType, FromRawError, Graph, NodeIdx, NodeRawPayload,
        NodeType, ODPair, StationIdx, VehicleId,
    },
    timpass::LineInfoMap,
};

#[derive(FromPrimitive)]
enum DBNodeType {
    Wait = 0,
    Depart = 1,
    Arrive = 2,
}

#[derive(FromPrimitive)]
enum DBEdgeType {
    Board = 0,
    Wait = 1,
    Drive = 2,
    Alight = 3,
    Dwell = 5,
}

pub fn export_graph(graph: &Graph, line_info_map: &LineInfoMap, out_filename: &str) {
    let connection = sqlite::Connection::open_with_flags(
        out_filename,
        OpenFlags::default()
            .with_create()
            .with_no_mutex()
            .with_read_write(),
    )
    .unwrap();
    connection.execute("BEGIN TRANSACTION;").unwrap();

    connection
        .execute(
            "
        CREATE TABLE node (
            id INTEGER PRIMARY KEY NOT NULL,
            station INTEGER NOT NULL,
            time INTEGER NOT NULL,
            node_type INTEGER NOT NULL
        );",
        )
        .unwrap();
    connection
        .execute(
            "CREATE TABLE edge (
            id INTEGER PRIMARY KEY NOT NULL,
            from_node INTEGER NOT NULL,
            to_node INTEGER NOT NULL,
            edge_type INTEGER NOT NULL,
            vehicle_id INTEGER
        )",
        )
        .unwrap();
    connection
        .execute(
            "CREATE TABLE vehicle (
            id INTEGER PRIMARY KEY NOT NULL,
            capacity REAL NOT NULL,
            line_id INTEGER NOT NULL,
            line_direction INTEGER NOT NULL,
            line_repetition INTEGER NOT NULL,
            period_repetition INTEGER NOT NULL
        )",
        )
        .unwrap();
    connection
        .execute(
            "CREATE TABLE commodity (
        id INTEGER PRIMARY KEY NOT NULL,
        origin INTEGER NOT NULL,
        departure_time INTEGER NOT NULL,
        destination INTEGER NOT NULL,
        outside_latest_arrival INTEGER NOT NULL,
        demand REAL NOT NULL,
        spawn_node INTEGER
    )",
        )
        .unwrap();
    let mut stmt = connection
        .prepare("INSERT INTO node (id, station, time, node_type) VALUES (?, ?, ?, ?)")
        .unwrap();
    for (id, node) in graph.nodes() {
        stmt.bind((1, id.0 as i64)).unwrap();
        let station = &graph.station(id);
        stmt.bind((2, station.0 as i64)).unwrap();
        stmt.bind((3, node.time as i64)).unwrap();
        stmt.bind((
            4,
            match node.node_type {
                NodeType::Wait(_) => DBNodeType::Wait,
                NodeType::Depart() => DBNodeType::Depart,
                NodeType::Arrive() => DBNodeType::Arrive,
            } as i64,
        ))
        .unwrap();
        stmt.next().unwrap();
        stmt.reset().unwrap();
    }
    let mut stmt = connection
        .prepare(
            "INSERT INTO vehicle \
            (id, capacity, line_id, line_direction, line_repetition, period_repetition) \
            VALUES (?, ?, ?, ?, ?, ?)",
        )
        .unwrap();
    for vehicle in graph.vehicles() {
        let line_info = line_info_map.get(vehicle.id);
        stmt.bind((1, vehicle.id.0 as i64)).unwrap();
        stmt.bind((2, vehicle.capacity)).unwrap();
        stmt.bind((3, line_info.line_id as i64)).unwrap();
        stmt.bind((4, line_info.line_direction as i64)).unwrap();
        stmt.bind((5, line_info.line_freq_repetition as i64))
            .unwrap();
        stmt.bind((6, line_info.period_repetition as i64)).unwrap();
        stmt.next().unwrap();
        stmt.reset().unwrap();
    }

    let mut stmt = connection
        .prepare(
            "INSERT INTO edge (id, from_node, to_node, edge_type, vehicle_id) VALUES (?, ?, ?, ?, ?)",
        )
        .unwrap();
    for (id, edge) in graph.edges() {
        stmt.bind((1, id.0 as i64)).unwrap();
        stmt.bind((2, edge.from.0 as i64)).unwrap();
        stmt.bind((3, edge.to.0 as i64)).unwrap();
        stmt.bind((
            4,
            match edge.edge_type {
                EdgeType::Board(_) => DBEdgeType::Board,
                EdgeType::Wait => DBEdgeType::Wait,
                EdgeType::Drive(_) => DBEdgeType::Drive,
                EdgeType::Alight => DBEdgeType::Alight,
                EdgeType::Dwell => DBEdgeType::Dwell,
            } as i64,
        ))
        .unwrap();
        let vehicle_id: Option<i64> = match edge.edge_type {
            EdgeType::Board(vehicle_id) => Some(vehicle_id.0 as i64),
            EdgeType::Drive(_) => match graph.boarding_edge(id).unwrap().1.edge_type {
                EdgeType::Board(vehicle_id) => Some(vehicle_id.0 as i64),
                _ => panic!("Boarding edge is not a boarding edge!"),
            },
            EdgeType::Alight | EdgeType::Dwell => match graph
                .boarding_edge(graph.node(edge.from).incoming[0])
                .unwrap()
                .1
                .edge_type
            {
                EdgeType::Board(vehicle_id) => Some(vehicle_id.0 as i64),
                _ => panic!("Boarding edge is not a boarding edge!"),
            },
            EdgeType::Wait => None,
        };
        stmt.bind((5, vehicle_id)).unwrap();
        stmt.next().unwrap();
        stmt.reset().unwrap();
    }
    let mut stmt = connection
        .prepare("INSERT INTO commodity (id, origin, departure_time, destination, outside_latest_arrival, demand, spawn_node) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .unwrap();
    for (idx, commodity) in graph.commodities() {
        stmt.bind((1, idx.0 as i64)).unwrap();
        stmt.bind((2, commodity.od_pair.origin.0 as i64)).unwrap();
        stmt.bind((3, commodity.od_pair.departure as i64)).unwrap();
        stmt.bind((4, commodity.od_pair.destination.0 as i64))
            .unwrap();
        stmt.bind((5, commodity.outside_latest_arrival as i64))
            .unwrap();
        stmt.bind((6, commodity.demand)).unwrap();
        stmt.bind((7, commodity.spawn_node.map(|it| it.0 as i64)))
            .unwrap();
        stmt.next().unwrap();
        stmt.reset().unwrap();
    }

    connection.execute("END TRANSACTION;").unwrap();
}

#[derive(Debug)]
pub enum ImportGraphError {
    CouldNotOpen(sqlite::Error),
    Sqlite(sqlite::Error),
    InvalidNodeType { node_id: u32, node_type: i64 },
    MissingNodeIndex { node_idx: u32, got_index: i64 },
    DuplicateNodeIndex { node_idx: u32 },
    InvalidEdgeType { edge_id: u32, edge_type: i64 },
    MissingEdgeIndex { edge_idx: u32 },
    DuplicateEdgeIndex { edge_idx: u32 },
    MissingCommodityIndex { commodity_idx: u32 },
    DuplicateCommodityIndex { commodity_idx: u32 },
    BoardEdgeHasNoVehicleId { edge_idx: usize },
    DriveEdgeHasNoVehicleId { edge_idx: usize },
    VehicleOfEdgeDoesNotExist { edge_idx: usize, vehicle_id: u32 },
    InvalidGraph(FromRawError),
}

pub fn import_graph(in_fname: &str) -> Result<Graph, ImportGraphError> {
    info!("Importing graph from {}...", in_fname);
    let connection =
        sqlite::Connection::open_with_flags(in_fname, OpenFlags::default().with_read_only())
            .map_err(ImportGraphError::CouldNotOpen)?;

    struct DBNode {
        station: i64,
        time: u32,
        node_type: DBNodeType,
    }

    let db_nodes: Vec<_> = connection
        .prepare("SELECT id, station, time, node_type FROM node ORDER BY id ASC;")
        .map_err(ImportGraphError::Sqlite)?
        .iter()
        .enumerate()
        .map(|(idx, it)| match it {
            Err(it) => Err(ImportGraphError::Sqlite(it)),
            Ok(it) => {
                let id: i64 = it.read(0);
                if id > idx as i64 {
                    return Err(ImportGraphError::MissingNodeIndex {
                        node_idx: idx as u32,
                        got_index: id,
                    });
                } else if id < idx as i64 {
                    return Err(ImportGraphError::DuplicateNodeIndex {
                        node_idx: idx as u32,
                    });
                }
                let station = it.read(1);
                let time = it.read::<i64, _>(2) as u32;
                let node_type: i64 = it.read(3);
                let node_type: DBNodeType =
                    DBNodeType::from_i64(node_type).ok_or(ImportGraphError::InvalidNodeType {
                        node_id: id as u32,
                        node_type,
                    })?;
                Ok(DBNode {
                    station,
                    time,
                    node_type,
                })
            }
        })
        .collect::<Result<_, _>>()?;

    struct DBEdge {
        from_node: u32,
        to_node: u32,
        edge_type: DBEdgeType,
        vehicle_id: Option<i64>,
    }

    let db_edges: Vec<_> = connection
        .prepare("SELECT id, from_node, to_node, edge_type, vehicle_id FROM edge ORDER BY id ASC;")
        .map_err(ImportGraphError::Sqlite)?
        .iter()
        .enumerate()
        .map(|(idx, it)| match it {
            Err(it) => Err(ImportGraphError::Sqlite(it)),
            Ok(it) => {
                let id: i64 = it.read(0);
                if id > idx as i64 {
                    return Err(ImportGraphError::MissingEdgeIndex {
                        edge_idx: idx as u32,
                    });
                } else if id < idx as i64 {
                    return Err(ImportGraphError::DuplicateEdgeIndex {
                        edge_idx: idx as u32,
                    });
                }
                let from_node: i64 = it.read(1);
                let to_node: i64 = it.read(2);
                let edge_type = it.read(3);
                let vehicle_id = it.read(4);
                let edge_type: DBEdgeType =
                    DBEdgeType::from_i64(edge_type).ok_or(ImportGraphError::InvalidEdgeType {
                        edge_id: id as u32,
                        edge_type,
                    })?;
                Ok(DBEdge {
                    from_node: from_node as u32,
                    to_node: to_node as u32,
                    edge_type,
                    vehicle_id,
                })
            }
        })
        .collect::<Result<_, _>>()?;

    struct DBVehicle {
        id: i64,
        capacity: f64,
    }

    let db_vehicles: Vec<_> = connection
        .prepare("SELECT id, capacity FROM vehicle ORDER BY id ASC;")
        .map_err(ImportGraphError::Sqlite)?
        .iter()
        .map(|it| match it {
            Err(it) => Err(ImportGraphError::Sqlite(it)),
            Ok(it) => {
                let id: i64 = it.read(0);
                let capacity: f64 = it.read(1);
                Ok(DBVehicle { id, capacity })
            }
        })
        .collect::<Result<_, _>>()?;

    struct DBCommodity {
        origin: u32,
        departure_time: u32,
        destination: u32,
        outside_latest_arrival: u32,
        demand: f64,
        spawn_node: Option<u32>,
    }

    let db_commodities: Vec<_> = connection
    .prepare("SELECT id, origin, departure_time, destination, outside_latest_arrival, demand, spawn_node FROM commodity ORDER BY id ASC;")
    .map_err(ImportGraphError::Sqlite)?
    .iter()
    .enumerate()
    .map(|(idx, it)| match it {
        Err(it) => Err(ImportGraphError::Sqlite(it)),
        Ok(it) => {
            let id: i64 = it.read(0);
            if id > idx as i64 {
                return Err(ImportGraphError::MissingCommodityIndex {
                    commodity_idx: idx as u32,
                });
            } else if id < idx as i64 {
                return Err(ImportGraphError::DuplicateCommodityIndex {
                    commodity_idx: idx as u32,
                });
            }
            let origin: i64 = it.read(1);
            let departure_time: i64 = it.read(2);
            let destination: i64 = it.read(3);
            let outside_latest_arrival: i64 = it.read(4);
            let demand: f64 = it.read(5);
            let spawn_node: Option<i64> = it.read(6);
            Ok(DBCommodity {
                origin: origin as u32,
                departure_time: departure_time as u32,
                destination: destination as u32,
                outside_latest_arrival: outside_latest_arrival as u32,
                demand,
                spawn_node: spawn_node.map(|it| it as u32)
            })
        }
    })
    .collect::<Result<_, _>>()?;

    let station_by_node = db_nodes
        .iter()
        .map(|it| StationIdx(it.station as u32))
        .collect_vec();
    let raw_nodes = db_nodes
        .into_iter()
        .map(|db_node| {
            Ok(NodeRawPayload {
                time: db_node.time,
                node_type: match db_node.node_type {
                    DBNodeType::Wait => NodeType::Wait(StationIdx(db_node.station as u32)),
                    DBNodeType::Arrive => NodeType::Arrive(),
                    DBNodeType::Depart => NodeType::Depart(),
                },
            })
        })
        .collect::<Result<_, _>>()?;

    let edges =
        db_edges
            .into_iter()
            .enumerate()
            .map(|(idx, db_edge)| {
                Ok(EdgePayload {
                    from: NodeIdx(db_edge.from_node),
                    to: NodeIdx(db_edge.to_node),
                    edge_type: match db_edge.edge_type {
                        DBEdgeType::Alight => EdgeType::Alight,
                        DBEdgeType::Dwell => EdgeType::Dwell,
                        DBEdgeType::Wait => EdgeType::Wait,
                        DBEdgeType::Board => {
                            EdgeType::Board(VehicleId(db_edge.vehicle_id.ok_or(
                                ImportGraphError::BoardEdgeHasNoVehicleId { edge_idx: idx },
                            )? as u32))
                        }
                        DBEdgeType::Drive => EdgeType::Drive({
                            let vehicle_id = VehicleId(db_edge.vehicle_id.ok_or(
                                ImportGraphError::DriveEdgeHasNoVehicleId { edge_idx: idx },
                            )? as u32);
                            let vehicle = db_vehicles
                                .iter()
                                .find(|it| it.id as u32 == vehicle_id.0)
                                .ok_or(ImportGraphError::VehicleOfEdgeDoesNotExist {
                                    edge_idx: idx,
                                    vehicle_id: vehicle_id.0,
                                })?;
                            vehicle.capacity
                        }),
                    },
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

    let commodities = db_commodities
        .into_iter()
        .map(|db_commodity| CommodityPayload {
            od_pair: ODPair {
                origin: StationIdx(db_commodity.origin),
                destination: StationIdx(db_commodity.destination),
                departure: db_commodity.departure_time,
            },
            demand: db_commodity.demand,
            outside_latest_arrival: db_commodity.outside_latest_arrival,
            spawn_node: db_commodity.spawn_node.map(NodeIdx),
        })
        .collect_vec();

    Graph::try_from_raw(raw_nodes, edges, commodities, station_by_node)
        .map_err(ImportGraphError::InvalidGraph)
}
