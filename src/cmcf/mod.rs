use std::iter::empty;

use itertools::Itertools;
use sqlite::OpenFlags;

use crate::{
    col::HashMap,
    flow::Flow,
    graph::{
        CommodityIdx, CostCharacteristic, EdgeIdx, EdgeNavigate, EdgeType, Graph, NodeIdx,
        NodeType, PathBox, StationIdx,
    },
    iter::swap::Swap,
    path_index::PathsIndex,
};

#[derive(Debug)]
pub enum WriteCmcfInputError {
    SqliteError(sqlite::Error),
}

pub fn write_cmcf_input(graph: &Graph, filename: &str) -> Result<(), WriteCmcfInputError> {
    // Assert (for now) that all commodities have a fixed departure time.
    assert!(graph.commodities().all(|it| matches!(
        it.1.cost_characteristic,
        CostCharacteristic::FixedDeparture(_)
    )));

    // Assert all commodities have the same outside option.
    let outside_option = graph
        .commodities()
        .next()
        .map(|it| it.1.outside_option)
        .unwrap_or(0);
    assert!(
        graph
            .commodities()
            .all(|it| it.1.outside_option == outside_option)
    );

    let connection = sqlite::Connection::open_with_flags(
        filename,
        OpenFlags::default()
            .with_create()
            .with_no_mutex()
            .with_read_write(),
    )
    .map_err(WriteCmcfInputError::SqliteError)?;
    connection.execute("BEGIN TRANSACTION;").unwrap();

    connection
        .execute(
            "
        CREATE TABLE node (
            id INTEGER PRIMARY KEY,
            X REAL NOT NULL,
            Y REAL NOT NULL
        );",
        )
        .map_err(WriteCmcfInputError::SqliteError)?;
    connection
        .execute(
            "CREATE TABLE link (
            id INTEGER PRIMARY KEY,
            node_from INTEGER NOT NULL,
            node_to INTEGER NOT NULL,
            length REAL NOT NULL,
            capacity REAL NOT NULL,
            lpf_param_1 REAL NOT NULL,
            lpf_param_2 REAL NOT NULL,
            lpf_param_3 REAL NOT NULL,
            lpf_mode VARCHAR(3) NOT NULL
        )",
        )
        .map_err(WriteCmcfInputError::SqliteError)?;
    let mut stmt = connection
        .prepare("INSERT INTO node (id, X, Y) VALUES (?, ?, ?)")
        .map_err(WriteCmcfInputError::SqliteError)?;

    // The first `graph.num_nodes()` ids are already (partially) occupied.
    // The super sink ids are placed in the range
    // graph.num_nodes() .. graph.num_nodes() + graph.num_stations()
    let sink_id_by_station_id = |station_id: StationIdx| graph.num_nodes() as u32 + station_id.0;

    // We create one spawn node per actual spawn node and departure time.
    let actual_spawn_nodes_map = graph
        .commodities()
        .map(|(_idx, commodity)| match &commodity.cost_characteristic {
            CostCharacteristic::FixedDeparture(fixed) => (fixed.spawn_node, fixed.departure),
            _ => todo!(),
        })
        .unique()
        .enumerate()
        .swap()
        .collect::<HashMap<_, _>>();
    let spawn_id_by_commodity_id = |commodity_idx: CommodityIdx| {
        let commodity = graph.commodity(commodity_idx);
        let fixed = match &commodity.cost_characteristic {
            CostCharacteristic::FixedDeparture(it) => it,
            _ => todo!(),
        };
        graph.num_nodes() as u32
            + graph.num_stations() as u32
            + actual_spawn_nodes_map[&(fixed.spawn_node, fixed.departure)] as u32
    };

    graph
        .nodes()
        // In-graph nodes
        .filter_map(|(node_id, node)| match node.node_type {
            NodeType::Arrive() | NodeType::Depart() => None,
            NodeType::Wait(_) => {
                let station_id = graph.station(node_id);
                Some((node_id.0 as i64, station_id.0 as f64, node.time as f64))
            }
        })
        // Super sink nodes
        .chain(graph.stations().map(|station_idx| {
            (
                sink_id_by_station_id(station_idx) as i64,
                station_idx.0 as f64,
                -1.0_f64,
            )
        }))
        // Super spawn nodes
        .chain(actual_spawn_nodes_map.iter().map(|(_actual_spawn, i)| {
            (
                (graph.num_nodes() + graph.num_stations() + i) as i64,
                -1.0_f64,
                -1.0_f64,
            )
        }))
        .for_each(|(node_id, x, y)| {
            stmt.bind((1, node_id)).unwrap();
            stmt.bind((2, x)).unwrap();
            stmt.bind((3, y)).unwrap();
            stmt.next().unwrap();
            stmt.reset().unwrap();
        });

    let mut stmt = connection
        .prepare(
            "INSERT INTO link (id, node_from, node_to, length, capacity, lpf_param_1, lpf_param_2, lpf_param_3, lpf_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .map_err(WriteCmcfInputError::SqliteError)?;

    graph
        .edges()
        .filter_map(|(edge_idx, edge)| {
            // Add driving and waiting edges from the graph
            let edge_delay = (graph.node(edge.to).time - graph.node(edge.from).time) as f64;
            match edge.edge_type {
                EdgeType::Board(_) | EdgeType::GetOff | EdgeType::StayOn => None,
                EdgeType::Wait => {
                    let capacity = f64::INFINITY;
                    Some((
                        edge_idx.0 as i64,
                        edge.from.0 as i64,
                        edge.to.0 as i64,
                        edge_delay,
                        capacity,
                    ))
                }
                EdgeType::Drive(capacity) => {
                    let wait_node_from = graph.boarding_edge(edge_idx).unwrap().1.from;
                    let wait_node_to = graph.offboarding_edge(edge_idx).unwrap().1.to;
                    let edge_delay =
                        (graph.node(wait_node_to).time - graph.node(wait_node_from).time) as f64;
                    Some((
                        edge_idx.0 as i64,
                        wait_node_from.0 as i64,
                        wait_node_to.0 as i64,
                        edge_delay,
                        capacity,
                    ))
                }
            }
        })
        .chain(
            // Add edges from on-platform nodes to the super sink.
            graph
                .nodes()
                .filter_map(|(node_id, node)| match node.node_type {
                    NodeType::Wait(station_id) => {
                        let edge_idx = graph.num_edges() + node_id.0 as usize;
                        let edge_delay = 0.0;
                        let capacity = f64::INFINITY;
                        Some((
                            edge_idx as i64,
                            node_id.0 as i64,
                            sink_id_by_station_id(station_id) as i64,
                            edge_delay,
                            capacity,
                        ))
                    }
                    _ => None,
                }),
        )
        .chain(
            // Add outside edges from the spawn nodes.
            graph
                .commodities()
                .map(|(c_idx, c)| (spawn_id_by_commodity_id(c_idx), c.od_pair.destination))
                .unique()
                .enumerate()
                .map(|(i, (from_id, to_station))| {
                    let edge_idx = graph.num_edges() + graph.num_nodes() + i;
                    let sink_node_idx = sink_id_by_station_id(to_station);
                    let edge_delay = outside_option as f64;
                    let capacity = f64::INFINITY;
                    (
                        edge_idx as i64,
                        from_id as i64,
                        sink_node_idx as i64,
                        edge_delay,
                        capacity,
                    )
                }),
        )
        .chain(
            // Add edges from the spawn nodes to the actual spawn nodes
            actual_spawn_nodes_map
                .iter()
                .filter_map(
                    |((actual_spawn, departure), super_spawn)| match actual_spawn {
                        None => None,
                        Some(actual_spawn_node) => {
                            let edge_idx = graph.num_edges()
                                + graph.num_nodes()
                                + graph.num_commodities()
                                + *super_spawn as usize;
                            let super_spawn_id =
                                graph.num_nodes() + graph.num_stations() + super_spawn;
                            let edge_delay =
                                (graph.node(*actual_spawn_node).time - *departure) as f64;
                            let capacity = f64::INFINITY;
                            Some((
                                edge_idx as i64,
                                super_spawn_id as i64,
                                actual_spawn_node.0 as i64,
                                edge_delay,
                                capacity,
                            ))
                        }
                    },
                ),
        )
        .for_each(
            |(edge_id, node_from, node_to, edge_delay, capacity): (i64, i64, i64, f64, f64)| {
                stmt.bind((1, edge_id)).unwrap();
                stmt.bind((2, node_from)).unwrap();
                stmt.bind((3, node_to)).unwrap();
                stmt.bind((4, edge_delay)).unwrap();
                stmt.bind((5, capacity)).unwrap();
                stmt.bind((6, edge_delay)).unwrap();
                stmt.bind((7, 0.0)).unwrap();
                stmt.bind((8, capacity)).unwrap();
                stmt.bind((9, "c")).unwrap();
                stmt.next().unwrap();
                stmt.reset().unwrap();
            },
        );

    connection
        .execute(
            "CREATE TABLE demand (
                id INTEGER PRIMARY KEY,
                node_from INTEGER NOT NULL,
                node_to INTEGER NOT NULL,
                flow REAL NOT NULL
            );",
        )
        .map_err(WriteCmcfInputError::SqliteError)?;

    let mut stmt = connection
        .prepare("INSERT INTO demand (id, node_from, node_to, flow) VALUES (?, ?, ?, ?)")
        .map_err(WriteCmcfInputError::SqliteError)?;
    graph.commodities().for_each(|(commodity_id, commodity)| {
        let source: i64 = spawn_id_by_commodity_id(commodity_id) as i64;
        let sink: i64 = sink_id_by_station_id(commodity.od_pair.destination) as i64;
        stmt.bind((1, commodity_id.0 as i64)).unwrap();
        stmt.bind((2, source)).unwrap();
        stmt.bind((3, sink)).unwrap();
        stmt.bind((4, commodity.demand)).unwrap();

        stmt.next().unwrap();
        stmt.reset().unwrap();
    });

    connection.execute("END TRANSACTION;").unwrap();

    Ok(())
}

#[derive(Debug)]
pub enum ReadCmcfOutputError {
    SqliteError(sqlite::Error),
    PathExpansionError {
        db_path_id: i64,
        error: ExpandPathError,
        path: Vec<EdgeIdx>,
    },
    PathIdNotFound {
        db_path_id: i64,
    },
}

#[derive(Debug)]
pub enum ExpandPathError {
    PathTooShort,
    EdgeIdxOutOfBounds {
        idx_in_path: usize,
        edge_idx: EdgeIdx,
    },
    DoesNotStartOnSpawn {
        first_node_idx: NodeIdx,
    },
    DoesNotEndOnWaitOrArrive,
    DoesNotEndOnDestination,
    CannotConnect {
        edge1: EdgeIdx,
        edge2: EdgeIdx,
        e1_idx_in_path: usize,
    },
    InvalidEdgeType,
    PathInsideButNoSpawn {
        commodity_idx: CommodityIdx,
    },
}
fn expand_path(
    graph: &Graph,
    path: &[EdgeIdx],
    commodity_idx: CommodityIdx,
) -> Result<PathBox, ExpandPathError> {
    // There is at least one edge (the outside edge).
    // If there is more than one, then the first one is a spawn edge, and the last one is the super sink edge.

    if path.is_empty() {
        return Err(ExpandPathError::PathTooShort);
    } else if path.len() == 1 {
        return Ok(PathBox::new(commodity_idx, empty()));
    } else if path.len() == 2 {
        return Err(ExpandPathError::PathTooShort);
    }
    if let Some((idx_in_path, &edge_idx)) = path[1..path.len() - 1]
        .iter()
        .enumerate()
        .find(|(_, it)| it.0 as usize >= graph.num_edges())
    {
        return Err(ExpandPathError::EdgeIdxOutOfBounds {
            idx_in_path,
            edge_idx,
        });
    }

    let commodity = graph.commodity(commodity_idx);
    let fixed = match &commodity.cost_characteristic {
        CostCharacteristic::FixedDeparture(it) => it,
        _ => todo!(),
    };
    let expected_spawn_node_idx = match fixed.spawn_node {
        None => return Err(ExpandPathError::PathInsideButNoSpawn { commodity_idx }),
        Some(it) => it,
    };

    let actual_spawn_idx = match graph.nav_edge(path[1]) {
        EdgeNavigate::Drive(e) => e.pre_boarding().edge().from,
        EdgeNavigate::Wait(e) => e.edge().from,
        _ => Err(ExpandPathError::InvalidEdgeType)?,
    };
    if actual_spawn_idx != expected_spawn_node_idx {
        return Err(ExpandPathError::DoesNotStartOnSpawn {
            first_node_idx: actual_spawn_idx,
        });
    }

    let commodity = graph.commodity(commodity_idx);
    let sink_idx = graph.edge(path[path.len() - 2]).to;
    let sink = graph.node(sink_idx);
    let sink_station_idx = match sink.node_type {
        NodeType::Wait(station_idx) => station_idx,
        NodeType::Arrive() => graph.station(sink_idx),
        _ => return Err(ExpandPathError::DoesNotEndOnWaitOrArrive),
    };
    if commodity.od_pair.destination != sink_station_idx {
        return Err(ExpandPathError::DoesNotEndOnDestination);
    }

    let mut expanded = Vec::new();
    for (e1_idx_in_path, (e1, e2)) in path.iter().tuple_windows().enumerate().skip(1) {
        // Add all edges between e1 (including) and e2 (excluding).
        let edge2 = if e1_idx_in_path == path.len() - 2 {
            // e2 is the super sink edge.
            None
        } else {
            Some(graph.nav_edge(*e2))
        };
        match graph.nav_edge(*e1) {
            EdgeNavigate::Wait(_) => {
                expanded.push(*e1);
            }
            EdgeNavigate::Drive(edge1) => {
                expanded.push(*e1);
                match edge2 {
                    None => {
                        expanded.push(edge1.post_get_off().id());
                    }
                    Some(EdgeNavigate::Wait(edge2)) => {
                        let get_off = edge1.post_get_off();
                        if get_off.edge().to != edge2.edge().from {
                            return Err(ExpandPathError::CannotConnect {
                                edge1: *e1,
                                edge2: *e2,
                                e1_idx_in_path,
                            });
                        }
                        expanded.push(get_off.id());
                    }
                    Some(EdgeNavigate::Drive(edge2)) => {
                        // Either same vehicle or not. If same, use stay_on. If not, use get_off and board.
                        if let Some(stay_on) = edge1.post_stay_on() {
                            if stay_on.edge().to == edge2.edge().from {
                                // Same vehicle
                                expanded.push(stay_on.id());
                                continue;
                            }
                        }
                        // Different vehicle.
                        if edge1.post_get_off().edge().to != edge2.pre_boarding().edge().from {
                            return Err(ExpandPathError::CannotConnect {
                                edge1: *e1,
                                edge2: *e2,
                                e1_idx_in_path,
                            });
                        }
                        expanded.push(edge1.post_get_off().id());
                        expanded.push(edge2.pre_boarding().id());
                    }
                    Some(EdgeNavigate::Board(_))
                    | Some(EdgeNavigate::GetOff(_))
                    | Some(EdgeNavigate::StayOn(_)) => {
                        return Err(ExpandPathError::InvalidEdgeType);
                    }
                }
            }
            EdgeNavigate::Board(_) | EdgeNavigate::GetOff(_) | EdgeNavigate::StayOn(_) => {
                return Err(ExpandPathError::InvalidEdgeType);
            }
        }
    }

    Ok(PathBox::new(commodity_idx, expanded.into_iter()))
}

pub fn read_cmcf_output<'a>(
    graph: &Graph,
    filename: &str,
) -> Result<(Flow, PathsIndex<'a>), ReadCmcfOutputError> {
    let connection =
        sqlite::Connection::open_with_flags(filename, OpenFlags::default().with_read_only())
            .unwrap();

    let path_meta_by_db_path_id: HashMap<i64, (CommodityIdx, f64)> = connection
        .prepare("SELECT id, demand_id, flow FROM path;")
        .map_err(ReadCmcfOutputError::SqliteError)?
        .iter()
        .map(|it| {
            it.map_or_else(
                |it| Err(ReadCmcfOutputError::SqliteError(it)),
                |it| {
                    let path_id: i64 = it.read(0);
                    let commodity: i64 = it.read(1);
                    let flow: f64 = it.read(2);
                    Ok((path_id, (CommodityIdx(commodity as u32), flow)))
                },
            )
        })
        .collect::<Result<_, _>>()?;

    let mut paths = PathsIndex::new();

    struct DBPathEdge {
        path_id: i64,
        edge_id: i64,
    }

    let mut flow = Flow::new();
    connection
        .prepare("SELECT path_id, edge_id FROM edge_path ORDER BY path_id, edge_index;")
        .map_err(ReadCmcfOutputError::SqliteError)?
        .iter()
        .map(|it| match it {
            Err(err) => Err(ReadCmcfOutputError::SqliteError(err)),
            Ok(it) => Ok(DBPathEdge {
                path_id: it.read(0),
                edge_id: it.read(1),
            }),
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .chunk_by(|it| it.path_id)
        .into_iter()
        .try_for_each(|(db_path_id, edges_iterator)| {
            let path = edges_iterator
                .map(|it| EdgeIdx(it.edge_id as u32))
                .collect_vec();
            let (commodity, path_flow) = *path_meta_by_db_path_id
                .get(&db_path_id)
                .ok_or(ReadCmcfOutputError::PathIdNotFound { db_path_id })?;
            let path = expand_path(graph, &path, commodity).map_err(|error| {
                ReadCmcfOutputError::PathExpansionError {
                    db_path_id,
                    error,
                    path,
                }
            })?;

            let path_id = paths.transfer_path(path);

            flow.add_flow_onto_path(&paths, path_id, path_flow, true, false);

            Ok(())
        })?;

    Ok((flow, paths))
}
