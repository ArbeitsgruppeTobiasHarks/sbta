use itertools::Itertools;
use log::warn;
use sqlite::OpenFlags;

use crate::{
    col::HashMap,
    flow::Flow,
    graph::{CommodityIdx, CostCharacteristic, EdgeIdx, Graph, NodeType, Path, PathBox},
    heuristic::Stats,
    path_index::{PathId, PathsIndex},
    regret::PathRegret,
};

pub fn export_flow(
    graph: &Graph,
    stats: Option<&Stats>,
    flow: &Flow,
    paths: &PathsIndex,
    regret_map: &HashMap<PathId, PathRegret>,
    out_filename: &str,
) {
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
            "CREATE TABLE edge_flow (
            edge_id INTEGER PRIMARY KEY,
            flow REAL NOT NULL
        )",
        )
        .unwrap();

    let mut stmt = connection
        .prepare("INSERT INTO edge_flow (edge_id, flow) VALUES (?, ?)")
        .unwrap();
    for (id, _edge) in graph.edges() {
        stmt.bind((1, id.0 as i64)).unwrap();
        stmt.bind((2, flow.on_edge(id))).unwrap();
        stmt.next().unwrap();
        stmt.reset().unwrap();
    }

    connection
        .execute(
            "
    CREATE TABLE path (
        id INTEGER PRIMARY KEY,
        commodity INTEGER NOT NULL,
        flow REAL NOT NULL,
        REGRET REAL NOT NULL,
        RELATIVE_REGRET REAL NOT NULL
    );",
        )
        .unwrap();

    connection
        .execute(
            "
            CREATE TABLE path_edge (
                path_id INTEGER NOT NULL,
                edge_id INTEGER NOT NULL,
                path_edge_index INTEGER NOT NULL
            );",
        )
        .unwrap();

    let mut stmt_path = connection
        .prepare("INSERT INTO path (id, commodity, flow, regret, relative_regret) VALUES (?, ?, ?, ?, ?)")
        .unwrap();

    let mut stmt_edge = connection
        .prepare("INSERT INTO path_edge (path_id, edge_id, path_edge_index) VALUES (?, ?, ?);")
        .unwrap();
    for (path_id, path_flow) in flow.path_flow_map().iter() {
        let path = paths.path(*path_id);

        stmt_path.bind((1, path_id.0 as i64)).unwrap();
        stmt_path.bind((2, path.commodity_idx().0 as i64)).unwrap();
        stmt_path.bind((3, *path_flow)).unwrap();
        stmt_path
            .bind((
                4,
                regret_map.get(path_id).map_or(0.0, |it| it.regret as f64),
            ))
            .unwrap();
        stmt_path
            .bind((
                5,
                regret_map.get(path_id).map_or(0.0, |it| it.relative_regret),
            ))
            .unwrap();
        stmt_path.next().unwrap();
        stmt_path.reset().unwrap();

        for (path_edge_index, edge_idx) in path.edges().iter().enumerate() {
            stmt_edge.bind((1, path_id.0 as i64)).unwrap();
            stmt_edge.bind((2, edge_idx.0 as i64)).unwrap();
            stmt_edge.bind((3, path_edge_index as i64)).unwrap();
            stmt_edge.next().unwrap();
            stmt_edge.reset().unwrap();
        }
    }
    if let Some(stats) = stats {
        connection
            .execute(
                "CREATE TABLE flow_stats (
                    COST REAL NOT NULL,
                    TOTAL_REGRET REAL NOT NULL,
                    MEAN_REGRET REAL NOT NULL,
                    MEAN_RELATIVE_REGRET REAL NOT NULL,
                    MAX_RELATIVE_REGRET REAL NOT NULL,
                    REGRETTING_DEMAND REAL NOT NULL,
                    NUM_REGRETTING_PATHS REAL NOT NULL,
                    DEMAND_OUTSIDE REAL NOT NULL,
                    COMPUTATION_TIME_MS INT NOT NULL,
                    NUM_ITERATIONS INT,
                    NUM_ITERATIONS_POST_PROCESSING INT,
                    DEMAND_PREROUTED REAL,
                    NUM_BOARDING_EDGES_BLOCKED_BY_PREROUTING INT,
                    NUM_COMMODITIES_FULLY_PREROUTED INT
                );",
            )
            .unwrap();
        let mut stmt = connection
            .prepare(
                "INSERT INTO flow_stats(
                        COST,
                        TOTAL_REGRET,
                        MEAN_REGRET,
                        MEAN_RELATIVE_REGRET,
                        MAX_RELATIVE_REGRET,
                        REGRETTING_DEMAND,
                        NUM_REGRETTING_PATHS,
                        DEMAND_OUTSIDE,
                        COMPUTATION_TIME_MS,
                        NUM_ITERATIONS,
                        NUM_ITERATIONS_POST_PROCESSING,
                        DEMAND_PREROUTED,
                        NUM_BOARDING_EDGES_BLOCKED_BY_PREROUTING,
                        NUM_COMMODITIES_FULLY_PREROUTED
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .unwrap();
        stmt.bind((1, stats.flow.cost)).unwrap();
        stmt.bind((2, stats.flow.total_regret)).unwrap();
        stmt.bind((3, stats.flow.total_regret / graph.total_demand()))
            .unwrap();
        stmt.bind((4, stats.flow.mean_relative_regret)).unwrap();
        stmt.bind((5, stats.flow.max_relative_regret)).unwrap();
        stmt.bind((6, stats.flow.regretting_demand)).unwrap();
        stmt.bind((7, stats.flow.num_regretting_paths as i64))
            .unwrap();
        stmt.bind((8, stats.flow.demand_outside)).unwrap();
        stmt.bind((9, stats.computation_time.as_millis() as i64))
            .unwrap();
        if let Some(stats) = &stats.heuristic {
            stmt.bind((10, stats.num_iterations as i64)).unwrap();
            stmt.bind((11, stats.num_iterations_postprocessing as i64))
                .unwrap();
            stmt.bind((12, stats.demand_prerouted)).unwrap();
            stmt.bind((13, stats.num_boarding_edges_blocked_by_prerouting as i64))
                .unwrap();
            stmt.bind((14, stats.num_commodities_fully_prerouted as i64))
                .unwrap();
        }

        stmt.next().unwrap();
    }

    connection
        .execute(
            "CREATE VIEW relative_regret_percentiles (
                percentile,
                flow,
                relative_regret
            ) AS
                SELECT
                    SUM(SUM(path.flow)) OVER (ORDER BY relative_regret GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) / (SUM(SUM(path.flow)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS percentile,
                    SUM(path.flow) AS flow,
                    relative_regret AS relative_regret
                FROM path
                GROUP BY relative_regret
                ORDER BY relative_regret;",
        )
        .unwrap();

    connection
        .execute(
            "CREATE VIEW regret_percentiles (
                percentile,
                flow,
                regret
            ) AS
                SELECT
                    SUM(SUM(path.flow)) OVER (ORDER BY regret GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) / (SUM(SUM(path.flow)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS percentile,
                    SUM(path.flow) AS flow,
                    regret AS regret
                FROM path
                GROUP BY regret
                ORDER BY regret
            ;",
        )
        .unwrap();

    connection.execute("END TRANSACTION;").unwrap();
}

#[derive(Debug)]
pub enum ImportFlowError {
    SqliteError(sqlite::Error),
    InvalidPath { path_id: i64 },
}

fn path_is_valid(graph: &Graph, path: &Path) -> bool {
    if path.commodity_idx().0 as usize >= graph.num_commodities() {
        warn!("Invalid commodity index: {}", path.commodity_idx().0);
        return false;
    }
    let commodity = graph.commodity(path.commodity_idx());
    if path
        .edges()
        .iter()
        .any(|it| it.0 as usize >= graph.num_edges())
    {
        warn!("Invalid edge index: {:?}", path.edges());
        return false;
    }

    if path
        .edges()
        .iter()
        .tuple_windows()
        .any(|(&e1, &e2)| graph.edge(e1).to != graph.edge(e2).from)
    {
        warn!("Invalid path: {:?}", path.edges());
        return false;
    }

    if let Some(&first_edge) = path.edges().first() {
        let first_node = graph.edge(first_edge).from;
        match &commodity.cost_characteristic {
            CostCharacteristic::FixedDeparture(fixed) => {
                if fixed.spawn_node != Some(first_node) {
                    warn!(
                        "Invalid path: first node {:?} does not match spawn node {:?}",
                        first_node, fixed.spawn_node
                    );
                    return false;
                }
            }
            CostCharacteristic::DepartureTimeChoice(_) => {
                if graph.node(first_node).node_type != NodeType::Wait(commodity.od_pair.origin) {
                    warn!(
                        "Invalid path: first node {:?} does not match origin {:?}",
                        first_node, commodity.od_pair.origin
                    );
                    return false;
                }
            }
        }
    }

    if let Some(&last_edge) = path.edges().last() {
        let last_node = graph.edge(last_edge).to;
        let node = graph.node(last_node);
        if node.node_type != NodeType::Wait(commodity.od_pair.destination) {
            return false;
        }
    }

    true
}

pub fn import_flow<'a>(
    graph: &Graph,
    paths: Option<PathsIndex<'a>>,
    filename: &str,
) -> Result<(Flow, PathsIndex<'a>), ImportFlowError> {
    let connection =
        sqlite::Connection::open_with_flags(filename, OpenFlags::default().with_read_only())
            .unwrap();

    let mut paths: PathsIndex = match paths {
        Some(it) => it,
        None => PathsIndex::new(),
    };

    struct DBPathEdge {
        path_id: i64,
        edge_id: i64,
    }

    let path_index = connection
        .prepare("SELECT path_id, edge_id FROM path_edge ORDER BY path_id, path_edge_index;")
        .map_err(ImportFlowError::SqliteError)?
        .iter()
        .map(|it| match it {
            Err(err) => Err(ImportFlowError::SqliteError(err)),
            Ok(it) => Ok(DBPathEdge {
                path_id: it.read(0),
                edge_id: it.read(1),
            }),
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .chunk_by(|it| it.path_id)
        .into_iter()
        .map(|(path_id, edges_iterator)| {
            let edges = edges_iterator
                .map(|it| EdgeIdx(it.edge_id as u32))
                .collect_vec();
            (path_id, edges)
        })
        .collect::<HashMap<i64, Vec<EdgeIdx>>>();

    let mut flow = Flow::new();

    connection
        .prepare("SELECT id, commodity, flow FROM path;")
        .map_err(ImportFlowError::SqliteError)?
        .iter()
        .map(|it| {
            it.map_or_else(
                |it| Err(ImportFlowError::SqliteError(it)),
                |it| {
                    let path_id: i64 = it.read(0);
                    let commodity_id: i64 = it.read(1);
                    let flow: f64 = it.read(2);
                    Ok((path_id, commodity_id, flow))
                },
            )
        })
        .try_for_each(|it| {
            it.and_then(|(path_id, commodity_id, path_flow)| {
                let empty = vec![];
                let edges = &path_index.get(&path_id).unwrap_or(&empty);
                let path = PathBox::new(CommodityIdx(commodity_id as u32), edges.iter().copied());
                if !path_is_valid(graph, path.payload()) {
                    return Err(ImportFlowError::InvalidPath { path_id });
                }
                let path_id = paths.transfer_path(path);
                flow.add_flow_onto_path(&paths, path_id, path_flow, true, false);
                Ok(())
            })
        })?;

    Ok((flow, paths))
}
