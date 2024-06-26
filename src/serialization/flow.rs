use sqlite::OpenFlags;

use crate::{flow::Flow, graph::Graph, heuristic::Stats, paths_index::PathsIndex};

pub fn export_flow(
    graph: &Graph,
    stats: Option<&Stats>,
    flow: &Flow,
    paths: &PathsIndex,
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
        flow REAL NOT NULL
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
        .prepare("INSERT INTO path (id, commodity, flow) VALUES (?, ?, ?)")
        .unwrap();

    let mut stmt_edge = connection
        .prepare("INSERT INTO path_edge (path_id, edge_id, path_edge_index) VALUES (?, ?, ?);")
        .unwrap();
    for (path_id, path_flow) in flow.path_flow_map().iter() {
        let path = paths.path(*path_id);

        stmt_path.bind((1, path_id.0 as i64)).unwrap();
        stmt_path.bind((2, path.commodity_idx().0 as i64)).unwrap();
        stmt_path.bind((3, *path_flow)).unwrap();
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
                    DEMAND_OUTSIDE REAL NOT NULL,
                    COMPUTATION_TIME_MS INT NOT NULL,
                    NUM_ITERATIONS INT NOT NULL,
                    DEMAND_PREROUTED REAL NOT NULL,
                    NUM_BOARDING_EDGES_BLOCKED_BY_PREROUTING INT NOT NULL,
                    NUM_COMMODITIES_FULLY_PREROUTED INT NOT NULL
                );",
            )
            .unwrap();
        let mut stmt = connection
            .prepare(
                "INSERT INTO flow_stats(
                        COST,
                        DEMAND_OUTSIDE,
                        COMPUTATION_TIME_MS,
                        NUM_ITERATIONS,
                        DEMAND_PREROUTED,
                        NUM_BOARDING_EDGES_BLOCKED_BY_PREROUTING,
                        NUM_COMMODITIES_FULLY_PREROUTED
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .unwrap();
        stmt.bind((1, stats.cost)).unwrap();
        stmt.bind((2, stats.demand_outside)).unwrap();
        stmt.bind((3, stats.computation_time.as_millis() as i64))
            .unwrap();
        stmt.bind((4, stats.num_iterations as i64)).unwrap();
        stmt.bind((5, stats.demand_prerouted)).unwrap();
        stmt.bind((6, stats.num_boarding_edges_blocked_by_prerouting as i64))
            .unwrap();
        stmt.bind((7, stats.num_commodities_fully_prerouted as i64))
            .unwrap();

        stmt.next().unwrap();
    }

    connection.execute("END TRANSACTION;").unwrap();
}
