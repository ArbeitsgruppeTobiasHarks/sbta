#![feature(alloc_layout_extra, ptr_metadata)]

use std::fs::File;
use std::path::Path;
use std::process::exit;

use heuristic::eq_heuristic;
use log::{error, info};

use clap::{Args, Parser, Subcommand};
use graph::Graph;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serialization::graph::import_graph;
use timpass::unroll_timetable;

use crate::serialization::flow::export_flow;
use crate::serialization::graph::export_graph;

mod a_star;
mod best_paths;
mod col;
mod flow;
mod graph;
mod heuristic;
mod iter;
mod paths_index;
mod relation;
mod serialization;
mod timpass;

#[derive(Parser, Debug)]
#[command(
    version,
    author,
    about = "Heuristic for computing schedule-based traffic equilibria with hard capacity constraints"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Clone, Debug)]
enum Commands {
    #[command(about = "Unroll a timetable from TimPassLib")]
    Unroll(UnrollArgs),

    #[command(about = "Run heuristic on an unrolled graph")]
    Run(RunArgs),
}

#[derive(Args, Clone, Debug)]
struct TimPassArgs {
    #[arg(short, long, default_value = "Events.csv")]
    events_path: String,

    #[arg(short, long, default_value = "Activities.csv")]
    activities_path: String,

    #[arg(short, long, default_value = "OD.csv")]
    demands_path: String,

    #[arg(short, long, default_value = "LBRTimetable.csv")]
    timetable_path: String,

    #[arg(short, long, default_value = "Config.csv")]
    config_path: String,

    #[arg(long = "cmcf-in", default_value = "cmcf-input.sqlite3")]
    cmcf_input_path: String,

    #[arg(
        short = 'f',
        long,
        default_value_t = 1.0,
        help = "A factor by which the nominal demand is scaled before starting the heuristic."
    )]
    demand_scale_factor: f64,

    #[arg(
        short = 'n',
        long,
        help = "The total nominal demand (in units of flow). If not given, the total nominal demand is derived from the demand matrix."
    )]
    nominal_demand: Option<f64>,

    #[arg(
        short = 'r',
        long,
        help = "The number of times the timetable should be unrolled"
    )]
    rolls: usize,

    #[arg(
        short = 'i',
        long,
        help = "The interval (in units of time) at which a commodity should be generated for every OD pair."
    )]
    commodity_generation_interval: u32,

    #[arg(
        short = 'q',
        long,
        help = "The outside option (in units of time) of every particle."
    )]
    outside_option: u32,

    #[arg(
        short = 'o',
        long,
        help = "The file to write the flow and stats to.",
        default_value = "sbta-eqflow.sqlite3"
    )]
    out_filename: String,
}

#[derive(Args, Clone, Debug)]
struct RunArgs {
    #[arg(short = 'i', long, default_value = "sbta-graph.sqlite3")]
    graph_filename: String,

    #[arg(
        short = 'o',
        long,
        help = "The file to write the equilibrium flow and stats to.",
        default_value = "sbta-eqflow.sqlite3"
    )]
    out_filename: String,

    #[arg(long, help = "Only log every n-th iteration.", default_value_t = 1)]
    log_iteration_count: usize,

    #[arg(
        long,
        help = "The number of times, the commodities should be shuffled bevor starting the heuristic.",
        default_value_t = 0
    )]
    shuffle_count: usize,
}
fn main_run(args: &RunArgs) {
    if Path::new(&args.out_filename).exists() {
        error!("Output file already exists: {}", args.out_filename);
        exit(1);
    }

    let mut graph = import_graph(&args.graph_filename).unwrap_or_else(|it| {
        error!("Could not import graph:\n{:#?}", it);
        exit(1);
    });

    let seed: <ChaCha8Rng as SeedableRng>::Seed = Default::default();
    let mut rng = ChaCha8Rng::from_seed(seed);
    if args.shuffle_count > 0 {
        info!("Shuffling commodities {} times", args.shuffle_count);
    }
    for _ in 0..args.shuffle_count {
        graph.shuffle_commodities(&mut rng);
    }

    let (flow, paths, stats) = eq_heuristic(&graph, None, args.log_iteration_count);

    export_flow(&graph, Some(&stats), &flow, &paths, &args.out_filename)
}

#[derive(Args, Clone, Debug)]
struct UnrollArgs {
    #[arg(short, long, default_value = "Events.csv")]
    events_path: String,

    #[arg(short, long, default_value = "Activities.csv")]
    activities_path: String,

    #[arg(short, long, default_value = "OD.csv")]
    demands_path: String,

    #[arg(short, long, default_value = "LBRTimetable.csv")]
    timetable_path: String,

    #[arg(short, long, default_value = "Config.csv")]
    config_path: String,

    #[arg(
        short = 'f',
        long,
        default_value_t = 1.0,
        help = "A factor by which the nominal demand is scaled before starting the heuristic."
    )]
    demand_scale_factor: f64,

    #[arg(
        short = 'n',
        long,
        help = "The total nominal demand (in units of flow). If not given, the total nominal demand is derived from the demand matrix."
    )]
    nominal_demand: Option<f64>,

    #[arg(
        short = 'r',
        long,
        help = "The number of times the timetable should be unrolled"
    )]
    rolls: usize,

    #[arg(
        short = 'i',
        long,
        help = "The interval (in units of time) at which a commodity should be generated for every OD pair."
    )]
    commodity_generation_interval: u32,

    #[arg(
        short = 'q',
        long,
        help = "The outside option (in units of time) of every particle."
    )]
    outside_option: u32,

    #[arg(
        short = 'o',
        long,
        help = "The file to write the unrolled time-expanded graph to.",
        default_value = "sbta-graph.sqlite3"
    )]
    out_filename: String,
}
fn main_unroll(args: &UnrollArgs) {
    if Path::new(&args.out_filename).exists() {
        error!("Output file already exists: {}", args.out_filename);
        exit(1);
    }

    let events = timpass::parse_events(File::open(&args.events_path).unwrap()).unwrap();
    let activities = timpass::parse_activities(File::open(&args.activities_path).unwrap()).unwrap();
    let mut demands = timpass::parse_demands(File::open(&args.demands_path).unwrap()).unwrap();
    let timetable = timpass::parse_timetable(File::open(&args.timetable_path).unwrap()).unwrap();
    let config = timpass::parse_config(File::open(&args.config_path).unwrap()).unwrap();

    let od_matrix_demand = demands.iter().map(|d| d.customers).sum::<f64>();
    let nominal_demand = args.nominal_demand.unwrap_or(od_matrix_demand);
    let nominal_scale_factor = nominal_demand / od_matrix_demand;
    let overall_scale_factor = args.demand_scale_factor * nominal_scale_factor;

    if overall_scale_factor != 1.0 {
        info!(
            "Scaling demand by {}   ( = {} [Nominal Scale Factor] * {} [Demand Scale Factor] )",
            overall_scale_factor, nominal_scale_factor, args.demand_scale_factor
        );
        for demand in &mut *demands {
            demand.customers *= overall_scale_factor;
        }
    }
    info!("Total Demand: {}", od_matrix_demand * overall_scale_factor);

    let (vehicles, line_info_map, commodities) = unroll_timetable(
        &events,
        &activities,
        &demands,
        &timetable,
        1000.0,
        &config,
        args.rolls,
        args.commodity_generation_interval,
        args.outside_option,
    );
    info!("Number vehicles: {}", vehicles.len());
    info!("Number commodities: {}", commodities.len());
    info!(
        "Average number stops: {}",
        vehicles
            .iter()
            .map(|v| v.middle_stops.len() + 2)
            .sum::<usize>() as f64
            / vehicles.len() as f64
    );
    let (graph, _station_idx) = Graph::create(vehicles, &commodities);
    info!("Number stations: {}", graph.num_stations());
    export_graph(&graph, &line_info_map, &args.out_filename);
}

fn main() {
    env_logger::builder().parse_env("LOG").init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Unroll(args) => main_unroll(&args),
        Commands::Run(args) => main_run(&args),
    }
}
