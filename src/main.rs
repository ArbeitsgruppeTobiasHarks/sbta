#![feature(alloc_layout_extra, ptr_metadata)]
#![allow(dead_code)]

use std::fs::File;
use std::path::Path;
use std::process::exit;
use std::time::Duration;
use std::{io, usize};

use col::map_new;
use heuristic::multi_phase_strategy::MultiPhaseStrategyPlan;
use heuristic::{FlowStats, Stats, eq_heuristic, eq_heuristic_with_buffer_initial};
use log::{error, info};

use clap::{Args, Parser, Subcommand};
use graph::Graph;
use path_index::PathsIndex;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serialization::graph::import_graph;
use test::random_samples;
use timpass::unroll_timetable;

use crate::cmcf::write_cmcf_input;
use crate::serialization::flow::export_flow;
use crate::serialization::graph::export_graph;

mod cmcf;
mod col;
mod collections;
mod flow;
mod graph;
mod heuristic;
mod indexer;
mod iter;
mod opt;
mod path_index;
mod primitives;
mod regret;
mod relation;
mod serialization;
mod shortest_path;
mod test;
mod timer;
mod timpass;
mod total_order;
mod vehicle;

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

    #[command(about = "Run random sample")]
    RunRandom,

    #[command(about = "Compute system optimal flow")]
    Opt(OptArgs),

    #[command(about = "Write a CMCF input file given an unrolled graph")]
    WriteCmcfInput(WriteCmcfInputArgs),

    #[command(about = "Reads a CMCF output file and transforms it to a sbta-compatible flow")]
    TransformCmcfOutput(TransformCmcfOutputArgs),
}

#[derive(Args, Clone, Debug)]
struct OptArgs {
    #[arg(
        short = 'i',
        long,
        default_value = "sbta-graph.sqlite3",
        help = "The unrolled graph file."
    )]
    graph_filename: String,

    #[arg(
        short = 'o',
        long,
        default_value = "sbta-optflow.sqlite3",
        help = "The file to write the system optimal flow to."
    )]
    out_filename: String,
}
fn main_opt(args: &OptArgs) {
    if Path::new(&args.out_filename).exists() {
        error!("Output file already exists: {}", args.out_filename);
        exit(1);
    }

    let Ok(graph) = import_graph(&args.graph_filename) else {
        error!("Could not import graph: {}", args.graph_filename);
        exit(1);
    };
    let mut paths = PathsIndex::new();
    let (flow, a_star_table, stats) = opt::compute_opt_with_stats(&graph, &mut paths);
    let regret_map = regret::get_regret_map(&graph, &flow, &paths, &a_star_table);
    export_flow(
        &graph,
        Some(&stats),
        &flow,
        &paths,
        &regret_map,
        &args.out_filename,
    );
}

#[derive(Args, Clone, Debug)]
struct WriteCmcfInputArgs {
    #[arg(
        short = 'i',
        long,
        default_value = "sbta-graph.sqlite3",
        help = "The unrolled graph file."
    )]
    graph_filename: String,

    #[arg(
        short = 'o',
        long,
        help = "The file to write the CMCF input file to.",
        default_value = "cmcf-input.sqlite3"
    )]
    out_filename: String,
}
fn main_write_cmcf_input(args: &WriteCmcfInputArgs) {
    if Path::new(&args.out_filename).exists() {
        error!("Output file already exists: {}", args.out_filename);
        exit(1);
    }

    let graph = import_graph(&args.graph_filename).unwrap_or_else(|it| {
        error!("Could not import graph:\n{:#?}", it);
        exit(1);
    });

    write_cmcf_input(&graph, &args.out_filename).unwrap_or_else(|it| {
        error!("Could not write CMCF input:\n{:#?}", it);
        exit(1);
    });
}

#[derive(Args, Clone, Debug)]
struct TransformCmcfOutputArgs {
    #[arg(
        short = 'i',
        long,
        default_value = "cmcf-output.sqlite3",
        help = "The CMCF output file."
    )]
    cmcf_output_filename: String,

    #[arg(
        short = 'g',
        long,
        default_value = "sbta-graph.sqlite3",
        help = "The unrolled graph file."
    )]
    sbta_graph_filename: String,

    #[arg(
        short = 'o',
        long,
        default_value = "sbta-optflow.sqlite3",
        help = "The file to write the sbta-compatible flow to."
    )]
    flow_out_filename: String,
}
fn main_flow_from_cmcf_output(args: &TransformCmcfOutputArgs) {
    if Path::new(&args.flow_out_filename).exists() {
        error!("Output file already exists: {}", args.flow_out_filename);
        exit(1);
    }

    let graph = import_graph(&args.sbta_graph_filename).unwrap_or_else(|it| {
        error!("Could not import graph:\n{:#?}", it);
        exit(1);
    });

    let (flow, paths) =
        cmcf::read_cmcf_output(&graph, &args.cmcf_output_filename).unwrap_or_else(|it| {
            error!("Could not parse CMCF output:\n{:#?}", it);
            exit(1);
        });

    let stats = Stats {
        flow: FlowStats {
            cost: flow.cost(&paths, &graph),
            demand_outside: flow
                .path_flow_map()
                .iter()
                .filter(|(path_id, _)| paths.path(**path_id).is_outside())
                .map(|(_, v)| v)
                .sum(),
            max_relative_regret: 0.0,
            mean_relative_regret: 0.0,
            total_regret: 0.0,
            num_regretting_paths: 0,
            regretting_demand: 0.0,
        },
        computation_time: std::time::Duration::from_secs(0),
        heuristic: None,
    };

    export_flow(
        &graph,
        Some(&stats),
        &flow,
        &paths,
        &map_new(),
        &args.flow_out_filename,
    );
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
struct HeuristicArgs {
    #[arg(long = "max-iterations", help = "The maximum number of iterations.")]
    max_iterations: Option<usize>,

    #[arg(
        long = "initial-solution",
        help = "The path to the initial solution flow. If given and a file exists, the heuristic will start from this solution. If given, and the file does not exist, the heuristic will start from scratch and write its initial solution to this path."
    )]
    initial_solution: Option<String>,
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

    #[clap(flatten)]
    heuristic_args: HeuristicArgs,
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

    let duration = Duration::from_secs(120 * 60);
    let strategy_builder = MultiPhaseStrategyPlan {
        iterations_simple: 0,
        duration_simple: duration,
        iterations_total_regret: 0,
        duration_total_regret: duration,
        iterations_total_rel_regret: usize::MAX, 
        duration_total_rel_regret: duration,
        iterations_max_rel_regret: 0,
        duration_max_rel_regret: duration,
    };

    let (flow, paths, stats, a_star_table) =
        if let Some(initial_solution_path) = &args.heuristic_args.initial_solution {
            eq_heuristic_with_buffer_initial(
                &graph,
                args.log_iteration_count,
                strategy_builder,
                initial_solution_path,
            )
        } else {
            eq_heuristic(&graph, args.log_iteration_count, strategy_builder)
        };

    info!("Computing regret map...");

    let regret_map = regret::get_regret_map(&graph, &flow, &paths, &a_star_table);

    info!("Exporting flow...");
    export_flow(
        &graph,
        Some(&stats),
        &flow,
        &paths,
        &regret_map,
        &args.out_filename,
    )
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

    #[arg(short, long, default_value = "Lines.csv")]
    lines_path: String,

    #[arg(long, default_value = "1000")]
    default_vehicle_capacity: f64,

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
        long,
        help = "The number of rolls without generating demand at the beginning and at the end of the time frame."
    )]
    rolls_without_demand: usize,

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

    #[arg(long, help = "Whether departure time choice should be allowed.")]
    departure_time_choice: bool,

    #[arg(
        short = 'o',
        long,
        help = "The file to write the unrolled time-expanded graph to.",
        default_value = "sbta-graph.sqlite3"
    )]
    out_filename: String,

    #[clap(flatten)]
    dynamic_profile_args: Option<DynamicProfileArgs>,
}

#[derive(Args, Clone, Debug)]
struct DynamicProfileArgs {
    #[arg(
        long,
        help = "The path to the dynamic profile file. A CSV file with columns [time, demand_share] where time is the time of day in hours."
    )]
    dp_path: String,

    #[arg(
        long,
        help = "The time (in hours) the dynamic profile is shifted to the left."
    )]
    dp_shift: f64,

    #[arg(
        long,
        help = "One hour in the time unit of the time table.",
        default_value_t = 60.0
    )]
    time_units_per_hour: f64,
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
    let lines = match File::open(&args.lines_path) {
        io::Result::Ok(file) => timpass::parse_lines(file).unwrap(),
        io::Result::Err(err) => {
            info!("No lines file found: {}", err);
            Vec::new().into_boxed_slice()
        }
    };
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
        &lines,
        args.default_vehicle_capacity,
        &config,
        args.rolls,
        args.rolls_without_demand,
        args.commodity_generation_interval,
        args.outside_option,
        args.departure_time_choice,
        args.dynamic_profile_args.as_ref(),
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
        Commands::Opt(args) => main_opt(&args),
        Commands::RunRandom => random_samples::run_samples(),
        Commands::WriteCmcfInput(args) => main_write_cmcf_input(&args),
        Commands::TransformCmcfOutput(args) => main_flow_from_cmcf_output(&args),
    }
}
