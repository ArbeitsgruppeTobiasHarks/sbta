# Computing User Equilibria for Schedule-Based Transit Networks with Hard Vehicle Capacities

This document describes how to run the heuristic for computing side-constrained user equilibria for schedule-based transit networks.

## Getting Started

These instructions assume GNU/Linux as operating system.
If compiled under Windows, the Rust binary should also work on Windows; however, the steps of the included shell script `experiment.sh` (see next section) need to be executed manually.

To get started, please install the Rust toolchain by installing [rustup](https://www.rust-lang.org/tools/install).
As our source code uses some experimental features of Rust, please use the nightly distribution of the Rust toolchain by running:
```sh
rustup default nightly
```

This project depends on the Gurobi solver in version 11.
To be able to compile the project, please first download the Gurobi API,
make sure to have a valid Gurobi license located in your home directory,
and add the environment variables
```sh
export GUROBI_HOME=/path/to/gurobi1100/linux64/
export LD_LIBRARY_PATH=/path/to/gurobi1100/linux64/lib
```

Furthermore, the project depends on `sqlite3`. Please make sure that this binary is available, e.g., on Ubuntu by running `sudo apt install sqlite3`.

To download all other dependencies and to compile the program, please run:
```sh
cargo run -r
```
This should also print usage instructions for the compiled binary.

Finally, make the shell scripts executable:
```sh
chmod +x all-experiments.sh experiment.sh
```

## Obtaining the data

You can obtain network instances from [TimPassLib](https://timpasslib.aalto.fi/).
The pre-configured experiments `exp-NETWORK.json` expect network instances to be located in the subfolder `data/NETWORK`.

## Running the experiments

To conduct the experiment for a specific network, please run `./experiment.sh exp-XYZ.json`.

To conduct the experiment for all networks sequentially, please run `./experiment_all.sh`.
