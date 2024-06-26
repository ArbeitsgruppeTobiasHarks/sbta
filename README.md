# Computing User Equilibria for Schedule-Based Transit Networks with Hard Vehicle Capacities

This document describes how to run the heuristic for computing side-constrained user equilibria for schedule-based transit networks.

## Getting Started

These instructions assume GNU/Linux as operating system.
If compiled under Windows, the Rust binary also works on Windows; however, the steps of the included shell script `experiment.sh` (see next section) need to be executed manually.
Please make sure to have the following tools installed:
* sqlite3, for accessing the generated sqlite files
* jq, for accessing JSON files

To get started, please install the Rust toolchain by installing [rustup](https://www.rust-lang.org/tools/install).
As our source code uses some experimental features of Rust, please use the nightly distribution of the Rust toolchain by running:
```sh
rustup default nightly
```

To download all dependencies and to compile the program, please run:
```sh
cargo run -r
```
This should also print usage instructions for the compiled binary.


## Running the experiments

To conduct the experiment for a specific network, please run `./experiment.sh exp-XYZ.json` where you may choose `XYZ` from `hamburg` and `swiss`.
After an equilibrium is computed for every demand factor, the results will be collected and written to `./data/XYZ/collated.json`.
