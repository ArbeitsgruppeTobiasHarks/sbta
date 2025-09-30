#!/bin/bash

##############
# INPUTS:
# $1: Path to the experiment JSON file
##############

JSON_FILE=$1

# Check if JSON file is provided:
if [ -z $JSON_FILE ]; then
    echo "Error: No file provided"
    exit 1
fi
# Check if JSON file exists:
if [ ! -f $JSON_FILE ]; then
    echo "Error: No file found at $JSON_FILE"
    exit 1
fi

BASE=$(realpath $(dirname $0))


DATA_FOLDER=$(jq -r ".folder" $JSON_FILE)
DATA_FOLDER=$(realpath $DATA_FOLDER)
NOMINAL_DEMAND=$(jq -r ".nominalDemand" $JSON_FILE)
ROLLS=$(jq -r ".rolls" $JSON_FILE)
ROLLS_WITHOUT_DEMAND=$(jq -r ".rollsWithoutDemand" $JSON_FILE)
COMMODITY_GENERATION_INTERVAL=$(jq -r ".commodityGenerationInterval" $JSON_FILE)
OUTSIDE_OPTION=$(jq -r ".outsideOption" $JSON_FILE)
DEFAULT_VEHICLE_CAPACITY=$(jq -r ".defaultVehicleCapacity" $JSON_FILE)
TIME_UNITS_IN_HOUR=$(jq -r ".timeUnitsInHour" $JSON_FILE)

cargo build -r || exit 1

SBTA=$(realpath "$(dirname $0)/target/release/sbta")
export LD_LIBRARY_PATH=$(realpath ~/Downloads/gurobi1100/linux64/lib/)

for DEMAND_IDX in $(jq -r ".demands | keys | .[]" $JSON_FILE); do

    DEMAND_FACTOR=$(jq -r ".demands[$DEMAND_IDX].factor" $JSON_FILE)
    NUM_SHUFFLES=$(jq -r ".demands[$DEMAND_IDX].num_shuffles" $JSON_FILE)

    echo ""
    echo ""
    echo "DEMAND FACTOR $DEMAND_FACTOR"
    echo "================="
    echo ""
    echo ""


    mkdir -p "$DATA_FOLDER/$DEMAND_FACTOR"
    mkdir -p "$DATA_FOLDER/$DEMAND_FACTOR-no-dtc"

    (
        cd "$DATA_FOLDER" ;

        UNROLL_ARGS=""
        UNROLL_ARGS+="--default-vehicle-capacity=$DEFAULT_VEHICLE_CAPACITY "
        UNROLL_ARGS+="-n $NOMINAL_DEMAND "
        UNROLL_ARGS+="-r $ROLLS "
        UNROLL_ARGS+="--rolls-without-demand $ROLLS_WITHOUT_DEMAND "
        UNROLL_ARGS+="-i $COMMODITY_GENERATION_INTERVAL "
        UNROLL_ARGS+="-f $DEMAND_FACTOR "
        UNROLL_ARGS+="-q $OUTSIDE_OPTION "
        UNROLL_ARGS+="--dp-path $BASE/dp.csv "
        UNROLL_ARGS+="--dp-shift 5 "
        UNROLL_ARGS+="--time-units-per-hour $TIME_UNITS_IN_HOUR "
        UNROLL_ARGS+="--departure-time-choice "
        UNROLL_ARGS+="-o $DEMAND_FACTOR/sbta-graph.sqlite3 "

        (set -x ; LOG=INFO $SBTA unroll $UNROLL_ARGS ) ;

        cd "$DEMAND_FACTOR" ;

        LOG=INFO $SBTA run --log-iteration-count 100 --shuffle-count $NUM_SHUFFLES --initial-solution sbta-initial-solution.sqlite3 ;

        (set -x ; LOG=INFO $SBTA opt ) ;

        # NOW WITHOUT DEPARTURE TIME CHOICE

        cd "$DATA_FOLDER" ;

        UNROLL_ARGS=""
        UNROLL_ARGS+="--default-vehicle-capacity=$DEFAULT_VEHICLE_CAPACITY "
        UNROLL_ARGS+="-n $NOMINAL_DEMAND "
        UNROLL_ARGS+="-r $ROLLS "
        UNROLL_ARGS+="--rolls-without-demand $ROLLS_WITHOUT_DEMAND "
        UNROLL_ARGS+="-i $COMMODITY_GENERATION_INTERVAL "
        UNROLL_ARGS+="-f $DEMAND_FACTOR "
        UNROLL_ARGS+="-q $OUTSIDE_OPTION "
        UNROLL_ARGS+="--dp-path $BASE/dp.csv "
        UNROLL_ARGS+="--dp-shift 5 "
        UNROLL_ARGS+="--time-units-per-hour $TIME_UNITS_IN_HOUR "
        UNROLL_ARGS+="-o $DEMAND_FACTOR-no-dtc/sbta-graph.sqlite3 "

        (set -x ; LOG=INFO $SBTA unroll $UNROLL_ARGS ) ;

        cd "$DEMAND_FACTOR-no-dtc" ;

        LOG=INFO $SBTA run --log-iteration-count 100 --shuffle-count $NUM_SHUFFLES --initial-solution sbta-initial-solution.sqlite3 ;

        (set -x ; LOG=INFO $SBTA opt ) ;

    )
done

python3 collate.py $JSON_FILE
