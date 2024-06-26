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


DATA_FOLDER=$(jq -r ".folder" $JSON_FILE)
NOMINAL_DEMAND=$(jq -r ".nominalDemand" $JSON_FILE)
ROLLS=$(jq -r ".rolls" $JSON_FILE)
COMMODITY_GENERATION_INTERVAL=$(jq -r ".commodityGenerationInterval" $JSON_FILE)
OUTSIDE_OPTION=$(jq -r ".outsideOption" $JSON_FILE)

cargo build -r || exit 1

SBTA=$(realpath "$(dirname $0)/target/release/sbta")

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
    
    (
        cd "$DATA_FOLDER" ;

        LOG=INFO $SBTA unroll -n $NOMINAL_DEMAND -r $ROLLS -i $COMMODITY_GENERATION_INTERVAL -f $DEMAND_FACTOR -q $OUTSIDE_OPTION -o "$DEMAND_FACTOR/sbta-graph.sqlite3" ;

        cd "$DEMAND_FACTOR" ;

        LOG=INFO $SBTA run --log-iteration-count 100 --shuffle-count $NUM_SHUFFLES ;
    )
done

python3 collate.py $JSON_FILE
