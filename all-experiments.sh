#/usr/bin/bash

for file in ./exp-*.json; do

    (set -x ; ./experiment.sh $file )

done;

python3 collate_new.py ./exp-*.json

mkdir -p plots/dtc
mkdir -p plots/fdt


for JSON_FILE in ./exp-*.json; do

    FOLDER=$(jq -r ".folder" $JSON_FILE)
    DEMAND_FACTOR=$(jq -r ".demands[0].factor" $JSON_FILE)
    sqlite3 "$FOLDER/$DEMAND_FACTOR/sbta-optflow.sqlite3" -csv "SELECT * FROM RELATIVE_REGRET_PERCENTILES;" > "plots/dtc/$JSON_FILE.opt.csv"
    sqlite3 "$FOLDER/$DEMAND_FACTOR/sbta-eqflow.sqlite3" -csv "SELECT * FROM RELATIVE_REGRET_PERCENTILES;" > "plots/dtc/$JSON_FILE.eq.csv"

    sqlite3 "$FOLDER/$DEMAND_FACTOR-no-dtc/sbta-optflow.sqlite3" -csv "SELECT * FROM RELATIVE_REGRET_PERCENTILES;" > "plots/fdt/$JSON_FILE.opt.csv"
    sqlite3 "$FOLDER/$DEMAND_FACTOR-no-dtc/sbta-eqflow.sqlite3" -csv "SELECT * FROM RELATIVE_REGRET_PERCENTILES;" > "plots/fdt/$JSON_FILE.eq.csv"

    (set -x ; ./experiment.sh $file )

done;
