import os
import json
import subprocess
import sys
from typing import List


def run(command_str: List[str]) -> str | None:
    proc = subprocess.run(command_str, text=True, capture_output=True)
    if proc.returncode != 0 or proc.stdout is None:
        print("Error running command: ", " ".join(command_str))
        print("Error code:   ", proc.returncode)
        print("Error output: ", proc.stderr.strip())
        return None
    return proc.stdout

def sqlite(file: str, column: str) -> float | None:
    test = run(["sqlite3", "-readonly", "-noheader", "-list", file, f"SELECT {column} FROM FLOW_STATS;"])
    if test is not None:
        return float(test.strip())
    return None


def collate(path_to_json: str):
    with open(path_to_json, 'r') as f:
        data = json.load(f)
    folder = data['folder']
    entries = []
    for demand in data["demands"]:
        factor = demand["factor"]
        subfolder = os.path.join(folder, str(factor))
        if not os.path.exists(subfolder):
            print("Could not find folder: ", subfolder)
            continue
        sbta_eqflow = os.path.join(subfolder, "sbta-eqflow.sqlite3")
        eq_cost = sqlite(sbta_eqflow, "COST")
        eq_total_regret = sqlite(sbta_eqflow, "TOTAL_REGRET")
        eq_mean_regret = sqlite(sbta_eqflow, "MEAN_REGRET")
        eq_mean_relative_regret = sqlite(sbta_eqflow, "MEAN_RELATIVE_REGRET")
        eq_max_relative_regret = sqlite(sbta_eqflow, "MAX_RELATIVE_REGRET")
        eq_demand_outside = sqlite(sbta_eqflow, "DEMAND_OUTSIDE")
        eq_compute_time = sqlite(sbta_eqflow, "COMPUTATION_TIME_MS")
        eq_num_iterations = sqlite(sbta_eqflow, "NUM_ITERATIONS")
        eq_demand_prerouted = sqlite(sbta_eqflow, "DEMAND_PREROUTED")
        eq_num_boarding_edges_blocked_by_prerouting = sqlite(sbta_eqflow, "NUM_BOARDING_EDGES_BLOCKED_BY_PREROUTING")
        eq_num_commodities_fully_prerouted = sqlite(sbta_eqflow, "NUM_COMMODITIES_FULLY_PREROUTED")

        sbta_optflow = os.path.join(subfolder, "sbta-optflow.sqlite3")
        opt_cost = sqlite(sbta_optflow, "COST")
        opt_total_regret = sqlite(sbta_optflow, "TOTAL_REGRET")
        opt_mean_regret = sqlite(sbta_optflow, "MEAN_REGRET")
        opt_mean_relative_regret = sqlite(sbta_optflow, "MEAN_RELATIVE_REGRET")
        opt_max_relative_regret = sqlite(sbta_optflow, "MAX_RELATIVE_REGRET")
        opt_demand_outside = sqlite(sbta_optflow, "DEMAND_OUTSIDE")
        opt_compute_time = sqlite(sbta_optflow, "COMPUTATION_TIME_MS")

        eq_mean_cost = eq_cost / (factor * data['nominalDemand']) if eq_cost is not None else None
        opt_mean_cost = opt_cost / (factor * data['nominalDemand']) if opt_cost is not None else None
        efficiency = eq_cost / opt_cost if eq_cost is not None and opt_cost is not None else None

        entries.append({
            "factor": factor,
            "eq_cost": eq_cost,
            "eq_mean_cost": eq_mean_cost,
            "eq_total_regret": eq_total_regret,
            "eq_mean_regret": eq_mean_regret,
            "eq_mean_relative_regret": eq_mean_relative_regret,
            "eq_max_relative_regret": eq_max_relative_regret,
            "eq_demand_outside": eq_demand_outside,
            "eq_compute_time": eq_compute_time / 1000 if eq_compute_time is not None else None,
            "eq_num_iterations": eq_num_iterations,
            "eq_demand_prerouted": eq_demand_prerouted,
            "eq_num_boarding_edges_blocked_by_prerouting": eq_num_boarding_edges_blocked_by_prerouting,
            "eq_num_commodities_fully_prerouted": eq_num_commodities_fully_prerouted,
            "opt_compute_time": opt_compute_time / 1000 if opt_compute_time is not None else None,
            "opt_cost": opt_cost,
            "opt_mean_cost": opt_mean_cost,
            "opt_total_regret": opt_total_regret,
            "opt_mean_regret": opt_mean_regret,
            "opt_mean_relative_regret": opt_mean_relative_regret,
            "opt_max_relative_regret": opt_max_relative_regret,
            "opt_demand_outside": opt_demand_outside,
            "efficiency": efficiency
        })

    with open(os.path.join(folder, "collated.json"), 'w') as f:
        json.dump(entries, f, indent=4)

    keys = entries[0].keys()
    with open(os.path.join(folder, "coordinates.tex"), 'w') as f:
        for key in keys:
            if key == 'factor':
                continue

            f.write(f"\n% {key}\n")
            for entry in entries:
                if entry[key] is None:
                    continue
                # write to file
                f.write(f"({entry['factor']}, {entry[key]})\n")

if __name__ == '__main__':
    collate(sys.argv[1])
