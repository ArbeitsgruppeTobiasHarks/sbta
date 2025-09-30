import os
import json
import subprocess
import sys
from typing import Callable, Dict, List, TypeVar


def run(command_str: List[str]) -> str | None:
    proc = subprocess.run(command_str, text=True, capture_output=True)
    if proc.returncode != 0 or proc.stdout is None:
        print("Error running command: ", " ".join(command_str))
        print("Error code:   ", proc.returncode)
        print("Error output: ", proc.stderr.strip())
        return None
    return proc.stdout

def run_sqlite(file: str, statement: str) -> float | None:
    test = run(["sqlite3", "-readonly", "-list", "-noheader", file, statement])
    if test is not None:
        return float(test.strip())
    return None

def sqlite(file: str, column: str, table: str = "FLOW_STATS") -> float | None:
    return run_sqlite(file, f"SELECT {column} FROM {table};")

T1 = TypeVar('T1')
T2 = TypeVar('T2')

def let(obj: T1 | None, map: Callable[[T1], T2]) -> T2 | None:
    if obj is not None:
        return map(obj)
    return None 

def collect_entry(network_config: dict, demand_config: dict, dtc: bool) -> dict:
    entry = {}
    entry["folder"] = network_config["folder"]
    entry["factor"] = demand_config["factor"]
    demand_folder = str(demand_config["factor"])
    if not dtc:
        demand_folder = demand_folder + "-no-dtc"
    subfolder = os.path.join(network_config["folder"], demand_folder)
    if not os.path.exists(subfolder):
        print("WARN: Could not find folder: ", subfolder)
        return entry
    
    time_units_in_hour = network_config["timeUnitsInHour"]
    mins_per_unit = 60 / time_units_in_hour

    entry["total_demand"] = entry["factor"] * network_config['nominalDemand']

    sbta_eqflow = os.path.join(subfolder, "sbta-eqflow.sqlite3")
    entry["eq_cost"] = sqlite(sbta_eqflow, "COST") * mins_per_unit
    entry["eq_mean_cost"] = entry["eq_cost"] / entry["total_demand"]
    entry["eq_total_regret"] = sqlite(sbta_eqflow, "TOTAL_REGRET") * mins_per_unit
    entry["eq_mean_regret"] = sqlite(sbta_eqflow, "MEAN_REGRET") * mins_per_unit
    entry["eq_mean_relative_regret"] = sqlite(sbta_eqflow, "MEAN_RELATIVE_REGRET")
    entry["eq_max_relative_regret"] = sqlite(sbta_eqflow, "MAX_RELATIVE_REGRET")
    entry["eq_demand_outside"] = sqlite(sbta_eqflow, "DEMAND_OUTSIDE")
    entry["eq_compute_time"] = let(sqlite(sbta_eqflow, "COMPUTATION_TIME_MS"), lambda x: x / 1000)
    entry["eq_num_iterations"] = sqlite(sbta_eqflow, "NUM_ITERATIONS")
    entry["eq_demand_prerouted"] = sqlite(sbta_eqflow, "DEMAND_PREROUTED")
    entry["eq_num_boarding_edges_blocked_by_prerouting"] = sqlite(sbta_eqflow, "NUM_BOARDING_EDGES_BLOCKED_BY_PREROUTING")
    entry["eq_num_commodities_fully_prerouted"] = sqlite(sbta_eqflow, "NUM_COMMODITIES_FULLY_PREROUTED")
    entry["eq_demand_no_regret"] = run_sqlite(sbta_eqflow, "SELECT percentile FROM REGRET_PERCENTILES WHERE REGRET = 0;")
    entry["eq_regret_99_percentile"] = run_sqlite(sbta_eqflow, "SELECT COALESCE(MAX(regret), 0) FROM REGRET_PERCENTILES WHERE percentile <= 0.99;") * mins_per_unit
    entry["eq_rel_regret_99_percentile"] = run_sqlite(sbta_eqflow, "SELECT COALESCE(MAX(relative_regret), 0) FROM RELATIVE_REGRET_PERCENTILES WHERE percentile <= 0.99;")

    sbta_optflow = os.path.join(subfolder, "sbta-optflow.sqlite3")
    entry["opt_cost"] = sqlite(sbta_optflow, "COST") * mins_per_unit
    entry["opt_mean_cost"] = entry["opt_cost"] / entry["total_demand"]
    entry["opt_total_regret"] = sqlite(sbta_optflow, "TOTAL_REGRET") * mins_per_unit
    entry["opt_mean_regret"] = sqlite(sbta_optflow, "MEAN_REGRET") * mins_per_unit
    entry["opt_mean_relative_regret"] = sqlite(sbta_optflow, "MEAN_RELATIVE_REGRET")
    entry["opt_max_relative_regret"] = sqlite(sbta_optflow, "MAX_RELATIVE_REGRET")
    entry["opt_demand_outside"] = sqlite(sbta_optflow, "DEMAND_OUTSIDE")
    entry["opt_compute_time"] = let(sqlite(sbta_optflow, "COMPUTATION_TIME_MS"), lambda x: x / 1000)
    entry["opt_demand_no_regret"] = run_sqlite(sbta_optflow, "SELECT percentile FROM REGRET_PERCENTILES WHERE REGRET = 0;")
    entry["opt_regret_99_percentile"] = run_sqlite(sbta_optflow, "SELECT COALESCE(MAX(regret), 0) FROM REGRET_PERCENTILES WHERE percentile <= 0.99;") * mins_per_unit
    entry["opt_rel_regret_99_percentile"] = run_sqlite(sbta_optflow, "SELECT COALESCE(MAX(relative_regret), 0) FROM RELATIVE_REGRET_PERCENTILES WHERE percentile <= 0.99;")

    entry["efficiency"] = entry["eq_cost"] / entry["opt_cost"]
    return entry

def fmt(val, decimals):
    if val is None:
        return "NA"
    return f"{val:.{decimals}f}"


def get_cell(entry: Dict[str, float], column: str, decimals=3, map: Callable[[float], float] = lambda x: x) -> str:
    eq_val = entry['eq_' + column]
    opt_val = entry['opt_' + column]
    return f"\\Cell{{{fmt(map(eq_val), decimals)}}}{{{fmt(map(opt_val), decimals)}}}"

def collate(path_to_jsons: List[str], dtc: bool):
    entries = []
    for path_to_json in path_to_jsons:
        with open(path_to_json, 'r') as f:
            data = json.load(f)
        for demand in data["demands"]:
            entries.append(collect_entry(data, demand, dtc))

    collated_name = "collated-dtc.json" if dtc else "collated-no-dtc.json"
    with open(os.path.join(os.getcwd(), collated_name), 'w') as f:
        json.dump(entries, f, indent=4)
    
    table_name = "table-dtc.txt" if dtc else "table-no-dtc.txt"
    with open(os.path.join(os.getcwd(), table_name), 'w') as f:
        for entry in entries:
            f.write(
            f"{entry['folder']} & "
            f"{get_cell(entry, 'mean_cost')} & "
            f"{get_cell(entry, 'mean_regret')} & "
            f"{get_cell(entry, 'regret_99_percentile', decimals=1)} & "
            f"{get_cell(entry, 'mean_relative_regret', map=lambda x: 1 + x, decimals=3)} & "
            f"{get_cell(entry, 'rel_regret_99_percentile', map=lambda x: 1 + x, decimals=3)} & "
            f"{get_cell(entry, 'demand_no_regret', map=lambda x: x*100, decimals=1)} & "
            f"{get_cell(entry, 'compute_time', 1)} \\\\ % "
            f"{fmt(entry['eq_mean_cost'] / entry['opt_mean_cost'], decimals=3)} \n"
            )
            
if __name__ == '__main__':
    collate(sys.argv[1:], True)
    collate(sys.argv[1:], False)
