from typing import Dict
import os
import pprint
import sys
import math
import matplotlib.pyplot as plt


def read_many_latencies(dir_path: str):
    latencies = {}
    for filename in os.listdir(dir_path):
        if "TPC-C" in filename and filename.endswith(".txt"):
            file_path = os.path.join(dir_path, filename)
            sf = filename.split("SF_")[-1].split(".")[0]
            latencies[sf] = calculate_average(read_latencies(file_path))

    return latencies


def read_latencies(file_path: str) -> Dict[int, Dict[str, float]]:
    latencies: Dict[int, Dict[str, float]] = {}
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(":")
            if len(parts) == 2:
                query_number, time_part = parts
                sqlite_time, duckdb_time = time_part.split(",")
                if latencies.get(query_number) is None:
                    latencies[query_number] = {
                        "sqlite": [float(sqlite_time.split("=")[1].split('s')[0])],
                        "duckdb": [float(duckdb_time.split("=")[1].split('s')[0])],
                    }
                else:
                    latencies[query_number]["sqlite"].append(
                        float(sqlite_time.split("=")[1].split('s')[0]))
                    latencies[query_number]["duckdb"].append(
                        float(duckdb_time.split("=")[1].split('s')[0]))

    return latencies


def calculate_average(latencies: Dict[int, Dict[str, float]]) -> Dict[int, Dict[str, float]]:
    for query_number, times in latencies.items():
        sqlite_times = times["sqlite"]
        duckdb_times = times["duckdb"]
        avg_sqlite_time = sum(sqlite_times) / len(sqlite_times)
        avg_duckdb_time = sum(duckdb_times) / len(duckdb_times)
        latencies[query_number] = {
            "sqlite": avg_sqlite_time,
            "duckdb": avg_duckdb_time
        }
    return latencies


def create_bar_graph(latencies, savename: str) -> None:
    """
    Creates a single grouped bar chart for all scale factors and queries,
    showing SQLite and DuckDB latencies per query, with a log y-scale.
    SQLite bars are different shades of blue (solid),
    DuckDB bars are different shades of orange (shaded with hatch).
    """
    import numpy as np
    import matplotlib.pyplot as plt

    scale_factors = sorted(latencies.keys(), key=lambda x: float(x))
    queries = sorted({q for sf in latencies.values() for q in sf.keys()})
    n_sf = len(scale_factors)
    n_queries = len(queries)
    engines = ['sqlite', 'duckdb']
    n_bars_per_query = n_sf * len(engines)

    # Define colors for each scale factor and engine
    sqlite_colors = plt.cm.Blues(np.linspace(0.5, 0.8, n_sf))
    duckdb_colors = plt.cm.Oranges(np.linspace(0.5, 0.8, n_sf))

    bar_width = 0.75 / n_bars_per_query
    group_gap = 0.35  # Space between scale factor groups

    x = np.arange(n_queries)

    plt.figure(figsize=(max(8, n_queries), 6))

    for i, sf in enumerate(scale_factors):
        for j, engine in enumerate(engines):
            vals = [latencies[sf][q][engine]
                    if q in latencies[sf] else 0.01 for q in queries]
            # Add extra gap between scale factor groups
            offset = ((i * len(engines) + j) + i * group_gap) - \
                n_bars_per_query / 2 + 0.5
            offset = offset * bar_width
            if engine == 'sqlite':
                color = sqlite_colors[i]
                plt.bar(
                    x + offset, vals, bar_width,
                    label=f'{engine.capitalize()} SF={sf}',
                    color=color, edgecolor='black'
                )
            else:
                color = duckdb_colors[i]
                plt.bar(
                    x + offset, vals, bar_width,
                    label=f'{engine.capitalize()} SF={sf}',
                    color=color, edgecolor='black', hatch="//"
                )

    plt.xticks(x, queries)
    plt.xlabel('Query')
    plt.ylabel('Latency (s) - Log Scale')
    plt.title('SQLite vs DuckDB Latencies for All Scale Factors TPC-C Benchmark')
    plt.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.savefig(savename)


def save_average_latencies(latencies: Dict[int, Dict[str, float]], filename: str) -> None:
    with open(filename, "w") as file:
        for sf, queries in sorted(latencies.items(), key=lambda x: float(x[0])):
            file.write(f"Scale Factor: {sf}\n")
            for query_number, times in queries.items():
                sqlite_time = times["sqlite"]
                duckdb_time = times["duckdb"]
                file.write(
                    f"{query_number}: SQLite={sqlite_time:.4f}s, DuckDB={duckdb_time:.4f}s\n")
            file.write("\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python make_graphs.py <latencies_dir>")
        sys.exit(1)

    latencies_dir: str = sys.argv[1]
    latencies = read_many_latencies(latencies_dir)
    print()
    print("Latencies:")
    pprint.pp(latencies)
    print()

    # Save average latencies to a file
    save_average_latencies(latencies, "tpc-c-latencies.txt")

    create_bar_graph(latencies, "tpc-c-graph.png")
    print()
