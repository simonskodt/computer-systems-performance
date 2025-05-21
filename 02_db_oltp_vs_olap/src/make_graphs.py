from typing import Dict
import os
import pprint
import sys
import math
import matplotlib.pyplot as plt


def read_latencies(file_path: str) -> Dict[int, Dict[str, float]]:
    latencies: Dict[int, Dict[str, float]] = {}
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(":")
            if len(parts) == 2:
                query_part, time_part = parts
                query_number = int(query_part.split()[1])
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


def create_bar_graph(latencies: Dict[int, Dict[str, float]], savename: str) -> None:
    queries = list(latencies.keys())
    sqlite_times = [latencies[q]["sqlite"] for q in queries]
    duckdb_times = [latencies[q]["duckdb"] for q in queries]

    x = range(len(queries))

    plt.figure(figsize=(10, 6))
    plt.bar(x, sqlite_times, width=0.4, label="SQLite",
            color='blue', align='center')
    plt.bar([i + 0.4 for i in x], duckdb_times, width=0.4,
            label="DuckDB", color='orange', align='center')

    plt.xlabel("Query Number")
    plt.ylabel("Time (seconds)")
    plt.title("SQLite vs DuckDB Latencies")
    plt.xticks([i + 0.2 for i in x], queries)
    plt.yscale('log')
    plt.legend()
    plt.tight_layout()
    plt.savefig(savename)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python make_graphs.py <latencies_file>")
        sys.exit(1)

    latencies_file: str = sys.argv[1]
    latencies = read_latencies(latencies_file)
    print()
    print("Latencies:")
    pprint.pp(latencies)
    print()

    avg_latencies = calculate_average(latencies)
    print("Average Latencies:")
    pprint.pp(avg_latencies)
    print()

    savename = latencies_file.split(".")[-3].split("/")[-1] + "_graph.png"
    create_bar_graph(avg_latencies, savename)
    print("Graph saved as " + savename)
    print()
