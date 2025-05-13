import os
import pprint
import sys
import math
import matplotlib.pyplot as plt


def read_latencies(file_path):
    latencies = {}
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(":")
            if len(parts) == 2:
                query_part, time_part = parts
                query_number = int(query_part.split()[1])
                sqlite_time, duckdb_time = time_part.split(",")
                latencies[query_number] = {
                    "sqlite": float(sqlite_time.split("=")[1].split('s')[0]),
                    "duckdb": float(duckdb_time.split("=")[1].split('s')[0]),
                }

    print("Latencies:")
    pprint.pp(latencies)
    return latencies


def create_bar_graph(latencies):
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
    plt.savefig("latencies.png")
    print("Graph saved as latencies.png")
    plt.show()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python make_graphs.py <latencies_file>")
        sys.exit(1)

    latencies_file = sys.argv[1]
    latencies = read_latencies(latencies_file)
    create_bar_graph(latencies)
