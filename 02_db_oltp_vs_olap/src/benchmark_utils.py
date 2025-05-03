from colors import Colors
import time
from tabulate import tabulate

def benchmark_query(db, query, print_results=True):
    """Benchmark the execution time of a query."""
    start_time = time.time()
    results, column_names, column_types = db.execute_query(query, print_results)
    end_time = time.time()

    execution_time = end_time - start_time

    return results, column_names, column_types, execution_time

def benchmark_sqlite(sqlite_db, query, print_results=True):
    """Benchmark and optionally print results for SQLite."""
    sqlite_results, \
    sqlite_columns, \
    sqlite_types, \
    sqlite_time = benchmark_query(sqlite_db, query, print_results)

    if print_results:
        __print_table(sqlite_results, sqlite_columns, sqlite_types, "SQLite")

    print(f"SQLite Time: {sqlite_time:.6f} seconds")

def benchmark_duckdb(duckdb_db, query, print_results=True):
    """Benchmark and optionally print results for DuckDB."""
    duckdb_results, \
    duckdb_columns, \
    duckdb_types, \
    duckdb_time = benchmark_query(duckdb_db, query, print_results)

    if print_results:
        __print_table(duckdb_results, duckdb_columns, duckdb_types, "DuckDB")

    print(f"DuckDB Time: {duckdb_time:.6f} seconds")

def __print_table(results, columns, types, db_name):
    """Print the results in a tabular format."""
    if results:
        # Add an overall header for the database name
        print(f" {db_name.upper()} RESULTS ".center(60))

        # Add column names and types as a header
        headers = [f"{col}\n{typ}" for col, typ in zip(columns, types)]
        print(tabulate(results, headers=headers, tablefmt="fancy_grid"))
    else:
        print("No results")