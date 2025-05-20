from colors import Colors
import time
from tabulate import tabulate
from typing import List, Tuple, Any

def benchmark_query(db: Any, query: str, print_results: bool = True) -> Tuple[List[Tuple[Any, ...]], List[str], List[str], float]:
    """Benchmark the execution time of a query."""
    start_time = time.time()
    results, column_names, column_types = db.execute_query(
        query, fetch_metadata=print_results)
    end_time = time.time()

    execution_time = end_time - start_time

    return results, column_names, column_types, execution_time

def benchmark_sqlite(sqlite_db: Any, query: str, print_results: bool = True) -> float:
    """Benchmark and optionally print results for SQLite."""
    sqlite_results, \
    sqlite_columns, \
    sqlite_types, \
    sqlite_time = benchmark_query(sqlite_db, query, print_results)

    if print_results:
        __print_table(sqlite_results, sqlite_columns, sqlite_types, "SQLite")

    return sqlite_time

def benchmark_duckdb(duckdb_db: Any, query: str, print_results: bool = True) -> float:
    """Benchmark and optionally print results for DuckDB."""
    duckdb_results, \
    duckdb_columns, \
    duckdb_types, \
    duckdb_time = benchmark_query(duckdb_db, query, print_results)

    if print_results:
        __print_table(duckdb_results, duckdb_columns, duckdb_types, "DuckDB")

    return duckdb_time

def __print_table(results: List[Tuple[Any, ...]], columns: List[str], types: List[str], db_name: str) -> None:
    """Print the results in a tabular format."""

    if results:
        Colors.print_colored(f" {db_name.upper()} RESULTS ".center(60), Colors.HEADER)

        Colors.print_colored(f"Debug: {db_name} - Results: {results}", Colors.WARNING)
        Colors.print_colored(f"Debug: {db_name} - Columns: {columns}", Colors.WARNING)
        Colors.print_colored(f"Debug: {db_name} - Types: {types}", Colors.WARNING)

        # Pretty format, uncomment to see
        # headers = [f"{col}\n{typ}" for col, typ in zip(columns, types)]
        # print(tabulate(results, headers=headers, tablefmt="fancy_grid"))
    else:
        Colors.print_colored(f"No results for query executed on {db_name}.", Colors.FAIL)
