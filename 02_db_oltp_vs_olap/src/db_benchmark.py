import os
import argparse

from typing import List, Dict
from sqlite_handler import SQLite
from duckdb_handler import DuckDB
from benchmark_utils import benchmark_sqlite, benchmark_duckdb
from colors import Colors
from enum import Enum

SQL_BENCHMARKS_DIR = "../sql_benchmarks"
TPC_H = SQL_BENCHMARKS_DIR + "/tpch"
TPC_C = SQL_BENCHMARKS_DIR + "/tpcc"

class BENCHMARK(Enum):
    TPC_H = 1
    TPC_C = 2

def parse_args() -> argparse.Namespace:
    """
    Parses the command-line arguments to select the benchmark to run.
    """
    parser = argparse.ArgumentParser(description="Select benchmark to use.")

    parser.add_argument(
        '--benchmark',
        type=str,
        choices=[b.name for b in BENCHMARK],
        required=True,
        help=f"Benchmark to use. Choices: {[b.name for b in BENCHMARK]}"
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help="Run all available queries instead of the default subset"
    )

    parser.add_argument(
        '-r',
        '--reuse',
        action='store_true',
        help="Reuse existing data files instead of generating new ones"
    )

    parser.add_argument(
        '--results',
        action='store_true',
        help="Show query results in the output"
    )

    return parser.parse_args()

def main() -> None:
    """
    Main entry point of the script. Initializes databases, parses arguments,
    and runs the selected benchmark.
    """
    # Parse arguments
    args = parse_args()
    selected_benchmark = BENCHMARK[args.benchmark]
    reuse_data = args.reuse
    show_results = args.results

    Colors.print_colored("Starting the database benchmark script...", Colors.HEADER)

    # If used the same files before, remove them
    files_to_remove: List[str] = ['latencies.txt']
    if not reuse_data:
        files_to_remove.extend(['sqlite.db', 'duckdb.db'])
    for p in files_to_remove:
        try:
            os.remove(p)
            Colors.print_colored(f"Removed file: {p}", Colors.OKGREEN)
        except FileNotFoundError:
            Colors.print_colored(f"File not found, skipping removal: {p}", Colors.WARNING)

    sqlite_db = SQLite('sqlite.db')
    duckdb_db = DuckDB('duckdb.db')

    Colors.print_colored(f"Selected benchmark: {selected_benchmark.name}", Colors.OKCYAN)

    if selected_benchmark == BENCHMARK.TPC_H:
        run_tpch(sqlite_db, duckdb_db, run_all=args.all,
                 reuse_data=reuse_data, show_results=show_results)
    elif selected_benchmark == BENCHMARK.TPC_C:
        run_tpcc(sqlite_db, duckdb_db, run_all=args.all,
                 reuse_data=reuse_data, show_results=show_results)

    # Clean up
    sqlite_db.close()
    duckdb_db.close()
    Colors.print_colored("Benchmark script completed successfully.", Colors.OKGREEN)

############################################
#                  TPC-H                   #
############################################

def run_tpch(sqlite_db: SQLite, duckdb_db: DuckDB, run_all: bool = False, reuse_data: bool = False, show_results: bool = False) -> None:
    """
    Executes the TPC-H benchmark by setting up schemas, generating data,
    loading data, and running queries.

    Args:
        sqlite_db (SQLite): SQLite database handler.
        duckdb_db (DuckDB): DuckDB database handler.
        run_all (bool): Whether to run all queries or a subset.
        reuse_data (bool): Whether to reuse existing data.
        show_results (bool): Whether to display query results.
    """
    if not reuse_data:
        # Create TPC-H schema in both engines
        schema_file = f"{TPC_H}/setup.sql"
        Colors.print_colored("Setting up TPC-H schema...", Colors.OKBLUE)
        exec_sql_file(sqlite_db, schema_file)
        exec_sql_file(duckdb_db, schema_file)
        Colors.print_colored("TPC-H schema setup completed.", Colors.OKGREEN)

        # Generate & export TPC-H data in DuckDB
        Colors.print_colored("Generating TPC-H data in DuckDB...", Colors.OKBLUE)
        duckdb_db.generate_tpch(scale_factor=1.0)

        Colors.print_colored("Exporting TPC-H tables to CSV...", Colors.OKBLUE)
        duckdb_db.export_tpch_to_csv()
        Colors.print_colored("TPC-H data generation and export completed.", Colors.OKGREEN)

        # Load data into SQLite
        tables: List[str] = ["region", "nation", "supplier", "customer",
                  "part", "partsupp", "orders", "lineitem"]
        for tbl in tables:
            csv_path = os.path.join(f"{TPC_H}/data", f"{tbl}.csv")
            Colors.print_colored(f"Loading {tbl} into SQLite from {csv_path}...", Colors.OKGREEN)
            sqlite_db.load_csv(tbl, csv_path)
        Colors.print_colored("TPC-H data loading into SQLite completed.", Colors.OKGREEN)
    else:
        Colors.print_colored("Reusing existing TPC-H data.", Colors.OKBLUE)

    # Read all queries from the queries.sql file
    queries: Dict[int, str] = {}
    with open(f"{TPC_H}/queries.sql", 'r') as f:
        content = f.read()
        # Split the content by query ID markers
        query_blocks = content.split('-- Query ID:')
        for block in query_blocks[1:]:  # Skip the first empty block
            lines = block.strip().split('\n')
            if not lines:
                continue
            query_id = int(lines[0].strip())
            query_text = '\n'.join(lines[1:]).strip()
            # Remove any trailing semicolons
            while query_text.endswith(';'):
                query_text = query_text[:-1].strip()
            queries[query_id] = query_text

    # Pick which queries to run
    query_numbers: List[int] = sorted(queries.keys()) if run_all else [1, 6, 13]

    Colors.print_colored(f"Running benchmarks on TPC-H queries: {query_numbers}...", Colors.OKBLUE)

    # Execute the queries
    for query_number in query_numbers:
        Colors.print_colored("=" * 60, Colors.HEADER)
        centered_text = f" Running TPC-H Query {query_number} ".center(60, "-")
        Colors.print_colored(centered_text, Colors.HEADER + Colors.BOLD)
        Colors.print_colored("=" * 60, Colors.HEADER)
        if query_number in queries:
            query = queries[query_number]

            # Store original query for DuckDB
            duckdb_query = query
            
            # Preprocess query to be compatible with SQLite
            sqlite_query = query.replace("CAST('", "DATE('")
            sqlite_query = sqlite_query.replace("AS date", "")

            if query_number == 13:
                sqlite_query = sqlite_query.replace("AS c_orders (c_custkey,\n        c_count)", "AS c_orders")
                sqlite_query = sqlite_query.replace("count(o_orderkey)", "count(o_orderkey) AS c_count")

            Colors.print_colored(f"Executing query:\n{sqlite_query.strip()}", Colors.OKCYAN)
            Colors.print_colored("-" * 60, Colors.HEADER)
            sqlite_time = benchmark_sqlite(sqlite_db, sqlite_query, show_results)
            Colors.print_colored(f"SQLite Time: {sqlite_time:.6f} seconds", Colors.OKGREEN)
            duckdb_time = benchmark_duckdb(duckdb_db, duckdb_query, show_results)
            Colors.print_colored(f"DuckDB Time: {duckdb_time:.6f} seconds", Colors.OKGREEN)

            with open("latencies.txt", "a") as log:
                log.write(
                    f"Query {query_number}: SQLite={sqlite_time:.6f}s, DuckDB={duckdb_time:.6f}s\n"
                )
        else:
            Colors.print_colored(f"Query {query_number} not found in queries.sql", Colors.FAIL)

    Colors.print_colored("\nFinished running TPC-H queries.", Colors.OKGREEN)

############################################
#                  TPC-C                   #
############################################

def run_tpcc(sqlite_db: SQLite, duckdb_db: DuckDB, run_all: bool = False, reuse_data: bool = False, show_results: bool = False) -> None:
    """
    Executes the TPC-C benchmark by setting up schemas, generating data,
    loading data, and running queries.

    Args:
        sqlite_db (SQLite): SQLite database handler.
        duckdb_db (DuckDB): DuckDB database handler.
        run_all (bool): Whether to run all queries or a subset.
        reuse_data (bool): Whether to reuse existing data.
        show_results (bool): Whether to display query results.
    """
    if not reuse_data:
        # Create TPC-C schema in both engines
        schema_file = f"{TPC_C}/setup.sql"
        Colors.print_colored("Setting up TPC-C schema...", Colors.OKBLUE)
        exec_sql_file(sqlite_db, schema_file)
        exec_sql_file(duckdb_db, schema_file)
        Colors.print_colored("TPC-C schema setup completed.", Colors.OKGREEN)

        # Load data into SQLite
        # Note: TPC-C data needs to be generated separately using the py-tpcc tool
        # located in tpcc_py/py-tpcc/pytpcc/tpcc.py
        tpcc_data_dir = os.path.join(f"{TPC_C}/data")
        tables: List[str] = ["CUSTOMER", "HISTORY", "NEW_ORDER", "WAREHOUSE",
                  "DISTRICT", "ITEM", "ORDER_LINE", "STOCK"]
        
        # Ensure the data directory exists
        if not os.path.exists(tpcc_data_dir):
            os.makedirs(tpcc_data_dir, exist_ok=True)
            Colors.print_colored(f"Created directory for TPC-C data: {tpcc_data_dir}", Colors.OKGREEN)
            Colors.print_colored(f"Please generate TPC-C data using the py-tpcc tool and place CSV files in this directory", Colors.WARNING)
        
        for tbl in tables:
            csv_path = os.path.join(tpcc_data_dir, f"{tbl}.csv")
            if not os.path.exists(csv_path):
                Colors.print_colored(f"Warning: {csv_path} not found!", Colors.WARNING)
                Colors.print_colored(f"Please generate TPC-C data using the py-tpcc tool first", Colors.WARNING)
                Colors.print_colored(f"Skipping data loading for table {tbl}", Colors.WARNING)
                continue
                
            Colors.print_colored(f"Loading {tbl} into SQLite from {csv_path}...", Colors.OKGREEN)
            sqlite_db.load_csv(tbl, csv_path)
        Colors.print_colored("TPC-C data loading into SQLite completed.", Colors.OKGREEN)
    else:
        Colors.print_colored("Reusing existing TPC-C data.", Colors.OKBLUE)

    # Read all queries from the queries.sql file
    queries: Dict[int, str] = {}
    with open(f"{TPC_C}/queries.sql", 'r') as f:
        content = f.read()
        # Split the content by query ID markers
        query_blocks = content.split('-- Query ID:')
        for block in query_blocks[1:]:  # Skip the first empty block
            lines = block.strip().split('\n')
            if not lines:
                continue
            query_id = int(lines[0].strip())
            query_text = '\n'.join(lines[1:]).strip()
            # Remove any trailing semicolons
            while query_text.endswith(';'):
                query_text = query_text[:-1].strip()
            queries[query_id] = query_text

    query_numbers: List[int] = sorted(queries.keys()) if run_all else [1, 4, 6]

    Colors.print_colored(f"Running TPC-C queries: {query_numbers}...", Colors.OKBLUE)

    for query_number in query_numbers:
        Colors.print_colored("=" * 60, Colors.HEADER)
        title = f" Running TPC-C Query {query_number} ".center(60, "-")
        Colors.print_colored(title, Colors.HEADER + Colors.BOLD)
        Colors.print_colored("=" * 60, Colors.HEADER)
        if query_number in queries:
            query = queries[query_number]
            Colors.print_colored(f"Executing query:\n{query.strip()}", Colors.OKCYAN)
            Colors.print_colored("-" * 60, Colors.HEADER)
            sqlite_time = benchmark_sqlite(sqlite_db, query, show_results)
            Colors.print_colored(f"SQLite Time: {sqlite_time:.6f} seconds", Colors.OKGREEN)
            duckdb_time = benchmark_duckdb(duckdb_db, query, show_results)
            Colors.print_colored(f"DuckDB Time: {duckdb_time:.6f} seconds", Colors.OKGREEN)

            with open("latencies.txt", "a") as log:
                log.write(
                    f"Query {query_number}: SQLite={sqlite_time:.6f}s, DuckDB={duckdb_time:.6f}s\n"
                )
        else:
            Colors.print_colored(f"Query {query_number} not found!", Colors.FAIL)

    Colors.print_colored("Finished running TPC-C queries.", Colors.OKGREEN)

def exec_sql_file(db: SQLite | DuckDB, path: str) -> None:
    """
    Executes all SQL statements in the specified file on the given database.

    Args:
        db (SQLite | DuckDB): Database handler.
        path (str): Path to the SQL file.
    """
    Colors.print_colored(f"Executing SQL file: {path}", Colors.OKCYAN)
    with open(path, 'r') as f:
        sql = f.read()
    for stmt in sql.split(';'):
        if stmt.strip():
            db.execute_query(stmt)
    Colors.print_colored(f"Finished executing SQL file: {path}", Colors.OKGREEN)


if __name__ == "__main__":
    main()
