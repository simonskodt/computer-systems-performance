import os
import argparse

from sqlite_handler import SQLite
from duckdb_handler import DuckDB
from benchmark_utils import benchmark_sqlite, benchmark_duckdb, benchmark_query
from colors import Colors
from enum import Enum

SQL_BENCHMARKS_DIR = "../sql_benchmarks"
TPC_H = SQL_BENCHMARKS_DIR + "/tpch"
TPC_C = SQL_BENCHMARKS_DIR + "/tpcc"

class BENCHMARK(Enum):
    TPC_H = 1
    TPC_C = 2

def parse_args():
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
    return parser.parse_args()

def main():
    """
    Main entry point of the script. Initializes databases, parses arguments,
    and runs the selected benchmark.
    """

    # if you used the same files before, remove them
    for p in ['sqlite.db', 'duckdb.db']:
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    sqlite_db = SQLite('sqlite.db')
    duckdb_db = DuckDB('duckdb.db')

    # Parse arguments
    args = parse_args()
    selected_benchmark = BENCHMARK[args.benchmark]

    if selected_benchmark == BENCHMARK.TPC_H:
        __run_tpch(sqlite_db, duckdb_db, run_all=args.all)
    elif selected_benchmark == BENCHMARK.TPC_C:
        __run_tpcc(sqlite_db, duckdb_db, run_all=args.all)

    # 5) Clean up
    sqlite_db.close()
    duckdb_db.close()

def __run_tpch(sqlite_db, duckdb_db, run_all: bool = False):
    """
    Executes the TPC-H benchmark by setting up schemas, generating data,
    loading data, and running queries.
    """
    # Create TPC-H schema in both engines
    schema_file = f"{TPC_H}/setup.sql"
    print(f"{Colors.OKBLUE}Setting up TPC-H schema...{Colors.ENDC}")
    __exec_sql_file(sqlite_db, schema_file)
    __exec_sql_file(duckdb_db, schema_file)

    # Generate & export TPC-H data in DuckDB
    print(f"{Colors.OKBLUE}Generating TPC-H data in DuckDB...{Colors.ENDC}")
    duckdb_db.generate_tpch(scale_factor=1.0)

    print(f"{Colors.OKBLUE}Exporting TPC-H tables to CSV...{Colors.ENDC}")
    duckdb_db.export_tpch_to_csv()

    # Load data into SQLite
    tables = ["region", "nation", "supplier", "customer",
            "part", "partsupp", "orders", "lineitem"]
    for tbl in tables:
        csv_path = os.path.join(f"{SQL_BENCHMARKS_DIR}/data", f"{tbl}.csv")
        print(f"{Colors.OKGREEN}Loading {tbl} into SQLite from {csv_path}...{Colors.ENDC}")
        sqlite_db.load_csv(tbl, csv_path)
    
    # Read all queries from the queries.sql file
    queries = {}
    with open(f"{SQL_BENCHMARKS_DIR}/tpch/queries.sql", 'r') as f:
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

    # --- pick which queries to run ---
    if run_all:
        query_numbers = sorted(queries.keys())
    else:
        query_numbers = [1, 4, 6]

    print(f"{Colors.OKBLUE}Running benchmarks on TPC-H queries: {query_numbers}...{Colors.ENDC}")
    
    # Execute the queries
    for query_number in query_numbers:
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        centered_text = f" Running TPC-H Query {query_number} ".center(60, "-")
        print(f"{Colors.HEADER}{Colors.BOLD}{centered_text}{Colors.ENDC}")
        print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
        if query_number in queries:
            query = queries[query_number]
            print(f"{query.strip()}")
            print(f"{Colors.HEADER}{'-' * 60}{Colors.ENDC}")
            sqlite_time = benchmark_sqlite(sqlite_db, query, False)
            print(f"SQLite Time: {sqlite_time:.6f} seconds")
            duckdb_time = benchmark_duckdb(duckdb_db, query, False)
            print(f"DuckDB Time: {duckdb_time:.6f} seconds")

            with open("latencies.txt", "a") as log:
                log.write(
                    f"Query {query_number}: SQLite={sqlite_time:.6f}s, DuckDB={duckdb_time:.6f}s\n"
                )
        else:
            print(f"{Colors.FAIL}Query {query_number} not found in queries.sql{Colors.ENDC}")

def __run_tpcc(sqlite_db, duckdb_db, run_all: bool = False):
    """
    Executes the TPC-C benchmark by setting up schemas, generating data,
    loading data, and running queries.
    """
    # Create TPC-C schema in both engines
    schema_file = f"{TPC_C}/setup.sql"
    print(f"{Colors.OKBLUE}Setting up TPC-C schema...{Colors.ENDC}")
    __exec_sql_file(sqlite_db, schema_file)
    __exec_sql_file(duckdb_db, schema_file)

    # Load data into SQLite
    tables = ["CUSTOMER", "HISTORY", "NEW_ORDER", "WAREHOUSE",
            "DISTRICT", "ITEM", "ORDER_LINE", "STOCK"]
    for tbl in tables:
        csv_path = os.path.join(f"/tmp/tpcc-tables", f"{tbl}.csv")
        print(f"{Colors.OKGREEN}Loading {tbl} into SQLite from {csv_path}...{Colors.ENDC}")
        sqlite_db.load_csv(tbl, csv_path)

    # Read all queries from the queries.sql file
    queries = {}
    with open(f"{SQL_BENCHMARKS_DIR}/tpcc/queries.sql", 'r') as f:
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

    if run_all:
        query_numbers = sorted(queries.keys())
    else:
        query_numbers = [1, 4, 6]

    print(f"{Colors.OKBLUE}Running TPC-C queries: {query_numbers}...{Colors.ENDC}")

    for query_number in query_numbers:
        print(f"{Colors.HEADER}{'='*60}{Colors.ENDC}")
        title = f" Running TPC-C Query {query_number} ".center(60, "-")
        print(f"{Colors.HEADER}{Colors.BOLD}{title}{Colors.ENDC}")
        print(f"{Colors.HEADER}{'='*60}{Colors.ENDC}")
        if query_number in queries:
            query = queries[query_number]
            print(query)
            print(f"{Colors.HEADER}{'-'*60}{Colors.ENDC}")
            sqlite_time = benchmark_sqlite(sqlite_db, query, False)
            print(f"SQLite Time: {sqlite_time:.6f} seconds")
            duckdb_time = benchmark_duckdb(duckdb_db, query, False)
            print(f"DuckDB Time: {duckdb_time:.6f} seconds")

            with open("latencies.txt", "a") as log:
                log.write(
                    f"Query {query_number}: SQLite={sqlite_time:.6f}s, DuckDB={duckdb_time:.6f}s\n"
                )
        else:
            print(f"{Colors.FAIL}Query {query} not found!{Colors.ENDC}")

def __exec_sql_file(db, path: str):
    """
    Executes all SQL statements in the specified file on the given database.
    """
    with open(path, 'r') as f:
        sql = f.read()
    for stmt in sql.split(';'):
        if stmt.strip():
            db.execute_query(stmt)

if __name__ == "__main__":
    main()
