import os

from sqlite_handler import SQLite
from duckdb_handler import DuckDB
from benchmark_utils import benchmark_sqlite, benchmark_duckdb
from colors import Colors

def main():
    sqlite_db = SQLite('sqlite.db')
    duckdb_db = DuckDB('duckdb.db')

    # 1) Create TPC-H schema in both engines
    schema_file = '../sql_benchmarks/tpch_setup.sql'
    print(f"{Colors.OKBLUE}Setting up TPC-H schema...{Colors.ENDC}")
    _exec_sql_file(sqlite_db, schema_file)
    _exec_sql_file(duckdb_db, schema_file)

    # 2) Generate & export TPC-H data in DuckDB
    print(f"{Colors.OKBLUE}Generating TPC-H data in DuckDB...{Colors.ENDC}")
    duckdb_db.generate_tpch(scale_factor=1.0)

    print(f"{Colors.OKBLUE}Exporting TPC-H tables to CSV...{Colors.ENDC}")
    duckdb_db.export_tpch_to_csv(output_dir='data')

    # 3) Load data into SQLite
    tables = ["region", "nation", "supplier", "customer",
              "part", "partsupp", "orders", "lineitem"]
    for tbl in tables:
        csv_path = os.path.join('data', f"{tbl}.csv")
        print(f"{Colors.OKGREEN}Loading {tbl} into SQLite from {csv_path}...{Colors.ENDC}")
        sqlite_db.load_csv(tbl, csv_path)

    # 4) Run benchmarks on each SQL query
    query_file = '../sql_benchmarks/queries.sql'
    print(f"{Colors.OKBLUE}Running benchmarks on queries from {query_file}...{Colors.ENDC}")
    with open(query_file, 'r') as f:
        all_sql = f.read().strip()
    for q in all_sql.split(';'):
        q = q.strip()
        if not q:
            continue
        print(f"{Colors.WARNING}Query:{Colors.ENDC} {q}")
        benchmark_sqlite(sqlite_db, q)
        benchmark_duckdb(duckdb_db, q)

    # 5) Clean up
    sqlite_db.close()
    duckdb_db.close()

def _exec_sql_file(db, path: str):
    """Utility to run all statements in a .sql file."""
    with open(path, 'r') as f:
        sql = f.read()
    for stmt in sql.split(';'):
        if stmt.strip():
            db.execute_query(stmt)

if __name__ == "__main__":
    main()
