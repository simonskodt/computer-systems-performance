import os
import argparse

from typing import List, Dict
from sqlite_handler import SQLite
from duckdb_handler import DuckDB
from benchmark_utils import benchmark_sqlite, benchmark_duckdb
from colors import Colors
from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
import time
import random


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
        '--sf',
        type=float,
        default=1.0,
        help="Scale factor for TPC-H data generation (default: 1.0)"
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
                 reuse_data=reuse_data, show_results=show_results, scale_factor = args.sf)
    elif selected_benchmark == BENCHMARK.TPC_C:
        run_tpcc(sqlite_db, duckdb_db, run_all=args.all,
                 reuse_data=reuse_data, show_results=show_results, scale_factor = args.sf)

    # Clean up
    sqlite_db.close()
    duckdb_db.close()
    Colors.print_colored("Benchmark script completed successfully.", Colors.OKGREEN)

############################################
#                  TPC-H                   #
############################################

def run_tpch(sqlite_db: SQLite, duckdb_db: DuckDB, run_all: bool = False, reuse_data: bool = False, show_results: bool = False, scale_factor: float = 1) -> None:
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
        duckdb_db.generate_tpch(scale_factor=scale_factor)

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

def run_tpcc(sqlite_db: SQLite, duckdb_db: DuckDB, run_all: bool = False, reuse_data: bool = False, show_results: bool = False, scale_factor: float = 1) -> None:
    """
    Executes the TPC-C benchmark by setting up schemas, generating data,
    loading data, and running queries.
    """
    tables = [
        "WAREHOUSE",
        "DISTRICT",
        "CUSTOMER",
        "HISTORY",
        "ITEM",
        "STOCK",
        "ORDERS",
        "NEW_ORDER",
        "ORDER_LINE"
    ]
    for tbl in tables:
        csv_path = os.path.join(f"/tmp/tpcc-tables", f"{tbl}.csv")
        print(f"{Colors.OKGREEN}Loading {tbl} into SQLite from {csv_path}...{Colors.ENDC}")
        sqlite_db.load_csv(tbl, csv_path)

    for tbl in tables:
        csv_path = os.path.join("/tmp/tpcc-tables", f"{tbl}.csv")
        print(f"{Colors.OKGREEN}Loading {tbl} into DuckDB from {csv_path}...{Colors.ENDC}")
        # HEADER FALSE because the CSVs are headerless
        duckdb_db.con.execute(
            f"COPY {tbl} FROM '{csv_path}' (DELIMITER ',', HEADER FALSE);"
        )

    print(f"{Colors.OKBLUE}Running TPC-C transactions {Colors.ENDC}")

    warehouse_id = random.randint(1,int(scale_factor))
    district_id = random.randint(1,10)

    q = stock_level_query(warehouse_id, district_id, 100)
    print(f"{Colors.OKCYAN}Stock‐Level SQL:{Colors.ENDC}\n{q}")
    sqlite_db.connection.isolation_level = None
    sqlite_db.connection.execute("begin")
    benchmark_sqlite(sqlite_db, q, True)
    sqlite_db.connection.execute("commit")



    duckdb_db.con.begin()
    benchmark_duckdb(duckdb_db, q, True)
    duckdb_db.con.commit()


    print("Running Delivery transaction")
    # SQLite
    start = time.perf_counter()
    delivery_transaction(sqlite_db, w_id=warehouse_id, o_carrier_id=5)
    sqlite_latency = time.perf_counter() - start
    print(f"SQLite Delivery latency: {sqlite_latency:.6f} seconds")
    # DuckDB
    start = time.perf_counter()
    delivery_transaction(duckdb_db, w_id=warehouse_id, o_carrier_id=5)
    duckdb_latency = time.perf_counter() - start
    print(f"DuckDB  Delivery latency: {duckdb_latency:.6f} seconds")

    print("Running Order-Status transaction")
    # SQLite
    start = time.perf_counter()
    order_status_transaction(sqlite_db, w_id=warehouse_id, d_id=district_id, by_name=True)
    sqlite_latency = time.perf_counter() - start
    print(f"SQLite Order-Status latency: {sqlite_latency:.6f} seconds")
    # DuckDB
    start = time.perf_counter()
    order_status_transaction(duckdb_db,  w_id=warehouse_id, d_id=district_id, by_name=True)
    duckdb_latency = time.perf_counter() - start
    print(f"DuckDB  Order-Status latency: {duckdb_latency:.6f} seconds")

    Colors.print_colored("Finished running TPC-C queries.", Colors.OKGREEN)


def stock_level_query(w_id: int, d_id: int, threshold: int) -> str:
    return f"""
    WITH d AS (
      SELECT d_next_o_id AS next_o_id
        FROM district
       WHERE d_w_id = {w_id}
         AND d_id   = {d_id}
    )
    SELECT
      COUNT(DISTINCT s.s_i_id) AS stock_count
    FROM order_line AS ol
      JOIN stock AS s
        ON s.s_i_id = ol.ol_i_id
       AND s.s_w_id = {w_id}
    WHERE
          ol.ol_w_id = {w_id}
      AND ol.ol_d_id = {d_id}
      AND ol.ol_o_id <  (SELECT next_o_id      FROM d)
      AND ol.ol_o_id >= (SELECT next_o_id - 20 FROM d)
      AND s.s_quantity <  {threshold}
    ;
    """


def delivery_transaction(db, w_id: int, o_carrier_id: int, districts_per_warehouse: int = 10):
    now = datetime.now().isoformat(sep=' ')

    for d_id in range(1, districts_per_warehouse + 1):
        # 1) Find the smallest new_order ID for this (w_id, d_id)
        sql = f"""
            SELECT MIN(no_o_id)
              FROM new_order
             WHERE no_w_id = {w_id}
               AND no_d_id = {d_id}
        """
        rows, _, _ = db.execute_query(sql)
        if not rows or rows[0][0] is None:
            # no pending orders
            continue
        no_o_id = rows[0][0]

        # 2) Delete it from new_order
        db.execute_query(f"""
            DELETE FROM new_order
             WHERE no_w_id = {w_id}
               AND no_d_id = {d_id}
               AND no_o_id = {no_o_id}
        """)

        # 3) Lookup the customer id for that order
        rows, _, _ = db.execute_query(f"""
            SELECT o_c_id
              FROM orders
             WHERE o_w_id = {w_id}
               AND o_d_id = {d_id}
               AND o_id   = {no_o_id}
        """)
        c_id = rows[0][0]

        # 4) Update the order’s carrier
        db.execute_query(f"""
            UPDATE orders
               SET o_carrier_id = {o_carrier_id}
             WHERE o_w_id       = {w_id}
               AND o_d_id       = {d_id}
               AND o_id         = {no_o_id}
        """)

        # 5) Stamp order_line rows with delivery timestamp
        db.execute_query(f"""
            UPDATE order_line
               SET ol_delivery_d = '{now}'
             WHERE ol_w_id       = {w_id}
               AND ol_d_id       = {d_id}
               AND ol_o_id       = {no_o_id}
        """)

        # 6) Sum up the line amounts
        rows, _, _ = db.execute_query(f"""
            SELECT COALESCE(SUM(ol_amount), 0)
              FROM order_line
             WHERE ol_w_id = {w_id}
               AND ol_d_id = {d_id}
               AND ol_o_id = {no_o_id}
        """)
        total = rows[0][0]

        # 7) Add that to the customer’s balance
        db.execute_query(f"""
            UPDATE customer
               SET c_balance = c_balance + {total}
             WHERE c_w_id   = {w_id}
               AND c_d_id   = {d_id}
               AND c_id     = {c_id}
        """)


def order_status_transaction(
    db,
    w_id: int,
    d_id: int,
    by_name: bool,
    c_last: Optional[str] = None,
    c_id:   Optional[int] = None
) -> Dict[str, Any]:

    if by_name and c_last is None:
        rows, _, _ = db.execute_query(f"""
            SELECT DISTINCT c_last
              FROM customer
             WHERE c_w_id = {w_id}
               AND c_d_id = {d_id}
             LIMIT 1
        """)
        if not rows:
            raise RuntimeError(f"No customers found in W={w_id}, D={d_id}")
        c_last = rows[0][0]
        print(f"Auto-selected last name for Order-Status: {c_last}")

    if by_name:
        sql_c = f"""
        WITH numbered AS (
          SELECT
            c_balance, c_first, c_middle, c_id,
            ROW_NUMBER() OVER (ORDER BY c_first) AS rn,
            COUNT(*)       OVER ()          AS cnt
          FROM customer
          WHERE c_w_id = {w_id}
            AND c_d_id = {d_id}
            AND c_last = '{c_last}'
        )
        SELECT c_balance, c_first, c_middle, c_id
          FROM numbered
         WHERE rn = (cnt + 1) / 2
        ;
        """
    else:
        if c_id is None:
            raise ValueError("c_id must be provided when by_name=False")
        sql_c = f"""
        SELECT c_balance, c_first, c_middle, c_last, c_id
          FROM customer
         WHERE c_w_id = {w_id}
           AND c_d_id = {d_id}
           AND c_id   = {c_id}
        ;
        """

    rows, _, _ = db.execute_query(sql_c)
    if not rows:
        raise RuntimeError("No matching customer found")
    if by_name:
        c_balance, c_first, c_middle, found_c_id = rows[0]
    else:
        c_balance, c_first, c_middle, c_last, found_c_id = rows[0]

    sql_o = f"""
    SELECT o_id, o_carrier_id, o_entry_d
      FROM orders
     WHERE o_w_id = {w_id}
       AND o_d_id = {d_id}
       AND o_c_id = {found_c_id}
     ORDER BY o_id DESC
     LIMIT 1
    ;
    """
    rows, _, _ = db.execute_query(sql_o)
    if not rows:
        raise RuntimeError("Customer has no orders")
    o_id, o_carrier_id, o_entry_d = rows[0]

    sql_lines = f"""
    SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
      FROM order_line
     WHERE ol_w_id = {w_id}
       AND ol_d_id = {d_id}
       AND ol_o_id = {o_id}
    ;
    """
    line_rows, _, _ = db.execute_query(sql_lines)

    return {
        "customer": {
            "c_id":       found_c_id,
            "c_last":     c_last,
            "c_balance":  c_balance,
            "c_first":    c_first,
            "c_middle":   c_middle
        },
        "order": {
            "o_id":         o_id,
            "o_carrier_id": o_carrier_id,
            "o_entry_d":    o_entry_d
        },
        "lines": [
            {
                "ol_i_id":        lr[0],
                "ol_supply_w_id": lr[1],
                "ol_quantity":    lr[2],
                "ol_amount":      lr[3],
                "ol_delivery_d":  lr[4]
            }
            for lr in line_rows
        ]
    }

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
