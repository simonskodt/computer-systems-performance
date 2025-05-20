from typing import List, Tuple, Any
import duckdb
import os

SQL_BENCHMARKS_DIR = "../sql_benchmarks"
TPC_H = SQL_BENCHMARKS_DIR + "/tpch"

class DuckDB:
    """Interface for interacting with DuckDB database (incl. TPC-H extension)."""
    def __init__(self, db_path: str) -> None:
        self.con: duckdb.DuckDBPyConnection = duckdb.connect(db_path)
        self.con.execute("INSTALL tpch;")
        self.con.execute("LOAD tpch;")

        # Setting up the queries to be reviewed
        table = self.con.execute("FROM tpch_queries();")
        os.makedirs(SQL_BENCHMARKS_DIR, exist_ok=True)
        with open(f"{SQL_BENCHMARKS_DIR}/tpch/queries.sql", "w") as f:
            for query_id, query_text in table.fetchall():
                f.write(f"-- Query ID: {query_id}\n")
                f.write(f"{query_text}\n")
                f.write("\n")

    def generate_tpch(self, scale_factor: float = 1.0) -> None:
        """Drop any existing TPC-H tables and generate new ones at the given scale factor."""
        tables = ["region", "nation", "supplier", "customer",
                  "part", "partsupp", "orders", "lineitem"]
        for t in tables:
            self.con.execute(f"DROP TABLE IF EXISTS {t};")
        # This will create all 8 standard TPC-H tables
        self.con.execute(f"CALL dbgen(sf={scale_factor});")

    def export_tpch_to_csv(self) -> None:
        """Export each TPC-H table to a CSV (with HEADER) under tpch/data/."""
        os.makedirs(TPC_H + "/data", exist_ok=True)
        tables = ["region", "nation", "supplier", "customer",
                  "part", "partsupp", "orders", "lineitem"]
        for t in tables:
            dst = os.path.join(TPC_H + "/data", f"{t}.csv")
            # HEADER ensures column names in row 1
            self.con.execute(f"COPY {t} TO '{dst}' (HEADER, DELIMITER ',');")

    def __get_column_metadata(self, result: duckdb.DuckDBPyRelation) -> Tuple[List[str], List[str]]:
        col_names: List[str] = result.columns
        col_types: List[str] = [str(t).split('.')[-1] for t in result.types]
        return col_names, col_types

    def execute_query(self, query: str, fetch_metadata: bool = True) -> Tuple[List[Tuple[Any, ...]], List[str], List[str]]:
        result: duckdb.DuckDBPyRelation = self.con.sql(query)
        if result is None:
            return [], [], []
        rows: List[Tuple[Any, ...]] = result.fetchall()
        if fetch_metadata:
            names, types = self.__get_column_metadata(result)
        else:
            names, types = [], []
        return rows, names, types

    def close(self) -> None:
        self.con.close()
