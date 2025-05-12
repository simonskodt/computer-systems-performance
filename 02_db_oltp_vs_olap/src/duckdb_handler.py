import duckdb
import os

SQL_BENCHMARKS_DIR = "../sql_benchmarks"

class DuckDB:
    """Interface for interacting with DuckDB database (incl. TPC-H extension)."""
    def __init__(self, db_path: str):
        # Connect and ensure TPC-H extension is installed & loaded
        self.con = duckdb.connect(db_path)
        self.con.execute("INSTALL tpch;")
        self.con.execute("LOAD tpch;")

        # Setting up the queries to be reviewed
        table = self.con.execute("FROM tpch_queries();")
        os.makedirs(SQL_BENCHMARKS_DIR, exist_ok=True)
        with open(f"{SQL_BENCHMARKS_DIR}/queries.sql", "w") as f:
            for query_id, query_text in table.fetchall():
                f.write(f"-- Query ID: {query_id}\n")
                f.write(f"{query_text}\n")
                f.write("\n")

    def generate_tpch(self, scale_factor: float = 1.0):
        """Drop any existing TPC-H tables and generate new ones at the given scale factor."""
        tables = ["region", "nation", "supplier", "customer",
                  "part", "partsupp", "orders", "lineitem"]
        for t in tables:
            self.con.execute(f"DROP TABLE IF EXISTS {t};")
        # This will create all 8 standard TPC-H tables
        self.con.execute(f"CALL dbgen(sf={scale_factor});")

    def export_tpch_to_csv(self):
        """Export each TPC-H table to a CSV (with HEADER) under output_dir/."""
        os.makedirs(SQL_BENCHMARKS_DIR + "/data", exist_ok=True)
        tables = ["region", "nation", "supplier", "customer",
                  "part", "partsupp", "orders", "lineitem"]
        for t in tables:
            dst = os.path.join(SQL_BENCHMARKS_DIR + "/data", f"{t}.csv")
            # HEADER ensures column names in row 1
            self.con.execute(f"COPY {t} TO '{dst}' (HEADER, DELIMITER ',');")

    def __get_column_metadata(self, result):
        col_names = result.columns
        col_types = [str(t).split('.')[-1] for t in result.types]
        return col_names, col_types

    def execute_query(self, query: str, fetch_metadata: bool = True):
        result = self.con.sql(query)
        if result is None:
            return [], [], []  # e.g. for DDL
        rows = result.fetchall()
        if fetch_metadata:
            names, types = self.__get_column_metadata(result)
        else:
            names, types = [], []
        return rows, names, types

    def close(self):
        self.con.close()
