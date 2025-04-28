import sqlite3
import duckdb
import time
from colors import Colors
from tabulate import tabulate

class SQLite:
    """Interface for interacting with SQLite database."""
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        # Get column names and types from cursor.description
        column_names = []
        column_types = []
        if self.cursor.description:
            for col in self.cursor.description:
                column_names.append(col[0])
                column_types.append(type(col[0]).__name__ if col[0] is not None else "None")
        
        return results, column_names, column_types

    def close(self):
        self.connection.close()

class DuckDB:
    """Interface for interacting with DuckDB database."""
    def __init__(self, db_path):
        self.con = duckdb.connect(db_path)

    def execute_query(self, query):
        result = self.con.sql(query)
        
        # Handle DDL statements that return None
        if result is None:
            return [], [], []
        
        # Convert the DuckDBPyRelation object to a list of tuples
        results = result.fetchall()
        
        # Get column names from the result
        column_names = result.columns
        
        # Get column types from the result
        column_types = [str(type_info).split('.')[-1] for type_info in result.types]
        
        return results, column_names, column_types
    
    def close(self):
        self.con.close()

def execute_queries_from_file(db, file_path):
    """Execute all queries from a given SQL file."""
    with open(file_path, 'r') as file:
        queries = file.read()
        for query in queries.split(';'):
            if query.strip(): # Skip empty queries
                db.execute_query(query)

def benchmark_query(db, query):
    """Benchmark the execution time of a query."""
    start_time = time.time()
    results, column_names, column_types = db.execute_query(query)
    end_time = time.time()

    execution_time = end_time - start_time

    return results, column_names, column_types, execution_time

def benchmark_sqlite(sqlite_db, query):
    """Benchmark and print results for SQLite."""
    print(f"{Colors.OKBLUE}--- SQLite Benchmark ---{Colors.ENDC}")
    sqlite_results, sqlite_columns, sqlite_types, sqlite_time = benchmark_query(sqlite_db, query)
    if sqlite_results:
        # Add column names and types as a header
        headers = [f"{col}\n{typ}" for col, typ in zip(sqlite_columns, sqlite_types)]
        print(f"{Colors.OKBLUE}SQLite Results:{Colors.ENDC}")
        print(tabulate(sqlite_results, headers=headers, tablefmt="fancy_grid"))
    else:
        print(f"{Colors.OKBLUE}SQLite Results:{Colors.ENDC} No results")
    print(f"{Colors.OKCYAN}SQLite Time:{Colors.ENDC} {sqlite_time:.6f} seconds\n")


def benchmark_duckdb(duckdb_db, query):
    """Benchmark and print results for DuckDB."""
    print(f"{Colors.OKGREEN}--- DuckDB Benchmark ---{Colors.ENDC}")
    duckdb_results, duckdb_columns, duckdb_types, duckdb_time = benchmark_query(duckdb_db, query)
    if duckdb_results:
        # Add column names and types as a header
        headers = [f"{col}\n{typ}" for col, typ in zip(duckdb_columns, duckdb_types)]
        print(f"{Colors.OKGREEN}DuckDB Results:{Colors.ENDC}")
        print(tabulate(duckdb_results, headers=headers, tablefmt="fancy_grid"))
    else:
        print(f"{Colors.OKGREEN}DuckDB Results:{Colors.ENDC} No results")
    print(f"{Colors.OKCYAN}DuckDB Time:{Colors.ENDC} {duckdb_time:.6f} seconds\n")

def main():
    sqlite_db = SQLite('sqlite.db')
    duckdb_db = DuckDB('duckdb.db')

    # Schema setup
    setup_file = 'sql_benchmarks/setup.sql'
    execute_queries_from_file(sqlite_db, setup_file)
    execute_queries_from_file(duckdb_db, setup_file)

    # Benchmark queries
    query_file = 'sql_benchmarks/queries.sql'
    with open(query_file, 'r') as file:
        queries = file.read().split(';')

    for query in queries:
        if query.strip():  # Skip empty queries
            print(f"\n{Colors.HEADER}{'=' * 60}{Colors.ENDC}")
            print(f"{Colors.HEADER}Running query:{Colors.ENDC} {Colors.BOLD}{query.strip()}{Colors.ENDC}")
            print(f"{Colors.HEADER}{'=' * 60}{Colors.ENDC}\n")

            # Benchmark SQLite
            benchmark_sqlite(sqlite_db, query)

            # Benchmark DuckDB
            benchmark_duckdb(duckdb_db, query)

            print(f"{Colors.WARNING}{'-' * 60}{Colors.ENDC}")
    
    # Close connections
    sqlite_db.close()
    duckdb_db.close()

if __name__ == "__main__":
    main()