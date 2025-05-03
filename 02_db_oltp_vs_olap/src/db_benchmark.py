from sqlite_handler import SQLite
from duckdb_handler import DuckDB
from benchmark_utils import benchmark_sqlite, benchmark_duckdb
from colors import Colors

def main():
    sqlite_db = SQLite('sqlite.db')
    duckdb_db = DuckDB('duckdb.db')

    # Schema setup
    setup_file = '../sql_benchmarks/setup.sql'
    __execute_queries_from_file(sqlite_db, setup_file)
    __execute_queries_from_file(duckdb_db, setup_file)

    # Benchmark queries
    query_file = '../sql_benchmarks/queries.sql'
    with open(query_file, 'r') as file:
        queries = file.read().split(';')

    alternate_color = True  # Variable to alternate colors

    for query in queries:
        if query.strip():  # Skip empty queries
            header_color = Colors.HEADER if alternate_color else Colors.HEADER2
            alternate_color = not alternate_color  # Toggle the color

            print(f"\n{header_color}{'=' * 60}{Colors.ENDC}")

            centered_text = " RUNNING QUERY ".center(60, "-")
            print(f"{header_color}{Colors.BOLD}{centered_text}{Colors.ENDC}")
            print(f"{header_color}{'=' * 60}{Colors.ENDC}")
            print(f"{query.strip()}")
            print(f"{header_color}{'-' * 60}{Colors.ENDC}")

            PRINT_TABLES = True

            # Benchmark SQLite
            benchmark_sqlite(sqlite_db, query, PRINT_TABLES)

            print(f"{header_color}{'-' * 60}{Colors.ENDC}")

            # Benchmark DuckDB
            benchmark_duckdb(duckdb_db, query, PRINT_TABLES)

            print(f"{header_color}{'=' * 60}{Colors.ENDC}\n")
    
    # Close connections
    sqlite_db.close()
    duckdb_db.close()

def __execute_queries_from_file(db, file_path):
    """Execute all queries from a given SQL file."""
    with open(file_path, 'r') as file:
        queries = file.read()
        for query in queries.split(';'):
            if query.strip():  # Skip empty queries
                db.execute_query(query)

if __name__ == "__main__":
    main()