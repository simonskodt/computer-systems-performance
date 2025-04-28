import sqlite3
import duckdb
import time

class SQLite:
    """Interface for interacting with SQLite database."""
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        self.connection.close()

class DuckDB:
    """Interface for interacting with DuckDB database."""
    def __init__(self, db_path):
        self.con = duckdb.connect(db_path)

    def execute_query(self, query):
       return self.con.sql(query)
    
    def close(self):
        self.con.close()


def main():
    sqlite_db = SQLite('sqlite.db')
    duckdb_db = DuckDB('duckdb.db')

    # Create a sample table and insert data
    sqlite_db.execute_query("CREATE TABLE IF NOT EXISTS test (i INTEGER)")
    sqlite_db.execute_query("INSERT INTO test VALUES (42)")

    duckdb_db.execute_query("CREATE TABLE IF NOT EXISTS test (i INTEGER)")
    duckdb_db.execute_query("INSERT INTO test VALUES (42)")

    # timed queries

    start_time = time.time()
    sqlite_results = sqlite_db.execute_query('SELECT * FROM test')
    end_time = time.time()
    sqlite_time = end_time - start_time

    start_time = time.time()
    duckdb_results = duckdb_db.execute_query('SELECT * FROM test')
    end_time = time.time()
    duckdb_time = end_time - start_time

    # Print results
    print("SQLite Results:", sqlite_results)
    print("SQLite Time:", sqlite_time)
    print("DuckDB Results:\n", duckdb_results)
    print("DuckDB Time:", duckdb_time)
    
    # Close connections
    sqlite_db.close()
    duckdb_db.close()

if __name__ == "__main__":
    main()
    
    
    
    
