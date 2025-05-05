import sqlite3
import csv

class SQLite:
    """Interface for interacting with a SQLite database."""
    def __init__(self, db_path: str):
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()

    def load_csv(self, table_name: str, csv_path: str):
        """Bulk-load a CSV (with header) into the given table."""
        with open(csv_path, newline='') as f:
            reader = csv.reader(f)
            cols = next(reader)  # header row
            placeholder = ", ".join("?" for _ in cols)
            insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholder})"
            self.cursor.executemany(insert_sql, reader)
            self.connection.commit()

    def __get_column_metadata(self):
        names, types = [], []
        if self.cursor.description:
            for col in self.cursor.description:
                names.append(col[0])
                types.append(type(col[0]).__name__ if col[0] is not None else "None")
        return names, types

    def execute_query(self, query: str, fetch_metadata: bool = True):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        if fetch_metadata:
            names, types = self.__get_column_metadata()
        else:
            names, types = [], []
        return rows, names, types

    def close(self):
        self.connection.close()
