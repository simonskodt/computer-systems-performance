from typing import List, Tuple, Any
import sqlite3
import csv

class SQLite:
    def __init__(self, db_path: str) -> None:
        self.connection: sqlite3.Connection = sqlite3.connect(db_path)
        self.cursor: sqlite3.Cursor = self.connection.cursor()

    def load_csv(self, table_name: str, csv_path: str) -> None:
        # 1) Get the column names from the table schema
        self.cursor.execute(f"PRAGMA table_info({table_name});")
        cols_info = self.cursor.fetchall()
        if not cols_info:
            raise ValueError(f"Table {table_name} has no columns or does not exist")
        cols = [col[1] for col in cols_info]  # column-name is at index 1

        # 2) Build the INSERT statement
        placeholders = ", ".join("?" for _ in cols)
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES ({placeholders});"

        # 3) Read the CSV, detect header, and collect all data rows
        rows = []
        with open(csv_path, newline='') as f:
            reader = csv.reader(f)
            try:
                first = next(reader)
            except StopIteration:
                return  # empty file, nothing to load

            # If the first row *exactly* matches our column names, skip it
            if first != cols:
                rows.append(first)

            # Read the rest
            for row in reader:
                rows.append(row)

        # 4) Bulk‐insert
        if rows:
            self.cursor.executemany(insert_sql, rows)
            self.connection.commit()

    def __get_column_metadata(self) -> Tuple[List[str], List[str]]:
        names: List[str] = []
        types: List[str] = []
        if self.cursor.description:
            for col in self.cursor.description:
                names.append(col[0])
                types.append(type(col[0]).__name__ if col[0] is not None else "None")
        return names, types

    def execute_query(self, query: str, fetch_metadata: bool = True) -> Tuple[List[Tuple[Any, ...]], List[str], List[str]]:
        self.cursor.execute(query)
        rows: List[Tuple[Any, ...]] = self.cursor.fetchall()

        if fetch_metadata:
            names, types = self.__get_column_metadata()
        else:
            names, types = [], []

        return rows, names, types

    def close(self) -> None:
        self.connection.close()
