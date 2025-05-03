import sqlite3

class SQLite:
    """Interface for interacting with SQLite database."""
    def __init__(self, db_path):
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()

    def __get_column_metadata(self):
        """Extract column names and types from the cursor description."""
        column_names = []
        column_types = []
        if self.cursor.description:
            for col in self.cursor.description:
                column_names.append(col[0])
                column_types.append(type(col[0]).__name__ if col[0] is not None else "None")
        return column_names, column_types

    def execute_query(self, query, fetch_metadata=True):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        
        if fetch_metadata:
            column_names, column_types = self.__get_column_metadata()
        else:
            column_names, column_types = [], []
        
        return results, column_names, column_types

    def close(self):
        self.connection.close()