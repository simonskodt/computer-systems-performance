import duckdb

class DuckDB:
    """Interface for interacting with DuckDB database."""
    def __init__(self, db_path):
        self.con = duckdb.connect(db_path)

    def __get_column_metadata(self, result):
        column_names = result.columns
        column_types = [str(type_info).split('.')[-1] for type_info in result.types]
        return column_names, column_types

    def execute_query(self, query, fetch_metadata=True):
        result = self.con.sql(query)
        
        # Handle DDL statements that return None
        if result is None:
            return [], [], []
        
        # Convert the DuckDBPyRelation object to a list of tuples
        results = result.fetchall()
        
        if fetch_metadata:
            column_names, column_types = self.__get_column_metadata(result)
        else:
            column_names, column_types = [], []
        
        return results, column_names, column_types
    
    def close(self):
        self.con.close()