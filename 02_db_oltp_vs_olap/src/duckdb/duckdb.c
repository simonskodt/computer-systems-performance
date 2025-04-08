#include <stdio.h>
#include <stdlib.h>
#include "duckdb.h"

int main() {
    duckdb_database db;
    duckdb_connection con;

    // Open an in-memory database (use a file path for persistent storage)
    if (duckdb_open(NULL, &db) == DuckDBError) {
        fprintf(stderr, "Failed to open DuckDB database\n");
		goto cleanup;
    }

    // Create a connection to the database
    if (duckdb_connect(db, &con) == DuckDBError) {
        fprintf(stderr, "Failed to connect to DuckDB database\n");
		goto cleanup;
    }

    // Execute a query
    duckdb_result result;
    if (duckdb_query(con, "CREATE TABLE test (id INTEGER, name VARCHAR);", NULL) == DuckDBError) {
        fprintf(stderr, "Failed to execute query\n");
		goto cleanup;
    }

    // Insert data
    duckdb_query(con, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');", NULL);

    // Query data
    if (duckdb_query(con, "SELECT * FROM test;", &result) == DuckDBError) {
        fprintf(stderr, "Failed to execute query\n");
    } else {
        // Print results
        for (idx_t row = 0; row < duckdb_row_count(&result); row++) {
            printf("Row %llu: id=%d, name=%s\n", row,
                   duckdb_value_int32(&result, row, 0),
                   duckdb_value_varchar(&result, row, 1));
        }
        duckdb_destroy_result(&result);
    }

    // Cleanup
    duckdb_disconnect(&con);
    duckdb_close(&db);

    return EXIT_SUCCESS;
    
    cleanup:
        duckdb_destroy_result(&result);
        duckdb_disconnect(&con);
        duckdb_close(&db);
        return EXIT_FAILURE;
}