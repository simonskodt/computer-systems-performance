-- Drop the table if it exists to prevent primary key constraint violations
DROP TABLE IF EXISTS test_table;

-- Create a table with multiple columns
CREATE TABLE test_table (
    id INTEGER PRIMARY KEY,
    name TEXT,
    age INTEGER,
    salary REAL
);

-- Insert multiple rows into the table
INSERT INTO test_table (id, name, age, salary) VALUES
(1, 'Alice', 30, 70000.50),
(2, 'Bob', 25, 50000.00),
(3, 'Charlie', 35, 80000.75),
(4, 'Diana', 28, 60000.25),
(5, 'Eve', 40, 90000.00);