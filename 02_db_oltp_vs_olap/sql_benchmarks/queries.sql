-- Perform some basic SELECT queries

-- Select all rows
SELECT * FROM test_table;

-- Select rows with a condition
SELECT * FROM test_table WHERE age > 30;

-- Aggregate query
SELECT AVG(salary) AS average_salary FROM test_table;

-- Group by query
SELECT age, COUNT(*) AS count FROM test_table GROUP BY age;