# Comparing Joins in DuckDB and SQLite Databases

## A Bit About This Project

This project examines the performance of join operations between DuckDB and SQLite databases. The focus lies on comparing their performance in *OLTP (Online Transaction Processing)* and *OLAP (Online Analytical Processing)* workloads; the comparison utilises queries from the TPC-H benchmark — a standard for evaluating database performance.

## OLTP and OLAP

| **OLTP**                          | **OLAP**                          |
|-----------------------------------|-----------------------------------|
| Designed for transactional tasks. | Designed for analytical tasks.    |
| Handles a large number of short online transactions (e.g., INSERT, UPDATE). | Handles complex queries for data analysis. |
| Prioritizes speed and reliability. | Prioritizes query performance and insights. |
| Example: Banking systems.         | Example: Business intelligence.  |

## What is TPC-H?

To-do ...

## Benchmarking Script and Query Files

| **File**            | **Description**                                                                      |
|---------------------|--------------------------------------------------------------------------------------|
| `db_benchmark.py`   | Python script to benchmark query execution times in DuckDB and SQLite databases.     |
| `sql_benchmarks/`   | Contains SQL files for setting up the database schema and running benchmark queries. |

## Installation

Ensure you have the required dependencies installed. Run the following command:

```bash
pip install -r requirements.txt
```

## How to Run

To-do ...

## Results

The results demonstrate the execution time of queries in both DuckDB and SQLite; this comparison illustrates their performance characteristics in OLTP and OLAP contexts.

To-do ...

## Conclusion

To-do ...
