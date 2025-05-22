# Comparing DuckDB and SQLite Databases

## A Bit About This Project

This project examines the performance of join operations between DuckDB and SQLite databases. The focus lies on comparing their performance in *OLTP (Online Transaction Processing)* and *OLAP (Online Analytical Processing)* workloads; the comparison utilises queries from the TPC-H benchmark — a standard for evaluating database performance.

## OLTP and OLAP

| **OLTP**                          | **OLAP**                          |
|-----------------------------------|-----------------------------------|
| Designed for transactional tasks. | Designed for analytical tasks.    |
| Handles a large number of short online transactions (e.g., INSERT, UPDATE). | Handles complex queries for data analysis. |
| Prioritizes speed and reliability. | Prioritizes query performance and insights. |
| Example: Banking systems.         | Example: Business intelligence.  |

## Installation

Ensure you have the required dependencies installed. Run the following command:

```bash
pip install -r requirements.txt
```

## How to Run

Navigate to the src folder. From there, execute the following command to run either the TPC-H or TPC-C benchmark:

```bash
python3 db_benchmark.py --benchmark TPC_C
```

The benchmark flag is required; however, there are additional optional flags:

- `--sf`: changes the scale factor
- `--result`: prints the query result
- `--reuse`: skips dataset population and loading, instead running queries on an existing database setup

Here is an example command that uses these flags:

```bash
python3 db_benchmark.py --benchmark TPC_H --results --sf 5
```

> [!IMPORTANT]
> Note that some flag combinations are incompatible. For instance, you cannot use the `--reuse` flag while changing the 
scale factor, as this would create inconsistencies in the database setup.

## Results

The results demonstrate the execution time of queries in both DuckDB and SQLite; this comparison illustrates their performance characteristics in OLTP and OLAP contexts.

> [!NOTE]
> To change which TPC-H queries are executed, modify the query_numbers list in db_benchmark.py. 
Some queries are too complex for SQLite to process efficiently; for instance, query 4 requires examining 1 trillion rows 
and did not complete within an hour of execution time.

An example of the output could be:

```bash
> python3 db_benchmark.py --benchmark TPC_H --reuse
Starting the database benchmark script...
Selected benchmark: TPC_H
Reusing existing TPC-H data.
Running benchmarks on TPC-H queries: [1, 6, 13]...
============================================================
------------------ Running TPC-H Query 1 -------------------
============================================================
Executing query:
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE('1998-09-02' )
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
------------------------------------------------------------
SQLite Time: 4.987997 seconds
DuckDB Time: 0.036637 seconds
============================================================
------------------ Running TPC-H Query 6 -------------------
============================================================
Executing query:
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE('1994-01-01' )
    AND l_shipdate < DATE('1995-01-01' )
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24
------------------------------------------------------------
SQLite Time: 0.796389 seconds
DuckDB Time: 0.005441 seconds
============================================================
------------------ Running TPC-H Query 13 ------------------
============================================================
Executing query:
SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey) AS c_count
    FROM
        customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
------------------------------------------------------------
SQLite Time: 6.788656 seconds
DuckDB Time: 0.100556 seconds

Finished running TPC-H queries.
Benchmark script completed successfully.
```
