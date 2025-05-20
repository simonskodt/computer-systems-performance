# Computer Systems Performance

This repository contains assignments for the course Computer Systems Performance.

> [!NOTE]  
> This course focuses on analyzing and improving the performance of data-intensive computer systems. We learn to design performance experiments, use appropriate tools, and understand system layers to troubleshoot and optimize performance. Prior knowledge required: database systems, operating systems, and programming in C/C++.

## Project 01

In [`01_partitioning`](./01_partitioning), the task in this assignment is to reproduce two of the four partitioning algorithms described in the paper titled "Data Partitioning on Chip Multiprocessors." The purpose of the paper is to outline some partitioning strategies that are used in databases to parallelize computation in chip multiprocessors (CMPs).

## Project 02

In [`02_db_oltp_vs_olap`](./02_db_oltp_vs_olap), we compare two embedded database technologies: DuckDB and SQLite. We examine these databases to highlight their different designs, where DuckDB is optimised for OLAP (analytical processing) and SQLite for OLTP (transactional processing). We use TPC-H and TPC-C benchmarks to evaluate the strengths and weaknesses of these databases.
