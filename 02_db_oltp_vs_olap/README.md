# Comparing Joins in DuckDB and SQLite Databases




## How to Run with Make

Build the Project: `make`

Run the Program: `make run ARGS="your_arguments_here"`

Clean the Build: `make clean`

### CMake

...

### DuckDB

There are multiple ways of including the duckdb header file. 

1. One way is by installing the duckdb database using brew or winget, and then point to the path of the include folder using .vscode/c_cpp_properties.json file. 
2. The other way is installing the correct asset from the duckdb repository, and then manually copy the header file into the include folder in this repository. Find the assert here: https://github.com/duckdb/duckdb/releases/tag/v1.2.1.

Copy command: `gcc -o duckdb src/duckdb.c -I/opt/homebrew/Cellar/duckdb/1.2.1/include -L/opt/homebrew/Cellar/duckdb/1.2.1/lib -lduckdb`

### SQLite


