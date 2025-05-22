import sqlite3
from decimal import Decimal

db = sqlite3.connect('sqlite.db')
cur = db.cursor()

query = '''
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE('1994-01-01')
    AND l_shipdate < DATE('1995-01-01')
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24;
'''

print("=== EXPLAIN QUERY PLAN ===")
print("Shows the high-level execution strategy:")
explain_plan = cur.execute(f"EXPLAIN QUERY PLAN {query}").fetchall()
for row in explain_plan:
    print(f"ID: {row[0]}, Parent: {row[1]}, Detail: {row[3]}")

print("\n=== EXPLAIN (Bytecode) ===")
print("Shows the SQLite virtual machine bytecode (first 10 operations):")
explain_bytecode = cur.execute(f"EXPLAIN {query}").fetchall()
for i, row in enumerate(explain_bytecode):  # Show first 10 operations
    print(f"Op {row[0]}: {row[1]} P1={row[2]} P2={row[3]} P3={row[4]} P4={row[5]}")

db.close()