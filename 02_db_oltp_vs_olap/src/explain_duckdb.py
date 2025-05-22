import duckdb

db = duckdb.connect('duckdb.db')
cur = db.cursor()

query = '''
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24;
'''

print("=== EXPLAIN (Default - Optimized Physical Plan) ===")
explain_result = cur.execute(f"EXPLAIN {query}").fetchall()
# Access the second element (index 1) of the first tuple (index 0)
if explain_result:
    print(explain_result[0][1])

print("\n=== EXPLAIN ANALYZE ===")
print("Shows actual execution statistics and timing:")
analyze_result = cur.execute(f"EXPLAIN ANALYZE {query}").fetchall()
# Access the second element (index 1) of the first tuple (index 0)
if analyze_result:
    print(analyze_result[0][1])

db.close()