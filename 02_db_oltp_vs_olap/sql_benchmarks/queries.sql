-- ============================================================
-- TPC-H example benchmark queries
-- ============================================================

-- Q1: Pricing summary report
SELECT
  l_returnflag,
  l_linestatus,
  SUM(l_quantity) AS sum_qty,
  SUM(l_extendedprice) AS sum_base_price,
  SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
  SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
  AVG(l_quantity) AS avg_qty,
  AVG(l_extendedprice) AS avg_price,
  AVG(l_discount) AS avg_disc,
  COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- Q3: Shipping priority report
SELECT
  o_orderkey,
  o_orderdate,
  o_shippriority,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM customer
JOIN orders    ON c_custkey = o_custkey
JOIN lineitem  ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'BUILDING'
  AND o_orderdate   < '1995-03-15'
  AND l_shipdate    > '1995-03-15'
GROUP BY o_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC
LIMIT 10;

-- Q5: Regional supplier volume (for region 'ASIA')
SELECT
  r_name    AS region,
  SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM region
JOIN nation   ON r_regionkey = n_regionkey
JOIN supplier ON s_nationkey = n_nationkey
JOIN lineitem ON l_suppkey = s_suppkey
JOIN orders   ON l_orderkey = o_orderkey
WHERE r_name BETWEEN 'ASIA' AND 'ASIA'
  AND o_orderdate BETWEEN '1994-01-01' AND '1994-12-31'
GROUP BY r_name
ORDER BY revenue DESC;

-- Q6: Forecast revenue loss
SELECT
  SUM(l_extendedprice * l_discount) AS revenue_loss
FROM lineitem
WHERE l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24;
