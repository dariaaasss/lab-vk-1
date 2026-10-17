CREATE SCHEMA IF NOT EXISTS lake.gold;

CREATE OR REPLACE TABLE lake.gold.customer_refunds WITH (format = 'PARQUET') AS
SELECT c.custkey, c.name, sum(r.returned_value) AS revenue,
       c.acctbal, c.nation, c.address, c.phone, c.comment
FROM lake.silver.customer_directory c
JOIN lake.silver.order_returns r ON r.custkey = c.custkey
GROUP BY c.custkey, c.name, c.acctbal, c.nation, c.address, c.phone, c.comment
ORDER BY revenue DESC
LIMIT 20;
