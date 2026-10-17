CREATE SCHEMA IF NOT EXISTS lake.silver;

CREATE OR REPLACE TABLE lake.silver.customer_directory WITH (format = 'PARQUET') AS
SELECT c.custkey, c.name, c.acctbal, n.name AS nation,
       c.address, c.phone, c.comment
FROM lake.bronze.customer c
JOIN lake.bronze.nation n ON n.nationkey = c.nationkey;

CREATE OR REPLACE TABLE lake.silver.order_returns WITH (format = 'PARQUET') AS
SELECT o.orderkey, o.custkey, o.orderdate,
       count(*) AS returned_positions,
       sum(l.extendedprice * (1 - l.discount)) AS returned_value
FROM lake.bronze.orders o
JOIN lake.bronze.lineitem l ON l.orderkey = o.orderkey
WHERE o.orderdate >= DATE '1993-10-01'
  AND o.orderdate < DATE '1994-01-01'
  AND l.returnflag = 'R'
GROUP BY o.orderkey, o.custkey, o.orderdate;
