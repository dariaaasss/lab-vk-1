CREATE TABLE lake.{schema}.customer WITH (format = '{format}') AS
SELECT custkey, name, acctbal, nationkey, address, phone, comment
FROM tpch.{scale}.customer;

CREATE TABLE lake.{schema}.orders WITH (format = '{format}') AS
SELECT orderkey, custkey, orderdate FROM tpch.{scale}.orders;

CREATE TABLE lake.{schema}.lineitem WITH (format = '{format}') AS
SELECT orderkey, extendedprice, discount, returnflag FROM tpch.{scale}.lineitem;

CREATE TABLE lake.{schema}.nation WITH (format = '{format}') AS
SELECT nationkey, name FROM tpch.{scale}.nation;
