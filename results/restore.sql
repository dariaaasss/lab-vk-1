UPDATE lake.gold.customer_refunds SET revenue = 0 WHERE custkey = 104737;

SELECT revenue FROM lake.gold.customer_refunds FOR VERSION AS OF 4671655699032731848 WHERE custkey = 104737;

ALTER TABLE lake.gold.customer_refunds EXECUTE rollback_to_snapshot(4671655699032731848);
