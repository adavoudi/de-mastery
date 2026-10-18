-- C3 / M1 — SCD2 dim_customer (Paimon PK table, versioned).
-- A row per (customer_id, valid_from). is_current flags the live version.
-- Build it from the CDC changelog with a Flink job (see sql/01_dim_customer_scd2.sql).
--
-- Grading note: the no-Docker harness checks the CURRENT-version rows
-- (is_current = TRUE) against a DuckDB batch rebuild via sth_oracle.
CREATE TABLE dim_customer (
    customer_sk   STRING,              -- surrogate key: customer_id || '#' || valid_from
    customer_id   INT,
    company_id    INT,
    name          STRING,
    email         STRING,
    valid_from    STRING,              -- ISO ts the version became effective
    valid_to      STRING,              -- ISO ts it was superseded ('9999-12-31' if current)
    is_current    BOOLEAN,
    PRIMARY KEY (customer_sk) NOT ENFORCED
) WITH (
    'bucket' = '1',
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'lookup'   -- true before/after changelog (probe 2)
);
