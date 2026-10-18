-- C3 / M1 — Flink SQL stub: build SCD2 dim_customer from the CDC changelog.
-- TODO(learner): read capstones/C3/data/cdc_customers.jsonl via the debezium-json
-- filesystem connector, derive valid_from/valid_to/is_current, write to
-- dim_customer (ddl/dim_customer_scd2.sql).
--
-- This stub is intentionally incomplete so the acceptance test FAILS (G1).
-- Replace the body below.

-- Source: bounded debezium-json changelog (STH-04 no-Docker path).
CREATE TABLE cdc_customers (
    customer_id INT,
    company_id  INT,
    name        STRING,
    email       STRING,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'filesystem',
    'path' = '${CDC_CUSTOMERS_PATH}',
    'format' = 'debezium-json'
);

-- TODO: INSERT INTO dim_customer SELECT ... (SCD2 close-and-open logic)
