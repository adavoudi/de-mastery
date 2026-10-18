-- C3 / M3 — fct_orders with an FX temporal (as-of) join.
-- Each order is enriched with usd_amount computed at the FX rate IN EFFECT at
-- order time (not the latest rate). In Flink SQL this is a temporal join against
-- the versioned fx_rates table FOR SYSTEM_TIME AS OF the order timestamp.
--
-- Grading note: the oracle recomputes usd_amount in DuckDB by joining each order
-- to the fx row with the greatest rate_date <= order date; the streaming result
-- must match (sth_oracle, order-insensitive).
CREATE TABLE fct_orders (
    order_id     INT,
    customer_id  INT,
    company_id   INT,
    order_ts     STRING,
    currency     STRING,
    amount       DOUBLE,              -- native currency
    fx_rate      DOUBLE,              -- usd_per_unit as of order_ts
    usd_amount   DOUBLE,              -- amount * fx_rate
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '1',
    'merge-engine' = 'deduplicate'
);
