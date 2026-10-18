# Port three dbt models to Flink SQL (check the changelog)

You write three familiar dbt models again as continuously-running Flink SQL
queries. The new part is **changelog semantics**. One model is append-only and two are
updating. The tests check *which RowKinds* each query emits **and** that the
folded final state matches an independent DuckDB answer.

## Task

All three queries read a table named `orders`
`(order_id, company_id, customer_id, currency, amount)`. The test creates it.
Write each query in its own file under `queries/` (no trailing semicolon):

1. `queries/stg_valid_orders.sql` — a staging filter/project. Select the columns
   `WHERE currency IN ('USD','EUR','GBP','JPY')`. This is **append-only** (only
   `+I`).
2. `queries/count_per_customer.sql` — `COUNT(*)` of orders per `customer_id`
   (`GROUP BY customer_id`). This is an **updating** query (`-U`/`+U`).
3. `queries/gmv_per_company.sql` — `SUM(amount)` per `company_id`
   (`GROUP BY company_id`), over **all** orders. This is also **updating**.

## What the tests check

- `stg_valid_orders` emits **only `+I`** (append-only). Its rows equal the
  DuckDB filter result.
- `count_per_customer` emits **`-U`/`+U`** (updating). Its folded final state
  equals the DuckDB count per customer.
- `gmv_per_company` emits `-U`/`+U`. Its folded final state equals the DuckDB
  sum per company. `c1` must be 190 (the query does not filter by currency).

## Hints

- An unbounded `GROUP BY` is an updating query. It emits `+I`, then `-U`/`+U` when
  more rows for a key arrive. The final folded state is the batch answer.
- A plain `SELECT ... WHERE ...` is append-only.
- `COUNT(*)` returns `BIGINT`. `SUM(amount)` sums the integer amounts.
