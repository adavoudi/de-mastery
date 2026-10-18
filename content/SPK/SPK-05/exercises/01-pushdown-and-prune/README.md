# SPK-05 / 01 · Make the scan read less

The `orders` table is a 7-column Parquet file. A downstream report needs only the `order_id` and
`currency` of USD orders. Your job is to write the transformation so that **Catalyst pushes the
filter into the scan and prunes the unused columns**. You learned to read these two gains from
the physical plan.

## Task

Implement `usd_orders(df)`. It gets the raw `orders` DataFrame and returns a DataFrame that:

- keeps only rows where `currency == 'USD'`, and
- has exactly the columns `order_id` and `currency` (in any order).

## Constraints

- The filter must be a **native column predicate** (a `Column` expression or SQL string). Never
  use a Python UDF. A UDF would make the predicate opaque and `PushedFilters` would be empty.
- Write the `select`/`filter` as DataFrame operations. Then Catalyst can rewrite them into the
  scan.

## How it is graded

- **Data (DuckDB oracle):** the rows must equal `SELECT order_id, currency FROM orders WHERE
  currency = 'USD'`.
- **Plan — pushdown:** the `PushedFilters` of the physical plan must reference `currency`.
- **Plan — pruning:** the `ReadSchema` of the scan must list only `order_id` and `currency`, not
  all 7 columns.
- The test does not check row order or column order.
