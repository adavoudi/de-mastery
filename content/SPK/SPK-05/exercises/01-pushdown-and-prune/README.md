# SPK-05 / 01 · Make the scan read less

The `orders` table is a 7-column Parquet file. A downstream report only needs the `order_id` and
`currency` of USD orders. Your job is to write the transformation so **Catalyst pushes the filter
into the scan and prunes the unused columns** — the two wins you learned to read off the physical
plan.

## Task

Implement `usd_orders(df)` which, given the raw `orders` DataFrame, returns a DataFrame that:

- keeps only rows where `currency == 'USD'`, and
- has exactly the columns `order_id` and `currency` (in any order).

## Constraints

- The filter must be a **native column predicate** (a `Column` expression or SQL string), never a
  Python UDF — a UDF would make the predicate opaque and empty `PushedFilters`.
- Do the `select`/`filter` as DataFrame operations so Catalyst can rewrite them into the scan.

## How it is graded

- **Data (DuckDB oracle):** the rows must equal `SELECT order_id, currency FROM orders WHERE
  currency = 'USD'`.
- **Plan — pushdown:** the physical plan's `PushedFilters` must reference `currency`.
- **Plan — pruning:** the scan's `ReadSchema` must list only `order_id` and `currency`, not all 7
  columns.
- Row and column order are not checked.
