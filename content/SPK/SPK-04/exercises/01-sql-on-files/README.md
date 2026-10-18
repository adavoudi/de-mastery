# Build a revenue mart with SQL on files and a temp view

Rebuild a small dbt-style mart in Spark SQL. The source is the `order_items` Parquet file, which
contains a few **duplicate line rows** (the deliberate dirt). You must dedup before you aggregate,
or the duplicates inflate revenue.

Implement `company_revenue(spark, order_items_path)` in `solution.py`. It must:

1. Read the Parquet file at `order_items_path` (via the DataFrame reader, or directly in SQL with
   ``FROM parquet.`<path>` ``).
2. Remove exact duplicate line rows (dedup on the full business key:
   `company_id, order_item_id, order_id, product_code, qty, line_amount`).
3. Register the cleaned data as a temporary view and query it with `spark.sql(...)` (use at least
   one temp view — this is the `ref()` analogue).
4. Return a DataFrame with one row per company:

| column | value |
|---|---|
| `company_id` | the group key |
| `revenue` | `SUM(line_amount)` over deduped rows |
| `units` | `SUM(qty)` over deduped rows |

The test compares against a DuckDB oracle (which dedups the same way). Row/column order are
ignored; floats compared with tolerance.
