# Port a pandas aggregation to the Pandas API on Spark

You have an old pandas snippet that summarizes order lines per company. It works in a notebook, but
only on data that fits in memory. Port it to the **Pandas API on Spark** (`pyspark.pandas`), so that it
runs distributed and lazily. Keep the pandas ergonomics.

Implement `line_summary(order_items)` in `solution.py`. `order_items` is a Spark DataFrame with
`company_id`, `order_item_id`, `order_id`, `product_code`, `qty`, `line_amount`. Return a **Spark
DataFrame** grouped by `company_id` with these columns:

| column | meaning |
|---|---|
| `company_id` | the group key |
| `n_lines` | number of order-line rows in the company |
| `total_line_amount` | sum of `line_amount` in the company |

Do the aggregation through the Pandas API on Spark (`df.pandas_api()` → pandas-style `groupby` →
`.to_spark()`). Do not use `collect()` to move data to the driver. The test compares against a DuckDB
oracle. It ignores the order of rows and columns, and floats use tolerance.

> Note: the Pandas API on Spark is distributed pandas. It is the right tool when the data is too
> big for one machine. For small data, real pandas or DuckDB is simpler. This is a porting
> exercise, so the dataset is small on purpose.
