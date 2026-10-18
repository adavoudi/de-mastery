# SPK-07 / 02 · Reduce files after a filter without a shuffle

This is a common pipeline shape. A DataFrame is spread across many partitions. A selective filter makes
it smaller, and now you have many near-empty files. The cheap fix is `coalesce`. It **merges** existing
partitions with **no shuffle** (a narrow transformation, no `Exchange`). If you use `repartition` here,
you force a full shuffle that you do not need.

## Task

Implement `reduce_usd(df, num_files)`. It must do these two things:

1. Filter `df` to `currency == 'USD'`.
2. Reduce the result to exactly `num_files` partitions **without adding a shuffle**.

Return the resulting DataFrame.

## Constraints

- Use `coalesce`, not `repartition`, for the reduction (it must not add an `Exchange`).
- `num_files` is smaller than the input partition count (coalesce only reduces).

## How it is graded

- **Data (DuckDB oracle):** rows equal `SELECT * FROM orders WHERE currency = 'USD'`.
- **Partitions:** `df.rdd.getNumPartitions()` on your result equals `num_files`.
- **Plan — no added shuffle:** your reduction adds **no `Exchange`** over the base plan. The test
  gives you a base that already has many partitions. It records the `Exchange` count of the base and
  checks that your result has the same count (coalesce is narrow). A `repartition` would add one and fail.
