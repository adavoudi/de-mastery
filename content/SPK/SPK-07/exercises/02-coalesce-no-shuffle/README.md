# SPK-07 / 02 · Reduce files after a filter without a shuffle

A common pipeline shape: a DataFrame is spread across many partitions, a selective filter shrinks
it, and now you have many near-empty files. The cheap fix is `coalesce` — it **merges** existing
partitions with **no shuffle** (a narrow transformation, no `Exchange`). Reaching for
`repartition` here would force an unnecessary full shuffle.

## Task

Implement `reduce_usd(df, num_files)` that:

1. filters `df` to `currency == 'USD'`, and
2. reduces the result to exactly `num_files` partitions **without adding a shuffle**.

Return the resulting DataFrame.

## Constraints

- Use `coalesce`, not `repartition`, for the reduction (no `Exchange` must be added by it).
- `num_files` is smaller than the input partition count (coalesce only reduces).

## How it is graded

- **Data (DuckDB oracle):** rows equal `SELECT * FROM orders WHERE currency = 'USD'`.
- **Partitions:** `df.rdd.getNumPartitions()` on your result equals `num_files`.
- **Plan — no added shuffle:** your reduction adds **no `Exchange`** over the base plan. The test
  gives you a base that already has many partitions, records its `Exchange` count, and checks your
  result's count is the same (coalesce is narrow). A `repartition` would add one and fail.
