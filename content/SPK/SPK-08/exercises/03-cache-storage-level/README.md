# SPK-08 / 03 · Persist a reused DataFrame at the right storage level

Caching only helps when you **reuse** a DataFrame across multiple actions. The storage level
trades memory for CPU/disk. In this exercise, you build a shared aggregate once. You persist it at a
requested `StorageLevel`. You materialize it, so that Spark really stores the block. This uses the same
instinct as materializing a reused CTE in your warehouse.

## Task

Implement `persist_totals(spark, order_items, level)`. It must do these four things:

1. Compute the per-company sum of `line_amount` as columns `company_id`, `total`.
2. Persist that DataFrame at the given `level` (a `StorageLevel` attribute name, for example
   `"MEMORY_AND_DISK"` or `"DISK_ONLY"`).
3. Materialize it with an action, so that Spark stores the block.
4. Return the persisted DataFrame.

## Constraints

- Use `persist(StorageLevel.<level>)`. Do not hard-code `cache()` (it is always
  `MEMORY_AND_DISK`).
- Trigger an action, so that `df.storageLevel` shows a stored block.

## How it is graded

- **Storage level (API):** `df.storageLevel` equals the requested `StorageLevel` (checked for
  `MEMORY_AND_DISK` and `DISK_ONLY`).
- **Data (DuckDB oracle):** equals `SELECT company_id, sum(line_amount) FROM order_items GROUP BY
  company_id` (float tolerance).
- **Reuse:** two actions on the returned DataFrame give consistent results (it is a valid
  reusable, persisted frame).
