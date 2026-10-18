# SPK-08 / 03 · Persist a reused DataFrame at the right storage level

Caching only helps when a DataFrame is **reused** across multiple actions, and the storage level
trades memory for CPU/disk. Here you build a shared aggregate once and persist it at a requested
`StorageLevel`, materialized so the block is actually stored — the same instinct as materializing a
reused CTE in your warehouse.

## Task

Implement `persist_totals(spark, order_items, level)` that:

1. computes the per-company sum of `line_amount` as columns `company_id`, `total`,
2. persists that DataFrame at the given `level` (a `StorageLevel` attribute name, e.g.
   `"MEMORY_AND_DISK"` or `"DISK_ONLY"`),
3. materializes it with an action so the block is stored, and
4. returns the persisted DataFrame.

## Constraints

- Use `persist(StorageLevel.<level>)`, not a hard-coded `cache()` (which is always
  `MEMORY_AND_DISK`).
- Trigger an action so `df.storageLevel` reflects a stored block.

## How it is graded

- **Storage level (API):** `df.storageLevel` equals the requested `StorageLevel` (checked for
  `MEMORY_AND_DISK` and `DISK_ONLY`).
- **Data (DuckDB oracle):** equals `SELECT company_id, sum(line_amount) FROM order_items GROUP BY
  company_id` (float tolerance).
- **Reuse:** calling two actions on the returned DataFrame yields consistent results (it is a valid
  reusable, persisted frame).
