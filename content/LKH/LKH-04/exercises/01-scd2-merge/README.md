# SCD2 with MERGE INTO, checked against a DuckDB oracle

Apply a daily batch to a Slowly-Changing-Dimension (Type 2) stored in Iceberg, versioning history
exactly as you do in your Redshift/dbt dimensions. Implement in `solution.py`:

## `apply_scd2(spark, dim_ident, src_values_sql, batch_date)`

- `dim_ident` — the dimension table, e.g. `local.db.dim`, with columns
  `nk INT, attr STRING, valid_from DATE, valid_to DATE, is_current BOOLEAN`.
- `src_values_sql` — a SQL query yielding the day's natural-key rows with columns `nk, attr`
  (e.g. `SELECT * FROM VALUES (1,'z'),(3,'w') AS t(nk, attr)`).
- `batch_date` — the effective date for this batch as a `'YYYY-MM-DD'` string.

Do it in **two steps, in this order**:

1. **Close** every *current* row whose tracked attribute changed: set its `valid_to = batch_date`
   and `is_current = false` (use `MERGE INTO ... WHEN MATCHED AND t.attr <> u.attr THEN UPDATE ...`
   joining on `nk` and `t.is_current = true`).
2. **Insert** a new open version (`valid_from = batch_date`, `valid_to = NULL`, `is_current = true`)
   for every source key that is **not** currently present as a current row — this covers both the
   keys you just closed and brand-new keys.

Rules / traps:
- **Step 1 before step 2.** If you insert first, the new row looks current and the close step
  mis-fires.
- **Never overwrite the attribute in place** — that is SCD Type 1 and destroys history.
- **The old row's `valid_to` must equal the new row's `valid_from`** (contiguous, no gap, no
  overlap).
- Unchanged keys must be left exactly as they were; brand-new keys get one current row.

The test seeds a day-1 dimension, applies a day-2 batch through your function, and compares the full
table against an **independent DuckDB oracle** that computes the same close/insert over the same
batches (`assert_frames_equal`, order-independent). It also checks the SCD2 invariants directly
(one current row per key; contiguous dates).
