# SCD2 with MERGE INTO, checked against a DuckDB oracle

Apply a daily batch to a Slowly-Changing-Dimension (Type 2) stored in Iceberg. Version the history
exactly as you do in your Redshift/dbt dimensions. Implement this in `solution.py`:

## `apply_scd2(spark, dim_ident, src_values_sql, batch_date)`

- `dim_ident` — the dimension table, for example `local.db.dim`. It has the columns
  `nk INT, attr STRING, valid_from DATE, valid_to DATE, is_current BOOLEAN`.
- `src_values_sql` — a SQL query that yields the natural-key rows of the day with the columns
  `nk, attr` (for example `SELECT * FROM VALUES (1,'z'),(3,'w') AS t(nk, attr)`).
- `batch_date` — the effective date of this batch as a `'YYYY-MM-DD'` string.

Do it in **two steps, in this order**:

1. **Close** every *current* row whose tracked attribute changed. Set its `valid_to = batch_date`
   and `is_current = false`. Use `MERGE INTO ... WHEN MATCHED AND t.attr <> u.attr THEN UPDATE ...`
   and join on `nk` and `t.is_current = true`.
2. **Insert** a new open version (`valid_from = batch_date`, `valid_to = NULL`, `is_current = true`)
   for every source key that is **not** currently present as a current row. This covers both the
   keys you just closed and brand-new keys.

Rules and traps:
- **Do step 1 before step 2.** If you insert first, the new row looks current and the close step
  fires wrongly.
- **Never overwrite the attribute in place.** That is SCD Type 1 and it destroys history.
- **The `valid_to` of the old row must equal the `valid_from` of the new row.** The dates are
  contiguous: no gap and no overlap.
- Leave unchanged keys exactly as they were. Each brand-new key gets one current row.

The test seeds a day-1 dimension and applies a day-2 batch through your function. Then it compares
the full table against an **independent DuckDB oracle**. The oracle computes the same close and
insert over the same batches (`assert_frames_equal`, order-independent). The test also checks the
SCD2 invariants directly: one current row per key, and contiguous dates.
