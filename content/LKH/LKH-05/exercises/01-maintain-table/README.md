# Fix a small-files Iceberg table (compact, sort, expire)

A streaming job has been micro-batching into the Iceberg table `local.lkh05.events` — one tiny
commit at a time — so the table is now a few hundred rows spread across **many small data files**,
each its own snapshot. Query planning is slow and S3 is bloated with old snapshots. You are the
on-call engineer: run the maintenance.

Implement `maintain(spark, table, sort_col)` in `solution.py`. It receives:

- `spark`: an active Spark session with the Iceberg `local` catalog wired up.
- `table`: the fully-qualified table name, e.g. `"local.lkh05.events"`.
- `sort_col`: the column to cluster by (feeds file pruning), e.g. `"event_date"`.

Your function must, **without changing the table's data (every row, byte-for-byte)**:

1. **Set a sort order** on `table` so rewritten files are clustered by `sort_col`
   (`ALTER TABLE ... WRITE ORDERED BY ...`).
2. **Compact** the many small files into few larger ones (`rewrite_data_files`).
3. **Expire old snapshots**, keeping only the latest (compacted) snapshot
   (`expire_snapshots` with an `older_than` cutoff — `retain_last` alone is blocked by the default
   `max_snapshot_age`).

Return value is ignored; the test inspects the table afterwards.

The test (DuckDB oracle over a snapshot of the original rows taken before you run) checks:

- the live **data file count** drops well below the number of input batches;
- the **average live file size** rises into a healthy band (bytes), i.e. files were actually
  combined;
- the **snapshot count** is small (old snapshots were expired);
- the **data is unchanged** — same rows as before maintenance;
- each remaining file is **internally sorted** by `sort_col` (the sort order was applied).

### Notes

- Inside a `CALL`, the procedure's `table` argument is the **catalog-relative** name
  (`'lkh05.events'`), while `ALTER TABLE` takes the **fully-qualified** name
  (`local.lkh05.events`). The test passes you the fully-qualified `table`; derive the relative name
  by stripping the leading `local.`.
- Keep it to a few hundred rows; this must run in well under a minute.
