# Fix a small-files Iceberg table (compact, sort, expire)

A streaming job has been micro-batching into the Iceberg table `local.lkh05.events`, one tiny
commit at a time. So the table is now a few hundred rows spread across **many small data files**.
Each commit is its own snapshot. Query planning is slow, and S3 is full of old snapshots. You are
the on-call engineer. Run the maintenance.

Implement `maintain(spark, table, sort_col)` in `solution.py`. It receives these arguments:

- `spark`: an active Spark session with the Iceberg `local` catalog set up.
- `table`: the fully-qualified table name, for example `"local.lkh05.events"`.
- `sort_col`: the column to cluster by (it feeds file pruning), for example `"event_date"`.

Your function must follow these steps. It must **not change the data of the table (every row,
byte-for-byte)**:

1. **Set a sort order** on `table`, so that the rewritten files are clustered by `sort_col`
   (`ALTER TABLE ... WRITE ORDERED BY ...`).
2. **Compact** the many small files into a few larger files (`rewrite_data_files`).
3. **Expire old snapshots**. Keep only the latest (compacted) snapshot (`expire_snapshots` with an
   `older_than` cutoff). `retain_last` alone is blocked by the default `max_snapshot_age`.

The function ignores the return value. The test inspects the table afterwards.

The test uses a DuckDB oracle over a snapshot of the original rows. It takes this snapshot before
you run. The test checks these facts:

- The live **data file count** drops well below the number of input batches.
- The **average size of live files** rises into a healthy band (bytes). This means that Iceberg
  actually combined the files.
- The **snapshot count** is small (Iceberg expired the old snapshots).
- The **data is unchanged**. The rows are the same as before maintenance.
- Each remaining file is **internally sorted** by `sort_col` (the sort order was applied).

### Notes

- Inside a `CALL`, the `table` argument of the procedure is the **catalog-relative** name
  (`'lkh05.events'`). But `ALTER TABLE` takes the **fully-qualified** name
  (`local.lkh05.events`). The test gives you the fully-qualified `table`. To get the relative name,
  remove the leading `local.`.
- Keep the table to a few hundred rows. The run must finish in well under a minute.
