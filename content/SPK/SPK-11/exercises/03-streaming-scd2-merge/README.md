# Streaming SCD2 upsert into Iceberg with foreachBatch + MERGE INTO

You must fold a change stream of customer-tier updates into a **SCD2** dimension in Iceberg, in
streaming. Each change does one of three things:

- It starts a new customer.
- It changes an existing customer (close the old version, open a new one).
- It repeats the current value (do nothing).

This is your Kimball SCD2, streamed. `foreachBatch` gives you a normal batch DataFrame for each
micro-batch, so you can run Iceberg `MERGE INTO`.

Implement `upsert_scd2(spark, source_dir, table, checkpoint)` in `solution.py`. It must do these things:

- Read `source_dir` as a streaming JSON source with schema
  `cust_key INT, attr STRING, change_ts TIMESTAMP`.
- Use `foreachBatch` to apply each micro-batch to the existing Iceberg `table`. Its schema is
  `cust_key INT, attr STRING, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN`.
- Within the batch, use **one change per `cust_key`** (deduplicate the batch on `cust_key`). Then:
  - **Close** the current open row when the attribute changed. Set its `valid_to` to the `change_ts` of
    the change and set `is_current = false`.
  - **Insert** a new open row for a brand-new key or a changed key: `valid_from = change_ts`,
    `valid_to = NULL`, `is_current = true`.
  - An **unchanged repeat** (same `attr` as the current open row) must change nothing.
- Use `checkpoint` as the `checkpointLocation` and `trigger(availableNow=True)`. Then call
  `processAllAvailable()` and `stop()`.

The test runs several times (new key, change, no-op) and asserts the SCD2 invariants:

- **Exactly one open row** (`valid_to IS NULL` and `is_current = true`) per `cust_key`.
- **History is preserved.** Superseded versions remain with a non-null `valid_to` and `is_current =
  false` (versioning, not overwriting).
- The **current** attribute per key equals the latest change. This matches a DuckDB oracle.
- The `valid_to` of a superseded row equals the `valid_from` of the next version (no gaps/overlaps).

Tip: a clean way to do this is **two MERGE passes**. One pass closes the changed rows. The other pass
inserts the new open rows.
