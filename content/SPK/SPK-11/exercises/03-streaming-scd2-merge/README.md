# Streaming SCD2 upsert into Iceberg with foreachBatch + MERGE INTO

A change stream of customer-tier updates must be folded into a **SCD2** dimension in Iceberg, in
streaming. Each change either starts a new customer, changes an existing one (close the old version,
open a new one), or repeats the current value (do nothing). This is your Kimball SCD2, streamed —
with `foreachBatch` giving you a normal batch DataFrame per micro-batch so you can run Iceberg
`MERGE INTO`.

Implement `upsert_scd2(spark, source_dir, table, checkpoint)` in `solution.py`. It must:

- Read `source_dir` as a streaming JSON source with schema
  `cust_key INT, attr STRING, change_ts TIMESTAMP`.
- Use `foreachBatch` to apply each micro-batch to the existing Iceberg `table`, whose schema is
  `cust_key INT, attr STRING, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN`.
- Within the batch, treat **one change per `cust_key`** (deduplicate the batch on `cust_key`), then:
  - **Close** the current open row when the attribute changed: set its `valid_to` to the change's
    `change_ts` and `is_current = false`.
  - **Insert** a new open row for a brand-new key or a changed key: `valid_from = change_ts`,
    `valid_to = NULL`, `is_current = true`.
  - An **unchanged repeat** (same `attr` as the current open row) must change nothing.
- Use `checkpoint` as the `checkpointLocation`, `trigger(availableNow=True)`, then
  `processAllAvailable()` and `stop()`.

The test drives several runs (new key, change, no-op) and asserts the SCD2 invariants:

- **Exactly one open row** (`valid_to IS NULL` and `is_current = true`) per `cust_key`.
- **History preserved**: superseded versions remain with a non-null `valid_to` and `is_current =
  false` (versioning, not overwriting).
- The **current** attribute per key equals the latest change — matching a DuckDB oracle.
- A superseded row's `valid_to` equals the next version's `valid_from` (no gaps/overlaps).

Tip: a clean way to do this is **two MERGE passes** — one to close changed rows, one to insert new
open rows.
