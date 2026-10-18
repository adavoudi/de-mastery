# Exactly-once file stream into Iceberg across restarts

Build the ingestion half of a streaming pipeline: a **file source** of JSON event batches lands in
a directory, and you stream them into an **Iceberg table** so that stopping and restarting the job
never duplicates or loses a row. (The classic version of this is *Kafka → Iceberg*; Kafka arrives
in the KFK track in Phase 3. The exactly-once reasoning is identical — only the source changes.)

Implement `run_batch(spark, source_dir, table, checkpoint)` in `solution.py`. It must:

- Read `source_dir` as a **streaming** JSON source with this schema:
  `event_id INT, company_id STRING, amount DOUBLE, event_ts STRING`.
- Write the stream to the existing Iceberg `table` (a fully-qualified name like
  `local.stream.events`) in **append** mode.
- Use `checkpoint` as the `checkpointLocation`, and `trigger(availableNow=True)` so the call drains
  everything currently present and then returns.
- Start the query, wait for it to finish the available data (`processAllAvailable()`), and stop it.

The test will:

1. create the table, drop two batches of files, call `run_batch` → table has those rows,
2. drop a third batch, call `run_batch` **again with the same checkpoint** → only the new rows are
   added (resumed from committed offsets),
3. call `run_batch` a third time with **no new files** → nothing is added,

and assert the final table has **no duplicates and no loss** — exactly the set of events that were
dropped — matching a DuckDB oracle over all the JSON files.

Key points:
- The **same checkpoint** must be reused across calls. A new (or missing) checkpoint re-ingests
  everything.
- Use the **native table sink** (`.toTable(...)`), which tracks committed batches; do not collect
  rows and insert them yourself (that is not idempotent).
- `append` is the correct output mode — there is no aggregation here.
