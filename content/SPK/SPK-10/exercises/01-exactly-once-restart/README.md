# Exactly-once file stream into Iceberg across restarts

Build the ingestion half of a streaming pipeline. A **file source** of JSON event batches lands in
a directory. You stream the batches into an **Iceberg table**. When you stop and restart the job, it
must never duplicate or lose a row. (The classic version of this is *Kafka → Iceberg*. Kafka arrives
in the KFK track in Phase 3. The exactly-once reasoning is the same. Only the source changes.)

Implement `run_batch(spark, source_dir, table, checkpoint)` in `solution.py`. It must do these things:

- Read `source_dir` as a **streaming** JSON source with this schema:
  `event_id INT, company_id STRING, amount DOUBLE, event_ts STRING`.
- Write the stream to the existing Iceberg `table` (a fully-qualified name like
  `local.stream.events`) in **append** mode.
- Use `checkpoint` as the `checkpointLocation`. Use `trigger(availableNow=True)`, so that the call
  processes everything that is present and then returns.
- Start the query, wait until it finishes the available data (`processAllAvailable()`), and stop it.

The test does these steps:

1. It creates the table, puts two batches of files in the directory, and calls `run_batch`. The table has those rows.
2. It puts a third batch in the directory and calls `run_batch` **again with the same checkpoint**. Only
   the new rows are added (the job resumed from the committed offsets).
3. It calls `run_batch` a third time with **no new files**. Nothing is added.

The test then asserts that the final table has **no duplicates and no loss**. The table must have exactly
the set of events that the test put in the directory. The test compares it with a DuckDB oracle over all
the JSON files.

Key points:
- You must reuse the **same checkpoint** across calls. A new (or missing) checkpoint ingests
  everything again.
- Use the **native table sink** (`.toTable(...)`). It tracks committed batches. Do not collect
  rows and insert them yourself (that is not idempotent).
- `append` is the correct output mode, because there is no aggregation here.
