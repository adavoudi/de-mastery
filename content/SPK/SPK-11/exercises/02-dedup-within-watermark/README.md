# Bounded streaming deduplication with a watermark

An at-least-once source delivers each event one or more times. Write a stream that keeps **one** row
per `event_id` into an Iceberg table, using a **watermark** so the dedup state stays bounded (it
forgets ids that have fallen behind the watermark, instead of remembering every id forever).

Implement `dedup(spark, source_dir, table, checkpoint)` in `solution.py`. It must:

- Read `source_dir` as a streaming JSON source with schema
  `event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP`.
- Declare a watermark of **30 minutes** on the event-time column `event_ts`.
- Deduplicate on the business id with **`dropDuplicatesWithinWatermark(["event_id"])`** — the
  bounded form (not plain `dropDuplicates`, which keeps every id forever).
- Append the deduplicated rows to the existing Iceberg `table`
  (`event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP`) in **append** mode.
- Use `checkpoint` as the `checkpointLocation`, `trigger(availableNow=True)`, then
  `processAllAvailable()` and `stop()`.

The test drives multiple runs sharing the checkpoint, with duplicate `event_id`s appearing within
the watermark horizon (same run and across runs). The final table must contain **exactly one row
per distinct `event_id`** — matching a DuckDB oracle that keeps one row per id — with no duplicates
and no loss of distinct ids.
