# Bounded streaming deduplication with a watermark

An at-least-once source delivers each event one or more times. Write a stream that keeps **one** row
per `event_id` in an Iceberg table. Use a **watermark**, so that the dedup state stays bounded. The state
forgets ids that fell behind the watermark, instead of remembering every id forever.

Implement `dedup(spark, source_dir, table, checkpoint)` in `solution.py`. It must do these things:

- Read `source_dir` as a streaming JSON source with schema
  `event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP`.
- Declare a watermark of **30 minutes** on the event-time column `event_ts`.
- Deduplicate on the business id with **`dropDuplicatesWithinWatermark(["event_id"])`**. This is the
  bounded form. Do not use plain `dropDuplicates`, which keeps every id forever.
- Append the deduplicated rows to the existing Iceberg `table`
  (`event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP`) in **append** mode.
- Use `checkpoint` as the `checkpointLocation` and `trigger(availableNow=True)`. Then call
  `processAllAvailable()` and `stop()`.

The test runs this several times with the same checkpoint. Duplicate `event_id`s appear within
the watermark horizon (in the same run and across runs). The final table must contain **exactly one row
per distinct `event_id`**. It must match a DuckDB oracle that keeps one row per id. It must have no
duplicates and no loss of distinct ids.
