# Event-time windowed counts with a watermark into Iceberg

Produce a streaming **fact**: per 5-minute event-time window, per company, the number of events —
appended to an Iceberg table as each window finalizes. A **watermark** bounds state and defines how
late is too late: events arriving after their window has finalized are dropped.

Implement `windowed_counts(spark, source_dir, table, checkpoint)` in `solution.py`. It must:

- Read `source_dir` as a streaming JSON source with schema
  `event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP`.
- Declare a **watermark of 10 minutes** on the **event-time** column `event_ts`
  (`withWatermark("event_ts", "10 minutes")`). Do **not** use processing time.
- Aggregate into 5-minute **event-time** windows per `company_id`, counting events.
- Write the result to the existing Iceberg `table` with columns `company_id`, `w_start`
  (the window start timestamp), `n` (the count), in **append** output mode (legal because the
  watermark finalizes each window exactly once).
- Use `checkpoint` as the `checkpointLocation` and `trigger(availableNow=True)`; call
  `processAllAvailable()` then `stop()`.

The test drives this in several runs sharing the checkpoint: it writes an early window's events,
then a far-future event that advances the watermark past that window (finalizing it), then a **late**
event for the already-finalized window. The final table must contain each finalized window's count
exactly once, and the late event must **not** be counted — matching a DuckDB oracle computed over
only the in-window (non-late) events.
