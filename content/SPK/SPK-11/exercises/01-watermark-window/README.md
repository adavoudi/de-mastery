# Event-time windowed counts with a watermark into Iceberg

Produce a streaming **fact**: the number of events per 5-minute event-time window, per company. Append
the result to an Iceberg table when each window finalizes. A **watermark** bounds state and defines how
late is too late. Spark drops events that arrive after their window has finalized.

Implement `windowed_counts(spark, source_dir, table, checkpoint)` in `solution.py`. It must do these things:

- Read `source_dir` as a streaming JSON source with schema
  `event_id INT, company_id STRING, amount DOUBLE, event_ts TIMESTAMP`.
- Declare a **watermark of 10 minutes** on the **event-time** column `event_ts`
  (`withWatermark("event_ts", "10 minutes")`). Do **not** use processing time.
- Aggregate into 5-minute **event-time** windows per `company_id`, and count the events.
- Write the result to the existing Iceberg `table` with the columns `company_id`, `w_start`
  (the window start timestamp), and `n` (the count). Use **append** output mode. It is legal because the
  watermark finalizes each window exactly once.
- Use `checkpoint` as the `checkpointLocation` and `trigger(availableNow=True)`. Call
  `processAllAvailable()` and then `stop()`.

The test runs this several times with the same checkpoint. It does these steps:

1. It writes the events of an early window.
2. It writes a far-future event. This event moves the watermark past the early window and finalizes it.
3. It writes a **late** event for the window that is already finalized.

The final table must contain the count of each finalized window exactly once. The late event must **not**
be counted. The table must match a DuckDB oracle that the test computes over only the in-window
(non-late) events.
