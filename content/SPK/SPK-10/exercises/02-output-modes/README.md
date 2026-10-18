# Pick the right output mode for a streaming aggregation

An event stream lands as JSON files. You must maintain a **running per-company event count** in an
in-memory table that a dashboard reads. The query is an aggregation, so the **output mode** matters:

- `append` is illegal here. The counts keep changing, so nothing is ever "final" to append.
- `complete` emits the whole current result in each batch.
- `update` emits only the changed rows.

Implement `run_counts(spark, source_dir, query_name)` in `solution.py`. It must do these things:

- Read `source_dir` as a streaming JSON source with schema
  `event_id INT, company_id STRING, amount DOUBLE, event_ts STRING`.
- Aggregate to one row per `company_id` with column `n` = number of events.
- Write to the **in-memory** sink (`format("memory")`) under `query_name`. Choose an output mode
  that is **valid for an aggregation** and that leaves the complete current result available to query as
  `SELECT * FROM <query_name>`.
- Use `trigger(availableNow=True)`, call `processAllAvailable()`, then call `stop()`. **Return the
  query object**.

The test writes several batches and runs your query. Then it reads `SELECT company_id, n FROM
<query_name>` and compares the final counts to a DuckDB oracle. A version that uses `append` must
fail (Spark rejects it on this aggregation).
