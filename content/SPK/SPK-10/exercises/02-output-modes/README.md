# Pick the right output mode for a streaming aggregation

An event stream lands as JSON files; you must maintain a **running per-company event count** in an
in-memory table that a dashboard reads. The query is an aggregation, so the **output mode** matters:
`append` is illegal here (the counts keep changing, so nothing is ever "final" to append),
`complete` emits the whole current result each batch, and `update` emits only changed rows.

Implement `run_counts(spark, source_dir, query_name)` in `solution.py`. It must:

- Read `source_dir` as a streaming JSON source with schema
  `event_id INT, company_id STRING, amount DOUBLE, event_ts STRING`.
- Aggregate to one row per `company_id` with column `n` = number of events.
- Write to the **in-memory** sink (`format("memory")`) under `query_name`, choosing an output mode
  that is **valid for an aggregation** and leaves the complete current result queryable as
  `SELECT * FROM <query_name>`.
- Use `trigger(availableNow=True)`, call `processAllAvailable()`, then `stop()`, and **return the
  query object**.

The test writes several batches, runs your query, then reads `SELECT company_id, n FROM
<query_name>` and compares the final counts to a DuckDB oracle. A version that uses `append` must
fail (Spark rejects it on this aggregation).
