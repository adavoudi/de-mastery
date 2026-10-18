# STH-07 — Serving layer: OLAP on the streamhouse

## Why it exists

A streamhouse stores one copy of the data on the lake (Paimon / Fluss-tiered-to-Paimon /
Iceberg) that can be read two ways: a **streaming** read (the changelog, for pipelines and
materialized views) and a **batch/OLAP** read (a point-in-time scan, for dashboards and ad-hoc
queries). The *serving layer* is the second one: an engine — StarRocks, Trino, DuckDB, Spark,
or a Flink materialized table — that answers interactive queries over the lake tables.

The hard requirement is latency. A dashboard tile has a budget of roughly a second. The raw
streaming fact can be large and is laid out for *ingestion* (small files, frequent commits, LSM
levels, delete files), not for *scanning*. So the serving layer almost always reads a **small,
pre-aggregated** table, not the raw fact. This is the same move a batch warehouse makes: the
BI tool hits a rollup, not the event log.

## How it works

1. **Read the merged state, not the changelog.** For a primary-key / merge-engine table (STH-02),
   the serving engine must read the *merged* current state — the latest value per key, with
   partial-update and aggregation merges already applied. `pypaimon` and `pyiceberg` do this for
   you (a batch snapshot read). You never iterate the unbounded changelog for a dashboard.
2. **Pre-aggregate into a serving mart.** Build a rollup (e.g. revenue per company per day) once,
   as a small table. The dashboard query then reads *that* table and prunes it with a predicate
   (one company, a date range). Rows read drop from millions to dozens.
3. **Prune and project.** A columnar OLAP engine (DuckDB, StarRocks, Trino) only reads the
   columns and row groups the predicate needs. You can prove the serving query hits the rollup —
   not the raw fact — by reading its query plan (`EXPLAIN`).
4. **OLAP result == streaming result.** The invariant from TESTING.md G6 still holds: the OLAP
   answer over the merged snapshot must equal the batch rebuild of the same events. If the
   dashboard disagrees with a batch recompute, the serving layer is wrong (stale snapshot, wrong
   merge, double-counting un-merged versions).

## From your world

This is exactly your Athena/Redshift serving pattern. In your Kimball marts, Looker/QuickSight
never scans `fct_orders` at grain — it reads a daily/weekly aggregate (a BI rollup, a Redshift
materialized view, an Athena CTAS). The streamhouse changes *how fresh* that rollup is (seconds
to minutes instead of a nightly batch), but the serving discipline is identical: hit a small
pre-aggregated table, prune by the dashboard's filters, and reconcile against a full recompute.

## When NOT to use it

- **When the dashboard needs the raw grain** (drill-to-detail on a single key): a key-point
  lookup on the PK table is fine and a rollup wouldn't help.
- **When freshness beats latency and volume is tiny:** a direct scan of a small merged table is
  simpler than maintaining a rollup.
- **When the "serving" is itself a pipeline:** if the consumer is another streaming job, read the
  changelog (streaming), not an OLAP snapshot. The serving layer is for *interactive* reads.
- Don't point DuckDB's `iceberg_scan` at a Flink hadoop-catalog table — it rejects the `file:`
  manifest scheme. Read via pyiceberg → Arrow → DuckDB instead.

## Further reading

- Apache Paimon — Query / OLAP engines: https://paimon.apache.org/docs/2.0/
- Apache Iceberg — Spark/Trino/DuckDB queries: https://iceberg.apache.org/docs/latest/
- Flink materialized tables: https://nightlies.apache.org/flink/flink-docs-release-2.2/
