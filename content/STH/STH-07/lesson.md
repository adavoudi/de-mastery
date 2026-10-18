# STH-07 — Serving layer: OLAP on the streamhouse

## Why it exists

A streamhouse stores one copy of the data on the lake (Paimon / Fluss-tiered-to-Paimon /
Iceberg). You can read this copy in two ways:

- A **streaming** read (the changelog, for pipelines and materialized views).
- A **batch/OLAP** read (a point-in-time scan, for dashboards and ad-hoc queries).

The *serving layer* is the second way. It is an engine that answers interactive queries over the
lake tables. The engine can be StarRocks, Trino, DuckDB, Spark, or a Flink materialized table.

The hard requirement is latency. A dashboard tile has a budget of roughly one second. The raw
streaming fact can be large. Its layout is for *ingestion* (small files, frequent commits, LSM
levels, delete files). It is not for *scanning*. So the serving layer almost always reads a
**small, pre-aggregated** table and not the raw fact. A batch warehouse does the same: the BI tool
reads a rollup and not the event log.

## How it works

1. **Read the merged state, not the changelog.** Take a primary-key / merge-engine table (STH-02).
   The serving engine must read the *merged* current state. This is the latest value per key, with
   the partial-update and aggregation merges already applied. `pypaimon` and `pyiceberg` do this
   for you (a batch snapshot read). You never iterate the unbounded changelog for a dashboard.
2. **Pre-aggregate into a serving mart.** Build a rollup once (for example revenue per company per
   day). Store it as a small table. The dashboard query then reads *that* table. The query prunes
   it with a predicate (one company, a date range). The rows that the query reads drop from
   millions to dozens.
3. **Prune and project.** A columnar OLAP engine (DuckDB, StarRocks, Trino) reads only the columns
   and row groups that the predicate needs. You can prove that the serving query reads the rollup
   and not the raw fact. To do this, read its query plan (`EXPLAIN`).
4. **OLAP result == streaming result.** The invariant from TESTING.md G6 still holds. The OLAP
   answer over the merged snapshot must equal the batch rebuild of the same events. Suppose the
   dashboard disagrees with a batch recompute. Then the serving layer is wrong. There are three
   possible causes:
   - a stale snapshot,
   - a wrong merge,
   - double-counting of un-merged versions.

## From your world

This is exactly your Athena/Redshift serving pattern. In your Kimball marts, Looker/QuickSight
never scans `fct_orders` at grain. It reads a daily or weekly aggregate (a BI rollup, a Redshift
materialized view, an Athena CTAS). The streamhouse changes *how fresh* that rollup is. It is
fresh in seconds to minutes, not after a nightly batch. But the serving discipline is identical:

- Read a small pre-aggregated table.
- Prune it with the filters of the dashboard.
- Reconcile it against a full recompute.

## When NOT to use it

- **When the dashboard needs the raw grain** (drill-to-detail on a single key): A key-point
  lookup on the PK table is fine. A rollup would not help.
- **When freshness matters more than latency and the volume is tiny:** A direct scan of a small
  merged table is simpler than maintaining a rollup.
- **When the "serving" is itself a pipeline:** Suppose the consumer is another streaming job. Then
  read the changelog (streaming), not an OLAP snapshot. The serving layer is for *interactive*
  reads.
- Do not point the `iceberg_scan` of DuckDB at a Flink hadoop-catalog table. It rejects the `file:`
  manifest scheme. Read via pyiceberg → Arrow → DuckDB instead.

## Further reading

- Apache Paimon — Query / OLAP engines: https://paimon.apache.org/docs/2.0/
- Apache Iceberg — Spark/Trino/DuckDB queries: https://iceberg.apache.org/docs/latest/
- Flink materialized tables: https://nightlies.apache.org/flink/flink-docs-release-2.2/
