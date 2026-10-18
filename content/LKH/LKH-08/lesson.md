# LKH-08 · Engines on the lake

## Why many engines, one table

The whole point of an open table format is that the **data is not owned by an engine**. The same
Iceberg or Delta table can be read by Spark, Trino, Athena (which is managed Trino/Presto under the
hood), DuckDB, StarRocks and more. This is the lakehouse promise you already live on the consumer
side: you query Iceberg from Athena today. This module is about reasoning across engines — why the
*same* logical query runs differently on each, and how to read a plan to see whether the engine is
actually skipping the data it should.

The skill that matters in interviews and on the job: given a slow query, can you tell whether the
engine **pushed the filter down** to the scan and **pruned partitions/files**, or whether it read the
whole table and filtered late? That reasoning transfers across every engine; only the plan syntax
changes.

## How it works

**Predicate pushdown.** A `WHERE amt > 50` can be evaluated three places: by the engine after reading
every row (worst), at the file-format layer using Parquet row-group min/max to skip row groups
(better), or recorded in the table-format scan so whole files are skipped by their statistics (best).
A good engine *pushes the predicate down* to the scan. In Spark's plan you see it as `PushedFilters`
(file sources) or, for Iceberg, `filters=...` on the `BatchScan`. In Trino you see it in `EXPLAIN` as
predicates on the `TableScan`. If the predicate is **not** in the scan, the engine is reading
everything and filtering in memory.

**Partition pruning.** When a table is partitioned (Iceberg hidden partitioning, LKH-05), a predicate
on the partition column (or a column feeding its transform) lets the planner **skip entire
partitions** before reading any data file. In Spark's plan this appears as `PartitionFilters` (plain
file sources) or as the partition predicate inside the Iceberg scan's `filters=`. Pruning is coarser
and cheaper than per-file skipping and happens first.

**File/row-group skipping with statistics.** Within the partitions it must read, the engine uses
per-file and per-row-group min/max (and null counts) to skip chunks whose range cannot match the
predicate. Sort order (LKH-05) makes these bounds tight, which is why clustering by a filter column
pays off. This is the same column-stats pruning you built in FND-06.

**Puffin files and statistics.** Iceberg stores table-level statistics (e.g. NDV sketches —
Theta/`apache-datasketches`) in **Puffin** files, which cost-based optimizers in Trino/Spark use to
pick join orders and strategies. Without stats, an engine guesses; with them, it plans better joins.
This is why `ANALYZE`/stats collection matters on the lake just as it did on Redshift.

**Engine personalities (same table, different strengths).**

- **Trino / Athena**: MPP SQL engine, great for interactive BI-style queries across many sources;
  Athena is Trino/Presto-as-a-service with no cluster to run. Pushdown + pruning are essential to
  keep bytes-scanned (and the Athena bill) down.
- **Spark**: general compute (batch, streaming, ML); heavier startup, best for large ETL and complex
  pipelines, not sub-second BI.
- **DuckDB**: single-node, in-process, vectorized; astonishingly fast on one machine for datasets that
  fit (or stream through) local memory. Reads Parquet/Iceberg directly — ideal as a local oracle and
  for medium data without a cluster.
- **StarRocks**: MPP with a cost-based optimizer and caching, aimed at low-latency analytics on
  lakehouse tables.

The same `SELECT ... WHERE ... GROUP BY` is one logical query; each engine compiles it to its own
plan, but **all should push the filter and prune partitions** — if one doesn't, that's your bug.

## From your world

| Athena / Redshift | Lake engines |
|---|---|
| Athena bytes-scanned billing | pushdown + pruning minimize bytes read on every engine |
| Redshift `ANALYZE` / table stats | Iceberg Puffin stats feeding cost-based optimizers |
| `EXPLAIN` on Redshift | `EXPLAIN` / `df.explain()` to confirm pushdown & pruning |
| SORTKEY → zone maps skip blocks | sort order → tight file min/max → file skipping |
| "which warehouse is fastest?" | Trino vs Spark vs DuckDB vs StarRocks: pick by workload, same data |

## When NOT to add another engine

- **Don't reach for Spark for a sub-second interactive query.** Its startup and shuffle overhead lose
  to Trino/DuckDB on small, latency-sensitive queries.
- **Don't stand up a Trino cluster for data that fits on one node.** DuckDB (or Athena serverless)
  often beats a cluster you have to operate.
- **Don't trust an engine is pruning — verify it.** Read the plan. A missing `PushedFilters` /
  `PartitionFilters` (or a `TableScan` with no predicate) means a full read; the fix is usually a
  predicate on the partition column, better stats, or a sort order.

## Further reading

- Iceberg Spark queries & metadata:
  <https://iceberg.apache.org/docs/1.10.0/spark-queries/>
- Iceberg Puffin / statistics:
  <https://iceberg.apache.org/puffin-spec/>
- Trino Iceberg connector & EXPLAIN:
  <https://trino.io/docs/current/connector/iceberg.html>
- DuckDB Iceberg / Parquet:
  <https://duckdb.org/docs/stable/core_extensions/iceberg/overview.html>
- Spark SQL performance (predicate pushdown, partition pruning):
  <https://spark.apache.org/docs/4.0.0/sql-performance-tuning.html>
