# LKH-08 · Engines on the lake

## Why many engines, one table

The whole point of an open table format is that **no engine owns the data**. Many engines can read
the same Iceberg or Delta table. These include Spark, Trino, Athena (managed Trino/Presto under the
hood), DuckDB, StarRocks and more. This is the lakehouse promise. You already live it on the
consumer side: you query Iceberg from Athena today. This module is about reasoning across engines.
It explains why the *same* logical query runs differently on each engine. It also shows how to read
a plan, so that you can see whether the engine actually skips the data that it should skip.

This skill matters in interviews and on the job. You get a slow query. Can you tell whether the
engine **pushed the filter down** to the scan and **pruned partitions and files**? Or did it read
the whole table and filter late? This reasoning works for every engine. Only the syntax of the plan
changes.

## How it works

**Predicate pushdown.** An engine can evaluate `WHERE amt > 50` in three places:

1. The engine reads every row and then evaluates the predicate (worst).
2. The file-format layer evaluates it. It uses Parquet row-group min/max to skip row groups
   (better).
3. The table-format scan records it. Then the scan skips whole files by their statistics (best).

A good engine *pushes the predicate down* to the scan. In the plan of Spark, you see it as
`PushedFilters` (file sources). For Iceberg, you see `filters=...` on the `BatchScan`. In Trino,
you see it in `EXPLAIN` as predicates on the `TableScan`. If the predicate is **not** in the scan,
the engine reads everything and filters in memory.

**Partition pruning.** Suppose a table is partitioned (Iceberg hidden partitioning, LKH-05). A
predicate on the partition column (or on a column that feeds its transform) lets the planner
**skip entire partitions** before it reads any data file. In the plan of Spark, this appears as
`PartitionFilters` (plain file sources). Or it appears as the partition predicate inside the
`filters=` of the Iceberg scan. Pruning is coarser and cheaper than per-file skipping. It happens
first.

**File/row-group skipping with statistics.** The engine must read some partitions. Inside them, it
uses per-file and per-row-group min/max (and null counts). It skips the chunks whose range cannot
match the predicate. A sort order (LKH-05) makes these bounds tight. For this reason, clustering by
a filter column pays off. This is the same column-stats pruning that you built in FND-06.

**Puffin files and statistics.** Iceberg stores table-level statistics in **Puffin** files. An
example is NDV sketches (Theta/`apache-datasketches`). Cost-based optimizers in Trino and Spark use
these statistics to pick join orders and strategies. Without statistics, an engine guesses. With
them, it plans better joins. For this reason, `ANALYZE` and stats collection matter on the lake,
as they did on Redshift.

**Engine personalities (same table, different strengths).**

- **Trino / Athena**: an MPP SQL engine. It is great for interactive BI-style queries across many
  sources. Athena is Trino/Presto-as-a-service, and you run no cluster. Pushdown and pruning are
  essential to keep bytes-scanned (and the Athena bill) low.
- **Spark**: general compute (batch, streaming, ML). It starts slowly. It is best for large ETL and
  complex pipelines. It is not best for sub-second BI.
- **DuckDB**: single-node, in-process and vectorized. It is very fast on one machine for datasets
  that fit in local memory (or stream through it). It reads Parquet/Iceberg directly. It is ideal
  as a local oracle and for medium data without a cluster.
- **StarRocks**: MPP with a cost-based optimizer and caching. It targets low-latency analytics on
  lakehouse tables.

The same `SELECT ... WHERE ... GROUP BY` is one logical query. Each engine compiles it to its own
plan. But **all engines should push the filter and prune partitions**. If one engine does not, that
is your bug.

## From your world

| Athena / Redshift | Lake engines |
|---|---|
| Athena bytes-scanned billing | pushdown + pruning minimize bytes read on every engine |
| Redshift `ANALYZE` / table stats | Iceberg Puffin stats feeding cost-based optimizers |
| `EXPLAIN` on Redshift | `EXPLAIN` / `df.explain()` to confirm pushdown & pruning |
| SORTKEY → zone maps skip blocks | sort order → tight file min/max → file skipping |
| "which warehouse is fastest?" | Trino vs Spark vs DuckDB vs StarRocks: pick by workload, same data |

## When NOT to add another engine

- **Do not use Spark for a sub-second interactive query.** The startup and shuffle overhead of
  Spark is worse than Trino or DuckDB on small, latency-sensitive queries.
- **Do not set up a Trino cluster for data that fits on one node.** DuckDB (or Athena serverless)
  is often better than a cluster that you must operate.
- **Do not trust that an engine is pruning. Verify it.** Read the plan. A missing `PushedFilters`
  or `PartitionFilters` (or a `TableScan` with no predicate) means a full read. The fix is usually
  a predicate on the partition column, better stats, or a sort order.

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
