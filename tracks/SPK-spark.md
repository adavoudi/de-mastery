# SPK: Apache Spark (~70 h)

**Philosophy.** Spark is a general distributed compute engine. It builds a lazy, optimizable plan
(DAG) and runs it as stages split at shuffle boundaries. It is strong for large batch jobs,
complex transformations and micro-batch streaming. **When not to use it:** data under ~100 GB
(DuckDB/Polars is faster and simpler), millisecond-latency streaming (use Flink), simple
SQL-only transformations that a warehouse already handles well.

Language: PySpark (as on the exam). Scala is read-only: you learn to read stack traces and
Scala docs, not to write Scala. Lab profile: `core`.

## SPK-BYO: build your own mini query engine (~12 h, in addition to the modules)

You implement a columnar query engine in Python, with Arrow batches as the in-memory format.

| Stage | Build | Teaches |
|---|---|---|
| 1 | Scan Parquet in row-group batches | Columnar I/O |
| 2 | Projection and filter operators (Volcano/iterator model) | Physical operators |
| 3 | Predicate pushdown using Parquet statistics | Why `explain` shows `PushedFilters` |
| 4 | Hash aggregate | Partial vs. final aggregation |
| 5 | Hash join and broadcast join | Join strategies |
| 6 | Partitioned execution with a shuffle (multiprocessing) | Stages, exchanges |
| 7 | Rule-based optimizer (pushdown, pruning) | Catalyst |
| 8 | (optional) Spill to disk | Memory management |

The stage tests run SQL-like plans and compare results with DuckDB.
