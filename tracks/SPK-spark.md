# SPK: Apache Spark (~70 h)

**Philosophy.** Spark is a general distributed compute engine. It builds a lazy, optimizable plan
(DAG) and runs it as stages split at shuffle boundaries. It is strong for large batch jobs,
complex transformations and micro-batch streaming. **When not to use it:** data under ~100 GB
(DuckDB/Polars is faster and simpler), millisecond-latency streaming (use Flink), simple
SQL-only transformations that a warehouse already handles well.

Language: PySpark (as on the exam). Scala is read-only: you learn to read stack traces and
Scala docs, not to write Scala. Lab profile: `core`. Cert objective tags refer to
[certs/README.md](../certs/README.md#databricks-spark-developer-associate).

| ID | Module | h | Key ideas | Exercises (graded) | From your world | Cert objectives |
|---|---|---|---|---|---|---|
| SPK-01 | Architecture | 5 | Driver, executors, cores, memory; cluster managers; deploy modes; Spark Connect | Configure a local cluster; match each task in the UI to its executor | Redshift leader/compute nodes | spark-assoc:1.1, 1.2, 6.1, 6.2 |
| SPK-02 | Lazy evaluation & the DAG | 5 | Transformations vs. actions; narrow vs. wide; jobs → stages → tasks | Predict the stage count for 6 queries (the test checks the actual counts) | dbt DAG vs. Spark DAG | 1.4, 1.5, 1.6 |
| SPK-03 | DataFrame API | 7 | Select/filter/withColumn, explode, dates, nulls, dedup, aggregations | 10 small transformations on commerce data, each checked against a DuckDB oracle | dbt staging models in PySpark | 3.1–3.4, 3.7 |
| SPK-04 | Spark SQL & data sources | 5 | Temporary views, SQL on files, JDBC, save modes, partitionBy, managed vs. external tables | Rebuild a dbt mart in Spark SQL; write partitioned output | `{{ ref() }}` vs. temporary views | 2.1–2.4, 3.6 |
| SPK-05 | Catalyst & the physical plan | 6 | Logical → optimized → physical plan; predicate pushdown; column pruning; `explain("formatted")` | Make 5 slow queries fast; the tests assert plan properties | Redshift `EXPLAIN` | 1.3 |
| SPK-06 | Joins and aggregations at scale | 6 | Broadcast hash, sort-merge, shuffle hash join; broadcast variables, accumulators | Fix a skewed fact-to-dimension join; choose the join strategy | DISTSTYLE ALL ≈ broadcast | 3.5, 3.9, 3.10, 4.1 |
| SPK-07 | Partitioning, bucketing, files | 6 | repartition vs. coalesce, shuffle partitions, bucketing, small-file problem | Hit a target file size and count; remove a shuffle with bucketing | DISTKEY / SORTKEY | 1.5, 4.1 |
| SPK-08 | Memory, caching, AQE | 6 | Unified memory, storage levels, spill, GC; AQE coalescing, skew join, join switching | Diagnose an OOM from logs; turn AQE on/off and explain the difference in the plan | – | 1.3, 4.2, 4.3 |
| SPK-09 | UDFs & Pandas API | 4 | Python UDF cost, Arrow and pandas UDFs, Pandas API on Spark | Replace a slow Python UDF; the test asserts there is no `BatchEvalPython` | dbt Python models | 3.8, 7.1, 7.2 |
| SPK-10 | Structured Streaming I | 6 | Micro-batch model, sources/sinks, triggers, output modes, checkpoints, exactly-once | Kafka→console→Iceberg stream; restart it and prove there are no duplicates | dbt incremental models | 5.1, 5.2, 5.3 |
| SPK-11 | Structured Streaming II | 7 | Watermarks, stateful aggregation, stream dedup, stream-stream joins, `foreachBatch` + Iceberg `MERGE INTO`, state stores | Streaming SCD2 upsert into Iceberg; late-data mutants must fail | SCD2 snapshots | 3.8, 5.3, 5.4 |
| SPK-12 | Testing & production Spark | 7 | `assertDataFrameEqual`, project layout, packaging, config, EMR Serverless vs. Databricks vs. Glue | Test suite for your own pipeline; 💲 run SPK-07 at 50 GB on EMR Serverless | dbt CI | 4.3 (+ DE Pro §1) |

## SPK-BYO: build your own mini query engine (~12 h, on top of the module hours; included in the ROADMAP total)

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
