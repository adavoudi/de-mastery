# FND: Foundations of distributed data (~20 h)

Why this track exists: Spark, Kafka and Flink are three answers to the same few problems:
partitioning, replication, ordering, time and failure. If you learn those problems once,
each tool becomes a set of design choices instead of an API to memorize.

Lab profile: none (pure Python) plus `core` for FND-07.

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| FND-01 | Why distributed? | 2 | Scale up vs. scale out; when one big machine plus DuckDB wins; Amdahl's law | Measure a 10 GB aggregation on DuckDB vs. a 4-process pool; explain the result | Why Redshift has a leader node and compute nodes |
| FND-02 | Partitioning & shuffles | 3 | Hash/range partitioning, skew, why shuffles are expensive | Implement hash and range partitioners; detect skew from partition sizes | DISTKEY / DISTSTYLE choices |
| FND-03 | Replication & consensus | 3 | Leader/follower, quorums, ISR, Raft at a whiteboard level | Simulate leader election with a toy Raft log (pure Python) | Why Athena never "loses" an S3 object |
| FND-04 | Time | 3 | Event vs. processing time, out-of-order data, watermarks, lateness | Write a watermark generator; classify events as on-time / late / dropped | Late-arriving facts and dimensions in Kimball |
| FND-05 | Delivery semantics | 3 | At-most / at-least / exactly-once; idempotency; the two generals problem | Make a duplicate-producing pipeline idempotent with dedup keys | Re-running a dbt model safely; SOX "completeness and accuracy" |
| FND-06 | Storage formats | 3 | Row vs. columnar, Parquet internals (row groups, pages, stats, encodings), Avro, ORC | Read Parquet footers with `pyarrow`; predict which row groups a filter skips | Why Athena charges by scanned bytes |
| FND-07 | Back-of-envelope estimation | 3 | Throughput, latency and storage math; cost per TB scanned/stored | Size a pipeline for 5 PB/year; cost estimate is checked by the tests within ±20% | Sizing the commerce mart |

Interview drills: "How would you design for exactly-once?", "Explain watermarks to a PM",
"Why columnar?"
