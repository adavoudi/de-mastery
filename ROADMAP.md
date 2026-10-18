# Roadmap

## Goal

By the end you can:

- **Explain** why Spark, Kafka, Flink, Iceberg/Delta/Paimon and Fluss exist, what trade-offs each
  one makes, and when *not* to use each.
- **Build** batch, streaming and streamhouse pipelines that stay correct under failure
  (exactly-once delivery, idempotent writes, replayable).
- **Tune** them by reading physical plans, the Spark UI, Flink metrics and consumer lag, not by guessing.
- **Build** a small version of a query engine, a table format, a Kafka broker and a stream processor.

An interactive version of this page, with progress ticks, is on the course website (`/roadmap`).

## Tracks

| ID | Track | Hours | Build-your-own challenge |
|---|---|---|---|
| FND | [Foundations of distributed data](tracks/FND-foundations.md) | 20 | – |
| SPK | [Apache Spark](tracks/SPK-spark.md) | 70 + 12 | Mini query engine |
| LKH | [Lakehouse & open table formats](tracks/LKH-lakehouse.md) | 35 + 8 | Mini table format |
| JVM | [Java for data engineers](tracks/JVM-java.md) | 20 | – |
| KFK | [Apache Kafka](tracks/KFK-kafka.md) | 65 + 10 | Mini Kafka broker |
| FLK | [Apache Flink](tracks/FLK-flink.md) | 65 + 10 | Mini stream processor |
| STH | [Streamhouse](tracks/STH-streamhouse.md) | 35 | – |

Hours cover reading, exercises and tests. They are estimates, not deadlines. At 6 h/week the
whole course takes about 1.5 years. `de plan` recalculates the schedule from `learner.yaml`.

## Order

```
FND ──► SPK ──► LKH ──► JVM ──► KFK ──► FLK ──► STH
```

Why this order:

1. **Foundations first.** Partitioning, replication, time and delivery guarantees are the same
   few problems in every tool.
2. **Spark next.** It is closest to SQL and dbt, and it teaches distributed execution (shuffles,
   partitions, skew), which carries over to Flink.
3. **Lakehouse right after.** You already use Iceberg. Now you learn its internals and
   maintenance, plus Delta and Hudi, so you can work on other teams' stacks.
4. **Java before Kafka and Flink.** Kafka Streams and the Flink DataStream API are Java-first. JVM
   can run in parallel with LKH if you want to save time.
5. **Kafka before Flink.** Flink jobs read from and write to Kafka, so offsets, partitions and
   delivery semantics come first.
6. **Streamhouse last.** It needs the lakehouse, Kafka and Flink together.

## From your world to the new world

Each module has a "From your world" line that ties it to something you already know. The main ones:

| You already know | New-world equivalent | Where taught |
|---|---|---|
| dbt model (SELECT → table) | Spark DataFrame transformation / Flink SQL dynamic table | SPK-03, FLK-04 |
| dbt incremental model | Structured Streaming micro-batch; Iceberg `MERGE INTO` | SPK-10, SPK-11 |
| `EXPLAIN` in Redshift/Athena | Spark physical plan, Catalyst, AQE | SPK-05, SPK-08 |
| Redshift DISTKEY / SORTKEY | Spark partitioning and bucketing; Iceberg sort order; Kafka partition key | SPK-07, LKH-05, KFK-09 |
| Iceberg as a user | Iceberg metadata tree, commits, catalogs, maintenance | LKH-01..05 |
| SCD Type 2 dimension | CDC stream → versioned table; Flink temporal join | KFK-07, FLK-05, STH-05 |
| Late-arriving dimension | Watermarks, allowed lateness, retractions | FND-04, FLK-03 |
| Fact grain | Event and key design; topic per event type vs. one topic | KFK-09 |
| Conformed dimensions | Schema contracts and golden records in a stream | KFK-06 |
| SOX controls (completeness, accuracy, audit) | Exactly-once delivery, reconciliation, immutable logs, replay | KFK-05, FLK-06 |
| dbt tests | Data contracts, Schema Registry compatibility, streaming DQ | KFK-06, STH-06 |
| Athena on S3 | Trino, DuckDB and StarRocks on lakehouse tables | LKH-08, STH-07 |
| Daily batch SLA | Latency tiers and their cost (batch / micro-batch / streaming) | STH-08 |

## Versions

Version pins are in `labs/versions.lock`. As of October 2026 the upstream projects publish Spark 4.x,
Kafka 4.x (KRaft only, no ZooKeeper) and Flink 2.x. Lessons call out where older versions differ.
