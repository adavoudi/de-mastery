# Roadmap

## Target outcome

By the end you should be able to:

- **Explain** why Spark, Kafka, Flink, Iceberg/Delta/Paimon and Fluss exist, which trade-offs each
  one makes, and when *not* to use each.
- **Build** batch, streaming and streamhouse pipelines that are correct under failure
  (exactly-once delivery, idempotent writes, replayable).
- **Tune** them by reading physical plans, the Spark UI, Flink metrics and consumer lag, not by guessing.
- **Design** a pipeline for petabyte scale on a whiteboard, with capacity and cost numbers.
- **Prove it**: three capstones on GitHub, a design doc for each, and up to five certificates.

## Tracks

| ID | Track | Est. hours | Core "build your own" challenge | Certs fed |
|---|---|---|---|---|
| FND | [Foundations of distributed data](tracks/FND-foundations.md) | 20 | – | all |
| SPK | [Apache Spark](tracks/SPK-spark.md) | 70 + 12 BYO | Mini query engine | Spark Associate, (DE Pro) |
| LKH | [Lakehouse & open table formats](tracks/LKH-lakehouse.md) | 35 + 8 BYO | Mini table format | (DE Pro) |
| CDW | [Cloud data platforms (Snowflake, BigQuery, Databricks, Fabric)](tracks/CDW-cloud-platforms.md) | 60 | – | none (by choice) |
| JVM | [Java for data engineers](tracks/JVM-java.md) | 20 | – | CCDAK, DSE Flink |
| KFK | [Apache Kafka](tracks/KFK-kafka.md) | 65 + 10 BYO | Mini Kafka broker | DSE Foundations, CCDAK, CCAAK |
| FLK | [Apache Flink](tracks/FLK-flink.md) | 65 + 10 BYO | Mini stream processor | DSE Foundations (part), DSE Flink |
| STH | [Streamhouse](tracks/STH-streamhouse.md) | 35 | – | DSE Flink (partly) |
| ARC | [Architecture & system design](tracks/ARC-architecture.md) | 45 | – | interviews |
| CAP | [Projects & capstones](capstones/README.md) | 120 | – | portfolio |
| | **Total** | **~575** | | |

Hours are estimates for reading, exercises and tests. They are not wall-clock deadlines.
At 6 h/week this is about 96 weeks; at 10 h/week, about 58 weeks. `de plan` recalculates the
schedule whenever you edit `learner.yaml`. See [docs/PLATFORM.md](docs/PLATFORM.md#scheduling).

## Order and dependencies

```
FND ──► SPK ──► LKH ──► C1 (batch lakehouse capstone)
  │                ├──► CDW (cloud platforms; parallel with JVM, ports C1's gold layer in CDW-17)
  │                │
  │      JVM ──────┼──► KFK ──► C2 (streaming CDC capstone)
  │                │      │
  │                └──────┴──► FLK ──► STH ──► C3 (streamhouse capstone)
  │
  └── ARC modules are interleaved (see "ARC checkpoints" below)
```

Why this order:

1. **Spark first.** It is closest to SQL and dbt, appears in the most job postings, and teaches
   distributed execution (shuffles, partitions, skew). That knowledge carries over directly to Flink.
2. **Lakehouse right after Spark.** You already use Iceberg. Now you learn its internals and
   maintenance, plus Delta and Hudi, so you can work on other companies' stacks.
3. **Cloud platforms after the lakehouse.** Snowflake, BigQuery, Databricks and Fabric are
   easiest to learn once you know the engine (SPK) and the table formats (LKH) they wrap.
   Together with C1, this makes you interview-ready for batch/warehouse roles before the
   streaming tracks start. It runs in parallel with JVM.
4. **JVM before Kafka and Flink.** Kafka Streams and the Flink DataStream API are Java-first, and
   CCDAK uses Java examples. You can start JVM in parallel with LKH.
5. **Kafka before Flink.** Flink jobs in practice read from and write to Kafka. Offsets, partitions
   and transactions are prerequisites for understanding Flink's exactly-once guarantees.
6. **Streamhouse last.** It combines everything: Flink, a streaming lake format (Paimon/Fluss),
   Iceberg, Kafka and CDC.

### ARC checkpoints

| After | Do ARC modules |
|---|---|
| FND | ARC-01 |
| SPK | ARC-02, ARC-03 |
| LKH | ARC-07, ARC-08 |
| CDW | ARC-05 (reuses the CDW-02 cost model and the CDW-17 port) |
| KFK | ARC-04, ARC-06 |
| FLK | ARC-10, ARC-11 |
| STH | ARC-09 (system design interview practice, ongoing until the end) |

### Cert checkpoints (suggested, not mandatory)

| After | Exam | Cost |
|---|---|---|
| KFK-01..08 + FLK-01 | Confluent DSE Foundations | free |
| SPK | Databricks Spark Developer Associate | paid |
| KFK | Confluent CCDAK | paid |
| FLK | Confluent DSE Apache Flink | free |
| KFK-11..14 + C2 | Confluent CCAAK (optional) | paid |

Details and the objective-to-module mapping are in [certs/README.md](certs/README.md).

## From your world → new world

This table is the backbone of the "From your world" sections in each module.

| You already know | New-world equivalent | Where taught |
|---|---|---|
| dbt model (SELECT → table) | Spark DataFrame transformation / Flink SQL dynamic table | SPK-03, FLK-04 |
| dbt incremental model | Structured Streaming micro-batch; Iceberg `MERGE INTO` | SPK-10, SPK-11 |
| `EXPLAIN` in Redshift/Athena | Spark physical plan, Catalyst, AQE | SPK-05, SPK-08 |
| Redshift DISTKEY / SORTKEY | Spark partitioning, bucketing; Iceberg sort order; Kafka partition key | SPK-07, LKH-05, KFK-09 |
| Iceberg as a user | Iceberg metadata tree, commits, catalogs, maintenance | LKH-01..05 |
| SCD Type 2 dimension | CDC stream → versioned table; Flink temporal join | KFK-07, FLK-05, STH-05 |
| Late-arriving dimension | Watermarks, allowed lateness, retractions | FND-04, FLK-03 |
| Fact grain | Event and key design; topic per event type vs. one topic | KFK-09 |
| Conformed dimensions across companies | Multi-tenant topics, schema contracts, MDM golden record in a stream | KFK-06, ARC-08 |
| SOX controls (completeness, accuracy, audit) | Exactly-once delivery, reconciliation, immutable logs, replay, lineage | KFK-05, FLK-06, ARC-07 |
| dbt tests | Data contracts, Schema Registry compatibility, streaming DQ | KFK-06, STH-06, ARC-04 |
| Athena on S3 | Trino, DuckDB and StarRocks on lakehouse tables | LKH-08, STH-07 |
| Redshift / Athena as your warehouse | Snowflake, BigQuery, Databricks SQL, Fabric Warehouse: same ideas, different pricing and governance | CDW-01..17 |
| Kimball star schema | BigQuery nested/repeated fields vs. star schema | CDW-09 |
| Daily batch SLA | Latency tiers and their cost (batch / micro-batch / streaming) | STH-08 |

## Status

The platform is built in phases, so you can start learning early. A phase is done only when every
exercise in it passes the validity gates in [docs/TESTING.md](docs/TESTING.md).

| Phase | Deliverable | Unblocks | Status |
|---|---|---|---|
| 1 | `de` CLI (status/next/test/plan), Docker lab profile `core` (MinIO + Iceberg REST + Spark), FND and SPK-01..04 | start learning | ✅ done |
| 2 | Rest of SPK + SPK-BYO, LKH + LKH-BYO, test-validity tooling (mutants, flake check; local, no hosted CI) | C1 | ✅ done |
| 3 | JVM, KFK, KFK-BYO, lab profile `streaming` (Kafka + Schema Registry + Connect) | C2, CCDAK | ✅ done |
| 4 | FLK, FLK-BYO, lab profile `flink` | DSE Flink | ✅ done |
| 5 | STH (Paimon, Fluss), lab profile `streamhouse`, C3 scaffolding | C3 | ✅ done |
| 6 | Mock exams for each cert, spaced-repetition review, interview question bank | exams | planned |
| 7 | CDW: local-graded exercises (emulators: fakesnow, Snowpark local testing, bigquery-emulator, Kusto emulator, Spark + Delta), cloud-lab evidence collector + checkers, trial-window scheduling in `de plan` | CDW | planned |

Contributors: validate only the modules or stages you touched (`de validate <ID>` or
`--changed <ref>`), and run the full cumulative `de validate` before a release.

## Versions

Version pins are recorded in `labs/versions.lock`. As of October 2026, the
upstream projects publish Spark 4.x, Kafka 4.x (KRaft only, no ZooKeeper) and Flink 2.x. Exams
sometimes lag behind the latest release. Each cert page in `certs/` records which version its
exam targets, and lessons call out the differences.
