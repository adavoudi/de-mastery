# Certifications

**There is no official Apache certification** for Spark, Kafka or Flink. The recognized ones
come from vendors (Databricks for Spark, Confluent for Kafka and Flink). Formats and costs below
were checked on **2026-10-03**. Exam guides change, so `de mock <cert>` reminds you to re-check the
official page two weeks before booking.

Verification legend: ✅ checked on the official page/PDF · ⚠️ from a third-party listing
(the official guide is behind a login or wasn't retrievable). Re-verify these before studying.

## Recommended set and order

| # | Cert (id in `learner.yaml`) | Level | Cost | Format | When |
|---|---|---|---|---|---|
| 1 | Confluent **Data Streaming Engineer – Foundations** (`confluent-dse-foundations`) | entry | **free** | 30 Q / 60 min, unproctored ✅ | after KFK-01..08 + FLK-01 |
| 2 | Databricks **Certified Associate Developer for Apache Spark** (`databricks-spark-developer-associate`) | associate | paid | 45 Q / 90 min, proctored, Python ✅ | after SPK |
| 3 | Confluent **Certified Developer for Apache Kafka – CCDAK** (`confluent-ccdak`) | professional | paid | 90 min, proctored ✅ | after KFK |
| 4 | Confluent **Data Streaming Engineer – Apache Flink** (`confluent-dse-flink`) | associate | **free** | 30 Q / 60 min, unproctored ✅ | after FLK |
| 5 | Confluent **Certified Administrator for Apache Kafka – CCAAK** (`confluent-ccaak`) | professional | paid | 90 min, proctored ✅ | optional, after C2 |
| – | Databricks **Data Engineer Professional** (`databricks-de-professional`) | professional | $200 ✅ | 59 Q / 120 min ✅ | optional; Databricks-specific |

Notes:

- Confluent's paid certifications expire after 2 years; Databricks' are valid for 2 years ✅.
- Confluent also offers a Cloud Certified Operator exam, which is Confluent Cloud-specific and not included here.
- There is no widely recognized "Flink professional" exam as of this date. Ververica Academy gives
  course badges, not a proctored certification. DSE Flink plus C3 is the strongest Flink signal you can get.
- Look up current prices on the vendors' pages. Prices are only listed here where the official
  guide states them.
- Your Databricks Data Engineer Associate prep stays in its separate project, as you asked.

## Databricks Spark Developer Associate

Source ✅: *Databricks Certified Associate Developer for Apache Spark – Exam Guide*, version live as of
2025-09-15 (databricks.com, PDF). Section weights ⚠️ (third-party): 20 / 20 / 30 / 10 / 10 / 5 / 5 %.

Objective IDs number the bullets of the official guide in order.

| Obj | Objective (short) | Modules |
|---|---|---|
| 1.1 | Advantages and challenges of Spark | SPK-01, FND-01 |
| 1.2 | Core components: cluster, driver, workers/executors, cores, memory | SPK-01 |
| 1.3 | DataFrame/Dataset, SparkSession lifecycle, caching, storage levels, GC | SPK-05, SPK-08 |
| 1.4 | Execution hierarchy | SPK-02 |
| 1.5 | Partitioning, shuffles | SPK-02, SPK-07, FND-02 |
| 1.6 | Actions, transformations, lazy evaluation | SPK-02 |
| 1.7 | Spark modules (Core, SQL, DataFrames, Pandas API, Streaming, MLlib) | SPK-01 |
| 2.1 | Read/write sources (JDBC, files), overwrite, partitionBy | SPK-04 |
| 2.2 | SQL on files (ORC, JSON, CSV, text, Delta); save modes | SPK-04 |
| 2.3 | Persistent tables with sorting and partitioning | SPK-04, SPK-07 |
| 2.4 | Temporary views | SPK-04 |
| 3.1 | Manipulate columns/rows: add, drop, split, rename, filter, explode | SPK-03 |
| 3.2 | Deduplication and validation | SPK-03 |
| 3.3 | Aggregations (count, approx count distinct, mean, summary) | SPK-03 |
| 3.4 | Dates (Unix epoch, extract components) | SPK-03 |
| 3.5 | Joins (inner, left, broadcast, multi-key, cross), union / unionAll | SPK-06 |
| 3.6 | I/O with schemas | SPK-04 |
| 3.7 | Sort, iterate, printSchema, conversion to lists | SPK-03 |
| 3.8 | UDFs, with and without stateful operators / StateStores | SPK-09, SPK-11 |
| 3.9 | Broadcast variables, accumulators | SPK-06 |
| 3.10 | Broadcast joins: purpose and implementation | SPK-06 |
| 4.1 | Tuning: partitioning, repartition, coalesce, skew, fewer shuffles | SPK-06, SPK-07 |
| 4.2 | Adaptive Query Execution | SPK-08 |
| 4.3 | Driver/executor logs, OOM, underutilization | SPK-08, SPK-12 |
| 5.1 | Structured Streaming engine, micro-batch, exactly-once, fault tolerance | SPK-10 |
| 5.2 | Streaming DataFrames, output modes, sinks | SPK-10 |
| 5.3 | Selection, projection, windows, aggregation on streams | SPK-10, SPK-11 |
| 5.4 | Streaming deduplication with and without watermarks | SPK-11 |
| 6.1 | Spark Connect features | SPK-01 |
| 6.2 | Deployment modes: client, cluster, local | SPK-01 |
| 7.1 | Advantages of Pandas API on Spark | SPK-09 |
| 7.2 | Pandas UDFs | SPK-09 |

## Confluent CCDAK

Source: the official exam guide is at training.confluent.io (requires login, not retrieved).
Domains ⚠️ (third-party listing; weights not confirmed):

| Domain | Name | Modules |
|---|---|---|
| ccdak:1 | Apache Kafka fundamentals | KFK-01, KFK-02, KFK-09 |
| ccdak:2 | Application development (producers, consumers, Connect, Schema Registry, EOS) | KFK-03..07, JVM-* |
| ccdak:3 | Kafka Streams | KFK-08 |
| ccdak:4 | Application testing | KFK-10, JVM-06 |
| ccdak:5 | Application observability | KFK-10 |

## Confluent CCAAK

Source: official guide at training.confluent.io (login). Domains and weights ⚠️ (third-party listing;
they add up to 101%, so expect small differences in the official guide):

| Domain | Name | Weight | Modules |
|---|---|---|---|
| ccaak:1 | Kafka fundamentals | 15% | KFK-01, KFK-02 |
| ccaak:2 | Security | 15% | KFK-12 |
| ccaak:3 | Deployment architecture | 12% | KFK-11, KFK-13 |
| ccaak:4 | Kafka Connect | 12% | KFK-07 |
| ccaak:5 | Cluster configuration | 22% | KFK-11 |
| ccaak:6 | Observability | 10% | KFK-14, ARC-11 |
| ccaak:7 | Troubleshooting | 15% | KFK-14 |

## Confluent DSE – Foundations

Source ✅: developer.confluent.io/certificates. Free, 30 questions / 60 min, unproctored,
digital badge. No formal objective list. Confluent's suggested preparation is mapped below.

| Official prep item | Modules |
|---|---|
| Kafka 101 + console producer/consumer, specific partition/offset tutorials | KFK-01..04 |
| Producer and consumer API tutorials; optimizing the producer for throughput | KFK-03, KFK-04 |
| Kafka Connect 101 | KFK-07 |
| Schema Registry 101; multiple event types in one topic | KFK-06, KFK-09 |
| Kafka Streams 101 + intro application | KFK-08 |
| Flink 101; filtering with Flink SQL | FLK-01, FLK-04 |

## Confluent DSE – Apache Flink

Source ✅: developer.confluent.io/certificates and the official sample-question PDF. Free, 30
questions / 60 min, unproctored. Recommended content: Flink 101, Flink SQL, the Table API course
(Java), and tutorials on split/merge, interval joins, filtering and deduplication. The sample
questions cover checkpoint guarantees, which SQL operations produce updates, temporal joins with
idle watermarks, the JobManager's role, savepoints, and GROUP BY repartitioning.

| Topic (from the prep list and sample questions) | Modules |
|---|---|
| Architecture: JobManager, TaskManagers, slots | FLK-01 |
| Checkpoints, savepoints, exactly-once | FLK-06, FLK-12 |
| Watermarks, idleness, windows | FLK-03 |
| Flink SQL changelog semantics (append vs. update), GROUP BY, window TVFs, OVER | FLK-04, FLK-09 |
| Joins: interval, temporal | FLK-05 |
| Table API (Java) | FLK-08 |
| Split/merge, filter, dedup patterns | FLK-09 |

## Databricks Data Engineer Professional (optional)

Source ✅: *Databricks Certified Data Engineer Professional – Exam Guide*, live as of 2026-07-03.
59 Q / 120 min, USD 200, online or test center. Ten sections. Weights ⚠️ (third-party): 22 / 7 /
10 / 5 / 10 / 13 / 10 / 7 / 10 / 6 %.

The exam is about **Databricks product features** (Unity Catalog, Lakeflow Spark Declarative Pipelines,
Auto Loader, Declarative Automation Bundles, system tables, Delta Sharing). The generic skills come
from this platform. The product specifics need Databricks Free Edition practice, ideally in the
separate DEA project.

| Section | Covered here | Needs Databricks-specific study |
|---|---|---|
| 1 Developing code (Python/SQL, testing, pipelines) | SPK-03, SPK-09, SPK-12 | Lakeflow SDP, AUTO CDC, Bundles |
| 2 Ingestion & acquisition | SPK-04, SPK-10, KFK-07 | Auto Loader |
| 3 Transformation, cleansing, quality | SPK-03, SPK-06, STH-06 | SDP expectations/quarantine |
| 4 Sharing & federation | – | Delta Sharing, Lakehouse Federation |
| 5 Monitoring & alerting | SPK-08, ARC-11 | system tables, SQL alerts, Jobs API |
| 6 Cost & performance | SPK-05..08, LKH-05, LKH-06 | liquid clustering, deletion vectors, UC managed tables |
| 7 Security & compliance | KFK-12, ARC-07 | row filters, column masks |
| 8 Governance | LKH-03 | Unity Catalog permission model |
| 9 Debugging & deploying | SPK-08, SPK-12 | job repair, Git folders, Bundles |
| 10 Data modeling | STH-05, LKH-05, LKH-06, **your Kimball background** | liquid clustering vs. ZORDER |
