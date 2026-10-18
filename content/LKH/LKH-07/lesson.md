# LKH-07 · Hudi & the format landscape

## Why there is more than one format

Iceberg (LKH-01..05) and Delta (LKH-06) are not the whole story. **Apache Hudi** is the third major
open table format. It came from a different problem. Uber needed *incremental* ingestion and
record-level upserts at scale. Atomic batch commits were not enough. The real skill is to know when
the origin story of a format makes it the right pick. You must also know when the choice makes
little difference. A job posting can say "Hudi shop" or "we're evaluating Iceberg vs Delta". It
wants someone who can reason about this. It does not want someone who memorized one tool.

## How they differ

**Apache Hudi.** Hudi has two defining features:

- The **timeline**. This is an ordered log of commits and actions. It drives incremental queries.
- **Record-level indexing**. Hudi maps each record key to a file group with an index (bloom, or the
  Record Level Index). So an upsert finds exactly the files that it must touch. It does not scan or
  join the whole target.

For this reason, Hudi is strong for **mutable, upsert-heavy, near-real-time ingestion** (CDC
streams, frequent primary-key updates). It is also strong for **incremental pulls** ("give me only
what changed since time T"). Like the other formats, it offers two table types. Copy-on-Write
rewrites files on update. Merge-on-Read writes delta logs and merges them on read.

**Apache Iceberg.** Strengths:

- **Hidden partitioning** and **partition evolution** (you change the layout with no data rewrite).
- Broad **engine neutrality** (Spark, Trino, Flink, Athena, DuckDB, StarRocks).
- A clean catalog story (REST, Glue, Polaris, Nessie, Unity, Lakekeeper).

Pick Iceberg for a **vendor-neutral lake that many engines query**, where you want to evolve the
layout freely.

**Delta Lake.** Strengths: deep **Databricks** integration (Unity Catalog governance, liquid
clustering, Lakeflow pipelines), Change Data Feed, and a mature Spark story. Pick Delta when you
are **on Databricks**, or when your required tools speak Delta best. UniForm makes the interop gap
smaller. It exposes Iceberg-compatible metadata.

**Apache Paimon (preview).** This is a streaming-first lake format (from the Flink community). It
uses an LSM structure for high-throughput streaming updates. It is relevant to the streamhouse
track (STH). This module names it, so the name is not a surprise later.

**Apache XTable (interop).** XTable is not a fourth format. It is a translator. It writes the
metadata of one format from another (Iceberg ⇄ Delta ⇄ Hudi). Then you can read one copy of the
data as any of these formats. With XTable (or Delta UniForm), the "which format" choice is less of
a lock-in than it looks. But you still pick a *primary* writer format based on your workload.

## How to choose (the honest version)

For most **batch** lakehouse work, Iceberg and Delta have comparable features. Choose on
**ecosystem fit**, not on benchmarks:

- **Do you work on Databricks / Spark-centric with Unity Catalog + liquid clustering?** → **Delta**.
- **Do you have a vendor-neutral lake with many query engines, and do you want free partition
  evolution and hidden partitioning?** → **Iceberg**.
- **Do you have mutable, upsert-heavy, near-real-time ingestion with record-level indexing and
  incremental pulls (CDC)?** → **Hudi**.
- **Are you locked into one format and need another engine to read it?** → **UniForm / XTable**,
  not a migration.

The anti-pattern is to choose by hype. Another anti-pattern is to choose by a benchmark that a blog
ran on a workload unlike yours.

## From your world

| Choosing a warehouse / modeling decision | Choosing a table format |
|---|---|
| "Redshift vs Snowflake vs BigQuery for *our* workload" | "Iceberg vs Delta vs Hudi for *our* workload" |
| Picking a DISTKEY/SORTKEY for the query pattern | Picking the format whose strength matches the write/read pattern |
| CDC into a dimension (your SCD2) | Hudi upserts / incremental pulls, or Delta CDF |
| Vendor lock-in worries | engine neutrality (Iceberg) vs platform depth (Delta); XTable/UniForm to hedge |

## When NOT to overthink it

- **Do not adopt Hudi for a pure append-only analytics table.** You will not use its upsert and
  index machinery. It is overhead. Iceberg or Delta is simpler.
- **Do not switch formats to chase a benchmark.** Migrations are costly. Interop (UniForm/XTable)
  often solves the actual need (one engine must read the data). It does this without moving
  anything.
- **Do not let the format decide your governance.** Catalog and governance (Unity, Polaris, Glue)
  often matter more day to day than the on-disk format.

## Further reading

- Hudi concepts (timeline, indexes, table types):
  <https://hudi.apache.org/docs/concepts/> and <https://hudi.apache.org/docs/indexes/>
- Hudi record-level index & incremental queries:
  <https://hudi.apache.org/docs/indexes/> ,
  <https://hudi.apache.org/docs/next/incremental_processing/>
- Iceberg partitioning & evolution:
  <https://iceberg.apache.org/docs/1.10.0/partitioning/>
- Delta on Databricks (liquid clustering, Unity):
  <https://docs.delta.io/latest/delta-clustering.html>
- Apache XTable (cross-format interop): <https://xtable.apache.org/>
- Apache Paimon: <https://paimon.apache.org/>
