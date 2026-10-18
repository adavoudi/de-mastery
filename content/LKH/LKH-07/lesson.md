# LKH-07 · Hudi & the format landscape

## Why there is more than one format

Iceberg (LKH-01..05) and Delta (LKH-06) are not the whole story. **Apache Hudi** is the third major
open table format, and it came from a different problem: Uber needed *incremental* ingestion and
record-level upserts at scale, not just atomic batch commits. Knowing when a format's origin story
makes it the right pick — and when the choice is a near-wash — is the actual skill. A job posting
that says "Hudi shop" or "we're evaluating Iceberg vs Delta" wants someone who can reason about this,
not someone who memorized one tool.

## How they differ

**Apache Hudi.** Hudi's defining features are the **timeline** (an ordered log of commits/actions
that drives incremental queries) and **record-level indexing** — Hudi maps each record key to a file
group via an index (bloom, or the Record Level Index), so an upsert locates exactly the files it must
touch instead of scanning/joining the whole target. That makes Hudi strong for **mutable,
upsert-heavy, near-real-time ingestion** (CDC streams, frequent primary-key updates) and for
**incremental pulls** ("give me only what changed since time T"). Like the others it offers
Copy-on-Write (rewrite files on update) and Merge-on-Read (write delta logs, merge on read) table
types.

**Apache Iceberg.** Strengths: **hidden partitioning** and **partition evolution** (change the
layout with no data rewrite), broad **engine neutrality** (Spark, Trino, Flink, Athena, DuckDB,
StarRocks) and a clean catalog story (REST, Glue, Polaris, Nessie, Unity, Lakekeeper). Pick Iceberg
for a **vendor-neutral lake queried by many engines** where you want to evolve layout freely.

**Delta Lake.** Strengths: deep **Databricks** integration (Unity Catalog governance, liquid
clustering, Lakeflow pipelines), Change Data Feed, and a mature Spark story. Pick Delta when you are
**on Databricks** or your required tooling speaks Delta best. UniForm narrows the interop gap by
exposing Iceberg-compatible metadata.

**Apache Paimon (preview).** A streaming-first lake format (from the Flink community) built around an
LSM structure for high-throughput streaming updates — relevant to the streamhouse track (STH), noted
here so the name isn't a surprise.

**Apache XTable (interop).** Not a fourth format but a translator: it writes the metadata of one
format from another (Iceberg ⇄ Delta ⇄ Hudi) so one copy of the data can be read as any of them.
With XTable (or Delta UniForm) the "which format" choice is less of a lock-in than it looks — but you
still pick a *primary* writer format based on your workload.

## How to choose (the honest version)

For most **batch** lakehouse work, Iceberg and Delta are feature-comparable; choose on **ecosystem
fit**, not benchmarks:

- **On Databricks / Spark-centric with Unity Catalog + liquid clustering?** → **Delta**.
- **Vendor-neutral lake, many query engines, want free partition evolution / hidden partitioning?**
  → **Iceberg**.
- **Mutable, upsert-heavy, near-real-time ingestion with record-level indexing and incremental
  pulls (CDC)?** → **Hudi**.
- **Locked into one and need another engine to read it?** → **UniForm / XTable**, not a migration.

The anti-pattern is choosing by hype or by what a blog benchmarked on a workload unlike yours.

## From your world

| Choosing a warehouse / modeling decision | Choosing a table format |
|---|---|
| "Redshift vs Snowflake vs BigQuery for *our* workload" | "Iceberg vs Delta vs Hudi for *our* workload" |
| Picking a DISTKEY/SORTKEY for the query pattern | Picking the format whose strength matches the write/read pattern |
| CDC into a dimension (your SCD2) | Hudi upserts / incremental pulls, or Delta CDF |
| Vendor lock-in worries | engine neutrality (Iceberg) vs platform depth (Delta); XTable/UniForm to hedge |

## When NOT to overthink it

- **Don't adopt Hudi for a pure append-only analytics table.** Its upsert/index machinery is overhead
  you won't use; Iceberg or Delta is simpler.
- **Don't switch formats to chase a benchmark.** Migrations are costly; interop (UniForm/XTable) often
  solves the actual need (one engine must read the data) without moving anything.
- **Don't let the format decide your governance.** Catalog + governance (Unity, Polaris, Glue) often
  matters more day-to-day than the on-disk format.

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
