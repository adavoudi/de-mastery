# LKH: Lakehouse & open table formats (~35 h)

**Philosophy.** A table format turns a pile of files in object storage into a database table.
It provides ACID commits, snapshots, schema evolution and partition evolution, and keeps the data
open to any engine. You already *use* Iceberg. This track teaches its internals, how to
operate it, and the alternatives, so that "Delta shop" or "Hudi shop" in a job posting doesn't scare you.
**When not to use it:** small operational data (use Postgres), sub-second point lookups (use a KV
store), or a team that is fully inside one warehouse and has no need for openness.

Lab profile: `core` (Spark + Iceberg REST + MinIO + Trino).

| ID | Module | h | Key ideas | Exercises (graded) | From your world |
|---|---|---|---|---|---|
| LKH-01 | Iceberg metadata tree | 4 | Catalog → metadata.json → manifest list → manifests → data files; snapshots | Walk the metadata with `pyiceberg`; predict which files a query reads | The Iceberg tables you query in Athena |
| LKH-02 | Commits & concurrency | 4 | Optimistic concurrency, atomic catalog swap, conflict detection, isolation levels | Make two concurrent writers conflict and resolve it; the test asserts no lost update | Overlapping dbt runs |
| LKH-03 | Catalogs | 4 | Hive, Glue, REST (Polaris, Nessie, Unity, Lakekeeper); branching and tags | Use a branch for a write-audit-publish (WAP) flow | SOX: audited publish step |
| LKH-04 | Row-level changes | 5 | Copy-on-write vs. merge-on-read, position and equality deletes, deletion vectors (v3), `MERGE INTO` | Implement SCD2 with `MERGE`; benchmark CoW vs. MoR | Your SCD2 snapshots |
| LKH-05 | Layout & maintenance | 5 | Hidden partitioning, partition evolution, sort order, compaction, expiring snapshots, orphan files | Fix a table with 50k small files; the test checks file count, size and snapshot retention | Redshift VACUUM / SORTKEY |
| LKH-06 | Delta Lake | 4 | `_delta_log`, checkpoints, liquid clustering, Change Data Feed, UniForm | Do the same SCD2 task in Delta; compare the logs | – (DE Pro relevant) |
| LKH-07 | Hudi & the format landscape | 3 | Hudi timeline, MoR/CoW, indexes; Paimon preview; XTable interop; how to choose | Write a decision memo for 3 scenarios, graded with a rubric | – |
| LKH-08 | Engines on the lake | 6 | Trino, Athena, DuckDB, StarRocks; statistics; Puffin files; caching | Run the same query on 3 engines and explain the plan differences | Athena |

## LKH-BYO: build your own mini table format (~8 h, on top of the module hours; included in the ROADMAP total)

| Stage | Build | Teaches |
|---|---|---|
| 1 | Write Parquet files plus a JSON manifest | Metadata vs. data |
| 2 | Snapshots and time travel | Immutability |
| 3 | Atomic commit using put-if-absent (object-store compare-and-set, conditional writes on S3) | Optimistic concurrency |
| 4 | Concurrent writers with conflict detection and retry | Isolation |
| 5 | Delete files (merge-on-read) | Row-level changes |
| 6 | Manifest statistics plus file pruning | Planning speed |
| 7 | Compaction and snapshot expiration | Maintenance |

The tests act as a "reader engine" that must see consistent snapshots while concurrent writers run.
