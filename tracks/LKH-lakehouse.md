# LKH: Lakehouse & open table formats (~35 h)

**Philosophy.** A table format turns a pile of files in object storage into a database table.
It provides ACID commits, snapshots, schema evolution and partition evolution, and keeps the data
open to any engine. You already *use* Iceberg. This track teaches its internals, how to
operate it, and the alternatives, so that "Delta shop" or "Hudi shop" in a job posting doesn't scare you.
**When not to use it:** small operational data (use Postgres), sub-second point lookups (use a KV
store), or a team that is fully inside one warehouse and has no need for openness.

Lab profile: `core` (Spark + Iceberg REST + MinIO + Trino).

## LKH-BYO: build your own mini table format (~8 h, in addition to the modules)

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
