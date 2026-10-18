# LKH-05 · Table layout & maintenance

## Why maintenance exists

An Iceberg (or Delta, or Hudi) table is a pile of immutable data files plus metadata. The
metadata says which files are live. Every write adds files. `MERGE`, `UPDATE` and `DELETE` add
more files (and delete files on top). Streaming or frequent micro-batch writes are the worst case.
A commit every few seconds produces thousands of tiny files. Nothing rewrites those files for
you. If you leave the table alone, a healthy table decays into the **small-files problem**:

- Query planning must open and read the footers of tens of thousands of files.
- Each scan task does almost no useful work.
- Your Athena/Trino bill and latency climb, even though the data barely grew.

You already know this problem from two sides. On Redshift, tables fragment as you load and delete
data. So you run `VACUUM` to reclaim space and re-sort. You also choose a `SORTKEY`, so that
range-restricted scans skip blocks. On S3 + Athena, you have seen a Hive-style table with millions
of 10 KB files run very slowly. A lakehouse table needs the same housekeeping. The format gives
you explicit, atomic operations to do it: **compaction**, **sort order**, **snapshot expiration**,
and **orphan-file removal**. Running them is operations work. It is not a one-time setup.

## How it works

**Compaction (`rewrite_data_files`).** This operation reads many small data files. It rewrites
them into fewer files of the right size (the default target is ~512 MB). It commits the result as
a *new snapshot*. The old files stay referenced by old snapshots until you expire them. So
compaction never loses data. It is also safe to run at the same time as reads. In Spark:

```sql
CALL local.system.rewrite_data_files(
  table => 'db.events',
  options => map('min-input-files', '2', 'target-file-size-bytes', '134217728'))
```

The returned row tells you `rewritten_data_files_count` and `added_data_files_count`.

**Sort order (`WRITE ORDERED BY`).** Iceberg stores the min/max of each column in the metadata of
each data file. Suppose rows are clustered by a column. Then a predicate on that column lets the
planner skip whole files (file pruning, LKH-01/FND-06). You declare the order once. Compaction (and
future ordered writes) follow it:

```sql
ALTER TABLE local.db.events WRITE ORDERED BY event_date
```

This is the Iceberg equivalent of a Redshift `SORTKEY`. `WRITE DISTRIBUTED BY PARTITION LOCALLY
ORDERED BY ...` also controls the shuffle.

**Hidden partitioning & partition evolution.** Iceberg partitions on *transforms* of a column
(`days(ts)`, `bucket(16, id)`). It records the transform in metadata. So you query on the raw
column (`WHERE ts >= ...`) and Iceberg prunes partitions for you. You do not need a string column
such as `dt=2026-01-01` in your `WHERE`. The physical layout does not leak into your queries (the
Hive mistake). The partition spec lives in metadata. So you can **evolve** it
(`ALTER TABLE ... ADD PARTITION FIELD days(ts)`) without a rewrite of old data. New data uses the
new spec. Old data keeps its own spec. One rule never changes: **partition on a low-cardinality
transform.** Suppose you partition on a high-cardinality raw column (a user id or an order id).
Then Iceberg creates one tiny file per value. This is the small-files problem by construction.

**Snapshot expiration (`expire_snapshots`).** Every commit creates a snapshot. Time travel and
rollback depend on snapshots. But snapshots also pin old data files, so the disk use never shrinks.
Expiration deletes the snapshots that are older than a cutoff. It also deletes the data files and
manifest files that only those snapshots reference. It keeps a retention window:

```sql
CALL local.system.expire_snapshots(
  table => 'db.events',
  older_than => TIMESTAMP '2026-01-01 00:00:00',
  retain_last => 5)
```

This detail causes problems for everyone. `retain_last` alone will NOT reclaim recent snapshots.
The reason is that `expire_snapshots` also uses a default `max_snapshot_age_ms` (5 days). This
default protects all recent snapshots. To reclaim space in a maintenance job, you set `older_than`.
You must also keep enough history for your rollback and time-travel SLA. If you expire
*everything*, you lose the ability to recover from a bad write.

**Orphan files (`remove_orphan_files`).** Failed or aborted writes can leave data files on object
storage that no snapshot references. `expire_snapshots` does not reclaim these files. It only
touches files that are reachable from expired snapshots. `remove_orphan_files` lists the table
directory. It deletes unreferenced files that are older than a cutoff. Use a conservative
`older_than` (default 3 days). Then you never delete a file that an in-flight commit is about to
reference.

The recipe for healthy maintenance, which you run on a schedule:

1. Compact.
2. Keep the sort order.
3. Expire old snapshots.
4. Remove orphans, from time to time.

## From your world

| Redshift / Athena | Iceberg maintenance |
|---|---|
| `VACUUM` (reclaim + re-sort) | `rewrite_data_files` (compaction) + `expire_snapshots` (reclaim) |
| `SORTKEY` | `WRITE ORDERED BY` (clusters files; feeds min/max pruning) |
| `DISTKEY` / even distribution | partition transform + `WRITE DISTRIBUTED BY` |
| Hive `dt=...` partition columns in `WHERE` | hidden partitioning: query the raw column, Iceberg prunes |
| Rebuild table to re-partition | partition **evolution** (metadata-only, no rewrite) |
| Millions of tiny Athena files = slow+costly | the small-files problem; compaction fixes it |

## When NOT to do it (or when it backfires)

- **Do not compact a table that you only append to in large files.** Suppose each write already
  produces ~512 MB files. Then compaction only uses compute to rewrite them. Compact when you have
  *many small* files. Do not compact out of habit.
- **Do not expire snapshots inside your time-travel or rollback window.** Suppose your DR policy
  is "7 days of recovery". Then `older_than` must be at least 7 days ago. If you expire
  aggressively to save a little S3 space, you remove your safety net.
- **Do not partition on a high-cardinality column** to "spread the data out". This creates the
  exact small-files problem that compaction exists to fix. Partition on a low-cardinality
  transform (`days(ts)`, `bucket(N, key)`). Use the sort order for the high-cardinality column.
- **Do not run `remove_orphan_files` with a short `older_than` on a busy table.** You can race an
  in-flight commit and delete a file that it is about to reference. Keep the cushion of about 3
  days from the default.

## Further reading

- Iceberg maintenance / stored procedures:
  <https://iceberg.apache.org/docs/1.10.0/spark-procedures/#maintenance>
- Compaction (`rewrite_data_files`):
  <https://iceberg.apache.org/docs/1.10.0/spark-procedures/#rewrite_data_files>
- Expire snapshots:
  <https://iceberg.apache.org/docs/1.10.0/spark-procedures/#expire_snapshots>
- Partitioning & hidden partitioning:
  <https://iceberg.apache.org/docs/1.10.0/partitioning/>
- Sort orders / `WRITE ORDERED BY`:
  <https://iceberg.apache.org/docs/1.10.0/spark-ddl/#alter-table--write-ordered-by>
