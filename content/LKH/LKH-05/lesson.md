# LKH-05 · Table layout & maintenance

## Why maintenance exists

An Iceberg (or Delta, or Hudi) table is a pile of immutable data files plus metadata that says
which files are live. Every write adds files; `MERGE`/`UPDATE`/`DELETE` adds more (and delete
files on top). Streaming or frequent micro-batch writes are the worst: a commit every few seconds
produces thousands of tiny files. Nothing ever rewrites those files for you. Left alone, a healthy
table rots into the **small-files problem**: query planning has to open and read the footers of
tens of thousands of files, each scan task does almost no useful work, and your Athena/Trino bill
and latency climb even though the data barely grew.

You already know this disease from two angles. On Redshift, tables fragment as you load and delete,
so you run `VACUUM` to reclaim space and re-sort, and you choose a `SORTKEY` so range-restricted
scans skip blocks. On S3 + Athena you have felt a Hive-style table with millions of 10 KB files
crawl. A lakehouse table needs the same housekeeping, and the format gives you explicit, atomic
operations to do it: **compaction**, **sort order**, **snapshot expiration**, and **orphan-file
removal**. Running them is operations work, not a one-time setup.

## How it works

**Compaction (`rewrite_data_files`).** This reads many small data files and rewrites them into
fewer, right-sized ones (default target ~512 MB), committing the result as a *new snapshot*. The
old files stay referenced by old snapshots until you expire them, so compaction never loses data
and is safe to run concurrently with reads. In Spark:

```sql
CALL local.system.rewrite_data_files(
  table => 'db.events',
  options => map('min-input-files', '2', 'target-file-size-bytes', '134217728'))
```

The returned row tells you `rewritten_data_files_count` and `added_data_files_count`.

**Sort order (`WRITE ORDERED BY`).** Iceberg stores per-column min/max in each data file's
metadata. If rows are clustered by a column, a predicate on that column lets the planner skip whole
files (file pruning, LKH-01/FND-06). You declare the order once; compaction (and future ordered
writes) honor it:

```sql
ALTER TABLE local.db.events WRITE ORDERED BY event_date
```

This is the Iceberg analogue of a Redshift `SORTKEY`. `WRITE DISTRIBUTED BY PARTITION LOCALLY
ORDERED BY ...` controls the shuffle too.

**Hidden partitioning & partition evolution.** Iceberg partitions on *transforms* of a column
(`days(ts)`, `bucket(16, id)`), and the transform is recorded in metadata — so you query on the raw
column (`WHERE ts >= ...`) and Iceberg prunes partitions for you. No `dt=2026-01-01` string column
in your `WHERE`, no leaking the physical layout into queries (the Hive mistake). And because the
partition spec lives in metadata, you can **evolve** it (`ALTER TABLE ... ADD PARTITION FIELD
days(ts)`) without rewriting old data; new data uses the new spec, old data keeps its own. The one
rule that never changes: **partition on a low-cardinality transform.** Partitioning on a
high-cardinality raw column (a user id, an order id) creates one tiny file per value — the
small-files problem by construction.

**Snapshot expiration (`expire_snapshots`).** Every commit creates a snapshot; time travel and
rollback depend on them, but they also pin old data files so disk never shrinks. Expiration deletes
snapshots (and the data/manifest files only they reference) older than a cutoff, keeping a
retention window:

```sql
CALL local.system.expire_snapshots(
  table => 'db.events',
  older_than => TIMESTAMP '2026-01-01 00:00:00',
  retain_last => 5)
```

The subtlety that bites everyone: `retain_last` alone will NOT reclaim recent snapshots, because
`expire_snapshots` also honors a default `max_snapshot_age_ms` (5 days) that protects anything
recent. To actually reclaim in a maintenance job you set `older_than`. And you must keep enough
history for your rollback/time-travel SLA — expiring *everything* throws away your ability to
recover from a bad write.

**Orphan files (`remove_orphan_files`).** Failed or aborted writes can leave data files on object
storage that no snapshot references. These are not reclaimed by `expire_snapshots` (which only
touches files reachable from expired snapshots). `remove_orphan_files` lists the table directory and
deletes unreferenced files older than a cutoff. Use a conservative `older_than` (default 3 days) so
you never delete a file that an in-flight commit is about to reference.

The healthy-maintenance recipe, run on a schedule: **compact → (keep the sort order) → expire old
snapshots → occasionally remove orphans.**

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

- **Don't compact a table that is only ever appended in large files.** If each write already
  produces ~512 MB files, compaction just burns compute rewriting them. Compact when you have *many
  small* files, not on a reflex.
- **Don't expire snapshots inside your time-travel / rollback window.** If your DR policy is "7 days
  of recovery", `older_than` must be ≥7 days ago. Expiring aggressively to save a little S3 removes
  your safety net.
- **Don't partition on a high-cardinality column** to "spread the data out". It creates the exact
  small-files problem compaction exists to fix. Partition on a low-cardinality transform
  (`days(ts)`, `bucket(N, key)`), and use sort order for the high-cardinality column instead.
- **Don't run `remove_orphan_files` with a short `older_than` on a busy table.** You can race an
  in-flight commit and delete a file it is about to reference. Keep the default-ish 3-day cushion.

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
