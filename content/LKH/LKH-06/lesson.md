# LKH-06 · Delta Lake

## Why Delta exists

Delta Lake solves the same problem as Iceberg — turn a pile of Parquet files in object storage into
a transactional table — but it came out of the Databricks/Spark world and makes different design
choices. You will meet it in job postings as often as Iceberg ("Delta shop"), and the Databricks
Data Engineer exams assume it. The point of this module is not to relearn table formats from
scratch; you already did that with Iceberg (LKH-01..05). The point is to do a task you know cold —
a Slowly Changing Dimension Type 2 upsert — in Delta, read its transaction log, and see exactly
where Delta and Iceberg agree and where they differ.

## How it works

**The transaction log (`_delta_log`).** A Delta table is a directory of Parquet data files plus a
`_delta_log/` directory. Each commit is a new JSON file (`00000000000000000000.json`,
`...0001.json`, …) containing *actions*: `add` (a data file is now live), `remove` (a data file is
tombstoned), `metaData`, `commitInfo`. The current table state is the result of replaying all
commits in order. This is the key contrast with Iceberg: Iceberg's current state is a single
pointer in the catalog to one `metadata.json` that *names* the live manifest list; Delta's current
state is the *fold* of an ordered log of commits. Both are append-only and give atomic commits via
optimistic concurrency, but the shape of the metadata differs.

**Checkpoints.** Replaying thousands of JSON commits would be slow, so every N commits (default 10)
Delta writes a **checkpoint** — a Parquet file that materializes the full set of live files at that
version. A reader loads the latest checkpoint and only replays the JSON commits after it. This is
Delta's answer to the same scaling problem Iceberg solves with manifest lists and manifests.

**`MERGE INTO` for SCD2.** The upsert is the same shape you have written in dbt snapshots and SQL
marts. For a Type 2 dimension you keep history: when an incoming row changes an attribute, you
**close** the current version (set its `valid_to`) and **insert** a new current version
(`valid_to IS NULL`). In Delta SQL:

```sql
MERGE INTO dim t
USING updates u
ON t.business_key = u.business_key AND t.valid_to IS NULL
WHEN MATCHED AND t.attr <> u.attr
  THEN UPDATE SET t.valid_to = u.effective_date
WHEN NOT MATCHED
  THEN INSERT (business_key, attr, valid_from, valid_to)
       VALUES (u.business_key, u.attr, u.effective_date, NULL)
```

The classic bug — and the one the exercise's mutants encode — is to `UPDATE SET attr = u.attr`
(overwrite in place), which destroys history and makes it SCD **Type 1**. SCD2 never mutates the
old attribute; it closes the old row and adds a new one.

**Change Data Feed (CDF).** With `delta.enableChangeDataFeed=true`, Delta records row-level changes
so a downstream consumer can read *what changed* between versions instead of diffing snapshots:

```python
spark.read.format("delta").option("readChangeFeed", "true") \
     .option("startingVersion", 0).table("dim")
```

Each row carries a `_change_type` of `insert`, `update_preimage`, `update_postimage`, or `delete`,
plus `_commit_version`. This is the Delta analogue of consuming a changelog stream (your Kafka/CDC
world) and the engine behind incremental pipelines. Iceberg has an equivalent via its
`create_changelog_view` procedure / incremental scans; the concept is shared, the API differs.

**Liquid clustering & UniForm (context, not graded here).** Delta's newer layout feature, *liquid
clustering*, replaces hive-style partitioning + ZORDER with a single `CLUSTER BY` that it maintains
incrementally — the Delta counterpart to Iceberg sort orders + hidden partitioning (LKH-05).
*UniForm* writes Iceberg-compatible metadata alongside the Delta log so Iceberg readers can read a
Delta table. These matter for the Databricks exams; they are described in Further reading.

## From your world

| dbt / SQL / Iceberg | Delta |
|---|---|
| dbt snapshot (SCD2) | `MERGE INTO` that closes + inserts versions |
| Iceberg catalog pointer → one `metadata.json` | `_delta_log/` ordered JSON commits folded into state |
| Iceberg manifest list / manifests | Delta **checkpoint** Parquet every ~10 commits |
| Iceberg `snapshots` metadata table | `DeltaTable.forPath(...).history()` |
| Iceberg changelog view / incremental scan | **Change Data Feed** (`_change_type`) |
| Iceberg sort order + hidden partitioning | **liquid clustering** (`CLUSTER BY`) |
| Reading Delta from a non-Spark engine | **UniForm** (Iceberg-compatible metadata) |

## When NOT to use Delta (or when the choice is a wash)

- **Don't switch formats for its own sake.** If you already run Iceberg on Athena/Trino and it works,
  Delta buys you little; the formats are feature-comparable for most batch work. Choose Delta when
  you are on Databricks (it is the native, best-supported format there) or when a tool you must use
  only speaks Delta.
- **Don't assume CDF is free history.** It must be enabled *before* the changes you want to read,
  and it adds write cost and files. Enable it where you have a real incremental consumer, not
  everywhere by default.
- **Don't hand-edit `_delta_log`.** The log is the source of truth; a stray or malformed commit file
  corrupts the table. Treat it as read-only metadata you inspect, never patch.

## Further reading

- Delta Lake transaction log protocol:
  <https://docs.delta.io/latest/delta-utility.html> and
  <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- `MERGE INTO`: <https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge>
- Change Data Feed:
  <https://docs.delta.io/latest/delta-change-data-feed.html>
- Checkpoints & table history:
  <https://docs.delta.io/latest/delta-utility.html#history>
- Liquid clustering:
  <https://docs.delta.io/latest/delta-clustering.html>
- UniForm (Iceberg compatibility):
  <https://docs.delta.io/latest/delta-uniform.html>
