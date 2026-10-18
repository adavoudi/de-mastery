# LKH-04 · Row-level changes — SCD2, CoW & MoR

## Why row-level change is hard on immutable files

Parquet files are immutable. You cannot edit a row in place. But real tables need updates, deletes
and upserts. The best example is the **SCD Type 2** dimension that you already build. In this
dimension, a changed attribute closes the old version (it sets `valid_to`) and opens a new
version. Iceberg v2 makes row-level change possible on top of immutable files. It has two ways to
do this. The choice between them is the core operational decision of this module.

## Copy-on-write vs merge-on-read

A `MERGE`, `UPDATE` or `DELETE` can touch rows inside a data file. Then Iceberg must do one of two
things. It rewrites the file, or it records the change separately:

- **Copy-on-write (CoW).** Iceberg rewrites each affected data file without the changed or deleted
  rows. It writes the new versions as new data files. Reads stay fast, because every data file is
  "clean". But writes are expensive. Iceberg rewrites whole files, even to change one row. This is
  the Redshift or warehouse model: the table is always materialized.
- **Merge-on-read (MoR).** Iceberg leaves the data files alone. It writes a small **delete file**
  that marks which rows are logically removed. A delete file can mark rows by position (a
  *position delete*) or by value (an *equality delete*). v3 adds compact **deletion vectors**.
  Writes are cheap. But reads pay to merge the delete files against the data files at scan time.
  You trade a higher read cost for a lower write cost. You also accumulate deletes until
  compaction (LKH-05) folds them back in.

![The same UPDATE: copy-on-write rewrites the data file, merge-on-read adds a delete file merged at read time](diagrams/cow-vs-mor.svg)

You can **see** in the metadata which mode happened. This is how you verify the mode and do not
guess:

```sql
-- content: 0 = data file, 1 = position deletes, 2 = equality deletes
SELECT content, count(*) FROM local.db.t.files GROUP BY content;
```

Look at the result after a delete. A **CoW** table shows only `content = 0` (Iceberg rewrote the
data files). A **MoR** table shows `content = 0` **and** `content = 1` (Iceberg added a delete
file and did not rewrite data). Three table properties choose the mode: `write.delete.mode`,
`write.update.mode` and `write.merge.mode`. Their values are `copy-on-write` or `merge-on-read`.

## SCD2 with `MERGE INTO`

SCD2 is the standard row-level job. The dimension carries `valid_from`, `valid_to` (`NULL` or open
on the current version) and `is_current`. You apply a daily batch of natural-key rows in two
steps:

1. **Close** every current row whose tracked attribute changed:
   `MERGE INTO dim t USING src u ON t.nk = u.nk AND t.is_current WHEN MATCHED AND t.attr <> u.attr
   THEN UPDATE SET t.valid_to = <batch_date>, t.is_current = false`.
2. **Insert** a new open version for every changed key *and* every brand-new key:
   `INSERT INTO dim SELECT nk, attr, <batch_date> AS valid_from, NULL AS valid_to, true AS
   is_current FROM src LEFT ANTI JOIN current-rows`.

Three mistakes break SCD2. The tests look for all three:

- You **overwrite** the row instead of versioning it (history is lost).
- You leave `valid_to` **open** on the superseded row (a key has two "current" rows).
- You use an **off-by-one** effective date (ranges overlap or leave a gap).

The tests check the correct result against an **independent DuckDB oracle**. The oracle computes
the same close and insert over the same two daily batches. When two engines agree, the expected
answer is right. This is the G6 guarantee (oracle agreement, in `docs/TESTING.md`).

## From your world

| Redshift / dbt / SCD2 | Iceberg | Note |
|---|---|---|
| SCD2 snapshot: close old, open new | two-step `MERGE` + `INSERT` | same bitemporal logic |
| `UPDATE`/`DELETE`/`MERGE` on a warehouse table | CoW (rewrite) or MoR (delete files) | you choose the trade-off |
| VACUUM rewrites to reclaim space | compaction folds MoR deletes back (LKH-05) | deletes accumulate until then |
| "the dimension is always materialised" | copy-on-write | fast reads, costly writes |
| high-frequency upserts/CDC | merge-on-read | cheap writes, read-time merge |

## When NOT to…

- **Do not use MoR for a dimension that you read often and write rarely.** The cost of the
  read-time merge and the build-up of delete files are not worth it. CoW keeps reads clean.
- **Do not use CoW for high-frequency CDC or upserts into large files.** Iceberg rewrites whole
  files for each tiny change. This is write amplification. MoR (or deletion vectors) solves this.
- **Do not maintain SCD2 by hand with blind overwrites.** You lose history. History is the entire
  reason that the dimension is Type 2. Version the row. Never change the key fields of the old row
  in place.

## Further reading

- Iceberg spec — Row-level Deletes (position/equality deletes, deletion vectors):
  <https://iceberg.apache.org/spec/#row-level-deletes>
- Iceberg Spark writes (MERGE INTO, CoW/MoR modes):
  <https://iceberg.apache.org/docs/latest/spark-writes/>
- Iceberg configuration (`write.*.mode` properties):
  <https://iceberg.apache.org/docs/latest/configuration/>
