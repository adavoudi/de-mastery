# LKH-01 · The Iceberg metadata tree

## Why a table format exists

When you query an Iceberg table in Athena, you write `SELECT ... FROM glue_db.orders` and it
behaves like a database table: you get a consistent view, you can time-travel, schema changes
don't rewrite everything. But underneath there is no database — there is a bucket full of
immutable Parquet files in S3. The magic that turns "a pile of files" into "a table" is a tree of
**metadata** that Iceberg writes alongside the data. Learning to read that tree is the single most
useful Iceberg skill, because every operational question — *why is this query slow?*, *why did the
snapshot not expire?*, *which files does this filter actually read?* — is answered by walking it.

You already trust this tree every time Athena prunes partitions for you. This module opens the box
so the behaviour stops being magic.

## How the tree is laid out

Iceberg metadata is a four-level pointer chain, newest-first:

1. **Catalog entry → `metadata.json`.** The catalog (Glue, in your Athena world) stores *one*
   atomic pointer per table: the path of the current `metadata.json`. That file is the root of the
   tree. It lists the schema(s), partition spec(s), sort order(s), table properties, and the full
   list of **snapshots** with the id of the *current* one. A commit is just an atomic swap of this
   one pointer (LKH-02).
2. **`metadata.json` → manifest list.** Each snapshot points at exactly one **manifest list** (a
   `snap-*.avro` file). The manifest list is the snapshot's index: one row per **manifest**, with
   that manifest's partition-value ranges and added/existing/deleted file counts.
3. **manifest list → manifests.** Each **manifest** (`*.avro`) lists a batch of data (or delete)
   files, and — crucially — carries **per-file column statistics**: `record_count`, and
   `lower_bounds`/`upper_bounds`/`null_counts` per column.
4. **manifest → data files.** The leaves are the immutable **Parquet data files** (plus, in v2+,
   **delete files**, LKH-04).

```
Glue catalog  ──▶  vN.metadata.json
                        │  (schema, specs, snapshots[], current-snapshot-id)
                        ▼
                   snap-<id>.avro        ← manifest list (one row per manifest)
                        │
                        ▼
                   <uuid>-m0.avro        ← manifest (per-file stats: counts, min/max, nulls)
                        │
                        ▼
                   00000-….parquet       ← data files (and delete files)
```

**Why two index levels (manifest list *and* manifests)?** So a query planner can prune cheaply.
It reads the small manifest list, drops whole manifests whose partition range can't match, then
reads only the surviving manifests and uses their per-file min/max to drop individual data files —
the same row-group pruning idea from FND-06, but one level up, over *files* instead of row groups.
This is **scan planning**, and the number of files it decides to read is a measurable fact:
`DataScan.plan_files()` returns exactly the file-scan tasks a correct engine would execute.

Each write produces a new **snapshot** (a new `metadata.json` version, a new manifest list), while
old snapshots keep pointing at their own manifest lists — that is what makes **time travel** and
atomic commits possible: nothing is mutated, only added, and the catalog pointer flips.

## From your world

| Athena / Glue / dbt | Iceberg metadata | Note |
|---|---|---|
| The table you `SELECT` in Athena | catalog → `metadata.json` | one atomic Glue pointer |
| Partition pruning you never see | manifest-list partition ranges | drops whole manifests |
| "Athena scanned 1.2 GB" cost line | `plan_files()` result | which data files are read |
| A dbt snapshot's historical rows | the `snapshots` list | each write = one snapshot |
| `VACUUM` on Redshift | expiring snapshots (LKH-05) | removes unreferenced files |

## When NOT to lean on the metadata tree

- **Don't read the Avro files by hand in production** to answer a question `t.inspect.*` or the
  SQL metadata tables (`.snapshots`, `.files`, `.manifests`) already answer — hand-parsing is for
  learning and for the rare forensic case.
- **Don't treat the per-file min/max as a secondary index for point lookups.** Iceberg prunes
  files, it is not a KV store; if you need sub-second lookups by key, that is the "when not to use
  a lakehouse" case from the track intro.
- **Don't equate "number of manifests" with "number of data files".** One manifest lists many
  files; conflating the levels is the classic beginner metadata-reading bug.

## Further reading

- Iceberg table spec (metadata, manifests, manifest lists, snapshots):
  <https://iceberg.apache.org/spec/>
- Iceberg terms (snapshot, manifest, manifest list, data file):
  <https://iceberg.apache.org/terms/>
- PyIceberg API (metadata inspection, scans):
  <https://py.iceberg.apache.org/api/>
