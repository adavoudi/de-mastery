# Stage 1 — Parquet files + a JSON manifest (metadata vs. data)

A table format is just **metadata that points at data files**. In this stage you
split the two: row data goes into immutable **Parquet** files; a tiny **JSON
manifest** records which files make up the table.

You edit `tableformat.py`. The provided `objectstore.py` is a key→bytes store over
a local directory — treat it like S3. **Do not edit `objectstore.py`.**

## Implement

- `Table.create(store, name, schema) -> Table` — register a brand-new, empty
  table. Fail if it already exists. Write the first metadata object at key
  `"<name>/metadata/v1.json"`.
- `Table.load(store, name) -> Table` — open an existing table (raise if missing).
- `table.append(arrow_table) -> snapshot_id` — write the rows to a **new Parquet
  file** under `"<name>/data/<something>.parquet"`, then record that file in a new
  metadata object. Return the new snapshot id (an int).
- `table.scan() -> pyarrow.Table` — read all data files listed in the current
  metadata and return their concatenation as a single `pyarrow.Table`.

## Rules that make it a *format*, not a heap of files

- **Data and metadata are separate objects.** The manifest is JSON and lists data
  files by key, with at least `path` and `num_rows`. The tests open your JSON
  manifest *and* your Parquet files directly with a real reader.
- **Data files are immutable.** `append` never rewrites an existing file; it adds
  a new one.
- Each metadata object is a new version `v<N>.json`; the "current" table is the
  highest version present.

## API you are starting (later stages extend the same file)

```
Table.create(store, name, schema) -> Table
Table.load(store, name) -> Table
table.append(arrow_table) -> snapshot_id
table.scan(filter=None, snapshot_id=None) -> pyarrow.Table
table.snapshots() -> list[Snapshot]      # stage 2
table.delete(predicate) -> snapshot_id    # stage 5
table.compact() -> snapshot_id            # stage 7
table.expire(older_than_ms) -> int        # stage 7
```

Run `de test` to check this stage. Later stages re-run these tests as a
regression suite, so keep `append`/`scan` working.
