# Stage 5 — Delete files / merge-on-read (row-level changes)

Deleting a few rows by rewriting entire data files is wasteful. Real formats use
**merge-on-read**: write a small **delete file** that marks which rows are gone,
and apply it at read time. The data files stay immutable.

## Implement

- `table.delete(predicate) -> snapshot_id` where `predicate(arrow_table) ->`
  boolean mask (True = delete this row). For each data file in the current
  snapshot, compute which **positions** match and, if any, write a **position
  delete file** (e.g. JSON `{"data_file": <key>, "positions": [..]}`) under
  `"<name>/deletes/..."`. Record the delete files in the new snapshot (commit via
  your stage-3/4 path).
- `table.scan(...)` must now **subtract** deleted positions: when reading a data
  file, drop the rows whose positions are marked deleted in the snapshot being
  read.

## Rules

- **Merge-on-read, not copy-on-write.** `delete` must NOT rewrite the data files;
  it adds delete metadata only. (The tests check your data Parquet files are
  unchanged.)
- Deletes are **per snapshot**: time travel to a snapshot *before* the delete
  still shows the rows; the snapshot *after* hides them.
- Positions are relative to the data file they target. Keep that mapping exact.

Keep stages 1–4 green.
