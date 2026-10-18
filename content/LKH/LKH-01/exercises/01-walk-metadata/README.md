# Walk the Iceberg metadata tree

The Athena tables you query are backed by a tree of metadata. Here you walk it yourself with
PyIceberg — no SQL engine, no Docker — so the pruning Athena does for you stops being magic.

You are given a loaded PyIceberg `table` that has had **several appends** (so multiple snapshots,
manifests and data files exist). Implement two functions in `solution.py`.

## `walk(table) -> dict`

Walk the metadata tree of the table's **current snapshot** and return:

| key | value |
|---|---|
| `metadata_location` | the current `metadata.json` path (a string) |
| `num_snapshots` | total number of snapshots in the table |
| `num_manifests` | number of manifests referenced by the current snapshot |
| `num_data_files` | number of **data files** reachable from the current snapshot |
| `total_records` | sum of `record_count` across those data files |
| `data_file_paths` | a `list[str]` of the data-file paths (any order) |

Rules / traps:
- `num_data_files` is **not** `num_manifests` — one manifest lists many files. Count manifest
  *entries*, not manifests.
- `total_records` must come from the files' own `record_count` stat, not from scanning data.
- Walk from `table.current_snapshot()`, not from the raw file system.

## `records_per_file(table) -> dict[str, int]`

Return a mapping from each current-snapshot data-file path to that file's `record_count` (read from
the manifest entry, not by reading the Parquet). `sum(records_per_file(table).values())` must equal
`walk(table)["total_records"]`.

The tests build the table (known number of appends and rows), so the expected counts are known;
they also cross-check your file list against an independent read of the manifests.
