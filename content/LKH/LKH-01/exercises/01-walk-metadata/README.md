# Walk the Iceberg metadata tree

The Athena tables that you query are backed by a tree of metadata. In this exercise, you walk the
tree yourself with PyIceberg. You use no SQL engine and no Docker. Then the pruning that Athena
does for you does not look like magic.

You get a loaded PyIceberg `table`. It has had **several appends**, so it has multiple snapshots,
manifests and data files. Implement two functions in `solution.py`.

## `walk(table) -> dict`

Walk the metadata tree of the **current snapshot** of the table. Return this dict:

| key | value |
|---|---|
| `metadata_location` | the current `metadata.json` path (a string) |
| `num_snapshots` | total number of snapshots in the table |
| `num_manifests` | number of manifests referenced by the current snapshot |
| `num_data_files` | number of **data files** reachable from the current snapshot |
| `total_records` | sum of `record_count` across those data files |
| `data_file_paths` | a `list[str]` of the data-file paths (any order) |

Rules and traps:
- `num_data_files` is **not** `num_manifests`. One manifest lists many files. Count manifest
  *entries*, not manifests.
- `total_records` must come from the `record_count` stat of the files. Do not scan the data.
- Walk from `table.current_snapshot()`. Do not walk the raw file system.

## `records_per_file(table) -> dict[str, int]`

Return a mapping. Each key is the path of a data file of the current snapshot. Each value is the
`record_count` of that file. Read the value from the manifest entry. Do not read the Parquet file.
`sum(records_per_file(table).values())` must equal `walk(table)["total_records"]`.

The tests build the table with a known number of appends and rows. So the expected counts are
known. The tests also compare your file list with an independent read of the manifests.
