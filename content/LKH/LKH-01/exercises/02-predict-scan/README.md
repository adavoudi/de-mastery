# Predict which data files a filtered query reads

Every "Athena scanned N GB" line is scan planning in action: Iceberg uses the manifests' per-file
min/max to decide which Parquet files a filter can possibly touch, and reads only those. Here you
reproduce that decision with PyIceberg — no engine, no Docker.

You are given a loaded `table` whose data was written so each file covers a narrow value range (so
pruning is predictable). Implement these in `solution.py`:

## `files_for_equals(table, column, value) -> dict`

Plan a scan filtered by `column == value` and return:

| key | value |
|---|---|
| `num_files` | number of data files the scan will read |
| `paths` | the surviving data-file paths, **sorted** |

## `files_for_at_least(table, column, value) -> dict`

Same, for the predicate `column >= value`.

## `all_files(table) -> list[str]`

The sorted list of **all** current data-file paths (an unfiltered plan), so a test can confirm your
filtered plans are a subset and that pruning actually drops something.

Rules / traps:
- Use Iceberg **scan planning** (`table.scan(row_filter=...).plan_files()`), not a full data read
  then a Python filter. The whole point is to avoid opening pruned files.
- Build the predicate as a PyIceberg expression (`EqualTo`, `GreaterThanOrEqual`) so the planner
  can push it into the manifests.
- Return paths sorted so comparisons are order-independent.

The test knows, by construction, exactly which files hold which values, and also cross-checks your
answer against an independent correct scan plan (the oracle). A solution that "reads everything"
(no pruning) fails, because for these predicates strictly fewer files should be read.
