# Predict which data files a filtered query reads

Every "Athena scanned N GB" line is scan planning at work. Iceberg uses the per-file min/max of the
manifests. It decides which Parquet files a filter can possibly touch, and it reads only those
files. In this exercise, you reproduce that decision with PyIceberg. You use no engine and no
Docker.

You get a loaded `table`. Its data was written so that each file covers a narrow range of values.
This makes pruning predictable. Implement these functions in `solution.py`:

## `files_for_equals(table, column, value) -> dict`

Plan a scan that is filtered by `column == value`. Return this dict:

| key | value |
|---|---|
| `num_files` | number of data files the scan will read |
| `paths` | the surviving data-file paths, **sorted** |

## `files_for_at_least(table, column, value) -> dict`

Do the same for the predicate `column >= value`.

## `all_files(table) -> list[str]`

Return the sorted list of **all** current data-file paths (an unfiltered plan). A test uses this
list to confirm two facts. Your filtered plans are a subset of it. Pruning actually drops
something.

Rules and traps:
- Use Iceberg **scan planning** (`table.scan(row_filter=...).plan_files()`). Do not read all the
  data and then filter in Python. The goal is to avoid opening pruned files.
- Build the predicate as a PyIceberg expression (`EqualTo`, `GreaterThanOrEqual`). Then the planner
  can push it into the manifests.
- Return sorted paths. Then the comparisons do not depend on order.

By construction, the test knows exactly which files hold which values. It also compares your answer
with an independent correct scan plan (the oracle). A solution that "reads everything" (no
pruning) fails. For these predicates, the scan must read strictly fewer files.
