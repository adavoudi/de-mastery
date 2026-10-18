# Read Parquet footers and predict row-group pruning

Implement the row-group pruning logic that an engine uses to skip data. You read **only the
Parquet footer** (metadata + statistics of each column chunk). You never read the data pages. You
decide which row groups a filter must scan.

## Functions to implement in `solution.py`

### 1. `footer_stats(parquet_path, column) -> list[dict]`
Open the file with `pyarrow.parquet`. For the named `column`, return one dict per row group, in
row-group order:
```python
{"row_group": i, "num_rows": int,
 "min": <value or None>, "max": <value or None>, "null_count": <int or None>}
```
Read this from the **metadata only** (`ParquetFile(path).metadata` → `row_group(i).column(j)` →
`.statistics`). Do not read the column data.

### 2. `row_groups_to_scan(parquet_path, column, op, value) -> list[int]`
Return the sorted list of row-group indices that **you cannot prune** for the predicate
`column <op> value`. Here `op` is one of `">"`, `">="`, `"<"`, `"<="`, `"=="`.

The pruning rule: skip a row group **only** when its `[min, max]` makes a match impossible:
- `">"`  : skip iff `max <= value`
- `">="` : skip iff `max <  value`
- `"<"`  : skip iff `min >= value`
- `"<="` : skip iff `min >  value`
- `"=="` : skip iff `value < min` or `value > max`

**Safety rule:** suppose the statistics of a group are missing (`min`/`max` is `None`). Then you
must **not** prune it. Return it as a group to scan. An engine that pruned on absent stats would
drop rows.

## What the tests check (DuckDB is the oracle, G6)

- The test writes a Parquet file with a **known** number of row groups and controlled, sorted
  values. So the expected pruned and kept groups are known.
- `footer_stats` reports the correct `min`/`max`/`num_rows` for each group. It reads metadata only
  (the test verifies this against `parquet_metadata` of DuckDB).
- **Correctness/soundness invariant (Hypothesis):** for random data, predicates and row-group
  sizes, **your function says to scan every row group that holds a row that satisfies the
  predicate.** Pruning must never drop a matching row. (DuckDB computes which groups actually contain
  matches.)
- When statistics are absent, no group is pruned.

Constraints: pure Python + `pyarrow` + `duckdb` (oracle in tests). Deterministic. `lab: none`.
