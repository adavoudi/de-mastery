# Read Parquet footers and predict row-group pruning

Implement the row-group pruning logic an engine uses to skip data. You read **only the Parquet
footer** (metadata + per-column-chunk statistics) — never the data pages — and decide which row
groups a filter must scan.

## Functions to implement in `solution.py`

### 1. `footer_stats(parquet_path, column) -> list[dict]`
Open the file with `pyarrow.parquet` and return one dict per row group, in row-group order, for
the named `column`:
```python
{"row_group": i, "num_rows": int,
 "min": <value or None>, "max": <value or None>, "null_count": <int or None>}
```
Read this from the **metadata only** (`ParquetFile(path).metadata` → `row_group(i).column(j)` →
`.statistics`). Do not read the column data.

### 2. `row_groups_to_scan(parquet_path, column, op, value) -> list[int]`
Return the sorted list of row-group indices that **cannot be pruned** for the predicate
`column <op> value`, where `op` is one of `">"`, `">="`, `"<"`, `"<="`, `"=="`.

Pruning rule — a row group is **skipped** only when its `[min, max]` makes a match impossible:
- `">"`  : skip iff `max <= value`
- `">="` : skip iff `max <  value`
- `"<"`  : skip iff `min >= value`
- `"<="` : skip iff `min >  value`
- `"=="` : skip iff `value < min` or `value > max`

**Safety rule:** if the statistics for a group are missing (`min`/`max` is `None`), you must **not**
prune it — return it as a group to scan (an engine that pruned on absent stats would drop rows).

## What the tests check (DuckDB is the oracle, G6)

- The test writes a Parquet file with a **known** number of row groups and controlled, sorted
  values, so the expected pruned/kept groups are known.
- `footer_stats` reports the correct per-group `min`/`max`/`num_rows` and reads metadata only
  (verified by comparing against DuckDB's `parquet_metadata`).
- **Correctness/soundness invariant (Hypothesis):** for random data, predicates and row-group
  sizes, **every row that satisfies the predicate lives in a row group your function says to
  scan.** Pruning must never drop a matching row. (DuckDB computes which groups actually contain
  matches.)
- When statistics are absent, no group is pruned.

Constraints: pure Python + `pyarrow` + `duckdb` (oracle in tests). Deterministic. `lab: none`.
