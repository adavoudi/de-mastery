# Stage 3 — Predicate pushdown using Parquet row-group statistics

Real engines don't read data they can prove is irrelevant. Parquet stores
per-column **min/max statistics** for each row group, so a selective predicate
can skip whole row groups before reading them. This is what Spark's
`PushedFilters` and row-group pruning do.

## Task

When a `Scan` carries a `pushdown` predicate
(`new_scan(path, pushdown=Compare(Col("id"), ">", Lit(950)))`), skip any row
group whose statistics **prove** no row can match, and record how many you
skipped.

- Read stats via `pf.metadata.row_group(rg).column(idx).statistics`
  (`has_min_max`, `.min`, `.max`) — no data read required.
- Maintain the counters on `ScanStats`: `row_groups_total`, `row_groups_read`,
  `row_groups_skipped`.
- Surface them in `explain()`: a `PushedFilters=...` annotation and the
  read/skipped counts.

## Soundness (the one rule you must not break)

Skip a row group only when the predicate is **impossible** on `[min, max]`:

| predicate | skip the row group iff |
|---|---|
| `col > v`  | `max <= v` |
| `col >= v` | `max < v`  |
| `col < v`  | `min >= v` |
| `col <= v` | `min > v`  |
| `col = v`  | `v < min or v > max` |

If statistics are missing or the predicate isn't a simple `Col op Lit`, **do not
skip** (conservative). Pushdown only prunes row groups — keep the `Filter`
operator so surviving rows are still filtered exactly.

## Checks

- A selective predicate over a sorted file skips most row groups (counter).
- `explain()` shows `PushedFilters` and skipped counts.
- Correctness is unchanged vs DuckDB for every threshold (Hypothesis property):
  a skipped group must never have contained a matching row.
