"""Mutant: for '>' and '>=' it compares the group's MIN instead of MAX, so it prunes
groups that actually contain matching rows (min <= value but max > value). Unsound:
drops matching rows. Classic 'wrong stat for the comparison' bug."""

from __future__ import annotations

import pyarrow.parquet as pq


def _col_idx(rg, column):
    return next(j for j in range(rg.num_columns)
               if rg.column(j).path_in_schema == column)


def footer_stats(parquet_path, column: str) -> list[dict]:
    md = pq.ParquetFile(str(parquet_path)).metadata
    out = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        st = rg.column(_col_idx(rg, column)).statistics
        has = st is not None and st.has_min_max
        out.append({"row_group": i, "num_rows": rg.num_rows,
                    "min": st.min if has else None,
                    "max": st.max if has else None,
                    "null_count": st.null_count if st else None})
    return out


def _skip(op, value, mn, mx) -> bool:
    if mn is None or mx is None:
        return False
    if op == ">":
        return mn <= value   # BUG: should be mx <= value
    if op == ">=":
        return mn < value    # BUG: should be mx < value
    if op == "<":
        return mn >= value
    if op == "<=":
        return mn > value
    if op == "==":
        return value < mn or value > mx
    raise ValueError(op)


def row_groups_to_scan(parquet_path, column: str, op: str, value) -> list[int]:
    return sorted(s["row_group"] for s in footer_stats(parquet_path, column)
                  if not _skip(op, value, s["min"], s["max"]))
