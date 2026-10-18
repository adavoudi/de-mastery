"""Alt solution 1 for FND-06: uses pq.read_metadata and decides inclusion via a
POSITIVE overlap test (does the predicate's allowed interval intersect [min,max]?)
rather than enumerating skip conditions."""

from __future__ import annotations

import pyarrow.parquet as pq


def _col_idx(rg, column):
    return next(j for j in range(rg.num_columns)
               if rg.column(j).path_in_schema == column)


def footer_stats(parquet_path, column: str) -> list[dict]:
    md = pq.read_metadata(str(parquet_path))
    rows = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        st = rg.column(_col_idx(rg, column)).statistics
        if st is not None and st.has_min_max:
            rows.append({"row_group": i, "num_rows": rg.num_rows,
                         "min": st.min, "max": st.max,
                         "null_count": st.null_count})
        else:
            rows.append({"row_group": i, "num_rows": rg.num_rows,
                         "min": None, "max": None,
                         "null_count": st.null_count if st else None})
    return rows


def _matches(op, value, mn, mx) -> bool:
    if mn is None or mx is None:
        return True  # cannot prune without stats
    if op == ">":
        return mx > value
    if op == ">=":
        return mx >= value
    if op == "<":
        return mn < value
    if op == "<=":
        return mn <= value
    if op == "==":
        return mn <= value <= mx
    raise ValueError(op)


def row_groups_to_scan(parquet_path, column: str, op: str, value) -> list[int]:
    return sorted(s["row_group"] for s in footer_stats(parquet_path, column)
                  if _matches(op, value, s["min"], s["max"]))
