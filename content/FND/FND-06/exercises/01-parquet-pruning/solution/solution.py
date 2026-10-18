"""Reference solution for FND-06/01-parquet-pruning.

Reads only the Parquet footer (metadata + per-column statistics) to decide which row
groups a predicate can prune.
"""

from __future__ import annotations

import pyarrow.parquet as pq


def _column_index(rg_meta, column: str) -> int:
    for j in range(rg_meta.num_columns):
        if rg_meta.column(j).path_in_schema == column:
            return j
    raise KeyError(f"column {column!r} not found in row group schema")


def footer_stats(parquet_path, column: str) -> list[dict]:
    md = pq.ParquetFile(str(parquet_path)).metadata
    out = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        col = rg.column(_column_index(rg, column))
        st = col.statistics
        if st is None or not st.has_min_max:
            mn = mx = None
            nulls = st.null_count if st is not None else None
        else:
            mn, mx = st.min, st.max
            nulls = st.null_count
        out.append({"row_group": i, "num_rows": rg.num_rows,
                    "min": mn, "max": mx, "null_count": nulls})
    return out


def _skip(op: str, value, mn, mx) -> bool:
    # If stats are missing we cannot prove emptiness -> never skip.
    if mn is None or mx is None:
        return False
    if op == ">":
        return mx <= value
    if op == ">=":
        return mx < value
    if op == "<":
        return mn >= value
    if op == "<=":
        return mn > value
    if op == "==":
        return value < mn or value > mx
    raise ValueError(f"unsupported op {op!r}")


def row_groups_to_scan(parquet_path, column: str, op: str, value) -> list[int]:
    scan = []
    for s in footer_stats(parquet_path, column):
        if not _skip(op, value, s["min"], s["max"]):
            scan.append(s["row_group"])
    return sorted(scan)
