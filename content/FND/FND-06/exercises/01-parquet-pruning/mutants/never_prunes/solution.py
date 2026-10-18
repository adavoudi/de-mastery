"""Mutant: never prunes anything — returns every row group regardless of the
predicate. Sound (no dropped rows) but useless: it defeats the whole point of
footer-stat pruning and reads data it could have skipped."""

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


def row_groups_to_scan(parquet_path, column: str, op: str, value) -> list[int]:
    md = pq.ParquetFile(str(parquet_path)).metadata
    # BUG: ignores the predicate; scans every row group.
    return list(range(md.num_row_groups))
