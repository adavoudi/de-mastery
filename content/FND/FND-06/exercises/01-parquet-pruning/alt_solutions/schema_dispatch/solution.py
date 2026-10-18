"""Alt solution 2 for FND-06: resolves the column index via the Arrow schema and
dispatches the skip test through a dict of lambdas. Different lookup + control flow."""

from __future__ import annotations

import pyarrow.parquet as pq

# skip(value, min, max) -> True if the row group can be pruned.
_SKIP = {
    ">":  lambda v, mn, mx: mx <= v,
    ">=": lambda v, mn, mx: mx < v,
    "<":  lambda v, mn, mx: mn >= v,
    "<=": lambda v, mn, mx: mn > v,
    "==": lambda v, mn, mx: v < mn or v > mx,
}


def _col_index(pf: "pq.ParquetFile", column: str) -> int:
    names = pf.schema_arrow.names
    return names.index(column)


def footer_stats(parquet_path, column: str) -> list[dict]:
    pf = pq.ParquetFile(str(parquet_path))
    j = _col_index(pf, column)
    md = pf.metadata
    result = []
    for i in range(md.num_row_groups):
        col = md.row_group(i).column(j)
        st = col.statistics
        has = st is not None and st.has_min_max
        result.append({
            "row_group": i,
            "num_rows": md.row_group(i).num_rows,
            "min": st.min if has else None,
            "max": st.max if has else None,
            "null_count": st.null_count if st is not None else None,
        })
    return result


def row_groups_to_scan(parquet_path, column: str, op: str, value) -> list[int]:
    if op not in _SKIP:
        raise ValueError(f"unsupported op {op!r}")
    skip = _SKIP[op]
    keep = []
    for s in footer_stats(parquet_path, column):
        if s["min"] is None or s["max"] is None:
            keep.append(s["row_group"])          # never prune without stats
        elif not skip(value, s["min"], s["max"]):
            keep.append(s["row_group"])
    return sorted(keep)
