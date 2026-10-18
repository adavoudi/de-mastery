"""Reference solution for FND-01/01-scale-up-vs-out.

Same aggregation (sum of line_amount and row count per product_code) computed two
ways, plus a bytes-read measurement showing the columnar path reads fewer bytes.
"""

from __future__ import annotations

import csv
import multiprocessing as mp
from collections import defaultdict
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# 1. One big box: DuckDB over Parquet, projecting only the needed columns.
# ---------------------------------------------------------------------------
def agg_duckdb(parquet_path) -> list[dict]:
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            """
            SELECT product_code,
                   sum(line_amount) AS total_amount,
                   count(*)         AS n
            FROM read_parquet(?)
            GROUP BY product_code
            """,
            [str(parquet_path)],
        ).fetchall()
    finally:
        con.close()
    return [{"product_code": pc, "total_amount": float(ta), "n": int(n)}
            for (pc, ta, n) in rows]


# ---------------------------------------------------------------------------
# 2. Scale out: a multiprocessing pool over the CSV, row-oriented.
# ---------------------------------------------------------------------------
def _agg_chunk(chunk: list[tuple[str, float]]) -> dict[str, tuple[float, int]]:
    """Worker: aggregate a chunk of (product_code, line_amount) pairs.

    Returns product_code -> (partial_sum, partial_count). Combine is associative.
    """
    acc: dict[str, list[float | int]] = {}
    for pc, amt in chunk:
        if pc not in acc:
            acc[pc] = [0.0, 0]
        acc[pc][0] += amt
        acc[pc][1] += 1
    return {k: (v[0], v[1]) for k, v in acc.items()}


def _read_csv_pairs(csv_path) -> list[tuple[str, float]]:
    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        return [(r["product_code"], float(r["line_amount"])) for r in reader]


def agg_pool(csv_path, n_workers: int) -> list[dict]:
    pairs = _read_csv_pairs(csv_path)
    n_workers = max(1, int(n_workers))
    # Split into contiguous chunks.
    size = (len(pairs) + n_workers - 1) // n_workers if pairs else 0
    chunks = [pairs[i:i + size] for i in range(0, len(pairs), size)] or [[]]

    with mp.Pool(processes=n_workers) as pool:
        partials = pool.map(_agg_chunk, chunks)

    combined: dict[str, list[float | int]] = defaultdict(lambda: [0.0, 0])
    for part in partials:
        for pc, (s, c) in part.items():
            combined[pc][0] += s
            combined[pc][1] += c
    return [{"product_code": pc, "total_amount": float(s), "n": int(c)}
            for pc, (s, c) in combined.items()]


# ---------------------------------------------------------------------------
# 3. Bytes read: compressed column chunks (Parquet) vs. whole CSV file.
# ---------------------------------------------------------------------------
_PROJECTED = ("product_code", "line_amount")


def bytes_read(parquet_path, csv_path) -> dict:
    md = pq.ParquetFile(str(parquet_path)).metadata
    projected = set(_PROJECTED)
    pq_bytes = 0
    for rg in range(md.num_row_groups):
        row_group = md.row_group(rg)
        for col in range(row_group.num_columns):
            col_md = row_group.column(col)
            name = col_md.path_in_schema
            if name in projected:
                pq_bytes += col_md.total_compressed_size
    csv_bytes = Path(csv_path).stat().st_size
    return {"parquet": int(pq_bytes), "csv": int(csv_bytes)}
