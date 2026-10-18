"""Mutant: the pool combine OVERWRITES instead of summing partials.

A product that appears in more than one chunk ends up with only the last chunk's
partial, not the sum. With 1 worker (1 chunk) the bug is invisible; with several
workers the totals and counts are wrong. This is the real 'reduce step forgot to
aggregate' mistake.
"""

from __future__ import annotations

import csv
import multiprocessing as mp
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

_PROJECTED = ("product_code", "line_amount")


def agg_duckdb(parquet_path) -> list[dict]:
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            "SELECT product_code, sum(line_amount) AS total_amount, count(*) AS n "
            "FROM read_parquet(?) GROUP BY product_code",
            [str(parquet_path)],
        ).fetchall()
    finally:
        con.close()
    return [{"product_code": pc, "total_amount": float(ta), "n": int(n)}
            for (pc, ta, n) in rows]


def _agg_chunk(chunk):
    acc = {}
    for pc, amt in chunk:
        if pc not in acc:
            acc[pc] = [0.0, 0]
        acc[pc][0] += amt
        acc[pc][1] += 1
    return {k: (v[0], v[1]) for k, v in acc.items()}


def agg_pool(csv_path, n_workers: int) -> list[dict]:
    with open(csv_path, newline="") as fh:
        pairs = [(r["product_code"], float(r["line_amount"]))
                 for r in csv.DictReader(fh)]
    n_workers = max(1, int(n_workers))
    size = (len(pairs) + n_workers - 1) // n_workers if pairs else 0
    chunks = [pairs[i:i + size] for i in range(0, len(pairs), size)] or [[]]
    with mp.Pool(processes=n_workers) as pool:
        partials = pool.map(_agg_chunk, chunks)
    combined = {}
    for part in partials:
        for pc, (s, c) in part.items():
            # BUG: overwrite, not add. Multi-chunk products lose data.
            combined[pc] = (s, c)
    return [{"product_code": pc, "total_amount": float(s), "n": int(c)}
            for pc, (s, c) in combined.items()]


def bytes_read(parquet_path, csv_path) -> dict:
    md = pq.ParquetFile(str(parquet_path)).metadata
    projected = set(_PROJECTED)
    pq_bytes = sum(
        md.row_group(rg).column(c).total_compressed_size
        for rg in range(md.num_row_groups)
        for c in range(md.row_group(rg).num_columns)
        if md.row_group(rg).column(c).path_in_schema in projected
    )
    return {"parquet": int(pq_bytes), "csv": int(Path(csv_path).stat().st_size)}
