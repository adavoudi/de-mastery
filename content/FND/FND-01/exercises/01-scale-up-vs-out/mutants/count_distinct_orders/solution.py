"""Mutant: counts distinct order_ids instead of rows."""

from __future__ import annotations

from pathlib import Path

import csv
import multiprocessing as mp
from collections import defaultdict

import duckdb
import pyarrow.parquet as pq

_PROJECTED = ("product_code", "line_amount")


def agg_duckdb(parquet_path) -> list[dict]:
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(
            # BUG: count(distinct order_id) is not the number of line rows.
            "SELECT product_code, sum(line_amount) AS total_amount, "
            "count(distinct order_id) AS n "
            "FROM read_parquet(?) GROUP BY product_code",
            [str(parquet_path)],
        ).fetchall()
    finally:
        con.close()
    return [{"product_code": pc, "total_amount": float(ta), "n": int(n)}
            for (pc, ta, n) in rows]


def _agg_chunk(chunk):
    acc = defaultdict(lambda: [0.0, set()])
    for pc, amt, oid in chunk:
        acc[pc][0] += amt
        acc[pc][1].add(oid)
    return {k: (v[0], v[1]) for k, v in acc.items()}


def agg_pool(csv_path, n_workers: int) -> list[dict]:
    with open(csv_path, newline="") as fh:
        pairs = [(r["product_code"], float(r["line_amount"]), r["order_id"])
                 for r in csv.DictReader(fh)]
    n_workers = max(1, int(n_workers))
    size = (len(pairs) + n_workers - 1) // n_workers if pairs else 0
    chunks = [pairs[i:i + size] for i in range(0, len(pairs), size)] or [[]]
    with mp.Pool(processes=n_workers) as pool:
        partials = pool.map(_agg_chunk, chunks)
    combined = defaultdict(lambda: [0.0, set()])
    for part in partials:
        for pc, (s, ids) in part.items():
            combined[pc][0] += s
            combined[pc][1] |= ids
    return [{"product_code": pc, "total_amount": float(s), "n": int(len(ids))}
            for pc, (s, ids) in combined.items()]


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
