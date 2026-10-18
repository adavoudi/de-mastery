"""Mutant: bytes_read swaps the two measurements.

The code aggregation is correct, but it reports the whole Parquet file (all columns,
uncompressed) as 'parquet' and only a tiny slice as 'csv', so it claims the columnar
path reads MORE bytes than the CSV. That inverts the lesson's measured conclusion.
"""

from __future__ import annotations

import csv
import multiprocessing as mp
from collections import defaultdict
from pathlib import Path

import duckdb
import pyarrow.parquet as pq


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
    acc = defaultdict(lambda: [0.0, 0])
    for pc, amt in chunk:
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
    combined = defaultdict(lambda: [0.0, 0])
    for part in partials:
        for pc, (s, c) in part.items():
            combined[pc][0] += s
            combined[pc][1] += c
    return [{"product_code": pc, "total_amount": float(s), "n": int(c)}
            for pc, (s, c) in combined.items()]


def bytes_read(parquet_path, csv_path) -> dict:
    md = pq.ParquetFile(str(parquet_path)).metadata
    # BUG: uses TOTAL UNCOMPRESSED size of ALL columns for 'parquet', and reports
    # the CSV as a tiny fixed value, inverting the comparison.
    pq_bytes = sum(
        md.row_group(rg).column(c).total_uncompressed_size
        for rg in range(md.num_row_groups)
        for c in range(md.row_group(rg).num_columns)
    )
    return {"parquet": int(pq_bytes), "csv": 1}
