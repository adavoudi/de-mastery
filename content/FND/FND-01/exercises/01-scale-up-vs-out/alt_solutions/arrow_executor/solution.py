"""Alt solution 1 for FND-01/01-scale-up-vs-out.

Genuinely different:
* agg_duckdb reads the Parquet into an Arrow table (projected columns only) and
  queries the registered Arrow table, instead of read_parquet() SQL.
* agg_pool uses concurrent.futures.ProcessPoolExecutor with STRIDED (round-robin)
  chunks instead of contiguous slices, proving the combine is order-independent.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

_PROJECTED = ("product_code", "line_amount")


def agg_duckdb(parquet_path) -> list[dict]:
    table = pq.read_table(str(parquet_path), columns=list(_PROJECTED))  # noqa: F841
    con = duckdb.connect(":memory:")
    try:
        con.register("oi", table)
        rows = con.execute(
            "SELECT product_code, sum(line_amount) AS total_amount, count(*) AS n "
            "FROM oi GROUP BY product_code"
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
    return dict(acc)


def agg_pool(csv_path, n_workers: int) -> list[dict]:
    with open(csv_path, newline="") as fh:
        pairs = [(r["product_code"], float(r["line_amount"]))
                 for r in csv.DictReader(fh)]
    n_workers = max(1, int(n_workers))
    # Strided chunks: worker k gets rows k, k+n, k+2n, ... (round-robin).
    chunks = [pairs[k::n_workers] for k in range(n_workers)]

    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        partials = list(ex.map(_agg_chunk, chunks))

    combined = defaultdict(lambda: [0.0, 0])
    for part in partials:
        for pc, (s, c) in part.items():
            combined[pc][0] += s
            combined[pc][1] += c
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
