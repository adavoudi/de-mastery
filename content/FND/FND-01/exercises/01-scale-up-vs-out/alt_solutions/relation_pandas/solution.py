"""Alt solution 2 for FND-01/01-scale-up-vs-out.

Genuinely different:
* agg_duckdb uses the DuckDB relation/expression API (no SQL string).
* agg_pool reads the CSV with pandas, splits by numpy array_split, and each worker
  does a pandas groupby; partials are combined by concatenation + a final groupby.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from multiprocessing import Pool

_PROJECTED = ("product_code", "line_amount")


def agg_duckdb(parquet_path) -> list[dict]:
    con = duckdb.connect(":memory:")
    try:
        rel = con.read_parquet(str(parquet_path)).project("product_code, line_amount")
        agg = rel.aggregate(
            "product_code, sum(line_amount) AS total_amount, count(*) AS n",
            "product_code",
        )
        df = agg.df()
    finally:
        con.close()
    return [{"product_code": r.product_code,
             "total_amount": float(r.total_amount), "n": int(r.n)}
            for r in df.itertuples(index=False)]


def _agg_chunk(df_chunk: pd.DataFrame) -> pd.DataFrame:
    g = df_chunk.groupby("product_code", as_index=False).agg(
        total_amount=("line_amount", "sum"), n=("line_amount", "size"))
    return g


def agg_pool(csv_path, n_workers: int) -> list[dict]:
    df = pd.read_csv(csv_path, usecols=["product_code", "line_amount"])
    n_workers = max(1, int(n_workers))
    chunks = [c for c in np.array_split(df, n_workers) if len(c) > 0] or [df]

    with Pool(processes=n_workers) as pool:
        partials = pool.map(_agg_chunk, chunks)

    combined = pd.concat(partials, ignore_index=True)
    final = combined.groupby("product_code", as_index=False).agg(
        total_amount=("total_amount", "sum"), n=("n", "sum"))
    return [{"product_code": r.product_code,
             "total_amount": float(r.total_amount), "n": int(r.n)}
            for r in final.itertuples(index=False)]


def bytes_read(parquet_path, csv_path) -> dict:
    md = pq.ParquetFile(str(parquet_path)).metadata
    projected = set(_PROJECTED)
    pq_bytes = 0
    for rg in range(md.num_row_groups):
        g = md.row_group(rg)
        for c in range(g.num_columns):
            cc = g.column(c)
            if cc.path_in_schema in projected:
                pq_bytes += cc.total_compressed_size
    return {"parquet": int(pq_bytes), "csv": int(Path(csv_path).stat().st_size)}
