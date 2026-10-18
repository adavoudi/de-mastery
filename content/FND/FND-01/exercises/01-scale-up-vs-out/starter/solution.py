"""Starter for FND-01/01-scale-up-vs-out.

Fill in the three functions. See README.md. Do NOT assert timing anywhere.
"""

from __future__ import annotations


def agg_duckdb(parquet_path) -> list[dict]:
    # TODO: use DuckDB to read the Parquet file and return rows
    #       {"product_code": str, "total_amount": float, "n": int}
    #       projecting only the columns you aggregate.
    raise NotImplementedError


def agg_pool(csv_path, n_workers: int) -> list[dict]:
    # TODO: compute the identical result from the CSV using multiprocessing.Pool.
    raise NotImplementedError


def bytes_read(parquet_path, csv_path) -> dict:
    # TODO: return {"parquet": <compressed size of the 2 scanned column chunks>,
    #               "csv": <full CSV file size on disk>}
    raise NotImplementedError
