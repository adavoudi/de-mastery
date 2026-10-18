"""Starter for FND-06/01-parquet-pruning. See README.md."""

from __future__ import annotations


def footer_stats(parquet_path, column: str) -> list[dict]:
    # TODO: read ParquetFile(path).metadata and return per-row-group
    #       {"row_group", "num_rows", "min", "max", "null_count"} for `column`.
    raise NotImplementedError


def row_groups_to_scan(parquet_path, column: str, op: str, value) -> list[int]:
    # TODO: return row-group indices that cannot be pruned for column <op> value.
    #       Never prune when stats are missing.
    raise NotImplementedError
