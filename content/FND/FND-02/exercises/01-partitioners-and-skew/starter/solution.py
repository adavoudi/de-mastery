"""Starter for FND-02/01-partitioners-and-skew. See README.md."""

from __future__ import annotations


def hash_partition(keys, p: int) -> list[int]:
    # TODO: map each key to a partition in [0, p) using a STABLE hash.
    raise NotImplementedError


def range_partition(keys, p: int, sample=None) -> list[int]:
    # TODO: contiguous ranges; monotonic assignment.
    raise NotImplementedError


def skew_report(assignments, p: int) -> dict:
    # TODO: per-partition counts, max, mean, and skew_ratio = max/mean.
    raise NotImplementedError
