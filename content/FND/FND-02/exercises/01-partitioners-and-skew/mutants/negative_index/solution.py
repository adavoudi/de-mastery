"""Mutant: hash can be negative, so partition indices can be negative/out of range.

Uses Python's signed built-in hash without taking absolute value; `% p` in Python
is non-negative, but this mutant subtracts 1 'to make it 0-based', pushing some
indices to -1. Real mistake: off-by-one / sign handling on the partition index.
"""

from __future__ import annotations

import bisect
import hashlib


def _stable_hash(key) -> int:
    return int.from_bytes(hashlib.sha256(str(key).encode()).digest()[:8], "big")


def hash_partition(keys, p: int) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    # BUG: subtracts 1 after modulo, so key hashing to 0 becomes -1.
    return [(_stable_hash(k) % p) - 1 for k in keys]


def range_partition(keys, p: int, sample=None) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    if p == 1:
        return [0] * len(keys)
    pool = sorted(keys if sample is None else sample)
    if not pool:
        return [0] * len(keys)
    n = len(pool)
    bounds = [pool[min(n - 1, (i * n) // p)] for i in range(1, p)]
    return [min(bisect.bisect_right(bounds, k), p - 1) for k in keys]


def skew_report(assignments, p: int) -> dict:
    counts = [0] * p
    for a in assignments:
        counts[a] += 1
    total = sum(counts)
    if total == 0:
        return {"counts": counts, "max": 0, "mean": 0.0, "skew_ratio": 1.0}
    mean = total / p
    mx = max(counts)
    return {"counts": counts, "max": int(mx), "mean": float(mean),
            "skew_ratio": float(mx / mean)}
