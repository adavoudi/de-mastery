"""Mutant: range_partition actually HASHES instead of ranging.

Co-locates by hash, so the assignment is NOT monotonic — large keys can land in a
lower partition than small keys. Real mistake: confusing hash and range partitioning
(a very common conceptual slip).
"""

from __future__ import annotations

import hashlib


def _stable_hash(key) -> int:
    return int.from_bytes(hashlib.sha256(str(key).encode()).digest()[:8], "big")


def hash_partition(keys, p: int) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    return [_stable_hash(k) % p for k in keys]


def range_partition(keys, p: int, sample=None) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    # BUG: not a range partitioner at all.
    return [_stable_hash(k) % p for k in keys]


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
