"""Mutant: skew_report divides by the number of NON-EMPTY partitions.

This understates skew: with empty partitions the mean is too high, so skew_ratio looks
closer to 1.0 than it should. Real mistake: using len(nonempty) instead of p as the
denominator, hiding stragglers.
"""

from __future__ import annotations

import bisect
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
    nonempty = sum(1 for c in counts if c > 0)
    # BUG: denominator is nonempty partitions, not p.
    mean = total / nonempty
    mx = max(counts)
    return {"counts": counts, "max": int(mx), "mean": float(mean),
            "skew_ratio": float(mx / mean)}
