"""Alt solution 2 for FND-02: md5-based hashing, numpy-quantile boundaries for the
range partitioner, and collections.Counter for the skew report. Different libraries
and a statistically-chosen boundary set."""

from __future__ import annotations

import bisect
import hashlib
from collections import Counter


def hash_partition(keys, p: int) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    out = []
    for k in keys:
        h = int(hashlib.md5(str(k).encode("utf-8")).hexdigest(), 16)
        out.append(h % p)
    return out


def range_partition(keys, p: int, sample=None) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    if p == 1:
        return [0] * len(keys)
    pool = sorted(keys if sample is None else sample)
    if not pool:
        return [0] * len(keys)
    n = len(pool)
    # Interior boundaries at quantile positions i/p for i in 1..p-1.
    bounds = []
    for i in range(1, p):
        pos = (i * n) // p
        bounds.append(pool[min(n - 1, pos)])
    return [min(bisect.bisect_right(bounds, k), p - 1) for k in keys]


def skew_report(assignments, p: int) -> dict:
    c = Counter(assignments)
    counts = [int(c.get(i, 0)) for i in range(p)]
    total = sum(counts)
    if total == 0:
        return {"counts": counts, "max": 0, "mean": 0.0, "skew_ratio": 1.0}
    mean = total / p
    mx = max(counts)
    return {"counts": counts, "max": int(mx), "mean": float(mean),
            "skew_ratio": float(mx / mean)}
