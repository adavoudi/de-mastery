"""Reference solution for FND-02/01-partitioners-and-skew."""

from __future__ import annotations

import bisect
import hashlib


def _stable_hash(key) -> int:
    """A process-stable, run-stable non-negative hash of a key."""
    data = str(key).encode("utf-8")
    digest = hashlib.sha256(data).digest()
    return int.from_bytes(digest[:8], "big")


def hash_partition(keys, p: int) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    return [_stable_hash(k) % p for k in keys]


def _boundaries(keys, p: int, sample):
    pool = sorted(keys if sample is None else sample)
    if not pool or p <= 1:
        return []
    # p-1 interior boundaries at evenly spaced quantile positions of the sorted pool.
    bounds = []
    n = len(pool)
    for i in range(1, p):
        idx = min(n - 1, (i * n) // p)
        bounds.append(pool[idx])
    return bounds


def range_partition(keys, p: int, sample=None) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    if p == 1:
        return [0] * len(keys)
    bounds = _boundaries(keys, p, sample)
    # bisect_right: smallest index i such that key < bounds[i]; keys above the last
    # boundary fall into partition p-1.
    return [min(bisect.bisect_right(bounds, k), p - 1) for k in keys]


def skew_report(assignments, p: int) -> dict:
    counts = [0] * p
    for a in assignments:
        counts[a] += 1
    total = sum(counts)
    if total == 0:
        return {"counts": counts, "max": 0, "mean": 0.0, "skew_ratio": 1.0}
    mx = max(counts)
    mean = total / p
    ratio = mx / mean if mean > 0 else 1.0
    return {"counts": counts, "max": int(mx), "mean": float(mean),
            "skew_ratio": float(ratio)}
