"""Alt solution 1 for FND-02: zlib.crc32 hashing and an explicit linear boundary scan
(no bisect). Genuinely different hash function and range-assignment mechanics."""

from __future__ import annotations

import zlib


def hash_partition(keys, p: int) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    return [zlib.crc32(str(k).encode("utf-8")) % p for k in keys]


def _boundaries(keys, p, sample):
    pool = sorted(keys if sample is None else sample)
    if not pool or p <= 1:
        return []
    n = len(pool)
    return [pool[min(n - 1, (i * n) // p)] for i in range(1, p)]


def range_partition(keys, p: int, sample=None) -> list[int]:
    if p < 1:
        raise ValueError("p must be >= 1")
    if p == 1:
        return [0] * len(keys)
    bounds = _boundaries(keys, p, sample)
    out = []
    for k in keys:
        part = 0
        for b in bounds:
            if k > b:
                part += 1
            else:
                break
        out.append(min(part, p - 1))
    return out


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
