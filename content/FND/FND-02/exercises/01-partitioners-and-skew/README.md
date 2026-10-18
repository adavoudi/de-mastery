# Build hash and range partitioners; detect skew

Implement three functions in `solution.py`. All must be **deterministic across processes and
runs** (no `random`, no process-salted `hash()`).

### 1. `hash_partition(keys, p) -> list[int]`
Given a list of keys (ints or strings) and a partition count `p >= 1`, return a list the same
length as `keys`, where element `i` is the partition index in `[0, p)` for `keys[i]`.
- Equal keys must always map to the same partition.
- Use a **stable** hash (not Python's built-in `hash()`, which is randomized per process).

### 2. `range_partition(keys, p, sample=None) -> list[int]`
Return partition indices in `[0, p)` using **range** partitioning: contiguous key ranges, so
partition 0 holds the smallest keys and partition `p-1` the largest.
- Choose `p - 1` boundaries from a **sorted sample** of the keys (use all keys if `sample` is
  `None`). Keys `<=` boundary `k` and `>` boundary `k-1` go to partition `k`.
- The assignment must be **monotonic**: if `a <= b` then `partition(a) <= partition(b)`.

### 3. `skew_report(assignments, p) -> dict`
Given the list of partition indices and the partition count, return:
```python
{"counts": [n0, n1, ..., n_{p-1}],   # rows per partition, length p (zeros included)
 "max": int, "mean": float,
 "skew_ratio": float}                 # max / mean, or 1.0 if there are no rows
```
`skew_ratio == 1.0` means perfectly balanced; a large ratio means a hot partition.

## What the tests check

- Both partitioners assign **every** row to exactly one partition in `[0, p)`.
- `hash_partition` is **deterministic** and **key-stable** (equal keys → equal partition), and is
  reasonably **balanced** on many distinct keys (property test with Hypothesis).
- `range_partition` is **monotonic** (property test) and produces contiguous ranges.
- A deliberately **hot key** (90% of rows one value) yields `skew_ratio` well above 1 under
  hashing — the detection signal from the lesson.
- `skew_report` math is correct, including empty input and empty partitions.

Constraints: pure Python only (`hashlib`/`zlib`/`bisect` are fine). `lab: none`.
