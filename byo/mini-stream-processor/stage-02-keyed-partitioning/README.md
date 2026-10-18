# Stage 2 — Keyed partitioning across workers (keyBy)

Flink's `keyBy` routes every record for a key to the **same** parallel subtask
(key group), so keyed state and keyed aggregation are correct regardless of
parallelism. You implement that routing.

## New API this stage

```python
stream.key_by(key_fn) -> KeyedStream
keyed.reduce(reduce_fn, parallelism=1) -> KeyedResult
run_parallel(KeyedResult) -> dict          # {key: reduced_value}
partition_of(key, num_partitions) -> int   # which worker a key lands on
worker_assignment(keys, n) -> {key: worker}
stable_hash(value) -> int                  # provided; deterministic across procs
```

## This stage's task

- `partition_of(key, n)` = `stable_hash(key) % n`. Use the provided
  `stable_hash` (blake2b of `repr`), **not** Python's `hash()` (which is salted
  per interpreter — a test runs `stable_hash` in a child with a different
  `PYTHONHASHSEED` and asserts equality).
- `run_parallel` drives the chain, groups data events by key, routes each key to
  its worker, and reduces per key with `reduce_fn`.
- Because each key lives on exactly one worker, the merged result is **invariant
  to parallelism** — `parallelism=1`, `3`, `7` all give the same answer, equal to
  a plain per-key fold (the oracle).
- `worker_assignment(keys, n)` returns `{key: partition_of(key, n)}`.

## Constraints

- Deterministic routing (stable hash). No sleeps. Suite < 60 s.
- Earlier stages still pass (regression).

## Run it

```bash
de start FLK-BYO/02
de test
```
