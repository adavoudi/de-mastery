# Stage 6 — Partitioned execution with a hash shuffle (multiprocessing)

Scale out across CPU cores. Implement a **hash shuffle**: hash-partition the data
by the group keys so each group lands wholly in one partition, then aggregate the
partitions in **separate OS processes** and combine the results. This is the
`Exchange` / stage boundary in Spark.

## Task

```python
stable_hash(value) -> int                         # deterministic across processes
hash_partition(table, keys, num_partitions) -> list[pyarrow.Table]
execute_parallel(agg_plan, num_partitions=4, processes=2) -> pyarrow.Table
```

`execute_parallel` runs an `Aggregate` plan as a shuffled job and must equal the
single-process `execute()` / DuckDB.

## The two subtle bugs this stage teaches

1. **Salted hashing.** Python's built-in `hash()` is salted per interpreter via
   `PYTHONHASHSEED`, so a child process would hash the same key to a *different*
   partition. Use `hashlib` (e.g. `blake2b(repr(key))`) for a `stable_hash` that
   is identical in every process. A test runs `stable_hash` in a fresh
   interpreter with a different `PYTHONHASHSEED` and asserts it matches.
2. **Splitting a group across partitions.** Partition by the *key's* hash (not
   round-robin / row index), so every row of a group is in one partition and a
   simple union of per-partition partials is already the correct total.

## Implementation notes

- Model it like Spark shuffle: the map side writes one file per partition; reduce
  workers (child processes) read a partition file, compute a **partial**
  aggregate (reuse stage 4), and write the state back; the parent merges.
- Use `multiprocessing.get_context("fork")` so children share the loaded module —
  robust and fast under the test runner. Pass only picklable primitives to the
  worker. Keep the suite < 60 s (small data, 2 processes).
