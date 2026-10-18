# Stage 5 — Hash join and broadcast join

Join two tables. Implement a **hash join** (build a hash table on one side, probe
with the other) and a **broadcast join** (same result; the small build side is
"broadcast"). In this single-process engine both produce identical output — only
the intended data distribution differs, which is the whole point of the lesson.

## Task

```python
HashJoin(left, right, left_keys, right_keys, how="inner")
BroadcastJoin(left, right, left_keys, right_keys, how="inner")   # same result
```

Support `how="inner"` and `how="left"`. Output columns = all left columns + all
right columns except the (duplicate) right key columns.

## Semantics you must get right

- **Duplicate build keys fan out.** If the build side has two rows with key `3`,
  every probe row with key `3` matches **both** → store a *list* per key, not a
  single row. (A join that dedups the build side is a classic bug.)
- **NULL keys never match** (SQL). Drop rows whose key is NULL from both the
  build map and the probe — including a build-side NULL that must not match a
  probe-side NULL.
- **Left outer** emits each unmatched left row once, with NULLs for right columns.

## Checks

- Inner & left joins vs DuckDB (G6), including duplicate keys and NULL keys.
- `HashJoin` and `BroadcastJoin` give the same result.
- A Hypothesis property joins random tables (with NULLs and duplicate keys) and
  compares against DuckDB.
