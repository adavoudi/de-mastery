# Stage 8 — Spill to disk under a memory budget (optional)

The capstone stage. A real aggregate can't assume all groups fit in memory. Bound
the in-memory group count and **spill** partial state to disk when you exceed it,
then merge everything for the final answer. This mirrors Spark's spill under
memory pressure — and the key invariant is that spilling **never changes the
result**.

## Task

```python
aggregate_with_spill(table, keys, aggs, *, max_groups_in_memory, spill_dir=None)
    -> pyarrow.Table
```

- Fold rows into a hash table as in stage 4. When the table holds
  `max_groups_in_memory` groups and a **new** key arrives, flush (spill) the
  current partial state to a file and start a fresh, empty table.
- At the end, reload every spilled partial and `merge_partials()` them with the
  in-memory state (reuse stage 4), then finalize.
- Clean up the spill files you created.

## The bugs this stage teaches

- **Forgetting to merge the spill** → spilled groups vanish.
- **Not clearing memory after a spill** → groups counted twice.
- **Reusing one spill filename** → earlier spills overwritten.

## Checks

- Under a tight budget on many groups, the result equals the in-memory aggregate
  and DuckDB (G6).
- A roomy budget (no spill) and a tight budget (many spills) give identical
  results.
