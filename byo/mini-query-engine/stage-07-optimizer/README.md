# Stage 7 — Rule-based optimizer (filter pushdown, projection pruning)

This is your mini **Catalyst**. `optimize(plan)` takes a plan tree and returns a
rewritten, equivalent tree that does less work. The tests inspect the optimized
**tree shape** (not just the results), exactly like reading Spark's optimized
logical plan.

## Task

```python
optimize(plan) -> plan      # returns a rewritten tree; does NOT execute
columns_in(expr) -> set[str]  # columns referenced by a predicate (helper)
```

Implement two rules:

1. **Filter pushdown.** A `Filter` above a `Project` moves **below** it when the
   predicate's columns all survive the projection (so the top of the optimized
   tree becomes `Project(Filter(...))`). A `Filter` above a `Scan` records its
   predicate as `Scan.pushdown` (keep the `Filter` too — pushdown only prunes row
   groups). Never push a filter to the **wrong side of a join**.
2. **Projection pruning.** Thread a "needed columns" set down the tree so a `Scan`
   reads only the columns actually used upstream. A `Filter` must add its own
   predicate columns to that set; an `Aggregate` needs its keys + agg inputs.
   Never prune a column an upstream operator still needs.

## Checks

- After `optimize`, the tree shape reflects the rewrites (e.g. `Project` now sits
  above `Filter`; the `Scan` under a `Project` reads only the projected columns).
- A filter whose column is projected away at the top is still fed by the scan
  (pruning must keep columns the filter needs).
- The optimized plan computes the **same** result as the original and DuckDB
  (G6).
