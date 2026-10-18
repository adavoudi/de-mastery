# Stage 4 — Hash aggregate with partial vs. final aggregation

Group-by/aggregate is the heart of analytics. Build a **hash aggregate** and —
crucially — split it into a **partial** (map-side combine) step and a **final**
(reduce-side merge) step, the same split Spark uses to pre-aggregate before a
shuffle.

## Task

Implement:

```python
Aggregate(child, keys, aggs)   # aggs: tuple of (func, input_col, output_name)
                               # func in {sum, count, min, max, avg}; count uses input_col=None
partial_aggregate(table, keys, aggs) -> dict      # one chunk -> per-group state
merge_partials(states, aggs) -> dict              # combine many states -> one
```

`execute(Aggregate(...))` runs `partial_aggregate` then `merge_partials` then
finalizes to a `pyarrow.Table` (one row per group).

## The property that matters

`partial(A) merged with partial(B)` must equal `partial(A ∪ B)`. That means your
accumulators must **merge associatively**:

- `sum` → running sum; `count` → running count; `min`/`max` → running extreme.
- `avg` → carry `(sum, count)` and divide **only at finalize**. Never average
  two averages — that is wrong whenever chunk sizes differ, and a Hypothesis
  property that splits the data at a random point will catch it.

## Checks

- `sum`/`count`/`min`/`max`/`avg`, single- and multi-key, vs DuckDB (G6).
- `partial` + `merge` over a random split equals the whole-table aggregate.
- Empty input → 0 groups.
