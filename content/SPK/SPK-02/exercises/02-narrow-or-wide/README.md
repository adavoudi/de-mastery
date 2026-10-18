# Classify transformations as narrow or wide

A transformation is **narrow** if each output partition can be computed from a single input
partition (no data moves between machines), and **wide** if an output partition needs rows from
many input partitions — which forces a **shuffle** (an `Exchange` in the plan).

Implement one function in `solution.py`:

### `is_wide(op) -> bool`
Return `True` if the named transformation is wide (shuffles), `False` if it is narrow. The test
will ask about exactly these operation names:

| op | meaning |
|---|---|
| `filter` | `df.filter(...)` |
| `select` | `df.select(...)` |
| `withColumn` | `df.withColumn(...)` |
| `drop` | `df.drop(...)` |
| `union` | `df.union(other)` |
| `coalesce` | `df.coalesce(1)` (reduce partitions only) |
| `groupBy_agg` | `df.groupBy(...).count()` |
| `distinct` | `df.distinct()` |
| `orderBy` | `df.orderBy(...)` (global sort) |
| `repartition` | `df.repartition(n)` |

The test confirms each answer empirically: it applies the real operation to a DataFrame and checks
whether an `Exchange` (shuffle) appears in the executed plan. Your classification must match.

Watch the two that trip people up: **`coalesce` is narrow** (it merges partitions locally without
a shuffle), while **`repartition` is wide** (it shuffles to rebalance).
