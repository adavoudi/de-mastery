# Classify transformations as narrow or wide

A transformation is **narrow** if Spark can compute each output partition from a single input
partition (no data moves between machines). A transformation is **wide** if an output partition
needs rows from many input partitions. A wide transformation forces a **shuffle** (an
`Exchange` in the plan).

Implement one function in `solution.py`:

### `is_wide(op) -> bool`
Return `True` if the named transformation is wide (shuffles). Return `False` if it is narrow.
The test asks about exactly these operation names:

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

The test confirms each answer with evidence. It applies the real operation to a DataFrame. Then
it checks whether an `Exchange` (shuffle) appears in the executed plan. Your classification must
match.

Two operations confuse many people. **`coalesce` is narrow**: it merges partitions locally
without a shuffle. **`repartition` is wide**: it shuffles to rebalance.
