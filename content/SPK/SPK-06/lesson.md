# SPK-06 · Joins and aggregations at scale

## Why join strategy is the whole game

In Redshift, you learned that data movement is most of the cost of a join. Suppose both tables
are distributed on the join key. Then the join is local. Suppose one table must be redistributed
across the cluster (or broadcast with `DISTSTYLE ALL`). Then that network shuffle is the
expensive part. Spark works in the same way. The *logic* of a join (which rows match) is simple.
The *cost* is the shuffle. So the most valuable skill here is to choose the right **physical
join strategy**. You can also let Spark choose it. In both cases, prove the choice from the plan.

## The three strategies

- **Broadcast hash join (`BroadcastHashJoin`).** Spark collects the small side to the driver,
  broadcasts it to every executor, and hashes it in memory. The large side streams through with
  **no shuffle**. This is the fastest join when one side is small. It is the direct analogue of
  Redshift `DISTSTYLE ALL` on a small dimension. Spark broadcasts a side automatically when its
  estimated size is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). It also
  broadcasts a side when you wrap it in `F.broadcast(df)`.
- **Sort-merge join (`SortMergeJoin`).** Spark shuffles both sides on the join key, sorts them,
  and merges them. This is the default for two large tables. It costs **two shuffles** (one for
  each side) plus sorts. But it scales to any size and it is reliable.
- **Shuffle hash join (`ShuffledHashJoin`).** Spark shuffles both sides on the key. It builds
  one side into a hash table for each partition. Spark uses it when one side has a medium size
  and building a hash is cheaper than sorting. Spark picks it under specific size and
  configuration conditions.

![Where data moves in broadcast, shuffle hash and sort-merge joins](diagrams/join-strategies.svg)

This rule of thumb matches your warehouse intuition: **broadcast the small dimension, and
sort-merge two big facts.** Do not broadcast the *large* side. This is a classic, expensive
mistake. Spark tries to pull a huge table to the driver and send it everywhere.

## Broadcast variables and accumulators

Spark has two low-level primitives for shared state. They are separate from the join operator:

- **Broadcast variable** (`spark.sparkContext.broadcast(value)`): it sends a read-only Python
  object (a lookup dict, a model) to every executor once. Without it, Spark would serialize the
  object into every task. The broadcast *join* uses this mechanism internally. You can also use
  a broadcast variable directly inside a UDF/mapPartitions.
- **Accumulator** (`spark.sparkContext.accumulator(0)`): a counter for metrics (for example, a
  count of the bad rows that Spark saw). Executors can only write to it. The driver reads it.
  Accumulator values are reliable only in **actions**. They are not reliable in transformations
  that may run again. So treat them as approximate monitoring, not as business logic.

## Skew: when one key dominates

Suppose one join key value owns a large share of the rows (one giant customer, a NULL key, a
default company). The partition that holds this key becomes a straggler. Every other task
finishes, and one task runs for a very long time. This is **data skew**. These are the classic
fixes:

- **Broadcast the other side** if it is small. There is no shuffle, so there is no skewed
  partition.
- **Salting:** add a random suffix to the hot key on both sides. Then the rows of the key spread
  across many partitions. After that, aggregate back. Salting must **not change the result**. A
  salt that drops or duplicates rows is a bug.

![One hot key makes a straggler partition; salting splits it into even ones](diagrams/skew-salting.svg)
- **AQE skew-join handling (SPK-08):** `spark.sql.adaptive.skewJoin.enabled` splits oversized
  shuffle partitions at runtime. You will see this in the plan in SPK-08.

Here is a multi-company gotcha from your world. `order_id` is unique for each company, but it
can repeat across companies. A surrogate id can also collide after an acquisition. Always join
dimensions on the full **business key**. If you do not, you double-count. This is the Spark
version of an intercompany double-count.

## From your world

| Redshift / dbt | Spark | Note |
|---|---|---|
| `DISTSTYLE ALL` on a small dim | `F.broadcast(dim)` / auto-broadcast | ship small side everywhere |
| Both tables on the dist key → local join | `SortMergeJoin` after shuffle on key | two shuffles |
| Skewed dist key straggler | skewed shuffle partition | salt or broadcast or AQE |
| Intercompany double-count | join on partial key | join on full business key |
| `COUNT`/`SUM` with `GROUP BY` | `groupBy().agg()` | one shuffle |

## When NOT to broadcast / not to salt

- **Do not broadcast a large table.** Spark collects it on the driver and the driver can run out
  of memory (OOM). It also wastes network. Check the estimated size. Do not rely on your
  intuition about the row count.
- **Do not salt a join that is not skewed.** Salting adds a shuffle-widening step and
  complexity. If the key distribution is even, you get no gain. Measure first.
- **Do not lower `autoBroadcastJoinThreshold` to force sort-merge** as a habit. Prefer to let
  AQE and the cost model decide, unless you have a specific reason.
- **Do not rely on accumulators for correctness.** They can over-count when tasks retry. Use
  them for monitoring only.

## Further reading

Pinned to Spark **4.0.0**:

- SQL performance tuning — join hints, broadcast, AQE skew:
  <https://spark.apache.org/docs/4.0.0/sql-performance-tuning.html>
- `pyspark.sql.functions.broadcast`:
  <https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html>
- RDD programming guide — broadcast variables and accumulators:
  <https://spark.apache.org/docs/4.0.0/rdd-programming-guide.html#shared-variables>
- Join hints in SQL:
  <https://spark.apache.org/docs/4.0.0/sql-ref-syntax-qry-select-hints.html>
