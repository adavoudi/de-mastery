# SPK-08 / 02 · Make AQE coalescing visible in the plan

Adaptive Query Execution re-optimizes a query at runtime using real shuffle statistics. Its most
common win is **coalescing**. It collapses many tiny post-shuffle partitions into fewer partitions of
the right size. This turns the static `spark.sql.shuffle.partitions` count into an adaptive count. You
will prove it. You will produce an aggregation whose **final** executed plan shows the AQE coalescing.

## Task

Implement `coalesced_aggregate(spark, order_items)`. It must do these three things:

1. Enable AQE and shuffle-partition coalescing for this query.
2. Set a high initial `spark.sql.shuffle.partitions` (so there are small partitions to coalesce).
   Also set `spark.sql.adaptive.coalescePartitions.minPartitionNum = 1` (so coalescing can reduce the
   count to a few partitions at this tiny scale).
3. Return the per-company sum of `line_amount` as columns `company_id`, `total`.

It is good practice to restore the session configs afterward, but it is not required.

## How it is graded

- **Data (DuckDB oracle):** equals `SELECT company_id, sum(line_amount) FROM order_items GROUP BY
  company_id` (float tolerance).
- **Plan — AQE on:** after an action, the final plan contains `AdaptiveSparkPlan` with
  `isFinalPlan=true` and a **coalesced** `AQEShuffleRead`. This proves that AQE coalesced the shuffle
  partitions. A solution that leaves AQE off has no such node.
