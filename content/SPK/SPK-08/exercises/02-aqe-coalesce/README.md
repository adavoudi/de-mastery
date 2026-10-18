# SPK-08 / 02 · Make AQE coalescing visible in the plan

Adaptive Query Execution re-optimizes a query at runtime using real shuffle statistics. Its most
common win is **coalescing** many tiny post-shuffle partitions into fewer right-sized ones — turning
the static `spark.sql.shuffle.partitions` count into an adaptive one. You will prove it by producing
an aggregation whose **final** executed plan shows the AQE coalescing.

## Task

Implement `coalesced_aggregate(spark, order_items)` that:

1. enables AQE and shuffle-partition coalescing for this query,
2. sets a high initial `spark.sql.shuffle.partitions` (so there are small partitions to coalesce),
   and `spark.sql.adaptive.coalescePartitions.minPartitionNum = 1` (so coalescing can reduce to few
   at this tiny scale),
3. returns the per-company sum of `line_amount` as columns `company_id`, `total`.

Restoring the session configs afterward is good hygiene but not required.

## How it is graded

- **Data (DuckDB oracle):** equals `SELECT company_id, sum(line_amount) FROM order_items GROUP BY
  company_id` (float tolerance).
- **Plan — AQE on:** after an action, the final plan contains `AdaptiveSparkPlan` with
  `isFinalPlan=true` and a **coalesced** `AQEShuffleRead` — proof AQE coalesced the shuffle
  partitions. A solution that leaves AQE off has no such node.
