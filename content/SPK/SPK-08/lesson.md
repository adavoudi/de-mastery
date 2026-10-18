# SPK-08 · Memory, caching, and Adaptive Query Execution

## Why memory is where Spark jobs die

Most Spark failures that reach you as a human are memory failures. These are the usual types:

- An executor runs out of memory (OOM).
- A stage spills to disk and runs very slowly.
- The driver dies because it collects too much data.

You may have tuned Redshift WLM memory slots and watched queries spill. This is the same world. Memory is
bounded. Work must fit in memory or spill to disk. A planner (now AQE) adapts to this. This lesson
teaches three skills:

1. Read the memory model well enough to diagnose an OOM from a log.
2. Cache data on purpose.
3. Know what AQE changes in the plan.

## The unified memory model

Spark splits the heap of each executor into regions:

- **Execution memory** — for shuffles, joins, sorts, and aggregations (transient).
- **Storage memory** — for cached/persisted DataFrame blocks.
- These two share a **unified region** and borrow from each other. Spark can evict cached blocks to
  give a shuffle room, and the reverse is also true (within limits).
- **User memory** — your data structures and UDF state.
- **Reserved memory** — a fixed slice that Spark keeps for itself.

![Execution and storage share the unified region and borrow across a movable boundary](diagrams/unified-memory.svg)

When execution memory is full, Spark **spills** sorted/aggregated data to disk. This is correct but
slow. Sometimes spilling is not enough, or a single record/partition is too big. Then you get an **OOM**.
These are the common causes. Each one has a signature:

- **Skew** (one giant partition) → one task OOMs or spills a lot, while the other tasks are fine.
- **A huge `collect()` / `toPandas()`** → the **driver** OOMs (not an executor).
- **A wide aggregation or explode** that makes the row count very large → execution memory is full.
- **Too few shuffle partitions** → each partition is too big to fit → spill or OOM.
- **A cartesian/exploding join** → the intermediate result has no bound.

## Diagnosing from logs

When an alert wakes you at 2 a.m., you will not have the UI. You have a log line. Learn these signatures:

- `java.lang.OutOfMemoryError: Java heap space` **on the driver** + a `collect`/`toPandas` in the
  stack → driver-side OOM. Fix it: do not collect the whole dataset.
- `OutOfMemoryError` on an **executor** during a shuffle/sort, with one task much larger → skew. Fix it
  with salting (SPK-06), more partitions, or AQE skew handling.
- `Spilling in-memory map ... to disk` many times → execution-memory pressure. Raise the number of
  partitions or the memory, or reduce the aggregation width.
- `Container killed by YARN for exceeding memory limits` / `OOMKilled` → off-heap/overhead. Raise the
  memory overhead or reduce the footprint of each task.

The exercise turns this into a task that a machine can grade. You get a log excerpt. You return a
structured diagnosis (where the OOM happened and the single best fix).

## Caching and storage levels

`df.cache()` = `persist(MEMORY_AND_DISK)`. Other `StorageLevel`s trade memory for CPU/disk:

- `MEMORY_ONLY` (fast, may recompute if evicted)
- `MEMORY_AND_DISK` (spill cached blocks to disk instead of recomputing)
- `DISK_ONLY`
- `*_SER` (serialized: smaller, but it needs CPU to deserialize)

These rules are a good guide. They use the same instinct as materializing a CTE in your warehouse:

- Cache a DataFrame **only if you reuse it** across 2 or more actions, and it is **small enough** to help.
- Caching is lazy. Spark stores nothing until the first action materializes the DataFrame.
- Call `unpersist()` when you finish. Stale cached blocks waste storage memory and can force eviction/spill.
- Caching a wide intermediate can **defeat pushdown/pruning** (SPK-05). Read the plan.

## Adaptive Query Execution

AQE (`spark.sql.adaptive.enabled`) re-optimizes the plan **at runtime**. It uses actual shuffle
statistics. The static Catalyst plan cannot do this. AQE has three big wins. You can see each one in the
final plan:

- **Coalesce shuffle partitions** (`...adaptive.coalescePartitions.enabled`): it collapses many tiny
  post-shuffle partitions into fewer partitions of the right size. You see fewer partitions or an
  `AQEShuffleRead coalesced` node, instead of the static `shuffle.partitions` count.
- **Skew-join handling** (`...adaptive.skewJoin.enabled`): it splits an oversized shuffle partition at
  runtime. The final plan shows a skew-handled `AQEShuffleRead`.
- **Dynamic join switching**: if a side turns out small after filtering, AQE can switch a planned
  sort-merge join to a broadcast join at runtime.

AQE changes the plan *after* the first stage runs. So you must look at the **final** executed
plan to see its effects. Use `df.explain()` after an action, or look for an
`AdaptiveSparkPlan isFinalPlan=true` node.

## From your world

| Redshift / warehouse | Spark | Note |
|---|---|---|
| WLM memory slots, spill to disk | execution/storage unified memory + spill | bounded memory, spill when over |
| Materialize a reused CTE | `cache()`/`persist()` a reused DataFrame | only if reused |
| Query queues adapting | AQE runtime re-optimization | uses real stats |
| OOM / disk-based query diagnosis | read executor/driver logs for OOM signatures | driver vs executor matters |

## When NOT to cache / NOT to rely on AQE

- **Don't cache a DataFrame that you use once.** It costs storage memory and gives no benefit from
  reuse. It can also force spills elsewhere.
- **Don't cache blindly to "make it faster".** A wide cached intermediate can read more data than a
  pushed-down pipeline. Measure, and read the plan.
- **Don't assume AQE fixes everything.** It coalesces partitions and handles some skew. But a severe skew
  or a driver-side `collect` OOM still needs the real fix (salting, not collecting).
- **Don't tune `shuffle.partitions` by hand** when AQE coalescing can do it adaptively, unless you
  have a specific reason.

## Further reading

Pinned to Spark **4.0.0**:

- Adaptive Query Execution:
  <https://spark.apache.org/docs/4.0.0/sql-performance-tuning.html#adaptive-query-execution>
- Tuning / memory management guide:
  <https://spark.apache.org/docs/4.0.0/tuning.html>
- RDD persistence / storage levels:
  <https://spark.apache.org/docs/4.0.0/rdd-programming-guide.html#rdd-persistence>
- `DataFrame.persist` / `StorageLevel`:
  <https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html>
