# SPK-02 · Lazy evaluation and the DAG

## Why laziness exists

When you run `dbt run`, dbt does not execute your models the moment you write them. It parses
every `ref()`, builds a dependency graph of the whole project, and only then decides the order to
materialize things. The graph is the unit of planning; the SQL is just the leaves. Spark works
the same way at the level of a single job — and for the same reason: **if the engine sees the
whole computation before running any of it, it can optimize across the whole thing.**

A Spark **transformation** (`select`, `filter`, `withColumn`, `join`, `groupBy`) does *not* run.
It records a step in a logical plan and returns a new DataFrame. Nothing touches the data. Only an
**action** (`count`, `collect`, `show`, `write`, `take`) forces Spark to compile that plan,
optimize it with Catalyst, split it into stages and tasks, and actually run it. This is **lazy
evaluation**, and it is the single most important mental model for predicting what Spark will do.

The payoff is real. Because Spark sees `filter(...).select(two columns).count()` as one plan
before running it, it can push the filter down to the scan and read only two columns — exactly
the predicate pushdown and column pruning you rely on Redshift's planner to do. If each
transformation ran eagerly, that cross-step optimization would be impossible.

## Transformations vs. actions

The question "is this a transformation or an action?" has one reliable test: **does it return a
DataFrame, or does it return data / a value / nothing?**

- Returns a DataFrame → **transformation** → lazy. `select`, `where`/`filter`, `withColumn`,
  `drop`, `join`, `groupBy().agg()`, `orderBy`, `distinct`, `union`, `repartition`.
- Returns rows, a number, or writes → **action** → triggers a job. `count`, `collect`, `take`,
  `first`, `show`, `foreach`, `write.save(...)`, `toPandas`.

`cache()`/`persist()` are a special case: they are lazy *markers* — the data is only cached when
the next action materializes it.

## Narrow vs. wide dependencies

Not all transformations are equal. The dividing line is whether a task can compute its output
from a single input partition, or whether it must pull data from *all* input partitions.

- **Narrow** dependency: each output partition depends on **one** input partition. `select`,
  `filter`, `withColumn`, `map`-like work. No data moves between machines. Narrow transformations
  pipeline together into one stage — they cost almost nothing to chain.
- **Wide** dependency: each output partition depends on **many** input partitions, so Spark must
  **shuffle** — write intermediate data to disk, repartition it by key across the network, and
  read it back. `groupBy`, `join` (non-broadcast), `distinct`, `repartition`, `orderBy`. A shuffle
  is the expensive operation in Spark; it is where most of your tuning effort goes (SPK-06/07).

In Redshift terms: a narrow transformation is work a slice does on its own local data; a wide
transformation is a redistribution — data moving across slices because the join/aggregation key
doesn't match the current distribution. `DISTKEY` exists precisely to avoid that redistribution,
the same way a good partitioning (or a broadcast join) avoids a shuffle in Spark.

## Jobs → stages → tasks

The execution hierarchy (objective 1.4) is strict and worth memorizing:

- **Job**: one action triggers one job (sometimes more for multi-output writes).
- **Stage**: a job is split into stages at every **shuffle boundary**. All the narrow
  transformations between two shuffles pipeline into a single stage. So **the number of stages in
  a linear job is (number of shuffles) + 1.**
- **Task**: within a stage, one task runs per partition (SPK-01). Tasks are the units the
  scheduler sends to executors.

This gives you a predictive rule you will use for the rest of the track:

> **Count the wide transformations (shuffles) on the path to the action. Stages = shuffles + 1.**

A `filter().select().count()` has zero shuffles → 1 stage. Add a `groupBy().count()` → one shuffle
→ 2 stages. Add an `orderBy` on top → a second shuffle → 3 stages. A join adds a shuffle on *each*
side being shuffled (unless one side is broadcast, which adds none — SPK-06). The exercises in
this module make you predict these numbers and then check them against the plan Spark actually
built, so your intuition is calibrated against ground truth rather than a hand-written key.

The DAG itself is visible: `df.explain("formatted")` prints the physical plan, where each
`Exchange` node is a shuffle (a stage boundary). The Spark UI's DAG visualization draws the same
thing graphically. `Exchange` count + 1 = stage count for a linear plan.

## From your world

| dbt / Redshift | Spark | What transfers |
|---|---|---|
| `dbt` builds a model DAG from `ref()` before running | Spark builds a job DAG from transformations before any action | Plan the whole thing, then run |
| `dbt run` executes the graph | an **action** executes the job | Nothing happens until you "run" |
| Redshift redistributes rows when join/group key ≠ DISTKEY | a **shuffle** at a wide transformation | The expensive data-movement step |
| `EXPLAIN` in Redshift shows `DS_DIST_*` redistributions | `explain("formatted")` shows `Exchange` nodes | Read the plan to find data movement |
| A model is "lazy" until `dbt run` | a DataFrame is lazy until an action | Laziness enables cross-step optimization |

The sharpest transfer: a Redshift `DS_BCAST_INNER` / `DS_DIST_BOTH` in an `EXPLAIN` is the exact
analogue of a Spark `Exchange`. Learning to spot shuffles in a Spark plan is the same skill as
spotting redistributions in a Redshift plan — and both tell you where the cost is.

## When NOT to lean on laziness

- **Debugging intermediate results.** Laziness means an error in step 1 may only surface at the
  action in step 10, with a stack trace that points at the action, not the mistake. When
  debugging, materialize early (`.cache()` + `.count()`, or `.show()`), accepting the cost.
- **Reusing a DataFrame many times without caching.** Because transformations are lazy and *not*
  memoized, calling two actions on the same DataFrame recomputes the whole lineage twice. If you
  will act on a DataFrame repeatedly, `cache()` it (SPK-08) — otherwise laziness works against you.

## Further reading

Pinned to Spark **4.2.0**:

- RDD programming guide — transformations, actions, lazy evaluation, shuffle:
  <https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html#transformations>
- Shuffle operations (why wide deps cost):
  <https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html#shuffle-operations>
- SQL performance tuning (plans, Exchange):
  <https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html>
- Web UI (jobs/stages/tasks): <https://spark.apache.org/docs/4.2.0/web-ui.html>
