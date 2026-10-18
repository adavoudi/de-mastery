# Shape a job graph and its slot demand

The **job graph** and the **parallelism** decide the resource footprint of a streaming job. You do
not decide it by guessing the number of executors. In this exercise, you build a small keyed
pipeline and report how many task slots it needs.

## Task

Implement `de.flk.JobShape.plan(int parallelism)`:

1. Build a `StreamExecutionEnvironment` and set the **job-wide parallelism** to
   `parallelism`.
2. Wire this pipeline over `JobShape.sampleData()` (already provided):
   `fromData → filter(amount ≥ 0) → keyBy(key) → sum(amount) → sink`.
   Use any sink (for example `DiscardingSink`). The tests do not assert the data. They assert the
   *shape*.
3. Return a `PlanReport` with:
   - `plan` = `env.getExecutionPlan()` (the real JobGraph JSON),
   - `jobParallelism` = `env.getParallelism()`,
   - `slotsNeeded` = the number of task slots the job needs under **default slot
     sharing**.

## What the tests check

- `jobParallelism` equals the requested `parallelism`.
- `slotsNeeded` equals the **maximum** operator parallelism (= the job
  parallelism here). It does **not** equal the sum of the parallelism of every operator. Slot
  sharing lets one slot hold one slice of the whole pipeline.
- The plan contains a **HASH** exchange. This proves that the `keyBy` broke the
  operator chain between the filter region and the keyed aggregation.
- The keyed-aggregation vertex runs at the requested parallelism.

## Hints

- `env.getExecutionPlan()` returns the JobGraph as JSON. Each node has a `parallelism`
  field. Each edge has a `ship_strategy` (`FORWARD`/`REBALANCE`/`HASH`).
- A `keyBy` produces the `HASH` edge.
- Under default slot sharing, slot demand is the max parallelism, not the sum.
  This is the main point of the exercise.
