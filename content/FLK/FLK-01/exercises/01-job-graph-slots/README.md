# Shape a job graph and its slot demand

A streaming job's resource footprint is decided by its **job graph** and
**parallelism**, not by guessing executor counts. In this exercise you build a
small keyed pipeline and report what it will cost in task slots.

## Task

Implement `de.flk.JobShape.plan(int parallelism)`:

1. Build a `StreamExecutionEnvironment` and set the **job-wide parallelism** to
   `parallelism`.
2. Wire this pipeline over `JobShape.sampleData()` (already provided):
   `fromData → filter(amount ≥ 0) → keyBy(key) → sum(amount) → sink`.
   Use any sink (e.g. `DiscardingSink`); the data is not asserted, the *shape* is.
3. Return a `PlanReport` with:
   - `plan` = `env.getExecutionPlan()` (the real JobGraph JSON),
   - `jobParallelism` = `env.getParallelism()`,
   - `slotsNeeded` = the number of task slots the job needs under **default slot
     sharing**.

## What the tests check

- `jobParallelism` equals the requested `parallelism`.
- `slotsNeeded` equals the **maximum** operator parallelism (= the job
  parallelism here), **not** the sum of every operator's parallelism. Slot
  sharing lets one slot hold one slice of the whole pipeline.
- The plan contains a **HASH** exchange — proof that the `keyBy` broke the
  operator chain between the filter region and the keyed aggregation.
- The keyed-aggregation vertex runs at the requested parallelism.

## Hints

- `env.getExecutionPlan()` returns the JobGraph as JSON with a `parallelism`
  field per node and a `ship_strategy` (`FORWARD`/`REBALANCE`/`HASH`) per edge.
- A `keyBy` is what produces the `HASH` edge.
- Under default slot sharing, slot demand is the max parallelism, not the sum —
  this is the whole point of the exercise.
