# SPK-12 · Interview drills

**1. "How do you know your test suite is good, not just green?"**
> By the mutants it kills. I mentally (or literally) plant plausible bugs — `sum`→`avg`, drop the
> dedup, inner instead of left join, FX at the wrong date — and confirm a test fails for each. A
> suite that passes the correct code but also passes those is too weak. This is the same idea as
> dbt singular tests encoding the specific failures you fear.

**2. "What do you assert, and what do you refuse to assert, in a transform test?"**
> Assert row-level equality to an independent oracle (order-insensitive, float tolerance), schema
> where it's part of the contract, and invariants (`sum(debits)==sum(credits)`, no NULL keys). For
> performance, assert plan properties (`no BatchEvalPython`, `BroadcastHashJoin present`). I refuse
> to assert column order, which API was used, or exact floats without tolerance — those reject
> valid refactors.

**3. "Why pure transforms and a thin main?"**
> Pure `DataFrame -> DataFrame` functions take inputs and return outputs, so I can unit-test them
> with tiny in-memory DataFrames and no I/O. The thin `main` only wires read → transform → write and
> gets a lighter integration test. Config is injected so the same code runs locally and on the
> cluster.

**4. "assertDataFrameEqual vs comparing collect() lists?"**
> `assertDataFrameEqual` is order-insensitive (with `checkRowOrder=False`), has float tolerance, and
> prints a useful diff. Comparing `collect()` lists is order-sensitive and float-fragile, so it
> fails correct code for incidental reasons — a classic too-strict test.

**5. "You must benchmark a 50 GB job on EMR Serverless once in a shared account. How do you keep it
cheap and clean?"**
> Right-size the application (driver/executor vCPU + memory, sensible dynamic allocation), tag every
> resource `project=de-mastery` for cost attribution, inject config instead of hard-coding
> paths/secrets, estimate the cost up front, and ship a teardown script that deletes the application
> so it stops accruing charges. Run it only when the estimate is acceptable.

**6. "What config most affects correctness and cost together?"**
> `spark.sql.shuffle.partitions` (file sizes and shuffle overhead), AQE (`spark.sql.adaptive.*` for
> coalescing/skew), and memory fractions (spill vs OOM). Getting shuffle partitions wrong produces
> either a small-file storm or heavy spill — both slow and expensive.
