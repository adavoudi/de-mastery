# SPK-12 · Interview drills

**1. "How do you know your test suite is good, not just green?"**
> I look at the mutants that it kills. I plant plausible bugs, in my head or in real code: `sum`→`avg`,
> drop the dedup, inner instead of left join, FX at the wrong date. Then I confirm that a test fails
> for each bug. Suppose a suite passes the correct code and also passes those bugs. Then it is too
> weak. This is the same idea as dbt singular tests that encode the specific failures that you fear.

**2. "What do you assert, and what do you refuse to assert, in a transform test?"**
> I assert row-level equality to an independent oracle (order-insensitive, float tolerance). I assert
> the schema where it is part of the contract. I assert invariants (`sum(debits)==sum(credits)`, no
> NULL keys). For performance, I assert plan properties (`no BatchEvalPython`, `BroadcastHashJoin
> present`). I refuse to assert column order, which API was used, or exact floats without tolerance.
> These assertions reject valid refactors.

**3. "Why pure transforms and a thin main?"**
> Pure `DataFrame -> DataFrame` functions take inputs and return outputs. So I can unit-test them
> with tiny in-memory DataFrames and no I/O. The thin `main` only connects read → transform → write.
> It needs only a lighter integration test. I inject the config, so that the same code runs locally and
> on the cluster.

**4. "assertDataFrameEqual vs comparing collect() lists?"**
> `assertDataFrameEqual` is order-insensitive (with `checkRowOrder=False`), has float tolerance, and
> prints a useful diff. Comparing `collect()` lists is order-sensitive and fragile with floats. So it
> fails correct code for incidental reasons. This is a classic too-strict test.

**5. "You must benchmark a 50 GB job on EMR Serverless once in a shared account. How do you keep it
cheap and clean?"**
> I do these things:
> - Right-size the application (driver/executor vCPU + memory, sensible dynamic allocation).
> - Tag every resource `project=de-mastery` for cost attribution.
> - Inject config instead of hard-coding paths/secrets.
> - Estimate the cost up front.
> - Ship a teardown script that deletes the application, so that it stops accruing charges.
>
> I run the job only when the estimate is acceptable.

**6. "What config most affects correctness and cost together?"**
> `spark.sql.shuffle.partitions` (file sizes and shuffle overhead), AQE (`spark.sql.adaptive.*` for
> coalescing/skew), and memory fractions (spill vs OOM). If shuffle partitions are wrong, you get
> either many tiny files or heavy spill. Both are slow and expensive.
