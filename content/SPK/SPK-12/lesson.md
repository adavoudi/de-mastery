# SPK-12 · Testing and production Spark

## Why this module exists

A Spark job that "ran" is not always a Spark job that is "correct". Tests make the difference. You
already do this in dbt. These tests run in CI on every PR: `not_null`, `unique`, `relationships`,
`accepted_values`, and bespoke singular tests. A model does not ship until they pass. Spark pipelines
need the same discipline. This module teaches you how to build it:

- Write tests that **catch the bugs that matter**.
- Lay out a project so that you can test and deploy it.
- Configure it for the runtime that you will really use (EMR Serverless, Databricks, Glue).

The most important idea here is the idea that this whole platform uses (docs/TESTING.md): **a test is
only as good as the wrong code that it rejects.** Suppose a test suite passes your correct pipeline and
also passes a buggy pipeline. That suite has no value. So in this module, the platform grades *you* in the
same way that it grades our exercises. Your tests must pass a known-good pipeline **and fail every
planted buggy pipeline.**

## Testing Spark transformations

- **Make transformations pure functions of DataFrames.** You can test `def clean(orders: DataFrame) ->
  DataFrame`. You cannot easily test a function that reads a hard-coded path and writes to another
  path. Pass inputs in and return outputs. This is the same reason why dbt models are composed with
  `ref()` and do not have side effects.
- **Assert data, not incidental details.** Use `pyspark.testing.assertDataFrameEqual` (Spark
  3.5+/4.x). It compares rows, and it has options for row order and float tolerance. Assert
  row-level equality. Assert the schema where it matters. Assert invariants (`sum(debits) ==
  sum(credits)`, no NULL keys, one open SCD2 row per key). Do **not** assert column order, which API
  you used, or exact floats without tolerance. These assertions make tests brittle and reject correct
  refactors (the "too strict" failure mode).
- **Build small, deterministic inputs.** Use `spark.createDataFrame([...], schema)` with a few
  rows that cover the edge cases (NULLs, duplicates, boundary dates). This is better than a giant
  fixture. Fix the seeds and the partition counts, so that runs are reproducible.
- **Use an oracle for logic that is not trivial.** Compute the expected answer in a second,
  independent way (DuckDB, plain Python) and compare. If two engines agree, this is strong evidence
  that the expectation is right (the "wrong expected output" failure mode).
- **Assert plan properties for performance.** Parse `df.explain()`. Assert "a `BroadcastHashJoin`
  is present", "no `BatchEvalPython`", "at most one `Exchange`". Never assert wall-clock time, except
  as a generous timeout.

## What makes a test suite *strong* (the mutation mindset)

A good suite kills **mutants**. A mutant is a plausible wrong version of the code. Before you trust your
tests, ask this question: *if someone changed `sum` to `avg`, removed the dedup, used an inner join
instead of a left join, or converted FX at the wrong date, would a test fail?* If not, you are missing
a test. Our platform validates its own exercises in exactly this way (gate G4). In this module, you
write a suite, and we run it against planted bugs. The bugs that your suite misses show the tests that
you still need to write.

## Project layout and packaging

A production Spark project is a normal Python package, not a notebook:

```
my_pipeline/
  src/my_pipeline/
    __init__.py
    transforms.py      # pure DataFrame -> DataFrame functions (unit-tested)
    jobs.py            # thin main(): read -> transform -> write (integration-tested)
    config.py          # typed config, env-overridable; NO hard-coded paths/secrets
  tests/
    test_transforms.py # fast, local SparkSession, tiny data
  pyproject.toml       # pinned deps; entry point for the job
```

These are the principles:

- Keep pure transforms separate from I/O.
- Inject the config, so that the same code runs locally and on the cluster.
- Pin the dependencies.
- Make the entry point a thin `main` that connects read → transform → write.

This is the dbt discipline applied to PySpark. Models contain the logic. Sources and targets are
configured.

## Configuration for the runtime (and the cost of getting it wrong)

The same code runs on different managed runtimes. Each runtime has its own settings:

- **EMR Serverless** — you choose the application size (driver/executor vCPU, memory). You pay per
  vCPU-hour and GB-hour while jobs run, billed per second. Right-size the application and set
  `spark.dynamicAllocation` sensibly.
- **Databricks** — jobs clusters, Photon, autoscaling. Unity Catalog governs tables.
- **Glue** — DPUs, and bookmarks for incremental processing.

These settings affect both correctness and cost:

- `spark.sql.shuffle.partitions` (too high = tiny files + overhead; too low = spill/skew)
- AQE (`spark.sql.adaptive.enabled`, usually on in prod)
- memory fractions

The 💲 optional lab in this module runs the partition-sizing job of SPK-07 at 50 GB
on **EMR Serverless**, so that you can see real cost. The lab is **optional and ungraded**. You must
**tag** it `project=de-mastery`. It includes a **teardown script**. The exercise does *not* call
AWS for you. It gives you a cost estimate, the commands, and the teardown. You choose whether to
spend the money.

## From your world

| dbt / CI | Spark | Note |
|---|---|---|
| `not_null`/`unique`/`relationships` tests | `assertDataFrameEqual` + invariant assertions | data correctness in CI |
| Singular/bespoke tests | oracle comparison + plan assertions | catch logic + perf regressions |
| Models are `ref`-composed, not side-effecting | pure `DataFrame -> DataFrame` transforms | testable units |
| `profiles.yml` targets (dev/prod) | injected `config.py` + runtime knobs | same code, many runtimes |
| dbt in CI gates the PR | pytest suite gates the PR | tests are the contract |

## When NOT to over-invest

- **Don't assert incidental details** (column order, specific API). You will block legitimate
  refactors. This is the "too strict" trap.
- **Don't test Spark itself.** Trust that `groupBy` works. Test *your* logic.
- **Don't run huge data in unit tests.** Keep the tests fast (seconds) with tiny inputs. Keep scale runs
  for a separate, optional job that is flagged for cost (the 💲 EMR run here).
- **Don't hard-code paths or credentials** in code "to make the test pass". Inject config instead.

## Further reading

Pinned to Spark **4.0.0** (official docs only):

- Testing PySpark (`assertDataFrameEqual`):
  <https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/testing_pyspark.html>
- Configuration reference: <https://spark.apache.org/docs/4.0.0/configuration.html>
- Submitting applications: <https://spark.apache.org/docs/4.0.0/submitting-applications.html>
- Tuning guide: <https://spark.apache.org/docs/4.0.0/tuning.html>
- EMR Serverless (AWS docs):
  <https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/what-is-emr-serverless.html>
