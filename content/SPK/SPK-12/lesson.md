# SPK-12 · Testing and production Spark

## Why this module exists

A Spark job that "ran" is not a Spark job that is "correct". The difference is tests. You already
live this in dbt: `not_null`, `unique`, `relationships`, `accepted_values`, and bespoke singular
tests run in CI on every PR, and a model doesn't ship until they pass. Spark pipelines deserve the
same discipline, and this module is about building it: writing tests that **catch the bugs that
matter**, laying out a project so it is testable and deployable, and configuring it for the runtime
you'll actually run on (EMR Serverless, Databricks, Glue).

The deepest idea here is the one this whole platform is built on (docs/TESTING.md): **a test is only
as good as the wrong code it rejects.** A test suite that passes your correct pipeline but also
passes a buggy one is worthless. So in this module *you* are graded the way our exercises are: your
tests must pass a known-good pipeline **and fail every planted buggy one.**

## Testing Spark transformations

- **Make transformations pure functions of DataFrames.** `def clean(orders: DataFrame) -> DataFrame`
  is testable; a function that reads a hard-coded path and writes another is not. Inject inputs,
  return outputs — the same reason dbt models are `ref()`-composed rather than side-effecting.
- **Assert data, not incidentals.** Use `pyspark.testing.assertDataFrameEqual` (Spark 3.5+/4.x),
  which compares rows with options for order and float tolerance. Assert row-level equality, schema
  where it matters, and invariants (`sum(debits) == sum(credits)`, no NULL keys, one open SCD2 row
  per key). Do **not** assert column order, which API was used, or exact floats without tolerance —
  those make tests brittle and reject correct refactors (the "too strict" failure mode).
- **Build small, deterministic inputs.** `spark.createDataFrame([...], schema)` with a handful of
  rows that cover the edge cases (NULLs, duplicates, boundary dates) beats a giant fixture. Fix
  seeds and partition counts so runs are reproducible.
- **Use an oracle for non-trivial logic.** Compute the expected answer a second, independent way
  (DuckDB, plain Python) and compare — two engines agreeing is strong evidence the expectation is
  right (the "wrong expected output" failure mode).
- **Assert plan properties for performance.** Parse `df.explain()`; assert "a `BroadcastHashJoin`
  is present", "no `BatchEvalPython`", "at most one `Exchange`". Never assert wall-clock time except
  as a generous timeout.

## What makes a test suite *strong* (the mutation mindset)

A good suite kills **mutants** — plausible wrong versions of the code. Before you trust your tests,
ask: *if someone changed `sum` to `avg`, dropped the dedup, used an inner join instead of a left,
or converted FX at the wrong date — would a test fail?* If not, you are missing a test. This is
exactly how our platform validates its own exercises (gate G4). In this module you'll write a suite
and we run it against planted bugs; the ones your suite misses are the tests you still owe.

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

Principles: pure transforms separated from I/O; config injected (so the same code runs locally and
on the cluster); dependencies pinned; the entry point is a thin `main` that wires read → transform →
write. This is the dbt discipline — models are logic, sources/targets are configured — applied to
PySpark.

## Configuration for the runtime (and the cost of getting it wrong)

The same code runs on different managed runtimes, each with its own knobs:

- **EMR Serverless** — you pick application sizing (driver/executor vCPU, memory) and pay per
  vCPU-second and GB-second while jobs run. Right-size and set `spark.dynamicAllocation` sensibly.
- **Databricks** — jobs clusters, Photon, autoscaling; Unity Catalog governs tables.
- **Glue** — DPUs, bookmarks for incrementality.

Key config that affects both correctness and cost: `spark.sql.shuffle.partitions` (too high = tiny
files + overhead; too low = spill/skew), AQE (`spark.sql.adaptive.enabled`, usually on in prod),
and memory fractions. The 💲 optional lab in this module runs SPK-07's partition-sizing job at 50 GB
on **EMR Serverless** so you can see real cost — it is **optional and ungraded**, must be
**tagged** `project=de-mastery`, and ships a **teardown script**. The exercise does *not* call
AWS for you; it gives you a cost estimate, the commands, and the teardown so you choose whether to
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

- **Don't assert incidental details** (column order, specific API). You'll block legitimate
  refactors — the "too strict" trap.
- **Don't test Spark itself.** Trust that `groupBy` works; test *your* logic.
- **Don't run huge data in unit tests.** Keep them seconds-fast with tiny inputs; save scale runs
  for a separate, optional, cost-flagged job (the 💲 EMR run here).
- **Don't hard-code paths/credentials** in code "to make the test pass" — inject config instead.

## Further reading

Pinned to Spark **4.0.0** (official docs only):

- Testing PySpark (`assertDataFrameEqual`):
  <https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/testing_pyspark.html>
- Configuration reference: <https://spark.apache.org/docs/4.0.0/configuration.html>
- Submitting applications: <https://spark.apache.org/docs/4.0.0/submitting-applications.html>
- Tuning guide: <https://spark.apache.org/docs/4.0.0/tuning.html>
- EMR Serverless (AWS docs):
  <https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/what-is-emr-serverless.html>
