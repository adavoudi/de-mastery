# SPK-09 · Explain it back (Feynman)

**Prompt A.** "A colleague wrote a Python UDF to convert the amount of every order to USD. The UDF
looks up an fx rate for each row. The job is slow. Explain to that colleague *why* it is slow. Explain
what the `BatchEvalPython` node in the plan actually does. Then explain how you would rewrite the code so
that Catalyst can optimize it."

**Prompt B.** "You have per-customer logic that you cannot express in SQL (for example, fitting a
small curve per customer). Explain why you would use `applyInPandas` and not a scalar Python
UDF or `collect()` to the driver. Explain what crosses the JVM/Python boundary in each case."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes the per-row serialization round trip to a Python worker that `BatchEvalPython` implies.
- [ ] Names at least one more cost: lost pushdown / no column pruning / broken whole-stage codegen.
- [ ] Rewrites the FX case as a declarative join + arithmetic using built-ins (no `BatchEvalPython`).
- [ ] Distinguishes a plain Python UDF (`BatchEvalPython`, row-by-row) from a pandas/Arrow UDF
      (`ArrowEvalPython`, Arrow batch).
- [ ] Picks `applyInPandas` for group-local custom logic. Explains that each group arrives as a
      whole `pandas.DataFrame` via Arrow, once per group. It does not arrive once per row, and not on the driver.
- [ ] States the correct test: assert that there is **no `BatchEvalPython`** in the plan. Do not assert
      that a particular API was used.
- [ ] (Bonus) Notes that the Pandas API on Spark gives pandas ergonomics in a distributed and lazy way.
      Also notes when it is the wrong tool (tiny data).
