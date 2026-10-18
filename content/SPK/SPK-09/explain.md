# SPK-09 · Explain it back (Feynman)

**Prompt A.** "A colleague wrote a Python UDF to convert every order's amount to USD by looking up
an fx rate per row. The job is slow. Explain, as if to that colleague, *why* it is slow — what the
`BatchEvalPython` node in the plan actually does — and how you'd rewrite it so Catalyst can
optimize it."

**Prompt B.** "You have per-customer logic that genuinely isn't expressible in SQL (say, fitting a
small curve per customer). Explain why you'd use `applyInPandas` rather than either a scalar Python
UDF or `collect()`-ing to the driver, and what crosses the JVM/Python boundary in each case."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes the per-row serialization round trip to a Python worker that `BatchEvalPython` implies.
- [ ] Names at least one further cost: lost pushdown / no column pruning / broken whole-stage codegen.
- [ ] Rewrites the FX case as a declarative join + arithmetic using built-ins (no `BatchEvalPython`).
- [ ] Distinguishes a plain Python UDF (`BatchEvalPython`, row-by-row) from a pandas/Arrow UDF
      (`ArrowEvalPython`, Arrow batch).
- [ ] Picks `applyInPandas` for group-local custom logic and explains that each group arrives as a
      whole `pandas.DataFrame` via Arrow, once per group — not once per row and not on the driver.
- [ ] States the correct test: assert there is **no `BatchEvalPython`** in the plan, not that a
      particular API was used.
- [ ] (Bonus) Notes the Pandas API on Spark gives pandas ergonomics distributed/lazily, and when it
      is the wrong tool (tiny data).
