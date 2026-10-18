# SPK-12 · Explain it back (Feynman)

**Prompt A.** "Explain, to someone who writes dbt tests, how you'd test a PySpark transformation so
the suite actually catches bugs. What do you assert, what do you deliberately NOT assert, and how do
you convince yourself the suite is strong (not just green)?"

**Prompt B.** "You must run one SPK-07 partition-sizing job once at 50 GB on EMR Serverless in a
shared AWS account. Describe how you'd size it, keep it attributable and cheap, and make sure it
doesn't keep costing money — without hard-coding anything secret."

## Rubric — a strong explanation hits these (5–7):

- [ ] Makes transforms **pure** `DataFrame -> DataFrame` and injects inputs; keeps `main()` thin.
- [ ] Uses `assertDataFrameEqual` (order-insensitive, float tolerance) and asserts **invariants**.
- [ ] Builds **small, deterministic** inputs covering edge cases (NULLs, dups, boundaries).
- [ ] Uses an **independent oracle** for non-trivial expectations (DuckDB / plain Python).
- [ ] Explicitly does **not** assert column order / which API / exact floats (too-strict trap).
- [ ] Argues suite strength via the **mutation mindset**: would it fail sum→avg, missing dedup,
      inner-vs-left, wrong-date FX?
- [ ] For the 💲 run: **right-size**, **tag** `project=de-mastery`, inject config (no hard-coded
      secrets), and **tear down** to stop charges.
