# SPK-12 · Explain it back (Feynman)

**Prompt A.** "Explain to someone who writes dbt tests how you would test a PySpark transformation, so
that the suite really catches bugs. What do you assert? What do you deliberately NOT assert? How do
you convince yourself that the suite is strong (not just green)?"

**Prompt B.** "You must run one SPK-07 partition-sizing job once, at 50 GB, on EMR Serverless in a
shared AWS account. Describe how you would size it. Describe how you would keep it attributable and
cheap. Describe how you would make sure that it does not keep costing money. Do not hard-code
anything secret."

## Rubric — a strong explanation hits these (5–7):

- [ ] Makes transforms **pure** `DataFrame -> DataFrame` and injects inputs. Keeps `main()` thin.
- [ ] Uses `assertDataFrameEqual` (order-insensitive, float tolerance) and asserts **invariants**.
- [ ] Builds **small, deterministic** inputs that cover edge cases (NULLs, dups, boundaries).
- [ ] Uses an **independent oracle** for expectations that are not trivial (DuckDB / plain Python).
- [ ] Explicitly does **not** assert column order / which API / exact floats (too-strict trap).
- [ ] Argues suite strength with the **mutation mindset**: would the suite fail for sum→avg, missing
      dedup, inner-vs-left, wrong-date FX?
- [ ] For the 💲 run: **right-size**, **tag** `project=de-mastery`, inject config (no hard-coded
      secrets), and **tear down** to stop charges.
