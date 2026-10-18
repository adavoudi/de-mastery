# SPK-03 · Explain it back (Feynman)

**Prompt A.** "I have a raw orders feed with duplicate rows, NULL currencies, bad currency codes,
and timestamps with mixed UTC offsets. Walk me through, as a dbt-staging-model-in-Spark, exactly
which DataFrame operations clean it and in what order — and why order matters."

**Prompt B.** "Explain the timezone-naive date bug: how slicing the first 10 characters off an
offset-aware timestamp string produces the wrong calendar day, and the correct fix."

## Rubric — a strong explanation hits these (5–7):

- [ ] Uses declarative DataFrame ops (`select`/`filter`/`withColumn`), not Python row loops, and
      says why (optimizability).
- [ ] Validates (allowlist filter / NULL handling) **before** aggregating, and justifies the order.
- [ ] Dedups on the correct **business key** via `dropDuplicates([...])`, not a convenient single id.
- [ ] States NULL three-valued logic (`= NULL` is unknown; use `isNull`/`isNotNull`).
- [ ] Explains that `SUM`/`COUNT(col)` ignore NULLs and what that implies.
- [ ] Correctly diagnoses and fixes the timezone-naive extraction (parse offset → normalize → extract).
- [ ] (Bonus) Notes everything returns a new DataFrame and nothing runs until an action.
