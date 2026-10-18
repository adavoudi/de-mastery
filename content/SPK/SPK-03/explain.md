# SPK-03 · Explain it back (Feynman)

**Prompt A.** "I have a raw orders feed. It has duplicate rows, NULL currencies, bad currency
codes, and timestamps with mixed UTC offsets. Treat the job as a dbt staging model in Spark.
Walk me through exactly which DataFrame operations clean the feed, and in what order. Tell me
why the order matters."

**Prompt B.** "Explain the timezone-naive date bug. Show how slicing the first 10 characters of
an offset-aware timestamp string gives the wrong calendar day. Then explain the correct fix."

## Rubric — a strong explanation covers these (5–7):

- [ ] Uses declarative DataFrame ops (`select`/`filter`/`withColumn`), not Python row loops. Says
      why (optimizability).
- [ ] Validates (allowlist filter / NULL handling) **before** aggregating. Justifies the order.
- [ ] Dedups on the correct **business key** with `dropDuplicates([...])`, not a convenient
      single id.
- [ ] States NULL three-valued logic (`= NULL` is unknown; use `isNull`/`isNotNull`).
- [ ] Explains that `SUM`/`COUNT(col)` ignore NULLs, and what that means.
- [ ] Correctly diagnoses and fixes the timezone-naive extraction (parse offset → normalize → extract).
- [ ] (Bonus) Notes that every operation returns a new DataFrame and nothing runs until an action.
