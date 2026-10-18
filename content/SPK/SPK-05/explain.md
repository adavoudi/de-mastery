# SPK-05 · Explain it back (Feynman)

**Prompt A.** "I give you a slow query. It reads a wide Parquet table, filters on one column, and
keeps two columns. Walk me through what Catalyst does to make it cheap. Explain it like reading
a Redshift `EXPLAIN`. Name the stages. Name the two physical-plan properties that you would
check to *prove* that it worked."

**Prompt B.** "A colleague wrapped a filter predicate in a Python UDF, and now the job reads the
whole table. Explain why. Use the optimized logical plan and `PushedFilters` in your answer.
Then explain the fix."

## Rubric — a strong explanation covers these (5–7):

- [ ] Names the four plan stages (parsed → analyzed → optimized → physical). Says that pushdown
      and pruning happen in the **optimized logical** stage.
- [ ] Checks **`PushedFilters`** on the scan node to prove predicate pushdown.
- [ ] Checks a narrow **`ReadSchema`** on the scan node to prove column pruning.
- [ ] Explains the I/O gain: Spark skips row groups by using Parquet footer stats, and it decodes
      fewer columns.
- [ ] Diagnoses the UDF case: a Python UDF makes the predicate opaque, so Catalyst cannot push it
      and the scan reads everything.
- [ ] States the fix: express the filter as a native column expression, not a UDF.
- [ ] (Bonus) Notes that `explain` only plans (no job runs) and uses the `"formatted"` mode.
