# SPK-05 · Explain it back (Feynman)

**Prompt A.** "I give you a slow query that reads a wide Parquet table, filters on one column and
keeps two columns. Walk me, like a Redshift `EXPLAIN` reading, through what Catalyst does to make
it cheap — name the stages and the two physical-plan properties you'd check to *prove* it worked."

**Prompt B.** "A colleague wrapped a filter predicate in a Python UDF and now the job reads the
whole table. Explain why, in terms of the optimized logical plan and `PushedFilters`, and what the
fix is."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names the four plan stages (parsed → analyzed → optimized → physical) and says pushdown and
      pruning happen in the **optimized logical** stage.
- [ ] Checks **`PushedFilters`** on the scan node to prove predicate pushdown.
- [ ] Checks a narrow **`ReadSchema`** on the scan node to prove column pruning.
- [ ] Explains the I/O win: skipped row groups via Parquet footer stats; fewer columns decoded.
- [ ] Diagnoses the UDF case: a Python UDF makes the predicate opaque, so Catalyst cannot push it
      and the scan reads everything.
- [ ] States the fix: express the filter as a native column expression, not a UDF.
- [ ] (Bonus) Notes `explain` is planning-only (no job runs) and uses the `"formatted"` mode.
