# SPK-05 · Interview drills

**1. "Walk me through the Catalyst stages."**
> First is the parsed logical plan (as you wrote it, unresolved). Second is the analyzed plan
> (references resolved against the catalog, types checked). Third is the optimized logical plan
> (rule-based rewrites: predicate pushdown, column pruning, constant folding, filter reordering,
> boolean simplification). Fourth is the physical plan. A cost model picks one of several
> candidate physical plans, and this plan is what runs. `df.explain("extended")` shows all of
> them. `"formatted"` shows the physical tree.

**2. "How do you prove, from the plan, that predicate pushdown and column pruning happened?"**
> Pushdown: `PushedFilters` on the `FileScan`/`BatchScan` node is non-empty and lists your
> predicate. Pruning: `ReadSchema` on that same node lists only the columns that you need, not
> the whole table. You read both from `explain("formatted")`.

**3. "Why does an empty `PushedFilters` list matter, and what commonly causes it?"**
> It means that Spark will read all rows and filter them in the engine. You lose the row-group
> skipping that Parquet footer statistics give you. Common causes are a Python UDF or a
> non-pushable function that wraps the column, or a filter on a derived column that does not
> exist in the source.

**4. "Redshift zone maps vs. Parquet pushdown — same or different?"**
> The idea is the same. The mechanism is different. Both skip blocks of data whose min/max
> statistics cannot match the predicate. Redshift uses zone maps for each block. Parquet uses
> column statistics for each row group in the file footer. In both, a selective predicate on a
> well-clustered column reads far less data.

**5. "Someone says caching an intermediate DataFrame always speeds things up. Push back."**
> Caching materializes one specific intermediate result. This can block the pushdown and pruning
> that would otherwise flow through the whole pipeline. It also costs memory. Read the plan
> before and after. Suppose the cached frame is wide, or the downstream steps still prune. Then
> the cache can read *more* data. Cache only a result that you reuse and that is already narrow.

**6. "`explain` shows `BatchEvalPython` in the plan. What does that tell you?"**
> A Python UDF is in the pipeline. It serializes rows to Python. It blocks pushdown of any
> predicate that depends on it. It also prevents whole-stage codegen for that part of the plan.
> The fix is to replace it with native `pyspark.sql.functions` where possible (SPK-09).
