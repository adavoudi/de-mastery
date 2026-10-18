# SPK-05 · Interview drills

**1. "Walk me through the Catalyst stages."**
> Parsed logical plan (as written, unresolved) → analyzed (references resolved against the catalog,
> types checked) → optimized logical plan (rule-based rewrites: predicate pushdown, column pruning,
> constant folding, filter reordering, boolean simplification) → physical plan (cost model picks
> one of several candidate physical plans; this is what runs). `df.explain("extended")` shows them
> all; `"formatted"` shows the physical tree.

**2. "How do you prove, from the plan, that predicate pushdown and column pruning happened?"**
> Pushdown: `PushedFilters` on the `FileScan`/`BatchScan` node is non-empty and lists your
> predicate. Pruning: `ReadSchema` on that same node lists only the columns you actually need, not
> the whole table. Both are read off `explain("formatted")`.

**3. "Why does an empty `PushedFilters` list matter, and what commonly causes it?"**
> It means Spark will read all rows and filter them in-engine, losing the row-group skipping that
> Parquet footer statistics enable. Common causes: a Python UDF or a non-pushable function wrapping
> the column, or filtering on a derived column that doesn't exist in the source.

**4. "Redshift zone maps vs. Parquet pushdown — same or different?"**
> Same idea, different mechanism. Both skip blocks of data whose min/max statistics can't match the
> predicate. Redshift uses per-block zone maps; Parquet uses per-row-group column statistics in the
> file footer. In both, a selective predicate on a well-clustered column reads far less data.

**5. "Someone says caching an intermediate DataFrame always speeds things up. Push back."**
> Caching materialises a specific intermediate, which can defeat pushdown/pruning that would
> otherwise flow through the whole pipeline, and it costs memory. Read the plan before and after:
> if the cached frame is wide or the downstream still prunes, the cache can read *more* data. Cache
> only a reused, already-narrow result.

**6. "`explain` shows `BatchEvalPython` in the plan. What does that tell you?"**
> A Python UDF is in the pipeline. It serialises rows to Python, blocks pushdown of any predicate
> that depends on it, and prevents whole-stage codegen for that part of the plan. The fix is to
> replace it with native `pyspark.sql.functions` where possible (SPK-09).
