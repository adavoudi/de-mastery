# SPK-03 · Interview drills

**1. "distinct() vs dropDuplicates(subset) — when do you use each?"**
> `distinct()` removes rows that are identical across *all* columns. `dropDuplicates(subset)`
> removes rows that repeat on a chosen subset (the business key), keeping an arbitrary one. Use the
> subset form for real dedup, because a true duplicate often differs in a load-timestamp or audit
> column that `distinct()` would treat as "different". Choosing the subset is the real decision.

**2. "Why doesn't `filter(col = 'USD')` keep NULL-currency rows, and how would you include them?"**
> `NULL = 'USD'` evaluates to NULL (unknown), which isn't true, so NULL rows are dropped. To
> include them, test explicitly: `filter((col == 'USD') | col.isNull())`. Three-valued logic,
> same as SQL.

**3. "SUM over an all-NULL column returns what, and how do you make it 0?"**
> NULL, not 0. Wrap it: `coalesce(sum('amount'), lit(0))`. Likewise `count('amount')` skips NULLs
> while `count('*')` counts rows.

**4. "A teammate extracts the order date by taking the first 10 characters of an ISO timestamp
string that has a UTC offset. What's the bug?"**
> For an offset like `-05:00` near midnight, the instant falls on a different UTC calendar day, so
> slicing the local wall-clock string gives the wrong date. Parse to a real timestamp
> (`to_timestamp`) so the offset is honored, then `to_date`. The session runs in UTC, so parsing
> normalizes correctly.

**5. "When is a Python UDF the wrong tool for cleaning a column?"**
> Almost always, if a built-in exists. UDFs are opaque to Catalyst, serialize every row to Python,
> and block pushdown. `pyspark.sql.functions` covers trims, case-folding, coalesce, regex, dates —
> reach for a UDF only for genuinely custom logic (and prefer a pandas/Arrow UDF then, SPK-09).

**6. "How do you pull a single column into a Python list safely?"**
> Reduce first: `df.select('c').distinct()` (or otherwise shrink), then
> `[r['c'] for r in that.collect()]`. `collect()` is an action that moves data to the driver, so
> only collect small results.
