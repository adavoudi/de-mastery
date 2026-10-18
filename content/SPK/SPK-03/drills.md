# SPK-03 · Interview drills

**1. "distinct() vs dropDuplicates(subset) — when do you use each?"**
> `distinct()` removes rows that are identical across *all* columns. `dropDuplicates(subset)`
> removes rows that repeat on a chosen subset (the business key). It keeps an arbitrary one of
> the rows. Use the subset form for real dedup. A true duplicate often differs in a
> load-timestamp or audit column, and `distinct()` treats that row as "different". Choosing the
> subset is the real decision.

**2. "Why doesn't `filter(col = 'USD')` keep NULL-currency rows, and how would you include them?"**
> `NULL = 'USD'` evaluates to NULL (unknown), which is not true. So the filter drops the NULL
> rows. To include them, test explicitly: `filter((col == 'USD') | col.isNull())`. This is
> three-valued logic, the same as in SQL.

**3. "SUM over an all-NULL column returns what, and how do you make it 0?"**
> It returns NULL, not 0. Wrap it: `coalesce(sum('amount'), lit(0))`. In the same way,
> `count('amount')` skips NULLs and `count('*')` counts rows.

**4. "A teammate extracts the order date by taking the first 10 characters of an ISO timestamp
string that has a UTC offset. What's the bug?"**
> Take an offset like `-05:00` near midnight. The instant falls on a different UTC calendar day.
> So slicing the local wall-clock string gives the wrong date. Parse the string to a real
> timestamp (`to_timestamp`) so Spark uses the offset. Then use `to_date`. The session runs in
> UTC, so parsing normalizes the value correctly.

**5. "When is a Python UDF the wrong tool for cleaning a column?"**
> Almost always, if a built-in function exists. Catalyst cannot see inside UDFs. UDFs serialize
> every row to Python and block pushdown. `pyspark.sql.functions` covers trims, case-folding,
> coalesce, regex, and dates. Use a UDF only for truly custom logic. In that case, prefer a
> pandas/Arrow UDF (SPK-09).

**6. "How do you pull a single column into a Python list safely?"**
> Reduce the data first: `df.select('c').distinct()` (or shrink it in another way). Then use
> `[r['c'] for r in that.collect()]`. `collect()` is an action that moves data to the driver. So
> collect only small results.
