# SPK-03 · The DataFrame API

## Why the DataFrame exists

A dbt staging model is a `SELECT`. It renames, casts, cleans, and lightly reshapes a raw source
into a source that you can trust. You write it declaratively. You describe the *result*, not the
loop that produces it. The Redshift planner then decides how to run it. The Spark **DataFrame**
is the same idea in a Python API. It is a distributed table that has a schema. You transform it
with declarative operations (`select`, `filter`, `withColumn`, `groupBy`). Catalyst (SPK-05)
decides the physical execution.

Why not write Python loops over the rows? When you go to row-by-row Python, you lose these
things:

- The optimizer cannot push filters down.
- The optimizer cannot prune columns.
- The code cannot run in parallel across partitions without serialization overhead.
- The code cannot vectorize.

The DataFrame API is the contract that keeps your logic *optimizable*. Stay in the DataFrame/SQL
API and avoid Python UDFs (SPK-09). This is the most important performance habit in Spark. In
your dbt world, the same habit is to stay in SQL and not post-process in pandas.

This module is your "dbt staging in PySpark" module. You take the dirty commerce feed and make
clean tables. The tables have correct types, no duplicates, and correct dates. You verify each
table against an independent DuckDB oracle.

## Columns and rows

Everything uses **Column** expressions. `F.col("x")`, `df["x"]`, and `df.x` all name a column.
You build expressions with these tools:

- operators: `F.col("a") + F.col("b")`
- functions: `F.upper(...)`, `F.coalesce(...)`
- conditionals: `F.when(cond, a).otherwise(b)`

Here are the common operations:

- **Add / derive:** `df.withColumn("total", F.col("qty") * F.col("price"))`.
- **Rename:** `df.withColumnRenamed("old", "new")` (or alias inside a `select`).
- **Drop:** `df.drop("col")`.
- **Filter:** `df.filter(F.col("x") > 0)` or `df.where("x > 0")`. You can use a SQL string or a
  Column. The result is the same.
- **Split a string into columns:** `F.split(col, delim)` returns an array. You can index it or
  `explode` it.
- **Explode:** `df.select(F.explode("items").alias("item"))` turns one row that has an array
  into many rows. It is the DataFrame analogue of a SQL `LATERAL`/`UNNEST`. The dbt analogue is
  a model that spreads a packed column into one row per element.

Pandas is different in one important way. These operations return **new** DataFrames (laziness,
SPK-02). Nothing changes in place.

## Deduplication and validation

Real feeds are dirty. The commerce dataset deliberately contains these problems:

- duplicate order-item events
- NULL emails
- NULL payment amounts
- bad currency codes (`"XXX"`, lowercase `"usd"`, empty)
- timestamps in mixed time zones

Your job is to clean them.

- `df.distinct()` removes rows that are fully identical. `df.dropDuplicates(["k1", "k2"])`
  removes rows that repeat on a *subset* of columns. **The choice of subset is the whole
  exercise.** If you dedup on the wrong key, you silently merge records that are really
  different. This is a classic MDM bug. The commerce data has customer-id collisions across
  companies.
- **NULL handling:** `df.na.drop(subset=[...])`, `df.na.fill(...)`, `F.coalesce(a, b)`. Be
  careful. NULL is not equal to anything, including NULL. So `col = 'x'` never matches a NULL.
  Use `col.isNull()` / `isNotNull()`. This is the same as the SQL three-valued logic that you
  know from Redshift.
- **Validation:** filter to a known-good allowlist (for example, currency in the ISO set) before
  you aggregate. Then bad codes do not pollute the sums. In dbt, a staging model does the same:
  it quarantines bad rows before the mart uses the data.

## Aggregations

Use `groupBy(keys).agg(...)` with these functions: `F.count`, `F.sum`, `F.avg`/`F.mean`,
`F.min`, `F.max`, `F.countDistinct`, `F.approx_count_distinct`. `df.summary()` and
`df.describe()` give quick descriptive statistics. Two gotchas work exactly as in SQL:

- `count("col")` ignores NULLs. `count("*")` and `count(F.lit(1))` count rows.
- `sum` over a column that is entirely NULL is NULL, not 0.

The commerce `payments.amount` has NULLs on purpose, so you can see this behavior.

## Dates

The orders feed has two timestamp columns:

- `order_ts_utc` is a clean ISO UTC string.
- `order_ts_raw` is the same instant with a mixed UTC offset, such as `+00:00` or `-05:00`.

Silent bugs often live in date work:

- Parse: `F.to_timestamp(col)`, `F.to_date(col)`. To read a Unix epoch, use
  `F.from_unixtime(col)`. To produce one, use `F.unix_timestamp(col)`.
- Extract: `F.year`, `F.month`, `F.dayofmonth`, `F.hour`, `F.date_trunc("month", col)`.
- **The timezone trap:** Suppose you extract the date from `order_ts_raw` *without* accounting
  for the offset. Then a `-05:00` timestamp near midnight gets the wrong calendar day. The
  session runs in UTC (`spark.sql.session.timeZone=UTC`). When Spark parses an offset-aware
  string, it normalizes the value to the same instant. So extracting from the properly parsed
  timestamp is correct. Slicing the first 10 characters of the raw string is not correct. One of
  the exercises shows you this problem.

## Sort, inspect, convert

- Sort: `df.orderBy(F.col("x").desc())`, `df.sort(...)`.
- See types: `df.printSchema()`.
- Read types in code: `df.columns`/`df.dtypes`.
- Get rows: `df.collect()` → `list[Row]`.
- Pull a column to a Python list: `[r["x"] for r in df.collect()]` or
  `df.select("x").rdd.flatMap(lambda r: r).collect()`.

Remember that `collect()` is an action that brings data to the driver. It is fine for small
results. It is dangerous for large results.

## From your world

| dbt / Redshift / pandas | Spark DataFrame | Note |
|---|---|---|
| dbt staging `SELECT` with renames/casts | `select`/`withColumn`/`withColumnRenamed` | Declarative, optimizable |
| `WHERE` | `filter`/`where` | SQL string or Column |
| `LATERAL`/`UNNEST` | `explode` | One row → many |
| SQL three-valued NULL logic | `isNull`/`isNotNull`/`na.*` | Same semantics as Redshift |
| `COUNT(col)` ignores NULLs | `F.count("col")` | identical gotcha |
| pandas in-place mutation | returns new DataFrame | never mutates |
| dbt source freshness/quarantine | filter to an allowlist before aggregating | validate then aggregate |

## When NOT to use the DataFrame API this way

- **Do not use a Python UDF** when a built-in column function exists. Catalyst cannot see inside
  a UDF. A UDF serializes every row to Python and stops pushdown (SPK-09). You can write 95% of
  staging logic with `pyspark.sql.functions`.
- **Do not `collect()` a big DataFrame to clean it in Python.** This defeats the purpose of
  Spark. Do the cleaning as DataFrame transformations. Collect only the small final result.
- **Do not dedup on a convenient key without a check that it is the right key.** In
  multi-company data, a surrogate id can collide. Dedup on the natural/business key. Do not use
  the first column that looks unique.

## Further reading

Pinned to Spark **4.2.0**:

- DataFrame / Dataset guide: <https://spark.apache.org/docs/4.2.0/sql-programming-guide.html>
- `pyspark.sql.functions` reference:
  <https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/functions.html>
- `DataFrame.dropDuplicates`:
  <https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html>
- Datetime patterns:
  <https://spark.apache.org/docs/4.2.0/sql-ref-datetime-pattern.html>
