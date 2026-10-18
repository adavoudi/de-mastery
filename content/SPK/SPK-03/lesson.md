# SPK-03 · The DataFrame API

## Why the DataFrame exists

A dbt staging model is a `SELECT` that renames, casts, cleans, and lightly reshapes a raw source
into something trustworthy. You write it declaratively — you describe the *result*, not the loop
that produces it — and Redshift's planner figures out how to run it. The Spark **DataFrame** is
the same idea in a Python API: a distributed, schema-bearing table you transform with declarative
operations (`select`, `filter`, `withColumn`, `groupBy`), while Catalyst (SPK-05) decides the
physical execution.

Why not just write Python loops over the rows? Because the moment you drop to row-by-row Python,
you lose everything: the optimizer can't push filters down, can't prune columns, can't run in
parallel across partitions without serialization overhead, and can't vectorize. The DataFrame API
is the contract that keeps your logic *optimizable*. Staying in the DataFrame/SQL API — and out of
Python UDFs (SPK-09) — is the single biggest performance habit in Spark, exactly as staying in SQL
rather than post-processing in pandas is in your dbt world.

This module is your "dbt staging in PySpark" module: take the dirty commerce feed and produce
clean, well-typed, deduplicated, correctly-dated tables, each verified against an independent
DuckDB oracle.

## Columns and rows (objective 3.1)

Everything revolves around **Column** expressions. `F.col("x")`, `df["x"]`, and `df.x` all name a
column; you build expressions with operators (`F.col("a") + F.col("b")`), functions
(`F.upper(...)`, `F.coalesce(...)`), and conditionals (`F.when(cond, a).otherwise(b)`).

- **Add / derive:** `df.withColumn("total", F.col("qty") * F.col("price"))`.
- **Rename:** `df.withColumnRenamed("old", "new")` (or alias inside a `select`).
- **Drop:** `df.drop("col")`.
- **Filter:** `df.filter(F.col("x") > 0)` or `df.where("x > 0")` — SQL string or Column, same thing.
- **Split a string into columns:** `F.split(col, delim)` returns an array you index or `explode`.
- **Explode:** `df.select(F.explode("items").alias("item"))` turns one row with an array into many
  rows — the DataFrame analogue of a SQL `LATERAL`/`UNNEST`. The dbt analogue is a model that
  fans a packed column out into one row per element.

A crucial contrast with pandas: these return **new** DataFrames (laziness, SPK-02); nothing
mutates in place.

## Deduplication and validation (objective 3.2)

Real feeds are dirty — the commerce dataset deliberately ships duplicate order-item events, NULL
emails, NULL payment amounts, bad currency codes (`"XXX"`, lowercase `"usd"`, empty), and
timestamps rendered in mixed time zones. Cleaning them is the job.

- `df.distinct()` removes fully-identical rows. `df.dropDuplicates(["k1", "k2"])` removes rows that
  repeat on a *subset* of columns — and **the choice of subset is the whole exercise**. Dedup on
  the wrong key silently merges genuinely different records (a classic MDM bug given the
  commerce data's customer-id collisions across companies).
- **NULL handling:** `df.na.drop(subset=[...])`, `df.na.fill(...)`, `F.coalesce(a, b)`. Beware:
  NULL is not equal to anything, including NULL, so `col = 'x'` never matches a NULL; use
  `col.isNull()` / `isNotNull()`. This is identical to SQL three-valued logic you already know
  from Redshift.
- **Validation:** filter to a known-good allowlist (e.g. currency in the ISO set) before you
  aggregate, so bad codes don't pollute sums. Validating *then* aggregating is the dbt pattern of
  a staging model that quarantines bad rows before the mart consumes them.

## Aggregations (objective 3.3)

`groupBy(keys).agg(...)` with `F.count`, `F.sum`, `F.avg`/`F.mean`, `F.min`, `F.max`,
`F.countDistinct`, `F.approx_count_distinct`. `df.summary()` / `df.describe()` give quick
descriptive stats. Two gotchas that match SQL exactly: `count("col")` ignores NULLs while
`count("*")`/`count(F.lit(1))` counts rows; and `sum` over a column that is entirely NULL is NULL,
not 0. The commerce `payments.amount` has NULLs on purpose so you feel this.

## Dates (objective 3.4)

The orders feed carries two timestamp columns: `order_ts_utc` (a clean ISO UTC string) and
`order_ts_raw` (the same instant rendered with a mixed UTC offset like `+00:00` or `-05:00`). Date
work is where silent bugs live:

- Parse: `F.to_timestamp(col)`, `F.to_date(col)`. From a Unix epoch: `F.from_unixtime(col)`;
  to one: `F.unix_timestamp(col)`.
- Extract: `F.year`, `F.month`, `F.dayofmonth`, `F.hour`, `F.date_trunc("month", col)`.
- **The timezone trap:** if you extract the date from `order_ts_raw` *without* accounting for the
  offset, a `-05:00` timestamp near midnight lands on the wrong calendar day. The session runs in
  UTC (`spark.sql.session.timeZone=UTC`); parsing an offset-aware string normalizes it to the same
  instant, so extracting from the properly parsed timestamp is correct, while naively slicing the
  first 10 characters of the raw string is not. One of the exercises makes this bite.

## Sort, inspect, convert (objective 3.7)

`df.orderBy(F.col("x").desc())`, `df.sort(...)`; `df.printSchema()` to see types;
`df.columns`/`df.dtypes` to read them programmatically; `df.collect()` → `list[Row]`,
`[r["x"] for r in df.collect()]` or `df.select("x").rdd.flatMap(lambda r: r).collect()` to pull a
column to a Python list. Remember `collect()` is an action that brings data to the driver — fine
for small results, dangerous for large ones.

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

- **Don't reach for a Python UDF** when a built-in column function exists. A UDF is a black box to
  Catalyst, serializes every row to Python, and kills pushdown (SPK-09). 95% of staging logic is
  expressible with `pyspark.sql.functions`.
- **Don't `collect()` a big DataFrame to clean it in Python.** That defeats Spark entirely — do the
  cleaning as DataFrame transformations and only collect the small final result.
- **Don't dedup on a convenient key without checking it's the right one.** On multi-company data a
  surrogate id can collide; dedup on the natural/business key, not the first column that looks
  unique.

## Further reading

Pinned to Spark **4.2.0**:

- DataFrame / Dataset guide: <https://spark.apache.org/docs/4.2.0/sql-programming-guide.html>
- `pyspark.sql.functions` reference:
  <https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/functions.html>
- `DataFrame.dropDuplicates`:
  <https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html>
- Datetime patterns:
  <https://spark.apache.org/docs/4.2.0/sql-ref-datetime-pattern.html>
