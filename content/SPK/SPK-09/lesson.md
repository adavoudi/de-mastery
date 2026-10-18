# SPK-09 · UDFs and the Pandas API

## Why this module exists

Every Spark user eventually writes a Python UDF, and most of them regret it. A plain Python
user-defined function is the one place where you deliberately go *outside* the engine. Catalyst
can no longer see what you do, so it stops optimizing. The executor must also move every single row
out of the JVM and into a Python worker process. The worker runs your function and sends the result
back. This round trip happens row by row, with Python object (de)serialization at both ends. It is the
most common reason why a Spark job that "should be fast" is slow for no clear reason.

This module teaches two things:

- Do not use a UDF when a built-in function exists.
- When you really need Python, use the **vectorized** forms (pandas/Arrow UDFs). They move data in Arrow
  batches instead of one row at a time.

This module also covers the **Pandas API on Spark**. It is the pandas that you already know, but it runs
distributed. You do not have to collect a billion rows to a driver to call `.groupby()`.

## How a Python UDF actually runs

This is a scalar Python UDF:

```python
from pyspark.sql.functions import udf
to_usd = udf(lambda amt, rate: amt * rate, "double")
df.withColumn("usd", to_usd("amount", "rate"))
```

It appears in the physical plan as a **`BatchEvalPython`** node. This node is the sign. It means that
Spark will serialize the inputs of each row and send them to a Python worker. The worker calls your
lambda, and Spark reads the result back. Three costs follow:

1. **Serialization per row.** Pickle in, pickle out, for every row.
2. **No pushdown through the UDF.** Catalyst cannot see inside the lambda. So it cannot push a
   filter below it. It also cannot prune columns that the lambda "might" need. The UDF is an optimizer fence.
3. **No vectorization.** The whole-stage codegen of the JVM stops at the UDF boundary.

![One row per trip to the Python worker (BatchEvalPython) versus one Arrow batch per trip (ArrowEvalPython)](diagrams/udf-row-vs-arrow.svg)

These are the fixes, in order of preference:

- **Use a built-in.** 95% of what people write UDFs for already exists in `pyspark.sql.functions`.
  This includes arithmetic, string cleanup, dates, conditionals, hashing, and JSON. A built-in stays
  inside Catalyst and codegen. There is no `BatchEvalPython` at all.
- **If you must use Python, vectorize it.** A **pandas UDF** / **Arrow UDF**
  (`@pandas_udf`, or `functions.udf(..., useArrow=True)` in 4.x) receives a whole `pandas.Series`
  (or an iterator of Series) for each batch. Spark transfers it as an Arrow columnar batch, not one
  row at a time. It appears as **`ArrowEvalPython`** in the plan. `pandas_udf(... , GROUPED_MAP)` and
  `applyInPandas` appear as `FlatMapGroupsInPandas`. The Python code is the same, but the overhead is
  much smaller. The Python↔JVM boundary is crossed once per batch, in Arrow format.

So the important test is not "which API did you use?" The important test is **"is there a
`BatchEvalPython` in the plan?"** A correct solution has none. This applies to native functions,
a pandas UDF, and an Arrow UDF.

## Pandas UDF flavors

- **Series → Series** (scalar): vectorized element-wise, for example a custom numeric transform.
- **Iterator[Series] → Iterator[Series]**: the same, but it lets you spread an expensive setup (for
  example, loading a model once) across batches.
- **Grouped map** (`df.groupBy(...).applyInPandas(fn, schema)`): each group arrives as a whole
  `pandas.DataFrame`, and you return a `pandas.DataFrame`. This is the "dbt Python model per
  partition" shape. It works well for group-local logic that has no SQL equivalent, such as a
  per-customer curve fit.
- **Grouped aggregate** (`@pandas_udf` used in `groupBy().agg(...)`): Series in, one scalar out.

All of these cross the boundary in Arrow batches, so none of them produce `BatchEvalPython`.

## Pandas API on Spark

`import pyspark.pandas as ps` gives you an API with the shape of pandas (`ps.DataFrame`, `.groupby()`,
`.merge()`, `.describe()`). It compiles to Spark jobs. Compared with real pandas, it is
**distributed and lazy**. You keep the ergonomics of pandas, and you do not pull the data into the memory
of a single machine. Compared with hand-written DataFrame code, it is more familiar. You can port an
existing pandas notebook almost verbatim and make it scale.

It has caveats:

- It is not 100% compatible with pandas. Some operations trigger expensive shuffles, and some are
  unsupported.
- The default index can cost a shuffle. For large data, set
  `ps.set_option("compute.default_index_type", "distributed")`.

Think of it as "pandas ergonomics, Spark semantics". It does not promise that every pandas trick works.

## From your world

| dbt / pandas / Redshift | Spark | Note |
|---|---|---|
| A dbt **Python model** that does group-local logic | `applyInPandas` grouped map | per-group `pandas.DataFrame` in/out |
| `df.apply(lambda row: ...)` in pandas (slow) | **avoid** the scalar Python UDF equivalent | `BatchEvalPython`, row-by-row |
| A pandas vectorized `Series` op | `@pandas_udf` Series→Series | Arrow batch, `ArrowEvalPython` |
| Reaching for pandas because the SQL is awkward | first check `pyspark.sql.functions` | a built-in almost always exists |
| A pandas notebook you want to scale | `import pyspark.pandas as ps` | same API, distributed |

## When NOT to use a UDF (or the Pandas API)

- **Don't write a scalar Python UDF for anything that a built-in covers.** These are all native:
  currency math, `upper`/`trim`, `regexp_replace`, `to_date`, `coalesce`, `when/otherwise`, `sha2`,
  `from_json`. The UDF only gives you a `BatchEvalPython` fence and a slowdown.
- **Don't use a non-deterministic UDF without marking it.** Catalyst may evaluate it an
  unexpected number of times. This breaks assumptions (for example, a UDF that calls `random`).
- **Don't use the Pandas API on Spark for tiny data.** If the data already fits in memory, real pandas
  (or DuckDB) is simpler and faster. The Pandas API is worth using only when the data is too big
  for one machine.
- **Don't use a grouped-map UDF when a `groupBy().agg()` of built-ins is enough.** You lose
  partial aggregation, and you pay to materialize each whole group in Python.

## Further reading

Pinned to Spark **4.0.0** (official docs only):

- Python UDFs: <https://spark.apache.org/docs/4.0.0/api/python/user_guide/sql/python_udf.html>
- Pandas UDFs / function APIs:
  <https://spark.apache.org/docs/4.0.0/api/python/user_guide/sql/arrow_pandas.html>
- Pandas API on Spark:
  <https://spark.apache.org/docs/4.0.0/api/python/user_guide/pandas_on_spark/index.html>
- `pyspark.sql.functions` reference:
  <https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/functions.html>
