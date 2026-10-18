# FLK-08 · Table API & PyFlink — one pipeline, two languages

## Why the Table API (and PyFlink) exist

Flink SQL (FLK-04) is the fastest way to express a relational pipeline, but a string of SQL is hard
to compose, parameterize and unit-test. The **Table API** is the same relational model as a
*programmatic* API: `table.groupBy($("ccy")).select($("ccy"), $("amt").sum())` is the Table API
form of `SELECT ccy, SUM(amt) ... GROUP BY ccy`. It plans through the **same optimizer** as SQL and
produces the **same changelog semantics**, so you can mix SQL and Table API freely and get identical
results. You reach for it when you want host-language composition — build the pipeline from
functions, inject parameters, reuse fragments — while keeping SQL's declarative power.

**PyFlink** is the Python entry point to that same Table API (and DataStream). For an analytics
engineer this is the natural on-ramp: it feels like pandas/dbt-Python but runs on the JVM engine
underneath. The Python API builds the same logical plan and ships it to the same runtime, so a
Python Table pipeline and the equivalent Java one are **the same job** — they must produce the same
answer over the same data. That is the whole point of this module: write one pipeline (a per-currency
GMV aggregation) in **both** Java Table API and PyFlink and prove they agree against one oracle.

## How the two map to each other

| Concept | Java Table API | PyFlink Table API |
|---|---|---|
| Create the environment | `TableEnvironment.create(EnvironmentSettings.inBatchMode())` | `TableEnvironment.create(EnvironmentSettings.in_batch_mode())` |
| Build a table from rows | `t.fromValues(DataTypes.ROW(...), rows)` | `t.from_elements([...], schema)` |
| Group + aggregate | `tbl.groupBy($("ccy")).select($("ccy"), $("amt").sum().as("gmv"))` | `tbl.group_by(col("ccy")).select(col("ccy"), col("amt").sum.alias("gmv"))` |
| Run SQL instead | `t.executeSql("SELECT ...")` | `t.execute_sql("SELECT ...")` |
| Collect results | `tableResult.collect()` → `Row` | `table_result.collect()` → `Row` |

Both can also just run **SQL** via `executeSql` / `execute_sql` — in which case the SQL text is
identical across the two languages. The grader does not care which surface you use (DataStream vs
Table API vs SQL); it only checks the **result** equals the oracle, per docs/TESTING.md ("do not
assert which API was called").

## UDFs (scalar / table / aggregate)

When built-in functions are not enough, both languages let you register user-defined functions: a
**scalar** UDF (one row in, one value out), a **table** UDF (one row in, many rows out — like a
lateral join / `explode`), and an **aggregate** UDF (many rows in, one value out — a custom
`SUM`-like). Keep them pure and deterministic so results reconcile with a batch oracle. (Not graded
here, but see the Further reading.)

## From your world

| dbt / warehouse | Flink | Note |
|---|---|---|
| A dbt SQL model (SELECT → table) | Flink SQL / Table API | declarative, same optimizer |
| A dbt **Python** model | PyFlink Table API | same model, Python surface |
| `GROUP BY ccy` GMV | `groupBy(ccy).sum(amt)` | the exercise pipeline |
| A reusable macro | a Java/Python function returning a `Table` | host-language composition |

## When NOT to use (each)

- **Pure, static SQL with no composition** — just write Flink SQL; wrapping it in Table API calls
  adds nothing.
- **PyFlink for ultra-low-latency, UDF-heavy jobs** — crossing the Python/JVM boundary per record is
  costly; keep hot UDFs in Java, or use vectorized (pandas) UDFs, or stay in SQL/Table API so the
  work runs in the JVM.
- **Table API when you need per-record custom timers/state** — drop to the DataStream process
  functions (FLK-07); the Table API is relational, not event-at-a-time control.

## PyFlink runs in a separate environment

In this platform PyFlink lives in a **dedicated virtual environment** (its dependency pins conflict
with the main project), so the PyFlink side of the exercise runs through a separate interpreter. If
that environment is not installed, the Python test **skips cleanly** while the Java Table API test
still grades — the Java side is always the graded surface.

## Further reading

Official Flink 2.x docs only:

- Table API:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/tableapi/>
- PyFlink — Table API tutorial:
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/python/table/intro_to_table_api/>
- User-defined functions (scalar/table/aggregate):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/functions/udfs/>
- Concepts & common API (TableEnvironment, planners):
  <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/common/>
