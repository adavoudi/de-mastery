# FLK-08 · Table API & PyFlink — one pipeline, two languages

## Why the Table API (and PyFlink) exist

Flink SQL (FLK-04) is the fastest way to express a relational pipeline. But a string of SQL is hard
to compose, to parameterize and to unit-test. The **Table API** is the same relational model as a
*programmatic* API. This is the Table API form of `SELECT ccy, SUM(amt) ... GROUP BY ccy`:
`table.groupBy($("ccy")).select($("ccy"), $("amt").sum())`.

The Table API plans through the **same optimizer** as SQL. It produces the **same changelog semantics**.
So you can mix SQL and Table API freely and get identical results. Use the Table API when you want to compose
the pipeline in the host language. Then you build the pipeline from functions, inject parameters and reuse
fragments. You still keep the declarative power of SQL.

**PyFlink** is the Python entry point to that same Table API (and to DataStream). For an analytics
engineer, this is the natural way to start. It feels like pandas or dbt-Python, but it runs on the JVM engine
underneath. The Python API builds the same logical plan and sends it to the same runtime. So a
Python Table pipeline and the equivalent Java pipeline are **the same job**. They must produce the same
answer over the same data.

This is the purpose of this module. You write one pipeline (a per-currency
GMV aggregation) in **both** the Java Table API and PyFlink. Then you prove that they agree against one oracle.

## How the two map to each other

| Concept | Java Table API | PyFlink Table API |
|---|---|---|
| Create the environment | `TableEnvironment.create(EnvironmentSettings.inBatchMode())` | `TableEnvironment.create(EnvironmentSettings.in_batch_mode())` |
| Build a table from rows | `t.fromValues(DataTypes.ROW(...), rows)` | `t.from_elements([...], schema)` |
| Group + aggregate | `tbl.groupBy($("ccy")).select($("ccy"), $("amt").sum().as("gmv"))` | `tbl.group_by(col("ccy")).select(col("ccy"), col("amt").sum.alias("gmv"))` |
| Run SQL instead | `t.executeSql("SELECT ...")` | `t.execute_sql("SELECT ...")` |
| Collect results | `tableResult.collect()` → `Row` | `table_result.collect()` → `Row` |

Both languages can also run **SQL** with `executeSql` / `execute_sql`. Then the SQL text is
identical in the two languages. The grader does not care which surface you use (DataStream, Table API or
SQL). It only checks that the **result** equals the oracle. This follows docs/TESTING.md ("do not
assert which API was called").

## UDFs (scalar / table / aggregate)

Sometimes the built-in functions are not enough. Then both languages let you register user-defined functions.
There are three kinds:

- A **scalar** UDF: one row in, one value out.
- A **table** UDF: one row in, many rows out. It is like a lateral join or `explode`.
- An **aggregate** UDF: many rows in, one value out. It is a custom function like `SUM`.

Keep UDFs pure and deterministic, so results reconcile with a batch oracle. (The grader does not test UDFs
here. See the Further reading.)

## From your world

| dbt / warehouse | Flink | Note |
|---|---|---|
| A dbt SQL model (SELECT → table) | Flink SQL / Table API | declarative, same optimizer |
| A dbt **Python** model | PyFlink Table API | same model, Python surface |
| `GROUP BY ccy` GMV | `groupBy(ccy).sum(amt)` | the exercise pipeline |
| A reusable macro | a Java/Python function returning a `Table` | host-language composition |

## When NOT to use (each)

- **Pure, static SQL with no composition.** Write Flink SQL. If you wrap it in Table API calls,
  you add nothing.
- **PyFlink for ultra-low-latency, UDF-heavy jobs.** Each record crosses the Python/JVM boundary,
  and this is costly. Keep hot UDFs in Java. Or use vectorized (pandas) UDFs. Or stay in SQL/Table API, so the
  work runs in the JVM.
- **Table API when you need custom timers or state for each record.** Use the DataStream process
  functions (FLK-07) instead. The Table API is relational. It does not give event-at-a-time control.

## PyFlink runs in a separate environment

In this platform, PyFlink lives in a **dedicated virtual environment**. Its dependency pins conflict
with the main project. So the PyFlink side of the exercise runs through a separate interpreter. Suppose
that environment is not installed. Then the Python test **skips cleanly** and the Java Table API test
still grades. The Java side is always the graded surface.

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
