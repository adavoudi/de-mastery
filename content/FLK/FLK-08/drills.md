# FLK-08 · Interview drills

**1. "Table API vs Flink SQL — when and why?"**
> They have the same relational model, the same optimizer and the same changelog semantics. The Table API is the
> programmatic form. Use it when you want to compose in the host language (build the pipeline from
> functions, parameterize it, reuse fragments). You still keep the declarative power of SQL. You can mix the
> two in one job.

**2. "Will a PyFlink pipeline give the same result as the Java Table API version?"**
> Yes. PyFlink is a Python surface over the same Table API. It builds the same logical plan and runs
> on the same JVM engine. So over the same input the two are effectively the same job and must agree.
> It is the streaming analogue of a dbt Python model vs a dbt SQL model.

**3. "A grader checks a GMV aggregation. What should it assert?"**
> The aggregated RESULT equals an independent oracle (DuckDB / a Python sum). It does not matter which API
> produced the result. The grader must NOT assert which API was called, the code shape, the column order or the wall-clock
> time. It asserts only behavior and data, per the testing policy.

**4. "Scalar vs table vs aggregate UDF?"**
> Scalar: one row → one value. Table: one row → many rows (lateral/explode). Aggregate: many rows →
> one value (a custom function like SUM). Keep them pure and deterministic, so results reconcile with a batch
> oracle.

**5. "Why can a Python UDF hurt performance, and what do you do about it?"**
> A row-at-a-time Python UDF serializes each record across the Python/JVM boundary. You have three options.
> Keep the work in relational Table API/SQL (it runs in the JVM). Or use vectorized (pandas) UDFs to batch the crossing. Or
> implement the hot UDF in Java.

**6. "When is the Table API the wrong tool?"**
> When you need custom timers for each record or fine-grained control of keyed state. That is the
> DataStream process-function world (FLK-07). The Table API is relational. It does not give event-at-a-time control.
