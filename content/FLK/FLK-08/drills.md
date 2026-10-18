# FLK-08 · Interview drills

**1. "Table API vs Flink SQL — when and why?"**
> Same relational model, same optimizer, same changelog semantics — the Table API is the
> programmatic form you reach for when you want host-language composition (build the pipeline from
> functions, parameterize it, reuse fragments) while keeping SQL's declarative power. You can mix the
> two in one job.

**2. "Will a PyFlink pipeline give the same result as the Java Table API version?"**
> Yes. PyFlink is a Python surface over the same Table API; it builds the same logical plan and runs
> on the same JVM engine, so over the same input the two are effectively the same job and must agree.
> It is the streaming analogue of a dbt Python model vs a dbt SQL model.

**3. "A grader checks a GMV aggregation. What should it assert?"**
> The aggregated RESULT equals an independent oracle (DuckDB / a Python sum), regardless of which API
> produced it. It must NOT assert which API was called, the code shape, column order, or wall-clock
> time — only behavior/data, per the testing policy.

**4. "Scalar vs table vs aggregate UDF?"**
> Scalar: one row → one value. Table: one row → many rows (lateral/explode). Aggregate: many rows →
> one value (a custom SUM-like). Keep them pure and deterministic so results reconcile with a batch
> oracle.

**5. "Why can a Python UDF hurt performance, and what do you do about it?"**
> A row-at-a-time Python UDF serializes across the Python/JVM boundary per record. Keep the work in
> relational Table API/SQL (runs in the JVM), use vectorized (pandas) UDFs to batch the crossing, or
> implement the hot UDF in Java.

**6. "When is the Table API the wrong tool?"**
> When you need per-record custom timers or fine-grained keyed state control — that is the
> DataStream process-function world (FLK-07). The Table API is relational, not event-at-a-time.
