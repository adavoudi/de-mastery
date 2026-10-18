# FLK-08 · Explain it back (Feynman)

**Prompt A.** "Explain how the Table API relates to Flink SQL and to PyFlink. Why do a Java Table API
pipeline and the equivalent PyFlink pipeline produce the same result? What is the dbt analogy?"

**Prompt B.** "A test grades a per-currency GMV aggregation written in both Java Table API and
PyFlink against one oracle. Explain what the test asserts and what it deliberately does NOT assert.
When would you keep logic in Java rather than a Python UDF?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Table API is a programmatic form of the same relational model as SQL (same optimizer,
      same changelog semantics, freely mixable).
- [ ] PyFlink is a Python surface over the same Table API; it builds the same plan and runs on the
      same JVM runtime, so results match the Java version.
- [ ] Maps SQL model → dbt SQL model and PyFlink → dbt Python model.
- [ ] States the test asserts the RESULT equals an independent oracle, not which API was used, not
      code shape, not timing.
- [ ] Explains the Python/JVM boundary cost and that relational work stays in the JVM; hot UDFs are
      better in Java or vectorized.
- [ ] Knows to drop to DataStream process functions for per-record timer/state control.
- [ ] Notes PyFlink runs in a separate environment and the test skips cleanly if it is absent while
      the Java side still grades.
