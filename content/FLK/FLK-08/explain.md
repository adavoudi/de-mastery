# FLK-08 · Explain it back (Feynman)

**Prompt A.** "Explain how the Table API relates to Flink SQL and to PyFlink. Why do a Java Table API
pipeline and the equivalent PyFlink pipeline produce the same result? What is the dbt analogy?"

**Prompt B.** "A test grades a per-currency GMV aggregation. You write it in both Java Table API and
PyFlink, and the test compares both to one oracle. Explain what the test asserts and what it deliberately does NOT assert.
When do you keep logic in Java and not in a Python UDF?"

## Rubric — a strong explanation hits these (5–7):

- [ ] The Table API is a programmatic form of the same relational model as SQL (same optimizer,
      same changelog semantics, you can mix them freely).
- [ ] PyFlink is a Python surface over the same Table API. It builds the same plan and runs on the
      same JVM runtime, so the results match the Java version.
- [ ] Maps SQL model → dbt SQL model and PyFlink → dbt Python model.
- [ ] States that the test asserts that the RESULT equals an independent oracle. It does not assert which API was used,
      the code shape or the timing.
- [ ] Explains the cost of the Python/JVM boundary. Explains that relational work stays in the JVM. Hot UDFs are
      better in Java or vectorized.
- [ ] Knows to use DataStream process functions for control of timers and state for each record.
- [ ] Notes that PyFlink runs in a separate environment. The test skips cleanly if the environment is absent, and
      the Java side still grades.
