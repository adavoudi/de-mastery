# FLK-04 · Explain it back (Feynman)

**Prompt A.** "Explain the stream–table duality and the four RowKinds (+I, -U, +U, -D). Walk through
the exact changelog an unbounded `SELECT k, COUNT(*) FROM t GROUP BY k` emits as the rows
(k='x'), (k='y'), (k='x') arrive."

**Prompt B.** "Explain the difference between an append-only query and an updating query, give an
example of each, and say which Kafka sink each one needs and why."

## Rubric — a strong explanation hits these (5–7):

- [ ] A Flink SQL query produces a dynamic table (continuously-updating result) whose changes are a
      changelog stream.
- [ ] Names the RowKinds: +I insert, -U retract old, +U new value, -D delete.
- [ ] Traces the GROUP BY changelog: +I(x,1), +I(y,1), then -U(x,1) +U(x,2) when the second x arrives.
- [ ] Append-only = only +I, past output never changes (filter/project, window TVF); updating =
      -U/+U/-D, past output is revised (unbounded GROUP BY, latest-per-key).
- [ ] Append-only → plain `kafka` sink; updating → `upsert-kafka` keyed by the grain (null = delete).
- [ ] The final materialized state after all input equals the batch answer over the same events.
- [ ] (Bonus) A window TVF aggregation is append-only (one row per closed window), unlike unbounded
      GROUP BY.
