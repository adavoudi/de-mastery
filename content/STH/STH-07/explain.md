# Explain it back — STH-07

**Prompt (Feynman).** A teammate says: "The dashboard is slow, so just point Trino straight at
the raw Paimon fact table and add more CPU." Explain in your own words why a serving layer on a
streamhouse reads a *small pre-aggregated* table instead. Explain how you read the *merged* state of
a primary-key table (not its changelog). Explain how you would prove that the OLAP answer is
correct. Use the revenue-per-company-per-day mart as your running example.

## Rubric — your explanation should hit these (5–7)

- [ ] **Two read modes.** The same lake table serves a streaming (changelog) read and a
      batch/OLAP (snapshot) read. The serving layer is the batch/OLAP one, for *interactive*
      queries.
- [ ] **Merged state, not changelog.** For a PK / merge-engine table, you read the latest value
      per key with merges applied (pypaimon / pyiceberg batch snapshot). You never iterate the
      unbounded changelog.
- [ ] **Pre-aggregation.** The dashboard reads a small rollup (revenue per company per day), not
      the raw fact. The rows that it reads drop by orders of magnitude.
- [ ] **Pruning & projection.** A predicate (one company, a date range) lets a columnar engine
      skip row groups and columns. You can see this in the query plan.
- [ ] **The plan, not the timing, is the proof.** You verify that the dashboard query reads the
      rollup (via `EXPLAIN`). Wall-clock time is only a generous guardrail (warm cache, median of
      N).
- [ ] **Correctness invariant.** The OLAP answer over the merged snapshot must equal a batch
      rebuild of the same events (G6). A disagreement means a stale snapshot or double-counted
      un-merged versions.
- [ ] **When not to.** Drill-to-detail on one key is a point lookup, not a rollup. A pipeline
      consumer should read the changelog, not an OLAP snapshot.
