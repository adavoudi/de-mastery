# Explain it back — STH-07

**Prompt (Feynman).** A teammate says: "The dashboard is slow, so just point Trino straight at
the raw Paimon fact table and add more CPU." In your own words, explain why a serving layer on a
streamhouse reads a *small pre-aggregated* table instead, how you read the *merged* state of a
primary-key table (not its changelog), and how you'd prove the OLAP answer is correct. Use the
revenue-per-company-per-day mart as your running example.

## Rubric — your explanation should hit these (5–7)

- [ ] **Two read modes.** The same lake table serves a streaming (changelog) read and a
      batch/OLAP (snapshot) read; the serving layer is the batch/OLAP one, for *interactive*
      queries.
- [ ] **Merged state, not changelog.** For a PK / merge-engine table you read the latest value
      per key with merges applied (pypaimon / pyiceberg batch snapshot), never iterating the
      unbounded changelog.
- [ ] **Pre-aggregation.** The dashboard hits a small rollup (revenue per company per day), not
      the raw fact; rows read drop by orders of magnitude.
- [ ] **Pruning & projection.** A predicate (one company, a date range) lets a columnar engine
      skip row groups and columns; you can see this in the query plan.
- [ ] **Plan, not timing, is the proof.** You verify the dashboard query reads the rollup (via
      `EXPLAIN`); wall-clock is only a generous guardrail (warm cache, median of N).
- [ ] **Correctness invariant.** The OLAP answer over the merged snapshot must equal a batch
      rebuild of the same events (G6); disagreement means a stale snapshot or double-counted
      un-merged versions.
- [ ] **When not to.** Drill-to-detail on one key is a point lookup, not a rollup; a pipeline
      consumer should read the changelog, not an OLAP snapshot.
