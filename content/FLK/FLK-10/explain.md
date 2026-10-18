# FLK-10 · Explain it back (Feynman)

**Prompt A.** "Trace a single row through a Flink CDC → Iceberg pipeline: a Debezium insert, then an
update, then a delete. What RowKinds does the `debezium-json` source emit, and how does the Iceberg
upsert sink apply each one? What does the final table contain?"

**Prompt B.** "Explain 'commit on checkpoint'. Why does committing one Iceberg snapshot per
completed checkpoint give exactly-once, and what happens on recovery after a crash? Then explain why
`ALTER TABLE … ADD (region STRING)` is safe for rows that already exist."

## Rubric — a strong explanation hits these (5–7):

- [ ] `debezium-json` maps op c→+I, u→(-U,+U), d→-D.
- [ ] The target Iceberg table must be v2 + PRIMARY KEY so updates/deletes apply by key (not append).
- [ ] The sink buffers and commits an Iceberg snapshot only on checkpoint completion.
- [ ] Commit-on-checkpoint + idempotent committer per checkpoint id = exactly-once (no dup/no loss on recovery).
- [ ] Final table = current source state (updated keys latest, deleted keys absent).
- [ ] Schema evolution is metadata-only: old rows read NULL for the new column, no rewrite.
- [ ] The streaming result reconciles with a batch fold of the same change events (the oracle).
