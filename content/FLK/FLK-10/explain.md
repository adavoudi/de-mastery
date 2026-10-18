# FLK-10 · Explain it back (Feynman)

**Prompt A.** "Trace a single row through a Flink CDC → Iceberg pipeline. The row has a Debezium insert, then an
update, then a delete. What RowKinds does the `debezium-json` source emit? How does the Iceberg
upsert sink apply each one? What does the final table contain?"

**Prompt B.** "Explain 'commit on checkpoint'. Why does the commit of one Iceberg snapshot for each
completed checkpoint give exactly-once? What happens on recovery after a crash? Then explain why
`ALTER TABLE … ADD (region STRING)` is safe for rows that already exist."

## Rubric — a strong explanation hits these (5–7):

- [ ] `debezium-json` maps op c→+I, u→(-U,+U), d→-D.
- [ ] The target Iceberg table must be v2 + PRIMARY KEY, so updates and deletes apply by key (not append).
- [ ] The sink buffers changes. It commits an Iceberg snapshot only when a checkpoint completes.
- [ ] Commit-on-checkpoint + idempotent committer per checkpoint id = exactly-once (no duplicates and no loss on recovery).
- [ ] Final table = current source state (updated keys have the latest values, deleted keys are absent).
- [ ] Schema evolution is metadata-only: old rows read NULL for the new column, no rewrite.
- [ ] The streaming result reconciles with a batch fold of the same change events (the oracle).
