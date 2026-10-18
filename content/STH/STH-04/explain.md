# STH-04 · Explain it back (Feynman)

**Prompt A.** "A teammate streams a CDC feed into an Iceberg table with a 1-second checkpoint
interval. The teammate complains that Trino queries got slow after a day. Explain from first
principles why the checkpoint interval controls both freshness and the number of small files. Say
what the trade-off is. Say how compaction fits in."

**Prompt B.** "You write the *same* CDC feed into Iceberg v2 and into Paimon. Explain how each one
applies an update to an existing key. Explain why Iceberg leans toward merge-on-read with delete
files, while Paimon uses an LSM tree with compaction. Say which one you would pick for a mirror of
a high-churn OLTP table, and which one for a mostly-append event log."

## Rubric — a strong explanation hits these (5–7):

- [ ] A Flink Iceberg sink commits one snapshot **per checkpoint**. The checkpoint interval is the
      freshness setting.
- [ ] Each commit writes ≥1 data file per writer task per partition. So a short interval ⇒ many
      snapshots ⇒ many small files.
- [ ] Freshness and file count are the *same* setting. They trade off directly.
- [ ] Compaction (`rewrite_data_files`) separates them after the fact. It is a necessary cost, not
      a bug fix.
- [ ] A bounded source lets the streaming job finish and fire its final commit. An unbounded source
      with no checkpoints never commits (the hang).
- [ ] Iceberg v2 applies updates through row-level (equality/positional) delete files, which is
      merge-on-read. Paimon applies them as new LSM runs that background compaction merges.
- [ ] You pick correctly: Paimon LSM for high-rate upserts to the same keys. Iceberg for mostly-append
      feeds with occasional updates.
