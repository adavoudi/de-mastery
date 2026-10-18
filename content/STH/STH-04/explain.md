# STH-04 · Explain it back (Feynman)

**Prompt A.** "A teammate streams a CDC feed into an Iceberg table with a 1-second checkpoint
interval and complains that Trino queries got slow after a day. Explain, from first principles, why
the checkpoint interval controls both freshness and the number of small files, what the trade-off
is, and how compaction fits in."

**Prompt B.** "You write the *same* CDC feed into Iceberg v2 and into Paimon. Explain how each
applies an update to an existing key, why Iceberg leans toward merge-on-read with delete files while
Paimon uses an LSM tree with compaction, and which you'd pick for a mirror of a high-churn OLTP
table versus a mostly-append event log."

## Rubric — a strong explanation hits these (5–7):

- [ ] A Flink Iceberg sink commits one snapshot **per checkpoint**; the checkpoint interval is the
      freshness dial.
- [ ] Each commit writes ≥1 data file per writer task per partition, so a short interval ⇒ many
      snapshots ⇒ many small files.
- [ ] Freshness and file count are the *same* knob; they trade off directly.
- [ ] Compaction (`rewrite_data_files`) decouples them after the fact and is a necessary cost, not a
      bug fix.
- [ ] A bounded source lets the streaming job finish and fire its final commit; an unbounded source
      with no checkpoints never commits (the hang).
- [ ] Iceberg v2 applies updates via row-level (equality/positional) delete files — merge-on-read;
      Paimon applies them as new LSM runs merged by background compaction.
- [ ] Picks correctly: Paimon LSM for high-rate upserts to the same keys; Iceberg for mostly-append
      feeds with occasional updates.
