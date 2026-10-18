# KFK-01 · Explain it back (Feynman)

**Prompt A.** "Explain why a Kafka partition is a log and not a queue, and how 'the current state'
relates to 'the events'. Use the Iceberg-snapshot analogy: what plays the role of a snapshot id,
and what plays the role of time travel?"

**Prompt B.** "A new analytics team wants to build a table from a topic that has been running for a
year. Explain exactly how they rebuild it from scratch, including how they'd start 'from 09:00 last
Monday' instead of the very beginning, and what could stop them from going back that far."

## Rubric — a strong explanation hits these (5–7):

- [ ] Defines a partition as an append-only, immutable, ordered log with monotonic offsets.
- [ ] Distinguishes events (immutable facts) from state (a fold/projection over events).
- [ ] Maps offset ⇄ Iceberg snapshot id and replay ⇄ time travel.
- [ ] Explains replay from an **offset** (assign + read forward).
- [ ] Explains replay from a **timestamp** via `offsets_for_times` (first offset ≥ ts).
- [ ] Notes retention (time/size) and that replay only reaches the earliest *retained* offset.
- [ ] (Bonus) Notes per-key ordering within a partition and compaction as "latest per key".
