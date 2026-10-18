# SPK-10 · Explain it back (Feynman)

**Prompt A.** "Explain how Structured Streaming gives exactly-once output across a crash, using the
micro-batch loop. Where exactly are offsets committed, and why does that ordering (plus an
idempotent sink) prevent both duplicates and loss? Tie it to how a dbt incremental model remembers
its high-water mark."

**Prompt B.** "A teammate's streaming job to Iceberg duplicates all its data every time it
restarts. Give the two most likely causes involving the checkpoint, and the one involving the sink,
and how you'd verify each."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes the micro-batch loop: read new offsets → run plan → write sink → commit offsets.
- [ ] States that the offset commit happens **after** the write and is stored in the checkpoint.
- [ ] Explains why that ordering means a crash either redoes a batch or starts clean (no loss).
- [ ] Names the idempotent/transactional **sink** as the other half of exactly-once.
- [ ] Identifies "no checkpoint" and "a new checkpoint dir each run" as duplicate-causing bugs.
- [ ] Maps it to the dbt incremental high-water mark and idempotent MERGE.
- [ ] (Bonus) Picks the right output mode (append for non-aggregating streams) and notes
      `availableNow` for deterministic/incremental runs.
