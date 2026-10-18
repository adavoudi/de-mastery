# SPK-10 · Explain it back (Feynman)

**Prompt A.** "Explain how Structured Streaming gives exactly-once output across a crash. Use the
micro-batch loop. Where exactly does Spark commit the offsets? Why does that order (plus an
idempotent sink) prevent both duplicates and loss? Connect it to how a dbt incremental model remembers
its high-water mark."

**Prompt B.** "A streaming job to Iceberg duplicates all its data every time it restarts. Give the two
most likely causes that involve the checkpoint. Give the most likely cause that involves the sink.
Explain how you would verify each cause."

## Rubric — a strong explanation hits these (5–7):

- [ ] Describes the micro-batch loop: read new offsets → run plan → write sink → commit offsets.
- [ ] States that the offset commit happens **after** the write and is stored in the checkpoint.
- [ ] Explains why that order means a crash either redoes a batch or starts clean (no loss).
- [ ] Names the idempotent/transactional **sink** as the other half of exactly-once.
- [ ] Identifies "no checkpoint" and "a new checkpoint dir in each run" as bugs that cause duplicates.
- [ ] Maps it to the dbt incremental high-water mark and idempotent MERGE.
- [ ] (Bonus) Picks the right output mode (append for non-aggregating streams) and notes
      `availableNow` for deterministic/incremental runs.
