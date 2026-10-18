# SPK-11 · Explain it back (Feynman)

**Prompt A.** "Explain the watermark to someone who builds SCD2 dimensions over late-arriving facts.
What event-time threshold does it track, how does it both bound state and let a window be appended,
and what happens to an event that arrives after it? Why event time and not processing time?"

**Prompt B.** "Walk through a streaming SCD2 upsert with `foreachBatch` + Iceberg `MERGE INTO`. For
a change on a natural key, exactly what happens to the old row and the new row, and what invariants
must hold afterward? Name two mutations that would quietly corrupt the dimension."

## Rubric — a strong explanation hits these (5–7):

- [ ] Defines the watermark as a moving threshold on an **event-time** column (max seen − delay).
- [ ] Explains it **bounds state** (old windows/ids are dropped) and **finalizes windows** so append
      is legal.
- [ ] States that events older than the watermark are **dropped as late**, and that this is a
      deliberate completeness/latency trade-off.
- [ ] Justifies **event time over processing time** by reproducibility/auditability (replay-stable).
- [ ] Describes SCD2 upsert: **close** the old open row (`valid_to` = change time) and **insert** a
      new open row (`valid_to = NULL`), unchanged repeats do nothing.
- [ ] States the invariant: **exactly one open row per natural key**, history preserved (versioning,
      not overwriting).
- [ ] (Bonus) Notes `dropDuplicatesWithinWatermark` for bounded dedup and that `foreachBatch` batch
      DataFrames should be `persist()`ed if reused and the merge kept idempotent for recovery.
