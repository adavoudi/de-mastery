# SPK-11 · Explain it back (Feynman)

**Prompt A.** "Explain the watermark to someone who builds SCD2 dimensions over late-arriving facts.
Which event-time threshold does it track? How does it bound state and also let Spark append a window?
What happens to an event that arrives after the threshold? Why do we use event time and not processing
time?"

**Prompt B.** "Walk through a streaming SCD2 upsert with `foreachBatch` + Iceberg `MERGE INTO`. A change
arrives for a natural key. What exactly happens to the old row and to the new row? Which invariants must
hold afterward? Name two mutations that would corrupt the dimension without an error."

## Rubric — a strong explanation hits these (5–7):

- [ ] Defines the watermark as a moving threshold on an **event-time** column (max seen − delay).
- [ ] Explains that it **bounds state** (old windows/ids are dropped) and **finalizes windows**, so append
      is legal.
- [ ] States that events older than the watermark are **dropped as late**, and that this is a
      deliberate trade-off between completeness and latency.
- [ ] Justifies **event time over processing time** by reproducibility/auditability (replay-stable).
- [ ] Describes SCD2 upsert: **close** the old open row (`valid_to` = change time) and **insert** a
      new open row (`valid_to = NULL`). Unchanged repeats do nothing.
- [ ] States the invariant: **exactly one open row per natural key**, and history is preserved
      (versioning, not overwriting).
- [ ] (Bonus) Notes `dropDuplicatesWithinWatermark` for bounded dedup. Notes that you should
      `persist()` the `foreachBatch` batch DataFrame if you reuse it, and keep the merge idempotent for
      recovery.
