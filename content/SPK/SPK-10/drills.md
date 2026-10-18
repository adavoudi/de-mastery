# SPK-10 · Interview drills

**1. "What does the checkpointLocation actually store, and why can't you share one between two
different queries?"**
> It stores the committed batch log (source offsets per batch), query metadata, and — for stateful
> queries — the state store. It identifies a specific query's progress, so two queries sharing one
> checkpoint would corrupt each other's offset/state bookkeeping. One checkpoint per query, reused
> across that query's restarts.

**2. "Walk me through why exactly-once needs BOTH the checkpoint and an idempotent sink."**
> The checkpoint guarantees Spark re-reads the exact same input for a redone batch (no loss, and the
> same rows). The idempotent/transactional sink guarantees that re-writing that batch doesn't
> duplicate output. Checkpoint alone → correct input but a dumb sink still double-writes on redo;
> idempotent sink alone → without offsets you don't know what to redo. You need both.

**3. "When is append output mode illegal, and what makes it legal again?"**
> append is illegal on an aggregation whose groups can still change, because there's no final row to
> append. A watermark (SPK-11) lets a window close once event time passes it; after that the
> window's result is final and can be appended. For non-aggregating streams append is always fine.

**4. "processingTime trigger vs availableNow — when do you use each?"**
> `processingTime="N seconds"` runs a continuous micro-batch stream at a cadence — real-time
> ingestion. `availableNow` drains whatever is present and stops, honoring the checkpoint — perfect
> for incremental batch jobs (run on a schedule, pick up only new data) and for deterministic tests
> (no sleeping).

**5. "You restart a file-source → Iceberg stream and see every row twice. Diagnose."**
> Most likely the checkpoint was not reused (a new dir, or none), so the file source re-listed and
> re-ingested all files. Verify by checking that the same `checkpointLocation` path is passed every
> run and that it persists between runs. If the checkpoint is correct, suspect a non-idempotent
> custom sink; the native Iceberg table sink tracks committed batches and wouldn't duplicate.
