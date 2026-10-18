# SPK-10 · Interview drills

**1. "What does the checkpointLocation actually store, and why can't you share one between two
different queries?"**
> It stores the log of committed batches (source offsets per batch), query metadata, and — for
> stateful queries — the state store. It identifies the progress of one specific query. If two queries
> share one checkpoint, they corrupt the offset/state records of each other. Use one checkpoint per
> query, and reuse it across the restarts of that query.

**2. "Walk me through why exactly-once needs BOTH the checkpoint and an idempotent sink."**
> The checkpoint guarantees that Spark re-reads the exact same input for a redone batch (no loss, and
> the same rows). The idempotent/transactional sink guarantees that writing that batch again does not
> duplicate the output. With only the checkpoint, the input is correct, but a simple sink still writes
> twice on a redo. With only an idempotent sink, there are no offsets, so you do not know what to
> redo. You need both.

**3. "When is append output mode illegal, and what makes it legal again?"**
> append is illegal on an aggregation whose groups can still change, because there is no final row to
> append. A watermark (SPK-11) lets a window close once event time passes it. After that, the
> result of the window is final and Spark can append it. For non-aggregating streams, append is always
> fine.

**4. "processingTime trigger vs availableNow — when do you use each?"**
> `processingTime="N seconds"` runs a continuous micro-batch stream at a fixed interval. Use it for
> real-time ingestion. `availableNow` processes everything that is present and then stops. It honors
> the checkpoint. It is good for incremental batch jobs (run on a schedule, read only new data). It is
> also good for deterministic tests (no sleeping).

**5. "You restart a file-source → Iceberg stream and see every row twice. Diagnose."**
> Most likely, the job did not reuse the checkpoint (a new dir, or none). So the file source listed
> all files again and ingested them again. To verify, check that the job gets the same
> `checkpointLocation` path in every run, and that the path persists between runs. If the checkpoint
> is correct, suspect a custom sink that is not idempotent. The native Iceberg table sink tracks
> committed batches, so it would not duplicate.
