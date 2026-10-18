# SPK-10 · Structured Streaming I — the micro-batch engine and exactly-once

## Why Structured Streaming exists

You already run a kind of streaming: a **dbt incremental model**. Each run, you look at what is new
since the last run (a high-water mark on `updated_at`), transform only that slice, and merge it into
the target. The hard parts are always the same: *where is "last time" remembered?* and *what happens
if a run dies halfway?* If you reprocess an overlapping slice, do you create duplicates or does the
merge absorb them?

Structured Streaming is Spark's answer to those exact questions, generalized and automated. The
mental model is the one that makes everything else click: **a stream is an unbounded table that
grows, and a streaming query is the same query you'd write on a static table, re-run incrementally
as new rows arrive.** The engine, not you, remembers the high-water mark (the source *offsets*) and
guarantees that a failed-and-restarted run produces the same result as if it had never failed. That
guarantee is **exactly-once**, and the thing that makes it real is the **checkpoint**.

## The micro-batch model (objective 5.1)

By default Structured Streaming runs as a sequence of small **micro-batches**. Each micro-batch:

1. asks the source *"what's new since the last committed offset?"*,
2. runs the (unchanged) query plan over just those new rows,
3. writes the output to the sink,
4. **commits** the new offsets to the checkpoint — atomically, after the write.

Because the offset commit happens *after* the write and is recorded durably in the checkpoint, a
crash can only happen in one of two safe places: before the commit (the batch is simply redone on
restart, from the same offsets) or after it (the next batch starts cleanly). Combined with a sink
that either is idempotent or participates in the commit, you get **end-to-end exactly-once**. This
is the streaming analogue of your incremental model's "did we remember the right watermark, and is
the merge idempotent?"

## Sources, sinks, triggers, output modes (objective 5.2)

- **Sources:** files (a directory you drop files into), Kafka (KFK track — not until Phase 3 here),
  sockets (toy), and the `rate` source (synthetic rows, for testing). In this module we use a
  **file source**: new files appearing in a directory are new rows.
- **Sinks:** files, Kafka, `foreachBatch` (run arbitrary batch code per micro-batch — the Swiss
  army knife, used in SPK-11 for `MERGE INTO`), `console`/`memory` (testing), and table sinks. We
  write to an **Iceberg table** via `.toTable("local.db.t")`.
- **Triggers:** how often a micro-batch fires. `processingTime="10 seconds"`, `once`, and
  `availableNow` (process everything available right now, in one or more batches, then stop —
  perfect for deterministic tests and for "incremental batch" jobs).
- **Output modes** (objective 5.3) decide what the sink receives each batch:
  - **append** — only brand-new result rows. Correct for a stream with no aggregation (every input
    row maps to one output row) and for windowed aggregations once a window is finalized.
  - **complete** — the *entire* updated result table every batch. Only valid for aggregations, and
    it rewrites everything each time, so it is for small results / debugging.
  - **update** — only the result rows that changed this batch. For aggregations where you want
    incremental changes.
  Choosing the wrong mode is a classic bug: `append` on a running aggregation is rejected by Spark
  (the aggregate rows keep changing, so there is nothing final to append) unless a watermark lets
  windows close (SPK-11).

## Checkpoints and exactly-once in practice (objective 5.1)

The **`checkpointLocation`** is the whole game. It stores the source offsets, the committed batch
log, and (for stateful queries) the state store. Rules that the exercises make you feel:

- **Always set a checkpoint** for anything you want to recover. No checkpoint → on restart the
  query has no memory of what it already read, so it reprocesses from the beginning (duplicates) or
  can't recover at all.
- **Reuse the SAME checkpoint across restarts.** A *new* checkpoint directory each run is as bad as
  none: the query thinks it has never run, and re-ingests everything. This is the single most common
  "my restart duplicated all my data" bug.
- **Make the sink idempotent or transactional.** The file sink and Iceberg table sink track which
  output files/snapshots belong to which committed batch, so a redone batch doesn't double-write.
  A hand-rolled sink that just appends collected rows is *not* idempotent — a redo duplicates.

The payoff: stop the query, add more input, restart it against the same checkpoint, and the result
is exactly as if it had run continuously — no gaps, no duplicates. That is the property the main
exercise proves directly.

> **Kafka note.** The classic version of this exercise is *Kafka → stream → Iceberg*. Kafka is not
> available until Phase 3 (the KFK track), so here the source is a **file source** instead. The
> exactly-once reasoning — offsets in the checkpoint, idempotent sink — is identical; only the
> source changes. You revisit the Kafka source, consumer offsets and `startingOffsets` in KFK.

## From your world

| dbt / Redshift | Structured Streaming | Note |
|---|---|---|
| Incremental model's high-water mark | source **offsets** in the checkpoint | engine-managed, not a column you pick |
| `is_incremental()` slice | a **micro-batch** | just the new rows |
| Idempotent `MERGE` in an incremental model | idempotent/transactional **sink** | same anti-duplicate goal (SPK-11 does the MERGE) |
| "did the run die mid-way?" | checkpoint + atomic offset commit | **exactly-once** recovery |
| Full refresh | **complete** output mode | rewrites everything |

## When NOT to use Structured Streaming

- **Millisecond latency.** Micro-batch latency is seconds, not milliseconds. For true low-latency,
  per-event processing use Flink (FLK track).
- **A one-off backfill of static files.** If the data is bounded and already here, a plain batch job
  is simpler. (`trigger(availableNow=True)` blurs this line usefully for incremental batch, though.)
- **No checkpoint storage.** If you have nowhere durable to put the checkpoint, you cannot get the
  fault-tolerance guarantees — don't pretend a checkpoint-less stream is exactly-once.

## Further reading

Pinned to Spark **4.0.0** (official docs only):

- Structured Streaming Programming Guide:
  <https://spark.apache.org/docs/4.0.0/structured-streaming-programming-guide.html>
- Output modes & triggers:
  <https://spark.apache.org/docs/4.0.0/structured-streaming-programming-guide.html#output-modes>
- Recovery / checkpointing:
  <https://spark.apache.org/docs/4.0.0/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing>
- Iceberg Spark Structured Streaming writes:
  <https://iceberg.apache.org/docs/latest/spark-structured-streaming/>
