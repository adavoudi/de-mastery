# SPK-11 · Structured Streaming II — watermarks, state, and streaming SCD2

## Why watermarks and state exist

The streams in SPK-10 were *stateless*. Each input row produced an output row. Nothing had to be
remembered between rows. The more interesting streams are not like that. These all must **remember**
something across micro-batches:

- a per-window count
- a dedup
- a stream-stream join
- an SCD2 upsert

They remember the running count for each window, the ids that they saw recently, or the current version
of each dimension row. That memory is **state**. Spark keeps it in the state store inside the checkpoint.

State cannot grow forever. Suppose you keep the count of every window and every id that you ever saw.
The memory grows without a limit, and the job eventually dies. The **watermark** bounds the state. It is
a moving threshold on **event time**. It says *"I will not wait for data older than this."* When event
time moves past the end of a window plus the watermark delay, the window is **finalized**. Spark can
emit its result (even in `append` mode), and it drops the state of the window. Data that arrives
*later* than the watermark is **late**, and Spark discards it.

You know this problem from **SCD2 dimensions over late-arriving facts**. How long do you keep a dimension
"open" and wait for a correction? What do you do with a change that arrives after you closed the
version? The watermark is the streaming form of the decision "how long do we wait".

## Event time vs processing time

**Event time** is when the event happened (a column in the data: `event_ts`). **Processing time**
is when Spark saw the event. Correct streaming analytics use *event time*. Event time is the only
time that stays stable when data is replayed, reordered, or delayed. Suppose you make windows by
processing time. Then the same input gives different results, depending on when the job ran. This is a
serious error for anything that must be reproducible (think SOX). You define the watermark on an
**event-time** column:

```python
events.withWatermark("event_ts", "10 minutes") \
      .groupBy(window("event_ts", "5 minutes"), "company_id") \
      .count()
```

Here, Spark tracks the maximum `event_ts` that it saw. The watermark is 10 minutes behind it. Spark
finalizes a 5-minute window `[00:00, 00:05)` when the watermark passes `00:05`. This happens when an
event with `event_ts >= 00:15` arrives. Spark drops events for that window that arrive after that, as late.

![A 5-minute window is finalized, emitted once and its state dropped when the watermark (max event time minus 10 min) passes its end; later events for it are late](diagrams/watermark-event-time.svg)

## Stateful aggregation and the output modes it unlocks

With a watermark, you can write a windowed aggregation in **append** mode. Spark emits each window
exactly once, when it finalizes. (Without a watermark, only `complete`/`update` are legal — SPK-10.)
This is a clean and bounded way to produce "the event count per 5-minute window per company". You can
append this result to a fact table.

This is the key invariant that you can test (docs/TESTING.md). Suppose all input arrived and the
watermark passed every window. Then **the streaming result equals the batch result over the same
in-window events.** The batch answer does not include late events (beyond the watermark). Spark dropped them
correctly.

## Streaming deduplication

Duplicates are a normal part of streaming (at-least-once sources, retries). There are two tools:

- `dropDuplicates(["id"])` — remembers every id forever. It is correct, but the state is unbounded.
- `dropDuplicatesWithinWatermark(["id"])` — needs a watermark on event time. It remembers ids only
  within the watermark horizon, and then forgets them. The state is bounded. The cost is this: Spark
  does not recognize a duplicate that arrives *after* the horizon. This is acceptable, because it is
  the same contract "we don't wait forever". This function is new in Spark 3.5+/4.x. It is the
  correct dedup for production.

## foreachBatch + Iceberg MERGE INTO: streaming SCD2

The sink for a dimension upsert is not a plain append. It is a **merge**. `foreachBatch` gives you
each micro-batch as an ordinary (batch) DataFrame. So you can run any batch code, including an
Iceberg `MERGE INTO`. This is the SCD2 pattern:

- Each incoming change for a natural key that **differs** from the current open version must do two things:
  1. **Close** the current open row. Set its `valid_to` to the effective time of the change, so
     that the row is no longer open.
  2. **Insert** a new open row (new `valid_from`, `valid_to = NULL`, `is_current = true`).
- An unchanged repeat must do nothing.

A naive solution uses two MERGE passes (close, then insert). You can also use one MERGE with careful
matched/not-matched clauses. A correct SCD2 must satisfy these invariants. The mutants in the exercise
violate them:

- **Exactly one open (`valid_to IS NULL`) row per natural key.** (A mutant that overwrites instead
  of versioning, or that does not close the old row, breaks this.)
- **Versioning, not overwriting.** History is preserved. Old versions remain with a closed
  `valid_to`. (A mutant that overwrites loses history.)
- **Correct effective dating.** The `valid_from` of the new version equals the change time. The
  `valid_to` of the old version equals that same instant (no gaps, no overlaps).

These are the operational notes for `foreachBatch`:

- Spark re-evaluates the batch DataFrame for each action. Call `.persist()` if you use it more than once.
- Make the merge **idempotent**, so that a redone batch (recovery) does not apply twice. Key the merge
  on the natural key + effective time to achieve this.
- Use a **watermark** upstream, so that the state (and any dedup) stays bounded.

## From your world

| Kimball / dbt / SOX | Structured Streaming | Note |
|---|---|---|
| SCD2 dimension with `valid_from`/`valid_to`/`is_current` | `foreachBatch` + Iceberg `MERGE INTO` | the merge, streamed |
| "how long do we wait for a late fact?" | the **watermark** delay | the explicit wait bound |
| Event timestamp on the fact | **event-time** column + `withWatermark` | never processing time |
| Reproducible restatements (SOX) | event time + versioned history | replay-stable, auditable |
| Dedup a reloaded extract | `dropDuplicatesWithinWatermark` | bounded, keyed on the business id |

## When NOT to use these

- **Don't make windows by processing time** for anything that must be reproducible. Use event time.
- **Don't use unbounded `dropDuplicates`** on a high-volume stream. The state grows forever. Use
  the watermarked form.
- **Don't set a huge watermark delay** to "never lose late data". If you do, Spark also never finalizes
  windows and never frees state. The delay is a real trade-off between latency and completeness. Choose
  it on purpose.
- **Don't overwrite a dimension row in place** when you need history. That is SCD1. SCD2 keeps versions.

## Further reading

Pinned to Spark **4.0.0** (official docs only):

- Window operations & watermarks:
  <https://spark.apache.org/docs/4.0.0/structured-streaming-programming-guide.html#window-operations-on-event-time>
- Handling late data & watermarking:
  <https://spark.apache.org/docs/4.0.0/structured-streaming-programming-guide.html#handling-late-data-and-watermarking>
- Streaming deduplication (incl. `dropDuplicatesWithinWatermark`):
  <https://spark.apache.org/docs/4.0.0/structured-streaming-programming-guide.html#streaming-deduplication>
- `foreachBatch`:
  <https://spark.apache.org/docs/4.0.0/structured-streaming-programming-guide.html#foreachbatch>
- Iceberg `MERGE INTO`: <https://iceberg.apache.org/docs/latest/spark-writes/#merge-into>
