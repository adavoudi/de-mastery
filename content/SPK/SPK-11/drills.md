# SPK-11 · Interview drills

**1. "What exactly does `withWatermark('event_ts', '10 minutes')` track, and what does it enable?"**
> It tells Spark to track the maximum observed `event_ts` and to set a watermark 10 minutes behind it.
> It does three things:
> - It bounds state (Spark drops windows/ids older than the watermark).
> - It lets windowed aggregations finalize, so you can write them in append mode.
> - It defines "late" (Spark discards events with event_ts below the watermark).

**2. "Event time vs processing time — why does the distinction matter for correctness?"**
> Event time is when the event happened (a data column). Processing time is when Spark saw it. Only
> event-time windows are stable under delay, reordering and replay, so they are reproducible. This
> matters for anything that is audited (SOX). Processing-time windows give different answers when you
> rerun them.

**3. "`dropDuplicates` vs `dropDuplicatesWithinWatermark` — trade-off?"**
> `dropDuplicates(['id'])` remembers every id forever (unbounded state, which is eventually fatal).
> `dropDuplicatesWithinWatermark(['id'])` remembers ids only within the watermark horizon, so the state
> is bounded. The cost is that it does not catch a duplicate that arrives after the horizon. This is
> acceptable under the same contract "we don't wait forever".

**4. "Describe a correct streaming SCD2 upsert and the invariant that proves it's correct."**
> When a change arrives for a natural key, do these things: close the current open row (set `valid_to`
> to the effective time of the change). Insert a new open row (`valid_from` = change time,
> `valid_to` = NULL, `is_current` = true). An unchanged repeat does nothing. The invariant is this:
> **exactly one open row per natural key**. Full history stays as closed versions, and there are no
> gaps or overlaps in the `valid_from`/`valid_to` intervals.

**5. "Why foreachBatch for the merge, and what two operational gotchas does it have?"**
> `foreachBatch` gives you the micro-batch as a batch DataFrame, so you can run `MERGE INTO`. (The
> streaming sinks cannot express a merge.) These are the two gotchas:
> 1. Spark recomputes the batch DataFrame for each action. Call `persist()` if you use it more than once.
> 2. Make the merge idempotent (key it on the natural key + effective time). Then a batch that Spark
>    replays on recovery does not apply twice.

**6. "A late event arrives after its window finalized. What happens, and how would you change the
behavior?"**
> Spark drops it as late (it is not counted). To tolerate more lateness, increase the watermark delay.
> But this also delays finalization and makes the state bigger. Suppose you must capture data that
> arrives very late, with no limit. Then you need a different design (for example, a batch restatement
> job), not a bigger watermark.
