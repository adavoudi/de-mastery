# SPK-11 · Interview drills

**1. "What exactly does `withWatermark('event_ts', '10 minutes')` track, and what does it enable?"**
> It tells Spark to track the maximum observed `event_ts` and set a watermark 10 minutes behind it.
> That bounds state (windows/ids older than the watermark are dropped), lets windowed aggregations
> finalize and be written in append mode, and defines "late" (events with event_ts below the
> watermark are discarded).

**2. "Event time vs processing time — why does the distinction matter for correctness?"**
> Event time is when the event happened (a data column); processing time is when Spark saw it. Only
> event-time windows are stable under delay, reordering and replay, so they're reproducible — which
> matters for anything audited (SOX). Processing-time windows give different answers on reruns.

**3. "`dropDuplicates` vs `dropDuplicatesWithinWatermark` — trade-off?"**
> `dropDuplicates(['id'])` remembers every id forever (unbounded state, eventually fatal).
> `dropDuplicatesWithinWatermark(['id'])` remembers ids only within the watermark horizon, so state
> is bounded; the cost is that a duplicate arriving after the horizon won't be caught — acceptable
> under the same "we don't wait forever" contract.

**4. "Describe a correct streaming SCD2 upsert and the invariant that proves it's correct."**
> On a change for a natural key: close the current open row (set `valid_to` to the change's
> effective time) and insert a new open row (`valid_from` = change time, `valid_to` = NULL,
> `is_current` = true); an unchanged repeat does nothing. The invariant: **exactly one open row per
> natural key**, with full history preserved as closed versions and no gaps/overlaps in the
> `valid_from`/`valid_to` intervals.

**5. "Why foreachBatch for the merge, and what two operational gotchas does it have?"**
> Because `foreachBatch` exposes the micro-batch as a batch DataFrame, so you can run `MERGE INTO`
> (the streaming sinks can't express a merge). Gotchas: (1) the batch DataFrame is recomputed per
> action — `persist()` it if you use it more than once; (2) make the merge idempotent (key it on the
> natural key + effective time) so a replayed batch on recovery doesn't double-apply.

**6. "A late event arrives after its window finalized. What happens, and how would you change the
behavior?"**
> It is dropped as late (not counted). To tolerate more lateness, increase the watermark delay —
> but that also delays finalization and grows state. If you truly must capture arbitrarily late
> data, you need a different design (e.g. a batch restatement job), not a bigger watermark.
