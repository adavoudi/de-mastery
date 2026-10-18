# FLK-03 · Explain it back (Feynman)

**Prompt A.** "Explain why hourly GMV must use event time, not processing time. Then explain what a
watermark is, how `forBoundedOutOfOrderness(d)` computes it, and what happens to an event that
arrives after the watermark passes its window — both with and without a late side output."

**Prompt B.** "A global hourly rollup stalls one quiet night even though most regions are busy.
Explain the stuck-watermark trap (the minimum rule) and how `withIdleness` fixes it."

## Rubric — a strong explanation hits these (5–7):

- [ ] Event time buckets by when the event happened; processing time puts delayed events in the wrong
      hour and is not reproducible on replay.
- [ ] A watermark of T asserts no further events with timestamp ≤ T are expected; it fires windows
      whose end ≤ T.
- [ ] forBoundedOutOfOrderness(d) emits (max event timestamp seen − d); larger d = more tolerance,
      later firing.
- [ ] Without late handling, an event past the watermark for its window is SILENTLY dropped.
- [ ] allowedLateness(d) keeps the window to re-fire; sideOutputLateData(tag) routes too-late events
      to a stream (a dead-letter for late facts).
- [ ] The operator watermark is the MINIMUM across input partitions, so a quiet partition freezes it
      and stalls all windows.
- [ ] withIdleness(d) marks a quiet partition idle so it's excluded from the minimum and windows fire
      again on active partitions.
