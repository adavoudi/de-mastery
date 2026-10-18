# FLK-03 · Explain it back (Feynman)

**Prompt A.** "Explain why hourly GMV must use event time, not processing time. Then explain what a
watermark is and how `forBoundedOutOfOrderness(d)` computes it. Explain what happens to an event
that arrives after the watermark passes its window. Cover both cases: with a late side output and
without one."

**Prompt B.** "A global hourly rollup stalls one quiet night, but most regions are busy. Explain the
stuck-watermark trap (the minimum rule). Explain how `withIdleness` fixes it."

## Rubric — a strong explanation hits these (5–7):

- [ ] Event time buckets by when the event happened. Processing time puts delayed events in the wrong
      hour and is not reproducible on replay.
- [ ] A watermark of T says that Flink expects no more events with timestamp ≤ T. It fires windows
      whose end ≤ T.
- [ ] forBoundedOutOfOrderness(d) emits (max event timestamp seen − d). A larger d gives more tolerance
      and later firing.
- [ ] Without late handling, an event past the watermark for its window is SILENTLY dropped.
- [ ] allowedLateness(d) keeps the window so that it can re-fire. sideOutputLateData(tag) routes
      too-late events to a stream (a dead-letter for late facts).
- [ ] The watermark of the operator is the MINIMUM across input partitions. So a quiet partition
      freezes it and stalls all windows.
- [ ] withIdleness(d) marks a quiet partition idle. Flink excludes it from the minimum, and windows
      fire again on the active partitions.
