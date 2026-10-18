# A watermark strategy that survives an idle partition

A global rollup that runs fine in testing can stall in production the first quiet
night: one partition goes silent, the watermark minimum freezes, and every window
stops firing. The fix is a watermark strategy that tolerates out-of-order events,
reads event time from the data, **and** declares idleness.

## Task

Implement `de.flk.IdleWatermark.strategy(long outOfOrdernessMs, long idleMs)` to
return a `WatermarkStrategy<Event>` that:

1. tolerates bounded out-of-orderness of `outOfOrdernessMs`
   (`WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(outOfOrdernessMs))`),
2. assigns the event timestamp from `Event.tsMs`
   (`.withTimestampAssigner((e, ts) -> e.tsMs)`), and
3. marks a partition idle after `idleMs` of no events
   (`.withIdleness(Duration.ofMillis(idleMs))`).

## What the tests check

The tests drive the strategy's own watermark generator with a controllable clock
(no sleep, no wall-clock):

- After two events at event times 100 and 200, the generator emits a watermark of
  `200 − outOfOrdernessMs`. This proves the timestamp assigner reads `Event.tsMs`
  (a missing assigner would never produce 195).
- After a silent gap longer than `idleMs`, the generator marks the partition
  **idle**. A strategy without `withIdleness` never does this.

## Hints

- `withTimestampAssigner` and `withIdleness` each return a new composed
  `WatermarkStrategy`; chain them and return the final one.
- `forBoundedOutOfOrderness(d)` emits `(max event timestamp seen − d)`.
- Idleness is what keeps a quiet partition from pinning the watermark minimum.
