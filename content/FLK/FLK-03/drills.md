# FLK-03 · Interview drills

**1. "Event time vs processing time — when does the difference bite?"**
> Processing time buckets by arrival. A delayed event lands in the wrong window, and you cannot
> reproduce the result. Suppose you replay the same events after a network problem. You get a
> different hourly total. Event time buckets by the timestamp in the data. The results are correct
> and reproducible, regardless of when Flink saw each event. The difference bites when events are
> late or out of order. In practice this is always the case.

**2. "What is a watermark and when does a window fire?"**
> A watermark of T is a promise that no event with timestamp ≤ T is still coming. An event-time
> window fires when a watermark passes its end. forBoundedOutOfOrderness(d) emits (max seen − d). So
> d is a trade-off. A larger d tolerates more out-of-order data. A smaller d gives results sooner.

**3. "An event arrives late. Walk through the three outcomes."**
> 1. The event is within allowedLateness after the window end. The window re-fires with an updated
>    result.
> 2. The event is past end + allowedLateness, and a late side output is configured. The event goes
>    to that side-output stream. It is not lost.
> 3. The event is past end + allowedLateness, and there is no side output. Flink silently drops it.
>    This is the classic bug where totals are quietly too low.

**4. "Explain the stuck-watermark trap and how idleness solves it."**
> The watermark of an operator is the minimum across its input partitions. Time cannot advance past
> data that a lagging partition might still emit. If one partition goes quiet, its watermark stops
> advancing and holds the minimum. Every window stalls. Even busy partitions stop producing output.
> withIdleness(d) marks a partition idle after d of silence. Flink excludes it from the minimum. The
> combined watermark then advances on the active partitions, and windows fire again.

**5. "Tumbling vs sliding vs session windows?"**
> Tumbling: fixed, non-overlapping buckets (hourly GMV). Sliding: fixed size with a smaller step, so
> windows overlap (rolling 1h average every 5m). Session: gap-based. A window closes after a
> configurable gap of inactivity for the key (user sessions, bursts).

**6. "You own a global hourly revenue rollup. Which two watermark settings would you insist on and
why?"**
> First, a bounded out-of-orderness equal to the realistic maximum lateness. Then legitimate late
> events still count. Second, withIdleness. Then the quiet period of a region does not freeze the
> whole rollup. I would also add a late side output. It captures anything past the lateness bound
> for reconciliation, so Flink does not lose it silently. This is the SOX-style guarantee that no
> entry is silently dropped.
