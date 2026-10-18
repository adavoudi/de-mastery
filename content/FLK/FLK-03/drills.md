# FLK-03 · Interview drills

**1. "Event time vs processing time — when does the difference bite?"**
> Processing time buckets by arrival, so a delayed event lands in the wrong window and the result
> isn't reproducible — replay the same events after a network blip and you get a different hourly
> total. Event time buckets by the timestamp in the data, so results are correct and reproducible
> regardless of when Flink saw each event. The difference bites exactly when events are late or out
> of order, which in practice is always.

**2. "What is a watermark and when does a window fire?"**
> A watermark of T is a promise that no event with timestamp ≤ T is still coming. An event-time
> window fires when a watermark passes its end. forBoundedOutOfOrderness(d) emits (max seen − d), so
> d trades promptness for tolerance of out-of-order data.

**3. "An event arrives late. Walk through the three outcomes."**
> (1) If it's within allowedLateness after the window end, the window re-fires with an updated result.
> (2) If it's past end + allowedLateness and a late side output is configured, it goes to that
> side-output stream (not lost). (3) If it's past end + allowedLateness with no side output, it is
> silently dropped — the classic bug where totals come up quietly short.

**4. "Explain the stuck-watermark trap and how idleness fixes it."**
> An operator's watermark is the minimum across its input partitions, because time can't advance past
> data a lagging partition might still emit. If one partition goes quiet, its watermark stops
> advancing and pins the minimum, so every window stalls — even busy partitions stop producing
> output. withIdleness(d) marks a partition idle after d of silence and excludes it from the minimum,
> so the combined watermark advances on the active partitions and windows fire again.

**5. "Tumbling vs sliding vs session windows?"**
> Tumbling: fixed, non-overlapping buckets (hourly GMV). Sliding: fixed size with a smaller step, so
> windows overlap (rolling 1h average every 5m). Session: gap-based — a window closes after a
> configurable gap of inactivity for the key (user sessions, bursts).

**6. "You own a global hourly revenue rollup. Which two watermark settings would you insist on and
why?"**
> A bounded-out-of-orderness equal to the realistic max lateness (so legitimate late events still
> count), and withIdleness so a region's quiet period doesn't freeze the whole rollup. I'd also add a
> late side output to capture anything past the lateness bound for reconciliation rather than lose it
> silently — the SOX-style "no entry silently dropped" guarantee.
