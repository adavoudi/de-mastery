# Hourly GMV per company with event time and a late side output

Compute GMV (sum of amounts) per company per **event-time** hour (here a short
tumbling window). Capture late-arriving sales in a side output, so that Flink does not
silently drop any of them. This is the streaming form of a late-arriving-fact mart with a
reconciliation table.

## Task

Implement `de.flk.HourlyGmv.build(DataStream<Event> events, OutputTag<Event> lateTag, long windowMs)`.

The driver gives you a watermarked `DataStream<Event>` (each `Event` has
`companyId`, `amountCents`, `tsMs`). Wire the windowing:

1. `keyBy` the `companyId`.
2. Apply a tumbling **event-time** window of size `windowMs`
   (`TumblingEventTimeWindows.of(Duration.ofMillis(windowMs))`).
3. Route events that arrive after their window is closed to the late side
   output: `sideOutputLateData(lateTag)`.
4. Aggregate the amounts per window. Emit one row per (company, window) as
   `companyId|windowStartMs|sumCents`.

Return the windowed `SingleOutputStreamOperator<String>`. The driver reads its
main output and its `getSideOutput(lateTag)` stream.

## What the tests check

- Per-(company, window) sums equal the sums of an independent oracle over the on-time events.
- A late event (an event whose window already closed) appears in the **late side
  output**. It is **not** added to the window sum.
- Windows use **event time**. A window start is always a multiple of
  `windowMs`, and the 1500ms event lands in `[1000, 2000)`.

## Hints

- Flink 2.x uses `java.time.Duration` for window sizes. The old `Time` class is
  gone.
- The late events show up on `getSideOutput(lateTag)`. The driver reads them.
  You only need to call `sideOutputLateData(lateTag)` on the windowed stream.
- Use a `ProcessWindowFunction` and `ctx.window().getStart()` for the window
  start.
