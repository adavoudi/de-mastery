# Stage 3 — Event time + watermarks (min across inputs)

Flink is an **event-time** engine. A **watermark** of `t` asserts that no more
events with timestamp `<= t` will arrive, which lets downstream operators decide
when time has "passed" a point. You implement watermark generation and the rule
that governs multi-input operators: the output watermark is the **minimum** of
the input watermarks.

## New API this stage

```python
BoundedOutOfOrderness(delay)        # .on_event(ts); .current_watermark()
assign_watermarks(events, delay)    # -> [Event, Watermark, Event, Watermark, ...]
watermark_min(input_watermarks)     # -> min
merge_watermarks(streams)           # merge N inputs; combined wm = min across them
```

## This stage's task

- `BoundedOutOfOrderness(delay)`: track the max event ts seen; the current
  watermark is `max_ts - delay`, and it is **monotone non-decreasing** (an
  out-of-order older event must not lower it).
- `assign_watermarks(events, delay)`: emit each event, then — only when the
  watermark advances — a `Watermark` after it.
- `watermark_min(inputs)` = `min(inputs)`.
- `merge_watermarks(streams)`: read inputs round-robin; the combined watermark is
  `min` across all inputs' latest watermarks, advancing only when that min
  advances. A slow/silent input therefore **pins** the combined watermark (the
  classic stall that watermark *idleness* is designed to relieve).

## Constraints

- Watermarks strictly monotone. Deterministic; no sleeps. Suite < 60 s.
- Earlier stages still pass.

## Further reading

- Flink 2.x — Generating Watermarks and watermark alignment:
  https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/event-time/generating_watermarks/

## Run it

```bash
de start FLK-BYO/03
de test
```
