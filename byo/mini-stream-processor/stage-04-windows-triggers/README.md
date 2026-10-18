# Stage 4 — Tumbling/sliding windows with triggers

Windows turn an unbounded keyed stream into finite aggregations. In event time, a
window **fires** when the watermark passes its end; events that arrive after that
(beyond an allowed-lateness grace) are **late** and go to a side output.

## New API this stage

```python
TumblingWindows(size)                # .assign(ts) -> [Window]
SlidingWindows(size, slide)          # .assign(ts) -> [Window] (ts may be in many)
Window(start, end)                   # end exclusive
keyed.window(assigner).aggregate(agg_fn, allowed_lateness=0) -> WindowResult
run_windows(WindowResult) -> Fired   # list of (key, Window, result); .late side output
```

## This stage's task

- `TumblingWindows(size).assign(ts)` → one window `[start, start+size)`,
  `start = ts - (ts % size)`.
- `SlidingWindows(size, slide).assign(ts)` → every window containing `ts`
  (stepping start down by `slide`; windows may start before 0, matching Flink).
- `run_windows`: bucket each in-time event's value into a pane keyed by
  `(key, window)`. When the watermark passes a window's end, **fire** it —
  emit `(key, Window, agg_fn(values))` — in `(start, key)` order, and clear it.
- Drop events late beyond `allowed_lateness` to `result.late`.
- After all watermarks, the fired result equals a batch grouping of in-time
  events into windows (the oracle).

## Constraints

- Event-time triggers only (watermark-driven), never processing time or sleeps.
- Deterministic fire order. Earlier stages still pass. Suite < 60 s.

## Further reading

- Flink 2.x — Windows (assigners, triggers, allowed lateness):
  https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/

## Run it

```bash
de start FLK-BYO/04
de test
```
