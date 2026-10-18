# Stage 5 — Keyed state + timers (process functions)

`KeyedProcessFunction` is Flink's lowest-level primitive: per-key **state** plus
**event-time timers** you register and that fire when the watermark passes them.
It is how you build session windows, timeouts and custom triggers.

## New API this stage

```python
ValueState(default=None)   # value/update/clear/snapshot/restore (provided)
TimerService()             # .register(key, ts); .advance(watermark) -> fired timers
class ProcessFunction:     # subclass: process_element(event, out) / on_timer(key, ts, out)
keyed.process(process_fn)  # -> KeyedProcessResult
run_process(KeyedProcessResult) -> list   # collected outputs in order
```

## This stage's task

- `TimerService.advance(watermark)`: return the `(key, ts)` timers with
  `ts <= watermark`, in `(ts, key)` order, and remove them (fire once).
- `run_process`: open the process function with one shared `ValueState` and
  `TimerService`. For each data `Event`, set the current key and call
  `process_element`. For each `Watermark`, advance timers and call `on_timer` for
  each due timer in order. `out` is a callback that appends to the result.

The tests build a **session-timeout** (close a key's session `GAP` after its last
event) and a **counter** (emit per-key counts on a flush timer); outputs match an
independent Python recomputation.

## Constraints

- Event-time timers only (watermark-driven). Deterministic fire order. No sleeps.
- Earlier stages still pass. Suite < 60 s.

## Further reading

- Flink 2.x — Process Function (state + timers):
  https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/process_function/

## Run it

```bash
de start FLK-BYO/05
de test
```
