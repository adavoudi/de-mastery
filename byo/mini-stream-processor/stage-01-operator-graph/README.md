# Stage 1 — Operator graph + push-based execution

You are building a **mini stream processor** in pure Python — the engine behind
Flink's DataStream API. You grow a single file, `processor.py`, across 8 stages.
This stage lays the dataflow foundation: a chain of **operators** that a record
is **pushed** through, one element at a time.

## The public API (stable for every stage)

The dataclasses, operator classes and function signatures are defined in the
starter. Do not rename them — the tests call them directly. The surface grows
each stage; this stage uses:

```python
# records / markers that flow downstream
Event(key=None, value=None, ts=None)   # a data record
Watermark(t)                           # event-time marker (stage 3)
Barrier(id)                            # checkpoint marker  (stage 6)

# build + run a job
Environment().source(records)          # -> Stream
stream.map(fn).filter(fn).sink()       # fluent chain of operators
collect(stream) -> list                # run it; return the Sink's data events
```

## This stage's task

Make the operator chain actually execute, **push-based** (like Flink): the Source
emits each record to a downstream callback; each operator's `process(elem, out)`
transforms and forwards; the Sink collects.

- `Source.run(out)` pushes every record downstream in order.
- `MapOp` applies `fn` to **data events** and forwards `Watermark`/`Barrier`
  markers unchanged. `FilterOp` keeps data events for which `fn` is true, and
  always forwards markers. `SinkOp` collects data events and **drops** markers.
- `_drive_chain(ops)` wires the chain (compose each operator's callback into the
  next) and runs the Source through it; it returns `(sink_or_None, terminal_out)`.
- `collect(stream)` returns the Sink's collected data events; with no Sink, the
  terminal data events (markers excluded). An empty source yields `[]`.

## Constraints

- Push model, element-at-a-time: downstream order equals source order.
- Markers pass through transforms but never reach a Sink.
- Pure stdlib, deterministic, no sleeps. The suite runs in well under 60 s.

## Run it

```bash
de start FLK-BYO/01       # copies the cumulative starter into your workspace
de test                  # runs this stage (later: earlier stages too)
de hint                  # progressive hints
```
