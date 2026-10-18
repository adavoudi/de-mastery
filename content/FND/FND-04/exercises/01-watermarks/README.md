# A bounded-out-of-orderness watermark generator + late-event classifier

Implement a watermark generator and a classifier. They work on a single stream of events that
arrive in a given order. Everything is pure Python over integer timestamps (seconds). There is no
wall clock, and the result is fully deterministic.

## Functions / class to implement in `solution.py`

### 1. `class WatermarkGenerator`
```python
WatermarkGenerator(allowed_lateness: int)
.observe(event_time: int) -> int   # feed one event time, return the CURRENT watermark after it
.current() -> int                  # the current watermark without observing
```
- Track internally the maximum event time seen so far.
- The watermark is `max(previous_watermark, max_seen - allowed_lateness)`. It **never
  decreases**.
- Before the generator observes any event, `current()` returns negative infinity. Represent it as
  the module constant `NEG_INF` that you export (use `float("-inf")`).

### 2. `classify_events(events, allowed_lateness, grace=None) -> list[dict]`
`events` is a list of integer event times **in arrival order**. Use one `WatermarkGenerator`.
`grace` is `allowed_lateness` by default when `None`.

Classify each event against the watermark **that is in effect *before* the generator observes
that event**. An event cannot be late with respect to a watermark that it advanced itself. Then
observe the event. Return a list of dicts, one per input event, in input order:
```python
{"event_time": t, "watermark_before": W, "label": "on_time" | "late_accepted" | "dropped"}
```
Rules (with the pre-event watermark `W` and the grace `G`):
- `t > W` → `"on_time"`
- `W - G <= t <= W` → `"late_accepted"`
- `t < W - G` → `"dropped"`

(When `W` is `-inf`, every event is `on_time`.)

## What the tests check

- `observe` returns the correct watermark (`max_seen - allowed_lateness`, clamped to be
  monotonic).
- **Monotonicity property (Hypothesis):** for any arrival order, the sequence of watermarks that
  `observe` returns is **non-decreasing**. A late event with a small timestamp never pulls the
  watermark back.
- A hand-built out-of-order stream classifies into the exact expected `on_time / late_accepted /
  dropped` labels. The test recomputes the labels independently.
- The pre-event-watermark rule: the first event is always `on_time`.

Constraints: pure Python, deterministic. `lab: none`.
