# A bounded-out-of-orderness watermark generator + late-event classifier

Implement a watermark generator and a classifier for a single stream of events arriving in a given
order. Everything is pure Python over integer timestamps (seconds). No wall clock, fully
deterministic.

## Functions / class to implement in `solution.py`

### 1. `class WatermarkGenerator`
```python
WatermarkGenerator(allowed_lateness: int)
.observe(event_time: int) -> int   # feed one event time, return the CURRENT watermark after it
.current() -> int                  # the current watermark without observing
```
- Internally track the maximum event time seen so far.
- The watermark is `max(previous_watermark, max_seen - allowed_lateness)` — it **never decreases**.
- Before any event is observed, `current()` returns negative infinity, represented as the module
  constant `NEG_INF` you export (use `float("-inf")`).

### 2. `classify_events(events, allowed_lateness, grace=None) -> list[dict]`
`events` is a list of integer event times **in arrival order**. Use one `WatermarkGenerator`.
`grace` defaults to `allowed_lateness` when `None`.

For each event, classify it against the watermark **in effect *before* that event is observed**
(an event cannot be late with respect to a watermark it itself advanced), then observe it. Return a
list of dicts, one per input event, in input order:
```python
{"event_time": t, "watermark_before": W, "label": "on_time" | "late_accepted" | "dropped"}
```
Rules (with the pre-event watermark `W` and the grace `G`):
- `t > W` → `"on_time"`
- `W - G <= t <= W` → `"late_accepted"`
- `t < W - G` → `"dropped"`

(When `W` is `-inf`, every event is `on_time`.)

## What the tests check

- `observe` returns the correct watermark (`max_seen - allowed_lateness`, clamped monotonic).
- **Monotonicity property (Hypothesis):** across any arrival order, the sequence of watermarks
  returned by `observe` is **non-decreasing** — a late small-timestamp event never pulls it back.
- A hand-built out-of-order stream classifies into the exact expected `on_time / late_accepted /
  dropped` labels (independently recomputed by the test).
- The pre-event-watermark rule: the first event is always `on_time`.

Constraints: pure Python, deterministic. `lab: none`.
