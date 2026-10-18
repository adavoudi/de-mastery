# FND-04 · Time

## Why it exists

In batch, time is simple. You process yesterday's data today, and "yesterday" is a closed set.
In streaming, time splits into two different clocks. These clocks rarely agree:

- **Event time** — when the thing actually happened. The source stamps it. For example, the order
  was placed at 14:03:07.
- **Processing time** — when your system handled the record. For example, the record reached the
  operator at 14:05:40, after a mobile client reconnected.

Between these two moments there are network delays, retries, buffering and partitioning. So
events arrive **out of order** and **late**. Every correct streaming aggregation must answer one
question: *"How long do I wait for stragglers before I declare a window final?"*

**Watermarks** are the mechanism that answers this question. They exist because you can never be
*sure* that all events have arrived. So you make a bounded, explicit bet. You do not wait
forever.

## How it works

**A watermark is a claim about completeness.** A watermark of value `W` asserts: *"I believe I
have seen all events with event time ≤ `W`."* It is a moving low-water mark on event time. The
system derives it from the data itself, not from the wall clock.

**Bounded-out-of-orderness generator.** This is the most common watermark strategy. The generator
tracks the maximum event time seen so far, `maxEventTime`. It emits `W = maxEventTime −
allowedLateness`. The `allowedLateness` (for example, 5 seconds) is your bet on how out of order
the stream is. The watermark must have two properties:
1. **Monotonic (never goes backwards).** A watermark may only stay equal or increase. Suppose a
   late event arrives with a small timestamp. It does *not* pull the watermark down. If it did,
   windows would un-close.
2. **Lagging.** It trails the newest event by `allowedLateness`. This gives stragglers a grace
   period.

**Classifying an event against a watermark.** An event with time `t` arrives. The current
watermark is `W`:
- `t > W` → **on-time**. Its window is not yet declared complete. The event counts normally.
- `t ≤ W` but within an **allowed-lateness grace** → **late but accepted**. The system updates or
  retracts the window result.
- `t ≤ W` and beyond the grace → **dropped**. Or the system sends it to a *side output* /
  dead-letter, so that it is not lost silently.

![Events arrive out of order while maxEventTime moves right and the watermark trails it by the allowed delay; each event is on-time, late or dropped depending on where it lands relative to W](diagrams/watermark-event-time.svg)

The whole design is a trade-off:

- A larger `allowedLateness` catches more stragglers. But it delays results and costs state.
- A smaller `allowedLateness` is faster. But it drops more events.

There is no "correct" value. Only the observed distribution of lateness in the data can justify a
value.

## From your world

You have done a batch version of this for years. In Kimball, a **late-arriving fact** is a sale
that lands after the load of the day has run. A **late-arriving dimension** is a customer row
that shows up after the fact that references it.

You cope with these in several ways:

- A lookback window on the incremental dbt model (`WHERE event_date >= current_date - 3`).
- A "pending" bucket.
- A nightly reconciliation that restates recent days.

These are a *coarse, batch* watermark. "Reprocess the last 3 days" **is** `allowedLateness = 3
days`. The day you stop restating a period **is** the watermark advancing past it.

Streaming makes the same decision continuous and explicit. Later you will build Flink windows
(FLK-03) and CDC joins (FLK-05). Then "late-arriving dimension" becomes "the watermark had not
advanced past the event time of the dimension yet." Your Kimball instinct is exactly the right
instinct. The question is: *how long do I keep a period open before I trust the number?*

## When NOT to use it

- **Do not use event-time windows when you only care about processing-time rates** (for example,
  "requests per second as seen by the server"). Processing time is simpler. It has no late-data
  problem.
- **Do not set `allowedLateness` to "a lot, to be safe."** Every open window holds state. A huge
  grace period means unbounded state and delayed results. Size it from the measured distribution
  of lateness (p99), not from fear.
- **Do not drop late events silently in anything auditable.** For a SOX-relevant total, route
  late events to a side output and reconcile. Dropping them breaks completeness.
- **Do not let the watermark track wall-clock time** when the source can be idle or replayed.
  Derive it from event timestamps. If you do not, an idle partition will advance the watermark
  wrongly (or stall it).

## Further reading

- Akidau, T. et al. (2015). *The Dataflow Model.* VLDB.
  https://research.google/pubs/pub43864/
- Akidau, T. *Streaming 101 / 102.* (event time vs processing time, watermarks)
  https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/
- Apache Flink Documentation — *Timely Stream Processing: event time and watermarks.*
  https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/time/
- Apache Flink Documentation — *Generating Watermarks (bounded out-of-orderness).*
  https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/
