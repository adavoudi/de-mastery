# Interview drills — FND-04

### 1. "Explain watermarks to a PM."
**Model answer.** Events do not arrive in the order that they happened. Phones go offline and
networks retry. So when we compute "sales in the 2–3pm window", we must decide how long to wait
for stragglers before we publish a number. A watermark is that decision made explicit. It is a
moving marker that says "we believe we have now seen everything up to time X."

When the watermark passes 3pm, we publish the 2–3pm window. If we wait longer, the number is more
complete but later. If we wait less, it is faster but might miss a few late sales. It is the same
trade-off as deciding for how many days to keep re-running the nightly load.

### 2. "Event time vs processing time — when does each matter?"
**Model answer.** Event time is when the event actually happened (stamped at the source).
Processing time is when our system handled it.

Use event time when correctness depends on *when things occurred*. Examples are revenue per hour,
sessionization, and anything that a human will reconcile. Event time is stable under replays and
delays.

Use processing time only for operational rates ("requests/sec as the server saw them"). There is
no late-data concept, and simplicity wins.

### 3. "Why must a watermark be monotonic, and how is it generated?"
**Model answer.** A watermark can only stay the same or increase. Suppose a late event with a
small timestamp pulled it backward. Then windows that we already declared final would re-open.
Downstream consumers that already acted on those results would be wrong.

The common generator is bounded-out-of-orderness. It tracks the max event time seen and emits
`W = maxEventTime − allowedLateness`. So the watermark lags the newest event by a grace period and
never goes backward.

### 4. "How does this connect to late-arriving facts in Kimball?"
**Model answer.** A late-arriving fact is a sale that lands after the load of the day. A
late-arriving dimension is a customer that appears after a fact that references it.

My batch coping mechanism is a 3-day lookback in the incremental model, a pending bucket, and a
nightly restatement. It is a coarse watermark with `allowedLateness = 3 days`.

Streaming makes it continuous. The window stays open and accepts updates until the watermark
passes its end plus the grace. Anything later goes to a side output for reconciliation. The
system does not drop it. This matters for a SOX completeness control.
