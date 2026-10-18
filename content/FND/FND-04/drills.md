# Interview drills — FND-04

### 1. "Explain watermarks to a PM."
**Model answer.** Events don't arrive in the order they happened — phones go offline, networks
retry — so when we compute "sales in the 2–3pm window" we have to decide how long to wait for
stragglers before we publish a number. A watermark is that decision made explicit: it's a moving
marker that says "we believe we've now seen everything up to time X." When the watermark passes
3pm, we publish the 2–3pm window. Wait longer and the number is more complete but later; wait less
and it's faster but might miss a few late sales. It's the same trade-off as deciding how many days
to keep re-running the nightly load.

### 2. "Event time vs processing time — when does each matter?"
**Model answer.** Event time is when the event actually happened (stamped at the source);
processing time is when our system handled it. Use event time whenever correctness depends on *when
things occurred* — revenue per hour, sessionization, anything a human will reconcile — because it's
stable under replays and delays. Use processing time only for operational rates ("requests/sec as
the server saw them"), where there is no late-data concept and simplicity wins.

### 3. "Why must a watermark be monotonic, and how is it generated?"
**Model answer.** A watermark can only stay the same or increase. If a late event with a small
timestamp pulled it backward, windows we already declared final would re-open, and downstream
consumers that already acted on those results would be wrong. The common generator is
bounded-out-of-orderness: track the max event time seen and emit `W = maxEventTime −
allowedLateness`, so it lags the newest event by a grace period and never regresses.

### 4. "How does this connect to late-arriving facts in Kimball?"
**Model answer.** A late-arriving fact is a sale that lands after the day's load; a late-arriving
dimension is a customer that appears after a fact referencing it. My batch coping mechanism — a
3-day lookback in the incremental model, a pending bucket, nightly restatement — is a coarse
watermark with `allowedLateness = 3 days`. Streaming makes it continuous: the window stays open and
accepts updates until the watermark passes its end plus the grace, and anything later goes to a
side output for reconciliation rather than being dropped — which matters for a SOX completeness
control.
