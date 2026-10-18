# FLK-05 · Explain it back (Feynman)

**Prompt A.** "Explain the difference between a regular join, an interval join, and a temporal
(versioned) join on streams. For an orders × FX-rate enrichment where the business wants the rate
in effect on the order date, say which one is correct and why the other two give wrong or unbounded
results."

**Prompt B.** "You implement the as-of enrichment by hand with a KeyedCoProcessFunction. Walk
through what you store in state, when you emit a result, and why you must wait for the watermark to
pass the order's timestamp before picking a version. Connect it to how an SCD2 as-of join works in
your warehouse."

## Rubric — a strong explanation hits these (5–7):

- [ ] Regular join keeps both sides in state (unbounded) and emits retractions.
- [ ] Interval join bounds state to a time window around the match.
- [ ] Temporal/versioned join picks the dimension version effective at the fact's event time (as-of).
- [ ] Names the as-of rule: greatest `effective_from <= fact event time`.
- [ ] Explains the watermark dependency: emit only once the watermark passes the fact time, so every
      eligible version has arrived.
- [ ] Maps a dimension version to an SCD2 row (`valid_from`/`valid_to`) and the join to an as-of join.
- [ ] Explains why a lookup join (reads "now") is wrong for reprocessing / back-dated corrections.
