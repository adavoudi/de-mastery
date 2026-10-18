# FLK-05 · Explain it back (Feynman)

**Prompt A.** "Explain the difference between a regular join, an interval join and a temporal
(versioned) join on streams. Take an orders × FX-rate enrichment. The business wants the rate that was
in effect on the order date. Say which join is correct. Say why the other two give wrong or unbounded
results."

**Prompt B.** "You implement the as-of enrichment by hand with a KeyedCoProcessFunction. Explain what
you store in state and when you emit a result. Explain why you must wait for the watermark to pass the
timestamp of the order before you pick a version. Connect this to how an SCD2 as-of join works in
your warehouse."

## Rubric — a strong explanation hits these (5–7):

- [ ] Regular join keeps both sides in state (unbounded) and emits retractions.
- [ ] Interval join bounds state to a time window around the match.
- [ ] Temporal/versioned join picks the dimension version effective at the event time of the fact (as-of).
- [ ] Names the as-of rule: greatest `effective_from <= fact event time`.
- [ ] Explains the watermark dependency: emit only after the watermark passes the fact time, so every
      eligible version has arrived.
- [ ] Maps a dimension version to an SCD2 row (`valid_from`/`valid_to`) and the join to an as-of join.
- [ ] Explains why a lookup join (reads "now") is wrong for reprocessing / back-dated corrections.
