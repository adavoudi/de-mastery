# Explain it (Feynman)

Explain to a Kimball modeler who reprocesses "the last few days" every night:

> "What is a watermark, why must it only move forward, and how is `allowedLateness` the same
> decision as my dbt lookback window?"

## Rubric (self-check — aim for 5/6)

- [ ] Distinguishes **event time** from **processing time**. Says why they diverge (network,
      retries, buffering).
- [ ] Defines a **watermark** as a completeness claim: "seen all events with event time ≤ W".
- [ ] States the **monotonic / non-decreasing** rule and *why* (closed windows must not re-open).
- [ ] Gives the **bounded-out-of-orderness** formula `W = maxEventTime − allowedLateness`.
- [ ] Classifies events as **on-time / late-accepted / dropped** relative to W and the grace
      window. Says that late events in auditable data go to a side output, not to a silent drop.
- [ ] Maps `allowedLateness` to the dbt lookback window / Kimball late-arriving facts.
