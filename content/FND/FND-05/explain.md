# Explain it (Feynman)

Explain to a teammate who re-runs dbt models and worries about double-counting:

> "What do at-most-once, at-least-once and exactly-once really mean, why is exactly-once not a
> network feature, and how do I make a duplicate-producing pipeline safe?"

Tie it to a SOX completeness/accuracy control.

## Rubric (self-check — aim for 5/6)

- [ ] Names the **two generals problem** as the root cause (you cannot be certain → resend or give
      up).
- [ ] Defines the **three guarantees** and their trade-offs (lose / duplicate / effectively-once).
- [ ] States that exactly-once = **at-least-once + idempotent/transactional sink**, not magic on
      the wire.
- [ ] Gives the idempotency property `f(f(x)) = f(x)`. Gives the two **dedup-key requirements**
      (stable, unique at the right grain).
- [ ] Explains why a random or time-based key is wrong.
- [ ] Maps to **dbt MERGE on `unique_key`** and SOX **completeness (no loss) + accuracy (no double
      count)**.
