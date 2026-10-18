# KFK-04 · Explain it back (Feynman)

**Prompt A.** "Explain the difference between at-most-once and at-least-once purely in terms of
WHEN you commit the offset relative to doing the work. Then explain why at-least-once plus an
idempotent downstream write gives you effectively-correct results after a crash."

**Prompt B.** "A consumer with auto-commit crashes mid-batch and your teammate is surprised some
records 'disappeared'. Explain exactly what happened, and the one-line config change plus code
change that would have prevented the loss."

## Rubric — a strong explanation hits these (5–7):

- [ ] Defines the committed offset as the group's per-partition "handled up to here".
- [ ] Commit-before-work = at-most-once (loss on crash); commit-after-work = at-least-once (dup on
      crash).
- [ ] Explains auto-commit advancing the offset independent of processing → the loss trap.
- [ ] States that at-least-once never loses data and reprocesses from the last commit.
- [ ] Explains an idempotent sink (upsert by business key) absorbing the duplicate.
- [ ] Describes groups dividing partitions (one per consumer) and rebalancing for scale/failover.
- [ ] (Bonus) Mentions KIP-848 incremental rebalance and that exactly-once needs transactions.
