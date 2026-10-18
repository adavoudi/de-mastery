# KFK-14 · Explain it back (Feynman)

**Prompt A.** "Pick three of the six incidents (one availability, one durability, one
configuration). For each, state the measurable symptom, the root cause, the smallest safe fix, and
the health metric you'd check to confirm recovery. Frame it as a runbook entry."

**Prompt B.** "`min.insync.replicas` violation vs under-replicated partitions look similar (both
follow a broker loss). How do you tell them apart, and why is the *default* fix the same for both
even though the quick workaround differs?"

## Rubric — a strong explanation hits these (5–7):

- [ ] Treats each incident as symptom → cause → fix → verify (a runbook).
- [ ] Names a measurable symptom per incident (URP, ISR size, lag, retention, reachability,
      reassignment status).
- [ ] Gives the smallest safe fix (restore replica, raise retention, reset offsets to a sane point,
      correct advertised.listeners, raise/remove throttle).
- [ ] States the health check that confirms recovery (metric back to healthy).
- [ ] Distinguishes availability fixes (restore replica) from durability trade-offs (lowering
      min.insync.replicas) and marks the latter as last-resort.
- [ ] For URP vs min.insync: URP is "replica offline"; min.insync violation is "ISR below the floor
      so acks=all writes are rejected"; both are best fixed by restoring the replica.
- [ ] Avoids destructive non-fixes (don't delete the topic / don't blindly reset to earliest).
