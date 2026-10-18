# KFK-14 · Explain it back (Feynman)

**Prompt A.** "Pick three of the six incidents (one availability, one durability, one
configuration). For each incident, state the measurable symptom, the root cause, the smallest safe
fix, and the health metric that you would check to confirm recovery. Write it as a runbook entry."

**Prompt B.** "A `min.insync.replicas` violation and under-replicated partitions look similar (both
follow a broker loss). How do you tell them apart? Why is the *default* fix the same for both, even
though the quick workaround is different?"

## Rubric — a strong explanation covers these points (5–7):

- [ ] Treats each incident as symptom → cause → fix → verify (a runbook).
- [ ] Names a measurable symptom for each incident (URP, ISR size, lag, retention, reachability,
      reassignment status).
- [ ] Gives the smallest safe fix (restore replica, raise retention, reset offsets to a sane point,
      correct advertised.listeners, raise/remove throttle).
- [ ] States the health check that confirms recovery (metric back to healthy).
- [ ] Distinguishes availability fixes (restore replica) from durability trade-offs (lowering
      min.insync.replicas) and marks the durability trade-off as a last resort.
- [ ] For URP vs min.insync: URP means "replica offline". A min.insync violation means "ISR below the
      floor, so acks=all writes are rejected". Restoring the replica is the best fix for both.
- [ ] Avoids destructive non-fixes (don't delete the topic, don't reset blindly to earliest).
