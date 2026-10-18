# LKH-02 · Explain it back (Feynman)

**Prompt A.** "Two of my dbt runs overlapped and wrote the same Iceberg table. Walk me through, at
the commit level, how one run's change could be silently lost — and how Iceberg's optimistic
concurrency plus a refresh-and-reapply loop prevents it."

**Prompt B.** "Explain why 'catch the CommitFailedException and move on' and 'just retry the same
write again' are both wrong, and what the correct retry does differently."

## Rubric — a strong explanation hits these (5–7):

- [ ] States that a commit is an atomic compare-and-swap of the catalog pointer from the base
      version to the new one.
- [ ] Explains the lost-update scenario for a read-modify-write (both read the same base, second
      overwrites and erases the first).
- [ ] Names optimistic concurrency: do the work optimistically, detect conflict at commit, retry.
- [ ] Describes refresh-and-reapply: reload to the winner's state, recompute the change, retry.
- [ ] Argues serial equivalence: recomputing on the fresh base makes the result == serial order.
- [ ] Identifies "swallow the exception" (silent lost write) and "blind retry stale metadata"
      (re-conflict or lost update) as anti-patterns.
- [ ] (Bonus) Notes pure appends often re-apply automatically, while RMW must be recomputed.
