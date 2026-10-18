# LKH-02 · Explain it back (Feynman)

**Prompt A.** "Two of my dbt runs overlapped and wrote the same Iceberg table. Explain, at the
commit level, how the change of one run could be silently lost. Then explain how the optimistic
concurrency of Iceberg and a refresh-and-reapply loop prevent this."

**Prompt B.** "Explain why 'catch the CommitFailedException and move on' is wrong. Explain why
'just retry the same write again' is also wrong. Then explain what the correct retry does
differently."

## Rubric — a strong explanation hits these (5–7):

- [ ] States that a commit is an atomic compare-and-swap of the catalog pointer from the base
      version to the new version.
- [ ] Explains the lost-update scenario for a read-modify-write (both writers read the same base,
      and the second writer overwrites and erases the first).
- [ ] Names optimistic concurrency: do the work optimistically, detect the conflict at commit, and
      retry.
- [ ] Describes refresh-and-reapply: reload to the state of the winner, recompute the change, and
      retry.
- [ ] Argues serial equivalence: when you recompute on the fresh base, the result equals the
      result of a serial order.
- [ ] Identifies "swallow the exception" (silent lost write) and "blind retry of stale metadata"
      (new conflict or lost update) as anti-patterns.
- [ ] (Bonus) Notes that pure appends often re-apply automatically, but a read-modify-write (RMW)
      must be recomputed.
