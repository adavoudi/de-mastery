# SPK-08 · Explain it back (Feynman)

**Prompt A.** "Here are three OOM log excerpts:
(1) one with `collect` in the driver stack,
(2) one on an executor during a shuffle, where one task is 50× the others,
(3) one with repeated 'Spilling in-memory map to disk'.
Diagnose each one. Say where it happened and give the single best fix. Do it the way you would triage a
Redshift spill."

**Prompt B.** "Explain what AQE changes at runtime that static Catalyst cannot change. Explain why you
must read the *final* plan to see it."

## Rubric — a strong explanation hits these (5–7):

- [ ] Distinguishes a **driver** OOM (`collect`/`toPandas`) from an **executor** OOM, with different fixes.
- [ ] Identifies **skew** when one task is much larger than the others.
      Gives a fix that keeps the result the same (salt / more partitions / AQE skew handling).
- [ ] Reads **spill** messages as execution-memory pressure (slow, not a crash) and relieves it.
- [ ] Describes the **unified memory** model: execution and storage borrow from a shared region.
- [ ] Names AQE's runtime optimizations: **coalesce shuffle partitions**, **skew-join handling**,
      **dynamic join switch**.
- [ ] Explains that AQE adapts using real shuffle stats, so the effects appear only in the **final** plan.
- [ ] (Bonus) Caches only reused DataFrames. Warns that caching can defeat pushdown/pruning.
