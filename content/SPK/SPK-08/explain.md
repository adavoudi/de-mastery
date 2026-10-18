# SPK-08 · Explain it back (Feynman)

**Prompt A.** "Here are three OOM log excerpts: one with `collect` in the driver stack, one on an
executor during a shuffle where one task is 50× the others, and one with repeated 'Spilling
in-memory map to disk'. Diagnose each — where it happened and the single best fix — the way you'd
triage a Redshift spill."

**Prompt B.** "Explain what AQE changes at runtime that static Catalyst cannot, and why you must
read the *final* plan to see it."

## Rubric — a strong explanation hits these (5–7):

- [ ] Distinguishes a **driver** OOM (`collect`/`toPandas`) from an **executor** OOM, with different fixes.
- [ ] Identifies **skew** from one task dwarfing the others, and gives a result-preserving fix
      (salt / more partitions / AQE skew handling).
- [ ] Reads **spill** messages as execution-memory pressure (slow, not a crash) and relieves it.
- [ ] Describes the **unified memory** model: execution and storage borrow from a shared region.
- [ ] Names AQE's runtime optimizations: **coalesce shuffle partitions**, **skew-join handling**,
      **dynamic join switch**.
- [ ] Explains AQE adapts using real shuffle stats, so effects appear only in the **final** plan.
- [ ] (Bonus) Caches only reused DataFrames; warns caching can defeat pushdown/pruning.
