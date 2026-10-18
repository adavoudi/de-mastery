# LKH-03 · Explain it back (Feynman)

**Prompt A.** "Map our SOX audited-publish control onto Iceberg. A nightly load must not reach the
production mart until quality checks pass. Explain the steps write → audit → publish with an
Iceberg branch. Say exactly what readers of `main` see at each step."

**Prompt B.** "Name three ways to get WAP wrong: write to main, audit the wrong snapshot, and
publish before the audit. Explain why each one silently breaks the control."

## Rubric — a strong explanation hits these (5–7):

- [ ] Identifies the catalog as the holder of the atomic pointer and as the governance boundary.
- [ ] Distinguishes an immutable tag from a mutable branch.
- [ ] Writes the candidate data to a **branch**, not to `main`. Says that `main` readers are
      unaffected.
- [ ] Runs the audit checks against the snapshot of the **branch** (the candidate data).
- [ ] Publishes only when the audit passes. Uses a fast-forward (or cherry-pick) of `main` to the
      branch.
- [ ] Explains why order matters: the gate must be enforced before the data is visible.
- [ ] (Bonus) Names a failure mode (write-to-main, audit-wrong-snapshot or publish-first) and its
      consequence.
