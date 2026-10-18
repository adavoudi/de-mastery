# LKH-03 · Explain it back (Feynman)

**Prompt A.** "Map our SOX audited-publish control onto Iceberg. A nightly load must not reach the
production mart until quality checks pass. Walk through write → audit → publish using an Iceberg
branch, and say exactly what readers of `main` see at each step."

**Prompt B.** "Name three ways to get WAP wrong (write to main, audit the wrong snapshot, publish
before the audit) and explain why each one silently breaks the control."

## Rubric — a strong explanation hits these (5–7):

- [ ] Identifies the catalog as the holder of the atomic pointer and the governance boundary.
- [ ] Distinguishes an immutable tag from a mutable branch.
- [ ] Writes the candidate data to a **branch**, not `main`, and says `main` readers are unaffected.
- [ ] Runs the audit checks against the **branch's** snapshot (the candidate data).
- [ ] Publishes only on pass, via fast-forward (or cherry-pick) of `main` to the branch.
- [ ] Explains why order matters: the gate must be enforced before data is visible.
- [ ] (Bonus) Names a failure mode (write-to-main / audit-wrong-snapshot / publish-first) and its consequence.
