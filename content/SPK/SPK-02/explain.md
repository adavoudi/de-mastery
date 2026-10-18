# SPK-02 · Explain it back (Feynman)

**Prompt A.** "I wrote ten lines of PySpark transformations and nothing happened — no error, no
output, no cluster activity. Then I added one `.count()` and suddenly a job ran. Explain exactly
why, and what Spark was doing during those ten silent lines."

**Prompt B.** "Given a job that reads, filters, joins two tables, and groups the result, predict
the number of stages and justify each stage boundary. Then say how you'd confirm your prediction
without a hand-written answer key."

## Rubric — a strong explanation hits these (5–7):

- [ ] Distinguishes **transformation (returns a DataFrame, lazy)** from **action (returns
      data/value or writes, triggers a job)**.
- [ ] States that the plan is built but **not executed** until the action.
- [ ] Explains that laziness enables **cross-step optimization** (pushdown, pruning).
- [ ] Defines **narrow vs. wide** by the one-input-partition vs. many-input-partitions test, and
      names a shuffle as the cost of wide.
- [ ] Applies **stages = shuffles + 1** and identifies which transformations shuffle.
- [ ] Confirms prediction empirically via `explain("formatted")` / counting `Exchange` nodes (not
      a memorized key).
- [ ] (Bonus) Notes that uncached DataFrames recompute per action.
