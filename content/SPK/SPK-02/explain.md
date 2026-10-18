# SPK-02 · Explain it back (Feynman)

**Prompt A.** "I wrote ten lines of PySpark transformations and nothing happened. There was no
error, no output, and no cluster activity. Then I added one `.count()` and a job ran. Explain
exactly why. What was Spark doing during those ten silent lines?"

**Prompt B.** "A job reads a table, filters it, joins two tables, and groups the result. Predict
the number of stages and justify each stage boundary. Then say how you would confirm your
prediction without a hand-written answer key."

## Rubric — a strong explanation covers these (5–7):

- [ ] Distinguishes **transformation (returns a DataFrame, lazy)** from **action (returns
      data/value or writes, triggers a job)**.
- [ ] States that Spark builds the plan but **does not execute** it until the action.
- [ ] Explains that laziness enables **cross-step optimization** (pushdown, pruning).
- [ ] Defines **narrow vs. wide** with the test of one input partition vs. many input
      partitions. Names a shuffle as the cost of wide.
- [ ] Applies **stages = shuffles + 1** and identifies which transformations shuffle.
- [ ] Confirms the prediction with evidence from `explain("formatted")` / counting `Exchange`
      nodes (not a memorized key).
- [ ] (Bonus) Notes that uncached DataFrames recompute for each action.
