# SPK-06 · Explain it back (Feynman)

**Prompt A.** "I have a billion-row orders fact and a 500-row companies dimension to join on
`company_id`. Explain, like you'd explain `DISTSTYLE ALL` to a Redshift user, which physical join
strategy Spark should use, how you'd force it, and the one physical-plan property that proves it."

**Prompt B.** "One `company_id` owns 80% of the fact rows and the join has a straggler task.
Explain what skew is and give two fixes that provably do not change the join result."

## Rubric — a strong explanation hits these (5–7):

- [ ] Names **broadcast hash join** for small-vs-large and explains it ships the small side to every
      executor with **no shuffle** of the big side.
- [ ] Knows **sort-merge join** is the default for two large tables and costs **two shuffles** + sorts.
- [ ] Can force a broadcast with `F.broadcast(small)` and prove it via `BroadcastHashJoin` in the plan
      (and absence of `SortMergeJoin`).
- [ ] Warns that broadcasting the **large** side OOMs the driver — broadcast only the small side.
- [ ] Defines **skew** as one key value owning a disproportionate share, creating a straggler partition.
- [ ] Gives result-preserving fixes: broadcast the small side, or symmetric salting + reaggregation,
      or AQE skew-join handling.
- [ ] (Bonus) Joins on the full **business key** to avoid an intercompany double-count.
