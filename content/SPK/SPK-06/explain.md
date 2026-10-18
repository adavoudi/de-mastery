# SPK-06 · Explain it back (Feynman)

**Prompt A.** "I have a billion-row orders fact and a 500-row companies dimension. I join them on
`company_id`. Explain which physical join strategy Spark should use. Explain it as you would
explain `DISTSTYLE ALL` to a Redshift user. Say how you would force the strategy. Name the one
physical-plan property that proves it."

**Prompt B.** "One `company_id` owns 80% of the fact rows, and the join has a straggler task.
Explain what skew is. Give two fixes that provably do not change the join result."

## Rubric — a strong explanation covers these (5–7):

- [ ] Names **broadcast hash join** for small-vs-large. Explains that it sends the small side to
      every executor with **no shuffle** of the big side.
- [ ] Knows that **sort-merge join** is the default for two large tables and costs **two
      shuffles** + sorts.
- [ ] Can force a broadcast with `F.broadcast(small)`. Can prove it with `BroadcastHashJoin` in
      the plan (and no `SortMergeJoin`).
- [ ] Warns that broadcasting the **large** side causes an OOM on the driver. Broadcast only the
      small side.
- [ ] Defines **skew** as one key value that owns a disproportionate share, which creates a
      straggler partition.
- [ ] Gives fixes that preserve the result: broadcast the small side, or symmetric salting +
      reaggregation, or AQE skew-join handling.
- [ ] (Bonus) Joins on the full **business key** to avoid an intercompany double-count.
