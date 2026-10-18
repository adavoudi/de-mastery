# Explain it (Feynman)

Explain to a teammate who uses Redshift but has never tuned Spark:

> "What is a shuffle, why is it the most expensive thing in distributed processing, and how does
> my choice of partition key decide whether I pay for one?"

Use DISTKEY/DISTSTYLE as the bridge. Give a concrete skew example from a multi-company mart.

## Rubric (self-check — aim for 5/6)

- [ ] Defines **partitioning** as the row → machine mapping. States the goal: put rows that
      belong together in the same place (co-location).
- [ ] Defines a **shuffle** as cross-node redistribution and names its costs (serialize, disk,
      network; `O(mappers × reducers)`).
- [ ] Explains **hash** vs **range** partitioning and when each fits.
- [ ] Explains **skew**: hashing spreads distinct keys, not row volume. A hot or low-cardinality
      key overloads one partition.
- [ ] Gives the **detection signal** (max/mean of partition sizes) and a **fix** (salting / better
      key / broadcast).
- [ ] Maps to Redshift: KEY = collocated join, EVEN = always redistribute, ALL = broadcast.
