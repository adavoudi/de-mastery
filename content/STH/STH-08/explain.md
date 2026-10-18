# Explain it back — STH-08

**Prompt (Feynman).** A product manager says: "Make every mart second-fresh — storage is cheap."
Explain in your own words the four cost buckets of a freshness tier (compute, storage, operations,
people). Explain why the seconds tier can cost several times the hours tier even though the data is
the *same*. Explain how you would model all three tiers, so that two engineers land within ±20% of
each other. Use the revenue mart as the running example.

## Rubric — your explanation should hit these (5–7)

- [ ] **Freshness is the lever.** The decision is "how fresh?", and the answer sets the bill. The
      bill is not linear.
- [ ] **Four buckets.** Compute, storage, operations and people. A tier changes all four at once.
- [ ] **Compute dominates and grows steeply.** Batch pays only while a job runs. Minutes and seconds
      tiers are effectively always-on clusters. Compute is the dominant cost at low latency.
- [ ] **Storage grows mildly.** Frequent commits mean more, smaller files plus compaction work. But
      the retained data volume is roughly the same. So storage rises less steeply than compute.
- [ ] **Operations & people climb.** A sub-minute job that must not lag is a 24/7 on-call
      commitment. Batch is a cron job.
- [ ] **Same data, different bill.** The seconds tier can cost several times the hours tier. The
      reasons are only always-on compute, small-file/compaction overhead and on-call. Justify it
      with numbers.
- [ ] **Deterministic, reconcilable model.** You have a workload and per-unit prices. Each component
      is a simple formula. Two independent models of the same workload agree within a tolerance
      (±20%) and keep the ordering seconds > minutes > hours.
- [ ] **When not to.** If hourly meets the SLA, seconds gives you nothing. Do not over-provision
      freshness.
