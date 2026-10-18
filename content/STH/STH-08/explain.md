# Explain it back — STH-08

**Prompt (Feynman).** A product manager says: "Make every mart second-fresh — storage is cheap."
In your own words, explain the four cost buckets of a freshness tier (compute, storage, operations,
people), why the seconds tier can cost several times the hours tier even though it is the *same*
data, and how you'd model all three tiers so two engineers land within ±20% of each other. Use the
revenue mart as the running example.

## Rubric — your explanation should hit these (5–7)

- [ ] **Freshness is the lever.** The decision is "how fresh?", and the answer sets the bill; it is
      not linear.
- [ ] **Four buckets.** Compute, storage, operations, and people — a tier changes all four at once.
- [ ] **Compute dominates and grows steeply.** Batch pays only while a job runs; minutes/seconds
      are effectively always-on clusters, the dominant cost at low latency.
- [ ] **Storage grows mildly.** Frequent commits mean more, smaller files plus compaction work,
      but the retained data volume is roughly the same — so storage rises less steeply than compute.
- [ ] **Operations & people climb.** A sub-minute job that must not lag is a 24/7 on-call
      commitment; batch is a cron job.
- [ ] **Same data, different bill.** The seconds tier can be several times the hours tier purely
      from always-on compute + small-file/compaction overhead + on-call — justify it with numbers.
- [ ] **Deterministic, reconcilable model.** Given a workload and per-unit prices, each component
      is a simple formula; two independent models of the same workload agree within a tolerance
      (±20%) and preserve the ordering seconds > minutes > hours.
- [ ] **When not to.** If hourly meets the SLA, seconds buys nothing; don't over-provision freshness.
