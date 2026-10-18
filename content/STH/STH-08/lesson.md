# STH-08 — Latency tiers & cost: seconds, minutes, hours

## Why it exists

Every streamhouse decision reduces to one question: *how fresh does this mart need to be, and
what does that freshness cost?* Freshness is not free, and it is not linear. Moving a mart from
**hours** to **minutes** to **seconds** changes the bill in four places at once: **compute**,
**storage**, **operations**, and **people**. A good engineer can put a number on each tier and
defend the trade-off, instead of defaulting to "real-time" because it sounds better.

## How it works

Think of three freshness tiers over the *same* mart (same event volume, same retention):

1. **Hours (batch lakehouse).** A job wakes up on a schedule (say hourly or nightly), reads the
   new data, writes a few large files, exits. Compute is paid only while it runs — a few
   job-minutes per hour. Files are large and well-compacted, so **storage overhead is low** and
   there is little to no compaction work. Operationally it is a cron job: cheap to run, cheap to
   staff.
2. **Minutes (micro-batch streaming).** A streaming job checkpoints every minute or so. It is
   effectively always on, so compute rises sharply versus batch. Commits are frequent, so you
   write **many more, smaller files** and must run **compaction** to keep reads fast — extra
   compute and some write amplification. Operations now means watching a running job
   (lag, checkpoints, restarts).
3. **Seconds (true streaming).** Checkpoints every few seconds. Compute is a fully provisioned,
   always-on cluster — the dominant cost. The small-file / compaction problem is at its worst, so
   storage overhead and compaction compute both climb again. You now need on-call: a job that
   must not fall behind is a 24/7 operational commitment with real **people** cost.

The pattern: **compute** and **operations/people** grow steeply as latency shrinks (always-on
clusters, on-call), while **storage** grows more mildly (more small files + compaction, bounded by
the same retained data). The seconds tier is typically several times the hourly tier — the number
you need to justify sub-minute freshness.

A cost model makes this concrete and *deterministic*: given a workload (events/day, bytes/event,
retention days, per-unit prices) and a tier, compute each component with a simple formula and sum
them. Two people modelling the same workload should land within a tolerance of each other
(here: ±20%). The point is the *shape* — seconds ≫ minutes > hours — not the third decimal.

## From your world

This is your daily-SLA conversation, formalised. You already weigh "can this report be nightly?"
against "the business wants it by 9 a.m.". The streamhouse just adds finer tiers below *daily* —
minutes and seconds — and the same four cost buckets you already reason about for Redshift/Athena
and your dbt runs: cluster/warehouse compute, S3 storage (plus small-file and compaction
overhead), pipeline operations, and the engineer-hours to keep it alive. SOX-style reconciliation
and change control get *more* expensive, not less, as freshness increases.

## When NOT to use it (over-engineering freshness)

- **When hourly is fine.** If the SLA is "fresh by the morning", the hours tier is simpler and
  several times cheaper; paying for seconds buys nothing.
- **When the cost model would be guessed, not measured.** A model with no basis in real per-unit
  prices or volumes gives false precision. Anchor it to your cloud bill.
- **When one tier can serve many marts.** Don't price every mart at seconds because one dashboard
  needs it; tier *per mart* by its actual SLA.

## Further reading

- Apache Paimon — file layout, compaction & write performance:
  https://paimon.apache.org/docs/2.0/
- Apache Fluss — tiering & freshness: https://fluss.apache.org/
- Flink — checkpointing cost & tuning:
  https://nightlies.apache.org/flink/flink-docs-release-2.2/
