# STH-08 — Latency tiers & cost: seconds, minutes, hours

## Why it exists

Every streamhouse decision comes down to one question: *how fresh must this mart be, and what does
that freshness cost?* Freshness is not free, and its cost is not linear. Suppose you move a mart
from **hours** to **minutes** to **seconds**. The bill changes in four places at once: **compute**,
**storage**, **operations** and **people**. A good engineer can put a number on each tier and
defend the trade-off. A good engineer does not choose "real-time" by default because it sounds
better.

## How it works

Consider three freshness tiers over the *same* mart (the same event volume, the same retention):

1. **Hours (batch lakehouse).** A job starts on a schedule (for example hourly or nightly). It reads
   the new data, writes a few large files and exits. You pay for compute only while the job runs.
   This is a few job-minutes per hour. The files are large and well compacted. So the **storage
   overhead is low**, and there is little or no compaction work. Operationally, it is a cron job.
   It is cheap to run and cheap to staff.
2. **Minutes (micro-batch streaming).** A streaming job checkpoints about every minute. It is
   effectively always on, so compute rises sharply compared with batch. Commits are frequent. So
   you write **many more, smaller files**, and you must run **compaction** to keep reads fast. This
   costs extra compute and some write amplification. Operations now means that you watch a running
   job (lag, checkpoints, restarts).
3. **Seconds (true streaming).** The job checkpoints every few seconds. Compute is a fully
   provisioned, always-on cluster. This is the dominant cost. The small-file / compaction problem is
   at its worst. So the storage overhead and the compaction compute both climb again. You now need
   on-call staff. A job that must not fall behind is a 24/7 operational commitment. It has a real
   **people** cost.

The pattern:

- **Compute** and **operations/people** grow steeply as latency shrinks (always-on clusters,
  on-call).
- **Storage** grows more mildly (more small files + compaction, limited by the same retained data).
- The seconds tier typically costs several times the hours tier. You need to justify sub-minute
  freshness with this number.

A cost model makes this concrete and *deterministic*. You have a workload (events/day, bytes/event,
retention days, per-unit prices) and a tier. Compute each component with a simple formula and add
them. Two people who model the same workload should land within a tolerance of each other (here:
±20%). The point is the *shape* — seconds ≫ minutes > hours — and not the third decimal.

## From your world

This is your daily-SLA conversation, in a formal model. You already weigh "can this report be
nightly?" against "the business wants it by 9 a.m.". The streamhouse adds finer tiers below *daily*:
minutes and seconds. You also already reason about four cost buckets for Redshift/Athena and your
dbt runs:

- cluster/warehouse compute,
- S3 storage (plus small-file and compaction overhead),
- pipeline operations,
- the engineer-hours to keep it running.

SOX-style reconciliation and change control get *more* expensive, not less, as freshness increases.

## When NOT to use it (over-engineering freshness)

- **When hourly is fine.** Suppose the SLA is "fresh by the morning". Then the hours tier is simpler
  and several times cheaper. If you pay for seconds, you gain nothing.
- **When you would guess the cost model and not measure it.** A model with no basis in real
  per-unit prices or volumes gives false precision. Anchor it to your cloud bill.
- **When one tier can serve many marts.** Do not price every mart at seconds because one dashboard
  needs it. Choose the tier *per mart* by its actual SLA.

## Further reading

- Apache Paimon — file layout, compaction & write performance:
  https://paimon.apache.org/docs/2.0/
- Apache Fluss — tiering & freshness: https://fluss.apache.org/
- Flink — checkpointing cost & tuning:
  https://nightlies.apache.org/flink/flink-docs-release-2.2/
