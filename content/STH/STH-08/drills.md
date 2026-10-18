# Interview drills — STH-08

### 1. Why can the seconds tier cost several times the hours tier for the *same* data?
**Model answer.** The data volume is nearly the same, but three things change.

- Compute: Batch pays only while a scheduled job runs (a few job-minutes per hour). Seconds is a
  fully provisioned, always-on cluster. This is the dominant cost.
- Storage/compaction: Frequent commits write many small files. So you pay for extra compaction
  compute and some write amplification.
- Operations/people: A sub-minute job that must not lag is a 24/7 on-call commitment. Batch is a
  cron job.

Compute and people grow steeply as latency falls. Storage grows mildly. The net result is often a
multiple.

### 2. Which cost bucket grows *most* steeply as you go from hours to seconds, and which grows *least*?
**Model answer.** Compute grows most steeply. It goes from a few job-minutes per hour (batch) to a
24/7 always-on cluster (seconds). People/operations also grow steeply (on-call). Storage grows
least. The retained bytes are roughly constant. The extra cost is only small-file overhead and
compaction, which is a fraction of the base storage. The point is to model this ordering correctly.
A model that makes storage the dominant lever is wrong.

### 3. How do you make a cost model that two engineers can agree on within ±20%?
**Model answer.** Make it deterministic and driven by formulas.

- The inputs are the workload (events/day, bytes/event, retention days) and the per-unit prices
  (compute-hour, GB-month, ops-hour).
- Each component is an explicit formula. Examples: compute-hours × price, and
  retained GB × (1 + small-file overhead) × price.
- The tier changes only a few multipliers (always-on fraction, file-count multiplier, on-call
  hours).

Two independent implementations of the same formulas land within rounding, well inside ±20%. The
tolerance absorbs honest modelling differences. It does not absorb structural differences.

### 4. The business asks for second-level freshness on a mart whose SLA is "by 9 a.m.". What do you say?
**Model answer.** Price all three tiers and show the difference. Suppose hourly (or nightly) meets
the 9 a.m. SLA. Then the seconds tier gives you nothing, and it costs several times more (always-on
compute, compaction and on-call). Recommend the cheapest tier that meets the SLA. Choose the tier
*per mart*. Do not price every mart at seconds because one dashboard needs it. Freshness is a cost
lever, not a default.

### 5. Where does storage overhead actually come from at the seconds tier?
**Model answer.** Frequent checkpoints and commits write many small files (and, for MOR tables,
delete files). These files make the metadata larger and hurt scan performance. So you run
compaction continuously. The cost has two parts:

- A modest increase in the bytes that you store (small-file and old-snapshot overhead until expiry).
- Compaction *compute*.

The base retained data does not change. This is why storage rises mildly compared with the always-on
compute cost. But when you model it, the compaction compute belongs in the compute bucket and not
in storage.
