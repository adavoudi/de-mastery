# Interview drills — STH-08

### 1. Why can the seconds tier cost several times the hours tier for the *same* data?
**Model answer.** The data volume is nearly the same, but three things change. Compute: batch pays
only while a scheduled job runs (a few job-minutes/hour); seconds is a fully provisioned,
always-on cluster — the dominant cost. Storage/compaction: frequent commits write many small
files, so you pay extra compaction compute and some write amplification. Operations/people: a
sub-minute job that must not lag is a 24/7 on-call commitment, versus a cron job. Compute and
people grow steeply with lower latency; storage grows mildly. The net is often a multiple.

### 2. Which cost bucket grows *most* steeply as you go from hours to seconds, and which grows *least*?
**Model answer.** Compute grows most steeply — from a few job-minutes per hour (batch) to a 24/7
always-on cluster (seconds). People/operations also grow steeply (on-call). Storage grows least:
the retained bytes are roughly constant; the extra cost is only small-file overhead and compaction,
which is a fraction of the base storage. Modelling this ordering correctly is the point — a model
that makes storage the dominant lever is wrong.

### 3. How do you make a cost model two engineers can agree on within ±20%?
**Model answer.** Make it deterministic and formula-driven: inputs are the workload (events/day,
bytes/event, retention days) and per-unit prices (compute-hour, GB-month, ops-hour); each component
is an explicit formula (compute-hours × price, retained GB × (1 + small-file overhead) × price,
etc.); the tier only changes a few multipliers (always-on fraction, file-count multiplier, on-call
hours). Two independent implementations of the same formulas land within rounding, well inside
±20%. The tolerance absorbs honest modelling differences, not structural ones.

### 4. The business asks for second-level freshness on a mart whose SLA is "by 9 a.m.". What do you say?
**Model answer.** Price all three tiers and show the delta. If hourly (or nightly) meets the 9 a.m.
SLA, the seconds tier buys nothing but costs several times more — always-on compute, compaction,
and on-call. Recommend the cheapest tier that meets the SLA, and tier *per mart*: don't price every
mart at seconds because one dashboard needs it. Freshness is a cost lever, not a default.

### 5. Where does storage overhead actually come from at the seconds tier?
**Model answer.** Frequent checkpoints/commits write many small files (and, for MOR tables, delete
files), which inflate metadata and hurt scan performance, so you run compaction continuously. The
cost is twofold: a modest increase in bytes stored (small-file and old-snapshot overhead until
expiry) and compaction *compute*. The base retained data is unchanged, which is why storage rises
mildly compared with the always-on compute cost — but the compaction compute belongs in the compute
bucket, not storage, when you model it.
