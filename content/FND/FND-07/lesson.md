# FND-07 · Back-of-envelope estimation

## Why it exists

Every architecture decision in data engineering is secretly an arithmetic problem. The questions
are: *how many bytes, how many events per second, how much will the scan cost, will it fit on one
machine?*

The engineers who make good decisions fast do not have the most tools. They can make a
**back-of-envelope estimate** in sixty seconds, and the estimate is within a factor of ~2. The
estimate tells you which design is plausible before you write a line of code. It answers these
questions:

- Do you use DuckDB or a cluster (FND-01)?
- How do you partition (FND-02)?
- Will Parquet pruning save real money (FND-06)?

You can defend an estimate. You cannot defend an opinion. So an estimate is better than an
opinion.

The goal is never a precise number. The goal is the **right order of magnitude**. You write down
the assumptions, so that anyone can challenge them. "5 PB/year" and "$12k/month" are decisions.
"It feels big" is not a decision.

## How it works

**The three quantities.** Almost every sizing reduces to these three:
1. **Throughput** — events/sec and bytes/sec. `bytes_per_sec = events_per_sec × avg_event_bytes`.
2. **Storage** — bytes at rest over time. `yearly_bytes = bytes_per_sec × seconds_per_year ×
   (1 − compression_savings) × replication_factor`.
3. **Cost** — money per unit. Storage: `$/GB-month × GB × months`. Query: `$/TB-scanned ×
   TB_scanned`. Column pruning and partition/row-group pruning cut `TB_scanned` (FND-06).

**Numbers worth memorizing** (round, order-of-magnitude):
- `seconds/day ≈ 86,400 ≈ 10^5`. `seconds/year ≈ 3.15 × 10^7`.
- `1 day ≈ 10^5 s`, so **1 event/sec ≈ 86k/day ≈ 31.5M/year**.
- Powers of ten for data: KB 10^3, MB 10^6, GB 10^9, TB 10^12, PB 10^15.
- Rough object-store cost: **~$0.02–0.03 /GB-month** stored. A serverless query engine
  (Athena-class) costs **~$5 /TB scanned**. Use the numbers that your exercise states. Prices
  change, but the *method* does not.

**Working an estimate (the 5 PB/year example).** Suppose a feed does **2M events/sec** at **100
bytes/event** raw.
- Throughput: `2×10^6 × 100 = 2×10^8 B/s = 200 MB/s`.
- Raw/year: `2×10^8 × 3.15×10^7 ≈ 6.3×10^15 B ≈ 6.3 PB/year` raw.
- Columnar compression ~3× → `≈ 2.1 PB/year` stored (before replication).
- With 3× replication for durability, `≈ 6.3 PB/year` on disk.

That is the "5 PB/year" ballpark. The assumptions about compression and replication are *where
the number lives*. So state them.

**Latency math, briefly.** End-to-end latency ≈ queue wait + processing + network hops. A batch
SLA ("by 6am") buys you cheap throughput. A sub-second SLA forces streaming and more hardware.
Each latency tier has a cost. Name the tier before you size.

## From your world

This is **sizing the commerce mart**, made explicit.

- Decide whether a monthly fact fits DuckDB or needs Spark. You estimate rows × bytes/row and
  compare the result to RAM.
- Quote an **Athena bill**. You do `$/TB-scanned × TB_scanned`. Your instinct to "store Parquet
  and select fewer columns" minimizes `TB_scanned`. It is the pruning lesson of FND-06 turned
  into dollars.
- Finance asks: "what will the new event feed cost to retain for 7 years (a SOX retention
  requirement)?" The answer is `yearly_bytes × 7 × $/GB-month × 12`. The honest version states the
  compression and replication factors out loud.

This module builds one skill. You turn a vague question into that one line of defensible
arithmetic.

## When NOT to use it

- **Do not present an estimate without its assumptions.** A number with no stated
  compression/replication/retention factors cannot be checked. It will be wrong by 10×.
- **Do not refine too much.** Chasing a 5% improvement on a back-of-envelope number wastes
  effort. The inputs are already ±2×. Get the order of magnitude and move on.
- **Do not forget replication and overhead.** Raw event bytes ignore the 3× storage multiplier
  (or the smaller multiplier of erasure coding). They also ignore indexes and small-file
  overhead. These can double your real footprint.
- **Do not confuse raw and stored bytes.** Compression and columnar encoding change the stored
  size by 3–10×. If you use raw bytes for a storage bill, you overstate it. If you use stored
  bytes for network throughput, you understate it.

## Further reading

- Dean, J. *Numbers Everyone Should Know* (latency/throughput orders of magnitude).
  https://static.googleusercontent.com/media/research.google.com/en//people/jeff/stanford-295-talk.pdf
- Amazon S3 — *Storage pricing* (method, not a fixed price). https://aws.amazon.com/s3/pricing/
- Amazon Athena — *Pricing: charged per TB scanned.* https://aws.amazon.com/athena/pricing/
- Apache Parquet — *File Format* (compression/encoding that drives the stored-bytes factor).
  https://parquet.apache.org/docs/file-format/
