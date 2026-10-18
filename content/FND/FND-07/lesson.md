# FND-07 · Back-of-envelope estimation

## Why it exists

Every architecture decision in data engineering is secretly an arithmetic problem: *how many bytes,
how many events per second, how much will the scan cost, will it fit on one machine?* The engineers
who make good calls fast are not the ones with the most tools — they are the ones who can do a
**back-of-envelope estimate** in sixty seconds and be within a factor of ~2. The estimate tells you
which design is even plausible before you write a line of code: whether to use DuckDB or a cluster
(FND-01), how to partition (FND-02), whether Parquet pruning will save real money (FND-06). An
estimate you can defend beats an opinion you can't.

The goal is never a precise number. It is the **right order of magnitude**, with the assumptions
written down so anyone can challenge them. "5 PB/year" and "$12k/month" are decisions; "it feels
big" is not.

## How it works

**The three quantities.** Almost every sizing reduces to:
1. **Throughput** — events/sec and bytes/sec. `bytes_per_sec = events_per_sec × avg_event_bytes`.
2. **Storage** — bytes at rest over time. `yearly_bytes = bytes_per_sec × seconds_per_year ×
   (1 − compression_savings) × replication_factor`.
3. **Cost** — money per unit. Storage: `$/GB-month × GB × months`. Query: `$/TB-scanned ×
   TB_scanned`, where `TB_scanned` is cut by column pruning and partition/row-group pruning
   (FND-06).

**Numbers worth memorizing** (round, order-of-magnitude):
- `seconds/day ≈ 86,400 ≈ 10^5`; `seconds/year ≈ 3.15 × 10^7`.
- `1 day ≈ 10^5 s`, so **1 event/sec ≈ 86k/day ≈ 31.5M/year**.
- Powers of ten for data: KB 10^3, MB 10^6, GB 10^9, TB 10^12, PB 10^15.
- Rough object-store cost: **~$0.02–0.03 /GB-month** stored; **~$5 /TB scanned** for a serverless
  query engine (Athena-class). (Use the numbers your exercise states — prices change; the *method*
  does not.)

**Working an estimate (the 5 PB/year example).** Suppose a feed does **2M events/sec** at **100
bytes/event** raw.
- Throughput: `2×10^6 × 100 = 2×10^8 B/s = 200 MB/s`.
- Raw/year: `2×10^8 × 3.15×10^7 ≈ 6.3×10^15 B ≈ 6.3 PB/year` raw.
- Columnar compression ~3× → `≈ 2.1 PB/year` stored (before replication). With 3× replication for
  durability, `≈ 6.3 PB/year` on disk. That's the "5 PB/year" ballpark — and the compression and
  replication assumptions are *where the number lives*, so state them.

**Latency math, briefly.** End-to-end latency ≈ queue wait + processing + network hops. A batch SLA
("by 6am") buys you cheap throughput; a sub-second SLA forces streaming and more hardware. Latency
tiers have a cost; name the tier before sizing.

## From your world

This is **sizing the commerce mart**, made explicit. When you decide whether a monthly fact fits
DuckDB or needs Spark, you are estimating rows × bytes/row and comparing to RAM. When you quote an
**Athena bill**, you are doing `$/TB-scanned × TB_scanned`, and your instinct to "store Parquet and
select fewer columns" is you minimizing `TB_scanned` — the FND-06 pruning lesson turned into
dollars. When finance asks "what will the new event feed cost to retain for 7 years (a SOX
retention requirement)?", the answer is `yearly_bytes × 7 × $/GB-month × 12`, and the honest version
states the compression and replication factors out loud. The skill this module builds is turning a
vague ask into that one line of defensible arithmetic.

## When NOT to use it

- **Don't present an estimate without its assumptions.** A number with no stated
  compression/replication/retention factors can't be checked and will be wrong by 10×.
- **Don't over-refine.** Chasing a 5% improvement on a back-of-envelope number is wasted effort;
  the inputs are already ±2×. Get the order of magnitude and move on.
- **Don't forget replication and overhead.** Raw event bytes ignore the 3× (or EC-reduced) storage
  multiplier, indexes, and small-file overhead — these can double your real footprint.
- **Don't confuse raw and stored bytes.** Compression and columnar encoding change stored size by
  3–10×; using raw bytes for a storage bill overstates it, using stored bytes for network throughput
  understates it.

## Further reading

- Dean, J. *Numbers Everyone Should Know* (latency/throughput orders of magnitude).
  https://static.googleusercontent.com/media/research.google.com/en//people/jeff/stanford-295-talk.pdf
- Amazon S3 — *Storage pricing* (method, not a fixed price). https://aws.amazon.com/s3/pricing/
- Amazon Athena — *Pricing: charged per TB scanned.* https://aws.amazon.com/athena/pricing/
- Apache Parquet — *File Format* (compression/encoding that drives the stored-bytes factor).
  https://parquet.apache.org/docs/file-format/
