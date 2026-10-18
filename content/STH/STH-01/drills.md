# STH-01 · Interview drills

**1. "What problem did Lambda architecture solve, and what did it cost?"**
> It gave both low-latency and complete/reprocessable results when no single engine could do both:
> a batch layer recomputes the correct answer, a speed layer serves the recent window, and a serving
> layer merges them. The cost is maintaining the **same business logic twice** in two code bases,
> which drift and have to be reconciled — expensive and a correctness risk.

**2. "How is Kappa different from Lambda, and what is its weakness?"**
> Kappa deletes the batch layer: one streaming pipeline, with the **log (Kafka) as the system of
> record**, and you *replay* the log to reprocess history. One code path, no drift. Its weakness is
> that long-term, queryable history in Kafka is expensive, ad-hoc OLAP over the log is awkward, and
> replaying months of events to fix a bug is slow.

**3. "A lakehouse already unifies storage. Why would anyone add a streamhouse?"**
> A lakehouse commits at **batch/micro-batch latency** — snapshots land every few minutes at best —
> so it cannot answer sub-minute streaming questions, and frequent commits cause a small-files
> problem. A streamhouse puts **stream-native storage** (Paimon/Fluss) on the lake so one copy
> serves both a streaming/changelog read and a batch/OLAP read, adding the sub-minute tier the
> lakehouse lacks.

**4. "How do you classify an unknown platform without trusting the vendor label?"**
> Look at storage and pipeline shape. Two parallel pipelines merged at serving → Lambda. One
> streaming pipeline with an append-only log as the system of record → Kappa. One transactional lake
> table format at batch latency → lakehouse. A stream-native store serving both streaming and batch
> reads from one copy → streamhouse.

**5. "When is a streamhouse the wrong choice?"**
> When hourly/daily freshness is enough (a batch lakehouse is simpler and cheaper); when every
> consumer just replays a log and nobody runs OLAP (plain Kappa is less to operate); or when you
> genuinely need two different computations, e.g. an exact regulatory recompute plus a separate
> real-time estimate — that is a legitimate Lambda, not a smell to unify away.

**6. "Who coined 'streamhouse' and what does the term signal?"**
> Ververica coined it (~2024); it is now a cross-vendor working group (Aiven, Confluent, Redpanda,
> StreamNative, Ververica). The term signals the convergence of streaming storage and the lakehouse:
> stream-native storage on the lake, usually with Flink, so the stream gets a cheap queryable
> history and the lakehouse gets a sub-minute tier.
