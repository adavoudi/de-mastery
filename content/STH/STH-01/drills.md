# STH-01 · Interview drills

**1. "What problem did Lambda architecture solve, and what did it cost?"**
> It gave both low-latency results and complete, reprocessable results. No single engine could do
> both. A batch layer recomputes the correct answer. A speed layer serves the recent window. A
> serving layer merges them. The cost is that you maintain the **same business logic twice** in two
> code bases. The two copies drift, and people must reconcile them. This is expensive and a
> correctness risk.

**2. "How is Kappa different from Lambda, and what is its weakness?"**
> Kappa deletes the batch layer. It has one streaming pipeline, with the **log (Kafka) as the system
> of record**. You *replay* the log to reprocess history. There is one code path, so nothing drifts.
> Its weakness has three parts. Long-term, queryable history in Kafka is expensive. Ad-hoc OLAP over
> the log is awkward. Replaying months of events to fix a bug is slow.

**3. "A lakehouse already unifies storage. Why would anyone add a streamhouse?"**
> A lakehouse commits at **batch/micro-batch latency**. Snapshots land every few minutes at best.
> So it cannot answer sub-minute streaming questions. Frequent commits also cause a small-files
> problem. A streamhouse puts **stream-native storage** (Paimon/Fluss) on the lake. Then one copy
> serves both a streaming/changelog read and a batch/OLAP read. This adds the sub-minute tier that
> the lakehouse lacks.

**4. "How do you classify an unknown platform without trusting the vendor label?"**
> Look at the storage and pipeline shape.
> - Two parallel pipelines merged at serving → Lambda.
> - One streaming pipeline with an append-only log as the system of record → Kappa.
> - One transactional lake table format at batch latency → lakehouse.
> - A stream-native store that serves both streaming and batch reads from one copy → streamhouse.

**5. "When is a streamhouse the wrong choice?"**
> There are three cases.
> - Hourly or daily freshness is enough. A batch lakehouse is simpler and cheaper.
> - Every consumer only replays a log, and nobody runs OLAP. Plain Kappa is less to operate.
> - You genuinely need two different computations, for example an exact regulatory recompute plus
>   a separate real-time estimate. That is a legitimate Lambda. Do not unify it away.

**6. "Who coined 'streamhouse' and what does the term signal?"**
> Ververica coined it (~2024). It is now a cross-vendor working group (Aiven, Confluent, Redpanda,
> StreamNative, Ververica). The term signals that streaming storage and the lakehouse are
> converging. Stream-native storage sits on the lake, usually with Flink. The stream gets a cheap,
> queryable history. The lakehouse gets a sub-minute tier.
