# Interview drills — FND-01

### 1. "When would you NOT use Spark?"
**Model answer.** When the data fits one machine (roughly ≤ ~100 GB on a 32–128 GB box) and the
job isn't streaming or fault-tolerance-driven. A single DuckDB/Polars process reads only the
columns and row groups it needs, with no shuffle and no cluster warmup, so it usually wins on
both wall-clock and cost. I'd also avoid Spark for a job dominated by an inherently serial step
(Amdahl: more executors can't beat the serial wall) or for a single small low-latency query,
where scheduling overhead exceeds the whole runtime. The decision should follow a bytes estimate,
not a vibe.

### 2. "Explain Amdahl's law and give a real bottleneck you've seen."
**Model answer.** Speedup is `1 / ((1-p) + p/n)`, capped at `1/(1-p)` as workers `n → ∞`. If 5%
of the job is serial, 20× is the ceiling regardless of cluster size. Real bottlenecks: a Redshift
query ending in a leaderside `ORDER BY` without `LIMIT`, a Spark job that `collect()`s to the
driver, a single-reducer `COUNT(DISTINCT ...)`, or a non-vectorized Python UDF that serializes
each row. The fix is to shrink or parallelize the serial part, not to add nodes.

### 3. "Why can a laptop beat a cluster on a 10 GB aggregation?"
**Model answer.** Because the dominant cost is usually *bytes read*, not raw CPU. Columnar
formats let the engine prune to the needed columns and skip row groups via min/max stats, so a
single DuckDB process may touch a small fraction of the 10 GB with zero network shuffle. A cluster
pays JVM startup, task scheduling, and a shuffle for any cross-node join or aggregation — fixed
costs that a single in-process engine avoids. Add the cluster only when the working set truly
exceeds one machine or you need cross-machine fault tolerance.

### 4. "Scale up or scale out — how do you choose?"
**Model answer.** Start from the numbers: input size, bytes scanned per query, concurrency, and
the SLA. If the working set fits a big box and concurrency is modest, scale up (simpler, often
cheaper). Scale out when the working set exceeds any single machine, when aggregate throughput at
scale beats a bigger box on cost, or when you need fault tolerance across machines. Remember
scale-up has a hard ceiling and super-linear price at the top; scale-out trades that for shuffle
and coordination cost.
