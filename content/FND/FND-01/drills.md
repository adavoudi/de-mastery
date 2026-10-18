# Interview drills — FND-01

### 1. "When would you NOT use Spark?"
**Model answer.** I would not use Spark when the data fits one machine (roughly ≤ ~100 GB on a
32–128 GB box). The job must also not need streaming or fault tolerance. A single DuckDB or
Polars process reads only the columns and row groups that it needs. It has no shuffle and no
cluster warmup. So it usually wins on both wall-clock time and cost.

I would also avoid Spark in two other cases. The first is a job where an inherently serial step
dominates (Amdahl: more executors cannot beat the serial wall). The second is a single small
low-latency query, where the scheduling overhead is larger than the whole runtime. The decision
should follow an estimate of bytes, not a feeling.

### 2. "Explain Amdahl's law and give a real bottleneck you've seen."
**Model answer.** Speedup is `1 / ((1-p) + p/n)`. It has a cap of `1/(1-p)` as the workers
`n → ∞`. Suppose 5% of the job is serial. Then 20× is the ceiling, whatever the cluster size.

Real bottlenecks:

- A Redshift query that ends in a leaderside `ORDER BY` without `LIMIT`.
- A Spark job that `collect()`s to the driver.
- A single-reducer `COUNT(DISTINCT ...)`.
- A non-vectorized Python UDF that serializes each row.

The fix is to make the serial part smaller or parallel. Adding nodes does not fix it.

### 3. "Why can a laptop beat a cluster on a 10 GB aggregation?"
**Model answer.** The largest cost is usually *bytes read*, not raw CPU. Columnar formats let
the engine prune to the needed columns. They also let it skip row groups with min/max stats. So
a single DuckDB process may touch a small fraction of the 10 GB, with zero network shuffle.

A cluster pays for JVM startup and task scheduling. It also pays for a shuffle in any
cross-node join or aggregation. A single in-process engine avoids these fixed costs. Add the
cluster only in two cases. One is when the working set truly exceeds one machine. The other is
when you need cross-machine fault tolerance.

### 4. "Scale up or scale out — how do you choose?"
**Model answer.** Start from the numbers: input size, bytes scanned per query, concurrency, and
the SLA. Suppose the working set fits a big box and concurrency is modest. Then scale up. It is
simpler and often cheaper.

Scale out in these cases:

- The working set exceeds any single machine.
- Aggregate throughput at scale is cheaper than a bigger box.
- You need fault tolerance across machines.

Remember that scale-up has a hard ceiling and a super-linear price at the top. Scale-out removes
that limit. But it adds shuffle and coordination cost.
