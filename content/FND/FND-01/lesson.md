# FND-01 · Why distributed?

## Why it exists

Distributed data systems exist to beat two hard limits of a single machine: it runs out of
**capacity** (RAM, disk, cores) and it runs out of **time** before the deadline. The moment
your data or your SLA no longer fits one box, you are forced to spread the work across many
boxes connected by a network. Everything hard about Spark, Kafka and Flink — partitioning,
shuffles, replication, consensus, watermarks, exactly-once — is a consequence of that one
decision: *the data lives in more than one place, and the network between those places is slow
and unreliable.*

So the first engineering question is not "which framework?" It is **"do I need to distribute at
all?"** A surprising amount of "big data" is a few hundred gigabytes that a single modern server
with DuckDB or Polars will crunch in seconds — with none of the operational tax of a cluster.

## How it works

**Scale up vs. scale out.** Scaling *up* means buying a bigger machine (more cores, more RAM,
NVMe). Scaling *out* means adding more machines. Scale-up is simpler: no network shuffle, no
partitioning strategy, one process to reason about. It hits a ceiling — you cannot buy an
infinitely large box, and price grows super-linearly at the top end. Scale-out has no ceiling,
but every join or aggregation that needs data from more than one node pays for a **shuffle**:
data crosses the network, which is orders of magnitude slower than RAM.

**Amdahl's law** is the ceiling on how much parallelism can ever help. If a fraction `p` of the
work is parallelizable and `(1 - p)` is inherently serial, then with `n` workers the best
speedup is:

```
speedup(n) = 1 / ((1 - p) + p / n)
```

As `n → ∞`, speedup is capped at `1 / (1 - p)`. If 10% of your job is serial (`p = 0.9`), you can
never go faster than 10×, no matter how many machines you rent. The serial part — reading a file
header, a final global sort, a single-threaded UDF — becomes the wall. **Gustafson's law** is the
optimistic counterweight: in practice we grow the problem to fill the machines, so bigger
datasets keep more of the work parallel. Both are true; they answer different questions ("fixed
work, more workers" vs. "more workers, bigger work").

**The real cost model.** For a scan-heavy aggregation the dominant term is usually *bytes read*,
not CPU. Columnar formats + predicate/column pruning cut bytes read by 10–100×, which often beats
adding nodes. This is why a measured 10 GB aggregation on a laptop-class DuckDB can match a small
Spark cluster: DuckDB reads only the columns and row groups it needs, in one process, with no
shuffle.

## From your world

Redshift already taught you this architecture. A Redshift cluster has a **leader node** (plans
the query, does the final serial merge) and **compute nodes** (scan and aggregate slices in
parallel). The leader is Amdahl's `(1 - p)`: a query that ends in one giant `ORDER BY` with no
`LIMIT` funnels everything through the leader and stops scaling. That is the same reason a Spark
job with a final `collect()` to the driver, or a single-reducer `count distinct`, stops speeding
up when you add executors.

**DISTKEY** is the partitioning decision we study next (FND-02): choose it well and joins stay
*local* (no network); choose it badly and every join is a shuffle. And when a Yelp mart table is
"only" 20–80 GB — a monthly commerce fact, say — the honest answer is often *don't distribute*:
`CREATE TABLE ... AS SELECT` in DuckDB or a single Athena query reads far fewer bytes than
standing up a cluster, and finishes before the cluster would have launched its executors.

## When NOT to use it

- **Data fits in RAM (or close).** Up to ~100 GB on a 32–128 GB box, DuckDB/Polars/pandas usually
  win on wall-clock *and* cost. No shuffle, no JVM warmup, no cluster to babysit.
- **The job is embarrassingly serial.** If Amdahl's `(1 - p)` dominates (a tight sequential
  dependency, a single-threaded library), more machines add coordination cost for ~zero speedup.
- **Latency of a single small query matters more than throughput.** Cluster scheduling and task
  launch overhead can exceed the whole single-box runtime.
- **You cannot yet state the bytes.** If you have not estimated input size and bytes scanned
  (FND-07), you cannot justify a cluster. "It feels big" is not a sizing argument.

Distribute when the working set genuinely exceeds one machine, when throughput at scale beats a
bigger box on cost, or when you need fault tolerance across machines (FND-03).

## Further reading

- Amdahl, G. (1967). *Validity of the single processor approach to achieving large scale
  computing capabilities.* AFIPS Conference Proceedings.
  https://dl.acm.org/doi/10.1145/1465482.1465560
- Gustafson, J. (1988). *Reevaluating Amdahl's Law.* Communications of the ACM.
  https://dl.acm.org/doi/10.1145/42411.42415
- Dean, J. & Ghemawat, S. (2004). *MapReduce: Simplified Data Processing on Large Clusters.*
  https://research.google/pubs/pub62/
- Amazon Redshift Database Developer Guide — *Data warehouse system architecture.*
  https://docs.aws.amazon.com/redshift/latest/dg/c_high_level_system_architecture.html
- DuckDB Documentation — *Why DuckDB.* https://duckdb.org/why_duckdb
