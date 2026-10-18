# SPK-01 · Spark architecture

## Why Spark exists

You already move data at scale, but you do it by *pushing SQL into a warehouse*. When you write
a dbt model against Redshift, you hand a `SELECT` to a system that owns its own storage, its own
distribution keys, and its own query planner. You never think about *where the bytes live* or
*which machine runs which part of the join* — the warehouse hides all of it.

Apache Spark exists for the cases where that model breaks down: when the data does not live in
one warehouse, when the transformation is not expressible in SQL alone (iterative ML, graph
work, arbitrary Python), or when you want one engine to read Parquet on S3, join it to Kafka,
and write Iceberg — without first loading everything into a proprietary store. Spark is a
**general-purpose distributed compute engine**: you bring the storage (files, tables, streams),
Spark brings the parallelism, the fault tolerance, and the optimizer.

The core bet Spark makes is the same one Redshift makes — *divide the data, run the same code on
every piece, then combine* — but Spark decouples that engine from any single storage layer and
exposes it as a programming API, not only as SQL.

## How it works: the pieces

A Spark application has exactly one **driver** and one or more **executors**.

- The **driver** is the process running your `main` (your PySpark script or notebook kernel). It
  holds the `SparkSession`, builds the logical plan, asks the optimizer for a physical plan,
  splits that plan into **stages** and **tasks**, and schedules those tasks onto executors. It is
  the brain. If the driver dies, the application dies. This is your Redshift **leader node**: it
  plans, it coordinates, it does not do the bulk of the row-crunching.
- The **executors** are JVM processes on the worker machines. Each executor is given a slice of
  **cores** (its task slots) and a slice of **memory** (for execution — shuffles, joins, sorts —
  and for cached data). An executor runs the tasks the driver sends it, holds cached partitions,
  and reports results back. These are your Redshift **compute nodes / slices**: they do the
  actual scanning, filtering, and aggregating, each on its own shard of the data.

A **task** is the smallest unit of work: one function applied to **one partition** of the data.
If a stage reads a dataset split into 8 partitions and an executor has 4 cores, that executor
runs 4 tasks at a time and queues the rest. **Parallelism is bounded by `min(partitions, total
cores)`** — a lesson SPK-02 and SPK-07 drive home. On Redshift the equivalent lever is the number
of slices; in Spark you control it directly with partition counts.

The **cluster manager** sits underneath and hands Spark the machines. It can be Spark's own
Standalone manager, YARN, Kubernetes, or — for your laptop — the special `local[N]` "manager"
that runs the driver and a simulated set of `N` executor threads inside a single JVM. The lab
`spark` session in this course uses `local[2]`: one process, two task slots, real plans, tiny
data. Everything you learn about stages and shuffles is visible there; only the machine count
differs from production.

**Deploy mode** answers "where does the driver run?" In **client** mode the driver runs where you
launched it (your terminal, a notebook) — convenient, interactive, but your laptop must stay
connected for the whole job. In **cluster** mode the driver is shipped into the cluster and runs
next to the executors — resilient and the norm for scheduled production jobs. **Local** mode is a
third option: driver and executors collapse into one JVM, which is what you use to develop and to
run every exercise in this track.

**Spark Connect** (stable since Spark 3.4, the default remote protocol in 4.x) splits the client
from the server. Your Python process becomes a thin client that sends an *unresolved logical
plan* over gRPC to a remote Spark server, which plans and executes it. The practical wins: your
client no longer needs a JVM or the full Spark jars, multiple clients can share one cluster, and
a client crash no longer kills the job. Conceptually it is the same move Redshift made long ago —
a thin SQL client talking to a big remote engine — except the "query" you send is a whole
DataFrame plan, not a SQL string.

## Spark's modules

Spark is one engine with several libraries layered on **Spark Core** (the RDD execution engine,
the scheduler, shuffle, memory management):

- **Spark SQL / DataFrames** — the relational layer and the Catalyst optimizer. This is where you
  will live, and where SPK-03/04/05 go deep. The DataFrame is the typed, optimizable table.
- **Pandas API on Spark** (`pyspark.pandas`) — a pandas-compatible surface that compiles to the
  same DataFrame engine, for teams migrating pandas code (SPK-09).
- **Structured Streaming** — the same DataFrame API over unbounded input, run as micro-batches
  (SPK-10/11). Your dbt incremental models are the mental model.
- **MLlib** and **GraphX/GraphFrames** — machine learning and graph analytics. Out of scope here,
  but know they exist and that they ride on the same Core.

You only need to know *that these modules exist and sit on Core*, not their internals.

## From your world

| Redshift / dbt | Spark | What transfers |
|---|---|---|
| Leader node | Driver | Plans and coordinates; does not crunch the bulk |
| Compute nodes / slices | Executors / cores | Parallel workers, each on a data shard |
| Number of slices | Partitions × cores | Your parallelism knob (SPK-02, SPK-07) |
| `WLM` queue / cluster size | Cluster manager allocation | How many machines you actually get |
| SQL client → Redshift | Spark Connect client → server | Thin client, remote engine |
| One coupled storage+compute system | Engine decoupled from storage | Spark reads/writes many stores |

The biggest mindset shift: in Redshift, storage and compute are one product and the engine is a
black box. In Spark you *see and control* the plan, the partitions, and the memory — which is
power and also rope to hang yourself with.

## When NOT to use Spark

- **Data under ~100 GB.** A single machine with DuckDB or Polars will be *faster* and far simpler.
  Spark's shuffle, JVM startup, and serialization overhead are a tax you only want to pay when the
  data genuinely does not fit on one box. Most "big data" is not big.
- **Millisecond-latency streaming.** Spark Structured Streaming is micro-batch; its floor is
  hundreds of milliseconds. For true low-latency event processing, use Flink (the JVM/FLK tracks).
- **Pure SQL transformations a warehouse already does well.** If the data is already in Redshift
  and the job is a SQL `INSERT ... SELECT`, running it in Redshift (or dbt-on-Redshift) is simpler
  and cheaper than standing up Spark to read it out and write it back.
- **Tiny, latency-sensitive point lookups / OLTP.** Spark is a scan engine, not a key-value store.
  Use Postgres/DynamoDB.

The honest rule: reach for Spark when the data is too big for one machine, *or* when you need one
engine to span many stores and arbitrary (non-SQL) logic. Otherwise, don't.

## Further reading

Pinned to the Spark **4.2.0** docs (the version this course runs):

- Cluster mode overview: <https://spark.apache.org/docs/4.2.0/cluster-overview.html>
- Submitting applications (deploy modes): <https://spark.apache.org/docs/4.2.0/submitting-applications.html>
- Spark Connect overview: <https://spark.apache.org/docs/4.2.0/spark-connect-overview.html>
- RDD programming guide (Core concepts): <https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html>
- Configuration reference: <https://spark.apache.org/docs/4.2.0/configuration.html>
