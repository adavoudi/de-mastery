# SPK-01 · Spark architecture

## Why Spark exists

You already move data at scale, but you do it by *sending SQL to a warehouse*. When you write
a dbt model against Redshift, you give a `SELECT` to a system. That system owns its own storage,
its own distribution keys, and its own query planner. You never think about *where the bytes
are stored* or *which machine runs which part of the join*. The warehouse hides all of this.

Apache Spark exists for the cases where this model stops working:

- The data is not in one warehouse.
- The transformation is not possible in SQL alone (iterative ML, graph work, arbitrary Python).
- You want one engine to read Parquet on S3, join it to Kafka, and write Iceberg. You do not
  want to load everything into a proprietary store first.

Spark is a **general-purpose distributed compute engine**. You bring the storage (files, tables,
streams). Spark brings the parallelism, the fault tolerance, and the optimizer.

Spark and Redshift use the same basic idea. They divide the data, run the same code on every
piece, and then combine the results. But Spark separates this engine from any single storage
layer. It also gives you the engine as a programming API, not only as SQL.

## How it works: the pieces

A Spark application has exactly one **driver** and one or more **executors**.

- The **driver** is the process that runs your `main` (your PySpark script or notebook kernel).
  It does these jobs:
  - It holds the `SparkSession`.
  - It builds the logical plan.
  - It asks the optimizer for a physical plan.
  - It splits that plan into **stages** and **tasks**.
  - It schedules those tasks onto executors.

  The driver is the brain of the application. If the driver dies, the application dies. The
  driver is like your Redshift **leader node**. It plans and coordinates. It does not do most of
  the row processing.
- The **executors** are JVM processes on the worker machines. The cluster gives each executor a
  number of **cores** (its task slots) and an amount of **memory**. The memory is for execution
  (shuffles, joins, sorts) and for cached data. An executor runs the tasks that the driver sends
  to it. It holds cached partitions and reports results back. Executors are like your Redshift
  **compute nodes / slices**. They do the actual scanning, filtering, and aggregating. Each one
  works on its own shard of the data.

A **task** is the smallest unit of work. It is one function applied to **one partition** of the
data. Take a stage that reads a dataset split into 8 partitions. An executor with 4 cores runs
4 tasks at a time and puts the other tasks in a queue. **Parallelism is limited by
`min(partitions, total cores)`**. SPK-02 and SPK-07 teach this again. On Redshift, the matching
control is the number of slices. In Spark, you control it directly with the partition count.

The **cluster manager** works below Spark and gives Spark the machines. It can be one of these:

- Spark's own Standalone manager.
- YARN.
- Kubernetes.
- For your laptop, the special `local[N]` "manager". It runs the driver and `N` simulated
  executor threads inside a single JVM.

The lab `spark` session in this course uses `local[2]`. It has one process, two task slots, real
plans, and a small amount of data. You can see everything about stages and shuffles there. Only
the number of machines is different from production.

**Deploy mode** answers the question "where does the driver run?"

- In **client** mode, the driver runs where you started it (your terminal or a notebook). This
  is convenient and interactive. But your laptop must stay connected for the whole job.
- In **cluster** mode, the cluster receives the driver and runs it next to the executors. This
  mode is more resilient. It is the normal choice for scheduled production jobs.
- **Local** mode is a third option. The driver and the executors share one JVM. You use it to
  develop, and to run every exercise in this track.

**Spark Connect** (stable since Spark 3.4, the default remote protocol in 4.x) separates the
client from the server. Your Python process becomes a thin client. It sends an *unresolved
logical plan* over gRPC to a remote Spark server. The server plans and executes it. This gives
you three practical benefits:

- Your client does not need a JVM or the full Spark jars.
- Multiple clients can share one cluster.
- A client crash does not kill the job.

Redshift made the same change long ago. A thin SQL client talks to a big remote engine. The
difference is the "query" that you send. It is a whole DataFrame plan, not a SQL string.

## Spark's modules

Spark is one engine. It has several libraries on top of **Spark Core**. Spark Core is the RDD
execution engine, the scheduler, shuffle, and memory management.

- **Spark SQL / DataFrames** — the relational layer and the Catalyst optimizer. You will work
  mostly here. SPK-03/04/05 teach it in depth. The DataFrame is the typed table that Spark can
  optimize.
- **Pandas API on Spark** (`pyspark.pandas`) — an interface that is compatible with pandas. It
  compiles to the same DataFrame engine. It is for teams that move pandas code to Spark
  (SPK-09).
- **Structured Streaming** — the same DataFrame API over unbounded input. It runs as
  micro-batches (SPK-10/11). Your dbt incremental models are a good mental model.
- **MLlib** and **GraphX/GraphFrames** — machine learning and graph analytics. This course does
  not cover them. But you should know that they exist and that they use the same Core.

You only need to know *that these modules exist and sit on Core*. You do not need their
internals.

## From your world

| Redshift / dbt | Spark | What transfers |
|---|---|---|
| Leader node | Driver | Plans and coordinates; does not crunch the bulk |
| Compute nodes / slices | Executors / cores | Parallel workers, each on a data shard |
| Number of slices | Partitions × cores | Your parallelism knob (SPK-02, SPK-07) |
| `WLM` queue / cluster size | Cluster manager allocation | How many machines you actually get |
| SQL client → Redshift | Spark Connect client → server | Thin client, remote engine |
| One coupled storage+compute system | Engine decoupled from storage | Spark reads/writes many stores |

The biggest change in thinking is this. In Redshift, storage and compute are one product, and
you cannot see inside the engine. In Spark, you *see and control* the plan, the partitions, and
the memory. This gives you power. It also gives you more ways to make costly mistakes.

## When NOT to use Spark

- **Data under ~100 GB.** A single machine with DuckDB or Polars will be *faster* and much
  simpler. Spark has overhead from shuffle, JVM startup, and serialization. Pay this cost only
  when the data does not fit on one machine. Most "big data" is not big.
- **Millisecond-latency streaming.** Spark Structured Streaming uses micro-batches. Its lowest
  latency is hundreds of milliseconds. For true low-latency event processing, use Flink (the
  JVM/FLK tracks).
- **Pure SQL transformations that a warehouse already does well.** Suppose the data is already
  in Redshift and the job is a SQL `INSERT ... SELECT`. Then running it in Redshift (or
  dbt-on-Redshift) is simpler and cheaper. The other way is to set up Spark, read the data out,
  and write it back.
- **Tiny, latency-sensitive point lookups / OLTP.** Spark is a scan engine, not a key-value
  store. Use Postgres/DynamoDB.

This is the honest rule. Use Spark when the data is too big for one machine, *or* when you need
one engine for many stores and arbitrary (non-SQL) logic. Otherwise, do not use it.

## Further reading

Pinned to the Spark **4.2.0** docs (the version this course runs):

- Cluster mode overview: <https://spark.apache.org/docs/4.2.0/cluster-overview.html>
- Submitting applications (deploy modes): <https://spark.apache.org/docs/4.2.0/submitting-applications.html>
- Spark Connect overview: <https://spark.apache.org/docs/4.2.0/spark-connect-overview.html>
- RDD programming guide (Core concepts): <https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html>
- Configuration reference: <https://spark.apache.org/docs/4.2.0/configuration.html>
