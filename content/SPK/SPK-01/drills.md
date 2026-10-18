# SPK-01 · Interview drills

These are short questions with spoken answers. A model answer follows each question.

**1. "What is the difference between the driver and an executor?"**
> The driver runs your program and holds the `SparkSession`. It builds and optimizes the plan,
> and it schedules tasks. It is a single point of coordination. If it dies, the app dies.
> Executors are worker JVMs. Each one gets some cores (task slots) and memory. They run the
> tasks that the driver sends, hold cached partitions, and return results. Driver = Redshift
> leader node. Executors = compute nodes.

**2. "I gave my job 100 partitions but only 8 cores total. Does it run 100 tasks at once?"**
> No. Peak parallelism is `min(partitions, total cores)` = 8. The 100 partitions run in waves of
> 8 tasks. More partitions than cores can still help (smaller tasks, better load balancing, less
> skew). But it does not raise the peak concurrency.

**3. "Client vs. cluster deploy mode — which do you pick for a nightly scheduled job and why?"**
> Cluster mode. The driver runs inside the cluster. So the submitting machine can disconnect and
> the job survives. Client mode keeps the driver on the submitting machine. That is fine for
> interactive notebooks. It is bad for a job that must keep running after your laptop
> disconnects.

**4. "What problem does Spark Connect solve?"**
> It separates the client from the server. The client sends an unresolved logical plan over gRPC.
> The remote server plans and executes it. So the client needs no JVM. Many clients can share
> one cluster. A client crash does not kill the running job. It also makes non-JVM clients (thin
> Python, Go, and others) first-class.

**5. "When would you tell someone NOT to use Spark?"**
> When the data fits comfortably on one machine (under ~100 GB). DuckDB/Polars is simpler and
> faster. When they need millisecond latency, use Flink. When it is pure SQL that the warehouse
> already runs well, run it there. Spark's distributed machinery is overhead. Pay for it only
> when the data is too big for one machine, or when the logic spans many stores or is not SQL.
