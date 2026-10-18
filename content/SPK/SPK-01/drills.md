# SPK-01 · Interview drills

Short, spoken-answer questions. Model answers below each.

**1. "What is the difference between the driver and an executor?"**
> The driver runs your program, holds the `SparkSession`, builds and optimizes the plan, and
> schedules tasks. It is a single point of coordination — if it dies, the app dies. Executors are
> worker JVMs that each get some cores (task slots) and memory; they run the tasks the driver
> sends, hold cached partitions, and return results. Driver = Redshift leader node; executors =
> compute nodes.

**2. "I gave my job 100 partitions but only 8 cores total. Does it run 100 tasks at once?"**
> No. Peak parallelism is `min(partitions, total cores)` = 8. The 100 partitions run in waves of
> 8 tasks. More partitions than cores can still help (smaller tasks, better load balancing, less
> skew) but it does not raise the peak concurrency.

**3. "Client vs. cluster deploy mode — which do you pick for a nightly scheduled job and why?"**
> Cluster mode. The driver runs inside the cluster, so the submitting machine can disconnect and
> the job survives. Client mode keeps the driver on the submitting machine — fine for interactive
> notebooks, bad for a job that must outlive your laptop.

**4. "What problem does Spark Connect solve?"**
> It splits the client from the server. The client sends an unresolved logical plan over gRPC and
> the remote server plans/executes it. So the client needs no JVM, many clients can share one
> cluster, and a client crash doesn't kill the running job. It also makes non-JVM clients (thin
> Python, Go, etc.) first-class.

**5. "When would you tell someone NOT to use Spark?"**
> When the data fits comfortably on one machine (under ~100 GB) — DuckDB/Polars is simpler and
> faster. When they need millisecond latency — use Flink. When it's pure SQL the warehouse already
> runs well — just run it there. Spark's distributed machinery is overhead you only want to pay
> when the data is genuinely too big for one box or the logic spans many stores / isn't SQL.
