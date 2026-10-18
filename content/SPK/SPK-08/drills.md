# SPK-08 · Interview drills

**1. "Driver OOM vs executor OOM — how do you tell them apart and why does it matter?"**
> A driver OOM usually shows a `collect`/`toPandas`/`take(large)` in the stack. It happens because
> the job pulled all data to the driver. Fix it: do not collect the whole result. An executor OOM
> happens during a shuffle/join/sort, often with one task much larger than the rest (skew). Fix it with
> salting, more partitions, or AQE. The fixes are opposite, so the distinction is the whole triage.

**2. "What is the unified memory model?"**
> Spark splits the heap of each executor into execution (shuffles/joins/sorts), storage (cached blocks),
> user, and reserved memory. Execution and storage share a unified region and borrow from each other.
> Spark can evict cached blocks for execution, and execution can use idle storage space. Under
> pressure, Spark spills execution data to disk before it gives an OOM.

**3. "When should you cache, and what are the storage levels?"**
> Cache only when you reuse a DataFrame across 2 or more actions and it is small enough to help.
> `cache()` = `persist(MEMORY_AND_DISK)`. The levels trade memory for CPU/disk:
> - `MEMORY_ONLY` (recompute if evicted)
> - `MEMORY_AND_DISK` (spill cached blocks)
> - `DISK_ONLY`
> - `_SER` serialized variants
>
> Caching is lazy. Call `unpersist()` when you finish.

**4. "What does AQE do that the static plan can't?"**
> AQE re-optimizes at runtime using real shuffle statistics. It does three things:
> - It coalesces many tiny shuffle partitions into fewer partitions of the right size.
> - It handles skew by splitting oversized partitions.
> - It can switch a sort-merge join to a broadcast join when a side turns out small.
>
> These changes appear only in the final executed plan (`isFinalPlan=true`).

**5. "You see repeated 'Spilling in-memory map to disk'. Is the job failing?"**
> No. Spill is correct but slow. Execution memory was exceeded, so Spark wrote intermediate data to
> disk. Relieve it with more shuffle partitions, more executor memory, or a narrower aggregation.
> It is a performance signal, not a crash.

**6. "A teammate caches every intermediate DataFrame 'to be safe'. Explain why this is wrong."**
> Caching frames that you use once wastes storage memory. It can evict useful blocks or force spills.
> It can also be worse. A cached wide intermediate can defeat the pushdown/pruning that Catalyst would
> have applied across the pipeline. Cache only results that you reuse and that are already narrow. Call
> `unpersist` on them when you finish.
