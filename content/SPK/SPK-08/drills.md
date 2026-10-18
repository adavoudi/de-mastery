# SPK-08 · Interview drills

**1. "Driver OOM vs executor OOM — how do you tell them apart and why does it matter?"**
> A driver OOM usually shows a `collect`/`toPandas`/`take(large)` in the stack and happens because
> all data was pulled to the driver — fix by not collecting the whole result. An executor OOM
> happens during a shuffle/join/sort, often with one task far larger than the rest (skew) — fix by
> salting, more partitions, or AQE. The fixes are opposite, so the distinction is the whole triage.

**2. "What is the unified memory model?"**
> Each executor splits heap into execution (shuffles/joins/sorts), storage (cached blocks), user,
> and reserved. Execution and storage share a unified region and borrow from each other: cached
> blocks can be evicted for execution, and execution can use idle storage space. Under pressure
> Spark spills execution data to disk before OOMing.

**3. "When should you cache, and what are the storage levels?"**
> Cache only when a DataFrame is reused across 2+ actions and is small enough to help. `cache()` =
> `persist(MEMORY_AND_DISK)`. Levels trade memory for CPU/disk: `MEMORY_ONLY` (recompute if
> evicted), `MEMORY_AND_DISK` (spill cached blocks), `DISK_ONLY`, and `_SER` serialized variants.
> Caching is lazy; `unpersist()` when done.

**4. "What does AQE do that the static plan can't?"**
> AQE re-optimizes at runtime using real shuffle statistics: it coalesces many tiny shuffle
> partitions into fewer right-sized ones, handles skew by splitting oversized partitions, and can
> switch a sort-merge join to a broadcast join when a side turns out small. These appear only in
> the final executed plan (`isFinalPlan=true`).

**5. "You see repeated 'Spilling in-memory map to disk'. Is the job failing?"**
> No — spill is correct but slow: execution memory was exceeded so Spark wrote intermediate data to
> disk. Relieve it with more shuffle partitions, more executor memory, or a narrower aggregation.
> It's a performance signal, not a crash.

**6. "A teammate caches every intermediate DataFrame 'to be safe'. Push back."**
> Caching single-use frames wastes storage memory and can evict useful blocks or force spills.
> Worse, caching a wide intermediate can defeat the pushdown/pruning Catalyst would have applied
> across the pipeline. Cache only reused, already-narrow results, and `unpersist` them when done.
