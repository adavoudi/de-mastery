# JVM-05 · Concurrency and the JVM

## Why this matters for data engineers

You rarely write raw threads in Spark or Flink — the framework owns the parallelism. But you *do*
run into the JVM's two hard realities constantly: **concurrency** (a Kafka consumer poll loop, an
async producer, a connector's worker threads, parallel I/O) and **memory** (heap sizing, garbage
collection pauses, off-heap buffers, and the dreaded `OutOfMemoryError`). When a Flink TaskManager
or a Spark executor dies, the post-mortem is almost always a **heap/GC log**. This module gives you
enough concurrency to not shoot yourself in the foot, and enough JVM-memory literacy to read a GC
log and find a leak — the single most valuable JVM-ops skill for a data engineer.

## How JVM concurrency works (the safe subset)

- **Threads** run concurrently; you almost never manage them by hand. Use an **ExecutorService** (a
  thread pool) to submit tasks:

  ```java
  ExecutorService pool = Executors.newFixedThreadPool(4);
  Future<Integer> f = pool.submit(() -> expensive());
  int result = f.get();            // blocks until done
  pool.shutdown();
  ```

- **`CompletableFuture`** composes async work without blocking — the JVM analogue of Python
  `asyncio`/futures:

  ```java
  CompletableFuture
      .supplyAsync(() -> fetch(url), pool)      // run on the pool
      .thenApply(this::parse)                   // transform the result
      .thenAccept(this::store);                 // consume it
  ```

- **The hazards.** *Shared mutable state* across threads needs synchronization (`synchronized`,
  `java.util.concurrent.atomic`, concurrent collections) or you get **race conditions**. *Blocking*
  inside an async stage starves the pool. *Deadlocks* arise from lock-ordering mistakes. The data
  engineer's rule of thumb: keep per-record work pure and stateless, push shared state into the
  framework's keyed state (Flink) or a concurrent structure, and never `Thread.sleep` in a hot
  loop. (CDC/connector code is where you'll actually touch this.)

Think of it as Python's GIL being *absent*: Java threads really run in parallel, so data races are
real and you must protect shared state.

## How JVM memory and GC work

The heap is split into generations (with the default **G1** collector):

- **Young generation** (Eden + survivor spaces): new objects. **Minor GC** collects it; most
  objects die young and are cheap to reclaim.
- **Old generation** (tenured): objects that survived enough minor GCs get *promoted* here. **Major/
  Mixed/Full GC** collects it and is more expensive.
- **Metaspace** holds class metadata (off the main heap).
- **Off-heap / direct memory** (`ByteBuffer.allocateDirect`, Netty, Flink's managed memory, Spark's
  off-heap) lives outside the GC-managed heap — important because an off-heap leak won't show in
  heap-occupancy graphs.

**What a healthy heap looks like:** a sawtooth. Occupancy rises as you allocate, a GC cuts it back
down, repeat. The *floor* of the sawtooth — occupancy right **after** each GC (the **live set**) —
is roughly flat over time.

**What a leak looks like:** the sawtooth *floor rises* — after every GC, more memory is retained
than last time, because something keeps references alive (a growing cache/`Map`, an unbounded queue,
listeners never removed). GCs run more often, reclaim less, pauses grow, and eventually the JVM
throws `java.lang.OutOfMemoryError: Java heap space`. Reading that trend from a GC log is the skill.

### Reading a unified GC log

With `-Xlog:gc*` the JVM prints lines like:

```
[12.345s][info][gc] GC(7) Pause Young (G1 Evacuation Pause) 820M->410M(1024M) 12.1ms
[30.900s][info][gc] GC(21) Pause Full (G1 Compaction Pause) 1010M->980M(1024M) 180.4ms
```

Read `before->after(heap)`: `820M->410M` means a GC reclaimed down to a 410M live set.
Track the **after** value across GCs: flat floor = healthy; steadily rising floor to near the heap
ceiling + Full GCs that barely free anything = a **leak** heading for OOM.

## From your world

| Spark / lake ops | Plain JVM | Note |
|---|---|---|
| Spark executor OOM in logs (SPK-08) | `OutOfMemoryError: Java heap space` | same post-mortem skill |
| driver `collect()` blows the heap | retained live set grows | bound what you keep |
| shuffle spill to disk | GC pressure / off-heap buffers | memory tiers |
| `asyncio` futures | `CompletableFuture` | compose async, don't block |
| thread-unsafe pandas in a worker | shared mutable state → race | protect or avoid |

## When NOT to reach for threads / what NOT to conclude

- **Don't hand-roll threads in a Spark/Flink job.** The framework parallelizes; your per-record
  function should be pure. Manual pools in a UDF cause oversubscription and non-determinism.
- **Don't blame GC for an off-heap leak.** If heap-after is flat but the process RSS keeps growing,
  suspect direct/native memory (Netty, mmap, JNI), not the Java heap.
- **Don't just raise `-Xmx` to "fix" a leak.** A real leak will fill any heap; bigger heap only
  delays the OOM and lengthens pauses. Find the retained references.
- **Don't ignore allocation rate.** Frequent minor GCs with a flat floor isn't a leak — it's high
  churn; fix by allocating less, not by growing old gen.

## Further reading (official)

- HotSpot GC tuning guide (generations, G1): <https://docs.oracle.com/en/java/javase/21/gctuning/introduction-garbage-collection-tuning.html>
- Unified JVM logging (`-Xlog:gc*`): <https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html#enable-logging-with-the-jvm-unified-logging-framework>
- `java.util.concurrent` package: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/package-summary.html>
- `CompletableFuture`: <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletableFuture.html>
- Troubleshooting `OutOfMemoryError`: <https://docs.oracle.com/en/java/javase/21/troubleshoot/troubleshooting-memory-leaks.html>
