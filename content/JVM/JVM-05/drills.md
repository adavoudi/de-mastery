# JVM-05 · Interview drills

**1. "How do you spot a memory leak in a GC log?"**
> Track the post-GC 'after' value (the live set) across collections. A flat floor is healthy. A
> leak has these signs: the floor rises toward the heap ceiling, Full GCs reclaim almost nothing,
> and the log ends in `OutOfMemoryError`.

**2. "Explain `820M->410M(1024M)`."**
> The heap used 820M before this GC. The GC reclaimed memory down to 410M after it. The total heap
> is 1024M. The 410M is the live set at that moment. It is the number to watch over time.

**3. "Heap-after is flat but RSS climbs and the pod gets OOM-killed. Where do you look?"**
> Look at off-heap/native memory. GC logs do not cover it. Examples are direct `ByteBuffer`s,
> Netty pools, Flink managed memory, mmap, and JNI. Check native memory tracking and direct-buffer
> usage, not `-Xmx`.

**4. "Why not just bump `-Xmx` when you see OOM?"**
> If it is a genuine leak, a bigger heap only postpones the crash and makes GC pauses longer.
> Increase `-Xmx` only when the live set is legitimately large and stable. Otherwise, find and
> release the retained references.

**5. "ExecutorService vs new Thread()?"**
> A pool reuses a bounded set of threads, returns `Future`s, and shuts down cleanly. This prevents
> an explosion in the number of threads. `new Thread()` for each task is unbounded and uses many
> resources. For async composition, use `CompletableFuture`.

**6. "Why are races real in Java but not in single-threaded Python?"**
> Java has no GIL. Threads run in parallel across cores, so another thread can observe unsynchronized
> shared mutable state in the middle of an update. Protect it with `synchronized`,
> `java.util.concurrent.atomic`, or concurrent collections. Or keep per-record work stateless.
