# JVM-05 · Interview drills

**1. "How do you spot a memory leak in a GC log?"**
> Track the post-GC 'after' value (the live set) across collections. A flat floor is healthy; a
> floor that rises toward the heap ceiling, with Full GCs reclaiming almost nothing and ending in
> `OutOfMemoryError`, is a leak.

**2. "Explain `820M->410M(1024M)`."**
> 820M heap used before this GC, reclaimed to 410M after, out of a 1024M total heap. The 410M is
> the live set at that moment — the number to watch over time.

**3. "Heap-after is flat but RSS climbs and the pod gets OOM-killed. Where do you look?"**
> Off-heap/native memory, which GC logs don't cover: direct `ByteBuffer`s, Netty pools, Flink
> managed memory, mmap, JNI. Check native memory tracking and direct-buffer usage, not `-Xmx`.

**4. "Why not just bump `-Xmx` when you see OOM?"**
> If it's a genuine leak, a bigger heap only postpones the crash and makes GC pauses longer. Bump
> `-Xmx` only when the live set is legitimately large and stable; otherwise find and release the
> retained references.

**5. "ExecutorService vs new Thread()?"**
> A pool reuses a bounded set of threads, returns `Future`s, and shuts down cleanly, preventing
> thread explosion. `new Thread()` per task is unbounded and resource-hungry. For async composition
> use `CompletableFuture`.

**6. "Why are races real in Java but not in single-threaded Python?"**
> Java has no GIL; threads run in parallel across cores, so unsynchronized shared mutable state can
> be observed mid-update. Protect it with `synchronized`, `java.util.concurrent.atomic`, or
> concurrent collections — or keep per-record work stateless.
