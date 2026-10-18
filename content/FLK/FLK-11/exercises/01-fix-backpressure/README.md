# Fix a backpressured job — raise parallelism and redistribute

A streaming job runs a CPU-heavy map over a source. The **baseline** runs that map at
**parallelism 1** with a `FORWARD` edge. One subtask does all the work and backpressures the source.
The whole job runs at the speed of that one subtask. Your task is to **fix the bottleneck**. The
work of the heavy map must spread across several parallel subtasks. The result must not change.

## The provided computation (do not change its logic)

`de.flk.Work.busy(long)` is the per-record CPU work (it comes with the exercise). Both the baseline and
your version call the same `Work::busy`, so the comparison is fair.

## What to implement

Edit `de.flk.Fix.apply(DataStream<Long> source)` to return the heavy-mapped stream, optimized:

```java
public static DataStream<Long> apply(DataStream<Long> source) {
    // TODO: redistribute the records, then run the heavy map on more subtasks.
    // return source.rebalance()
    //              .map((MapFunction<Long,Long>) Work::busy)
    //              .name("heavy")
    //              .setParallelism(4);
}
```

Requirements:

- The heavy map must keep the name `"heavy"` and call `Work::busy` (unchanged result).
- Run the heavy map at **parallelism > 1**.
- Insert a **redistributing** edge before it, so the extra subtasks get records:
  `rebalance()` (even round-robin) or `rescale()`. **Do not** `keyBy` a constant. That sends every
  record to a single subtask, which is the bottleneck that you want to remove.

## How you are graded (robust, not a stopwatch)

The test builds the baseline itself and compares its job graph to yours:

1. **The plan changed in the right direction.** The **parallelism** of your `"heavy"` operator **increased**
   over the baseline. The edge that **feeds** it is a redistributing strategy
   (`REBALANCE` / `RESCALE`) and not `FORWARD`. The test rejects a `keyBy` (`HASH`) edge. The
   heavy map is not keyed, so hashing only brings back skew. This is the deterministic proof that
   throughput improves: more subtasks do balanced work.
2. **Correctness preserved.** The summed output of your job equals the baseline (and an independent
   DuckDB oracle over `Work::busy`).

The test grades nothing on wall-clock time, so results are stable even on a busy machine.
