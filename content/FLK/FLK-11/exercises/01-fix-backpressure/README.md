# Fix a backpressured job — raise parallelism and redistribute

A streaming job runs a CPU-heavy map over a source. The **baseline** runs that map at
**parallelism 1** with a `FORWARD` edge: one subtask does all the work, backpressures the source,
and the whole job runs at that one subtask's speed. Your job is to **fix the bottleneck** so the
heavy map's work is spread across several parallel subtasks — without changing the result.

## The provided computation (do not change its logic)

`de.flk.Work.busy(long)` is the per-record CPU work (ships with the exercise). Both the baseline and
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
- Insert a **redistributing** edge before it so the extra subtasks are actually fed:
  `rebalance()` (even round-robin) or `rescale()`. **Do not** `keyBy` a constant — that sends every
  record to a single subtask, which is the bottleneck you are removing.

## How you are graded (robust, not a stopwatch)

The test builds the baseline itself and compares its job graph to yours:

1. **The plan changed in the right direction.** Your `"heavy"` operator's **parallelism increased**
   over the baseline, and the edge **feeding** it is a redistributing strategy
   (`REBALANCE` / `RESCALE`) instead of `FORWARD`. A `keyBy` (`HASH`) edge is rejected: the
   heavy map is not keyed, so hashing would only reintroduce skew. This is the deterministic proof that
   throughput improves — more subtasks doing balanced work.
2. **Correctness preserved.** Your job's summed output equals the baseline's (and an independent
   DuckDB oracle over `Work::busy`).

Nothing is graded on wall-clock time, so results are stable even on a busy machine.
