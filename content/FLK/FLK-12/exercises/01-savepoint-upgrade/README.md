# Upgrade a stateful job via savepoint without losing state

A long-running job keeps a **running count** of the records that it processed, per key. You must
implement that counter so that its state **survives a savepoint upgrade**. You stop the job with a
savepoint and restart it from the savepoint. Then the count continues where it stopped. It does not reset.

## Provided (do not edit)

- **`Job.java`** — It wires the pipeline: a bounded source → `keyBy` → your `Counter` → a file sink.
  It enables checkpointing. It assigns a stable `.uid("counter-v1")` to your operator, so a savepoint
  can map its state back. It takes two program args: the output file path and `K` (how many records
  the source emits).
- **`BoundedThenBlock.java`** — This is the source. It emits `0 … K-1` and then blocks forever. It keeps
  its emitted **offset** in operator state, so it resumes correctly after a savepoint.

## What to implement — `Counter.java`

Write a `KeyedProcessFunction<Integer, Long, Long>`. It keeps a per-key running count in **Flink-managed
keyed state**. On each record, it writes the latest count to the output file:

```java
public class Counter extends KeyedProcessFunction<Integer, Long, Long> {
    private transient ValueState<Long> cnt;            // managed keyed state
    private final String outPath;
    public Counter(String outPath) { this.outPath = outPath; }

    @Override public void open(OpenContext ctx) {
        // TODO: cnt = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Long.class));
    }
    @Override public void processElement(Long v, Context ctx, Collector<Long> out) throws Exception {
        // TODO: n = (cnt.value() == null ? 0 : cnt.value()) + 1; cnt.update(n);
        //       write n to outPath (overwrite); out.collect(n);
    }
}
```

**Do not** keep the count in a plain `long` field. Flink does not snapshot instance fields, so the
count would reset to zero on restore. The count must not reset. This is the purpose of the exercise.

## How you are graded (deterministic, no stopwatch)

The test runs a two-phase cycle on a real local Flink cluster:

1. Start the job with `K=5`. Your counter reaches **5**. Take a savepoint and stop the job.
2. Restart the **same** job **from the savepoint** with `K=10`. The source replays its restored
   offset and emits 5 **more** records. Suppose your state was truly restored. Then the counter continues
   6…10 and ends at **10**. Suppose the state was lost. Then the counter would end at **5**.

The test asserts that the final count is **10**. This is the invariant for no loss and preserved state. The test polls readiness
through the cluster REST API and the output file. Nothing depends on wall-clock timing. (This
runs on the native dist with no Docker. The managed/cloud equivalents are flagged ☁️free / 💲 and
the test does not grade them.)
