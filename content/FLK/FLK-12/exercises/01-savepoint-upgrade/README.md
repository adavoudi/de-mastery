# Upgrade a stateful job via savepoint without losing state

A long-running job keeps a **running count** of the records it has processed, per key. You must
implement that counter so its state **survives a savepoint upgrade**: when the job is stopped with a
savepoint and restarted from it, the count continues where it left off instead of resetting.

## Provided (do not edit)

- **`Job.java`** — wires the pipeline: a bounded source → `keyBy` → your `Counter` → a file sink.
  It enables checkpointing and assigns a stable `.uid("counter-v1")` to your operator so a savepoint
  can map its state back. It takes two program args: the output file path and `K` (how many records
  the source emits).
- **`BoundedThenBlock.java`** — the source. It emits `0 … K-1` and then blocks forever, and it keeps
  its emitted **offset** in operator state so it resumes correctly after a savepoint.

## What to implement — `Counter.java`

A `KeyedProcessFunction<Integer, Long, Long>` that keeps a per-key running count in **Flink-managed
keyed state** and writes the latest count to the output file on each record:

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

**Do not** keep the count in a plain `long` field — instance fields are not snapshotted, so the
count would reset to zero on restore (the whole point of the exercise is that it must not).

## How you are graded (deterministic, no stopwatch)

The test runs a two-phase cycle on a real local Flink cluster:

1. Start the job with `K=5`; your counter reaches **5**. Take a savepoint and stop the job.
2. Restart the **same** job **from the savepoint** with `K=10`. The source replays its restored
   offset and emits 5 **more** records. If your state was truly restored, the counter continues
   6…10 and ends at **10**. If the state was lost, it would end at **5**.

The test asserts the final count is **10** — the no-loss, state-preserved invariant. Readiness is
polled via the cluster REST API and the output file; nothing depends on wall-clock timing. (This
runs on the native dist with no Docker; the managed/cloud equivalents are flagged ☁️free / 💲 and
are not graded.)
