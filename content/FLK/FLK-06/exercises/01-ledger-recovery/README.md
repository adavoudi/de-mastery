# Survive a TaskManager crash with exactly-once ledger invariants

Write a Flink streaming job that processes a ledger of `N` entries and writes each entry's id to an
exactly-once sink. The grader runs your job on a real cluster, **SIGKILLs a TaskManager mid-run**,
lets the job recover from its last checkpoint, and then asserts the **ledger invariants** over the
*committed* output: every id appears **exactly once** — no loss, no duplication.

Implement `de.flk.LedgerJob` with a `main`:

```java
public static void main(String[] args) throws Exception
```

- `args[0]` is the **output directory URI** (e.g. `file:///tmp/....`). Write committed files there.
- The source is the bounded, replayable sequence `0 .. N-1` where `N` is read from `args[1]`
  (use `NumberSequenceSource(0, N-1)`).

Your job must:

1. **Enable checkpointing** — `env.enableCheckpointing(intervalMs)` (keep the interval small, e.g.
   a few hundred ms, so a checkpoint completes before the crash). The default checkpointing mode is
   EXACTLY_ONCE; keyed state and source offsets are snapshotted together.
2. Build the pipeline from the `NumberSequenceSource`. Key the stream (e.g. by `id % 4`) and run it
   through a small `KeyedProcessFunction` that simulates per-record work (a tiny `Thread.sleep`) so
   the job is still running when the TaskManager is killed. Emit each id as its decimal string.
3. **Write with a commit-on-checkpoint sink**: a `FileSink.forRowFormat(new Path(args[0]), new
   SimpleStringEncoder<String>("UTF-8"))` with `.withRollingPolicy(OnCheckpointRollingPolicy.build())`.
   This makes each part file visible only when the checkpoint that produced it completes, so output
   produced since the last complete checkpoint is rolled back on restart (no duplicates).
4. `env.execute(...)`.

The committed output (the non-hidden part files) must contain each id in `0 .. N-1` **exactly once**
after the job finishes, even though a TaskManager was killed while it was running.

### Why the starter fails

The starter enables no checkpointing and uses a sink with no commit-on-checkpoint policy, so after
a crash the output either loses in-flight records or re-emits them — the invariants break. Add the
three ingredients above (replayable source already given, checkpointed state, committing sink).
